// whitebox_coverage_test.go — in-package (package httpapi) branch coverage
// for small unexported helpers the end-to-end HTTP tests leave sparse:
// paging, v2Header/firstNonEmpty, addonDisplayName, v2InvoiceDTO,
// publishEventWithIMDA and the v2 middleware/health helpers.
package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

func TestPaging(t *testing.T) {
	t.Parallel()
	// Defaults.
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	if off, lim := paging(r); off != 0 || lim != 50 {
		t.Fatalf("defaults: off=%d lim=%d", off, lim)
	}
	// Valid override.
	r2 := httptest.NewRequest(http.MethodGet, "/x?limit=25&offset=3", nil)
	if off, lim := paging(r2); off != 3 || lim != 25 {
		t.Fatalf("override: off=%d lim=%d", off, lim)
	}
	// Out-of-range limit ignored (defaults), invalid offset ignored.
	r3 := httptest.NewRequest(http.MethodGet, "/x?limit=999&offset=-1", nil)
	if off, lim := paging(r3); off != 0 || lim != 50 {
		t.Fatalf("invalid: off=%d lim=%d", off, lim)
	}
	// Non-numeric values ignored.
	r4 := httptest.NewRequest(http.MethodGet, "/x?limit=abc&offset=def", nil)
	if off, lim := paging(r4); off != 0 || lim != 50 {
		t.Fatalf("non-numeric: off=%d lim=%d", off, lim)
	}
}

func TestV2Header(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-Tenant-Id", "t-1")
	r.Header.Set("X-GCID", "g-1")
	r.Header.Set("traceparent", "00-abc-123-01")
	h := v2Header(r)
	if h.TenantID != "t-1" || h.GCID != "g-1" || h.Traceparent != "00-abc-123-01" {
		t.Fatalf("unexpected header %+v", h)
	}
	// GCID falls back to the lowercase gcid header.
	r2 := httptest.NewRequest(http.MethodGet, "/x", nil)
	r2.Header.Set("gcid", "g-2")
	if h := v2Header(r2); h.GCID != "g-2" {
		t.Fatalf("expected gcid fallback, got %q", h.GCID)
	}
	if got := firstNonEmpty("", "b"); got != "b" {
		t.Fatalf("firstNonEmpty(empty,b) = %q", got)
	}
}

func TestAddonDisplayName(t *testing.T) {
	t.Parallel()
	a := &addon.AddOn{DisplayName: "Training Suite"}
	if got := addonDisplayName(a); got != "Training Suite" {
		t.Fatalf("unexpected display name %q", got)
	}
	if got := addonDisplayName(nil); got != "" {
		t.Fatalf("nil addon should yield empty string, got %q", got)
	}
}

func TestV2InvoiceDTO(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	inv := &billing.Invoice{
		ID: "inv-1", TenantID: "t-1", PeriodStart: start, PeriodEnd: start.Add(24 * time.Hour),
		TotalCents: 4900, Status: billing.InvoiceStatusIssued,
	}
	out := v2InvoiceDTO(inv)
	if out["id"] != "inv-1" || out["tenant_id"] != "t-1" || out["total_cents"] != int64(4900) {
		t.Fatalf("unexpected DTO %v", out)
	}
	if out["status"] != string(billing.InvoiceStatusIssued) {
		t.Fatalf("unexpected status %v", out)
	}
	if _, ok := out["period_start"].(string); !ok || out["period_start"] == "" {
		t.Fatalf("expected formatted period_start, got %v", out["period_start"])
	}
}

func TestPublishEventWithIMDA(t *testing.T) {
	t.Parallel()
	deps := NewDefaultV2Deps()
	deps.Events = events.NewRecorder()
	hdr := events.Header{TenantID: "t-1", GCID: "g-1"}
	// Nil payload → synthesised map with the mandated dimension.
	publishEventWithIMDA(deps, "chora.tenancy.tenant.created.v1", hdr, nil)
	// Non-nil payload → dimension injected.
	publishEventWithIMDA(deps, "chora.tenancy.tenant.created.v1", hdr, map[string]interface{}{"id": "t-1"})
	rec, _ := deps.Events.(*events.Recorder)
	recs := rec.Recorded()
	if len(recs) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(recs))
	}
	for _, ev := range recs {
		if ev.Payload["chora_imda_dimension"] != "accountability" {
			t.Fatalf("IMDA dimension missing from %+v", ev.Payload)
		}
	}
}

func TestV2RequireTenantAndHealth(t *testing.T) {
	t.Parallel()
	// 401-ish without header → 400 gate.
	missing := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := v2RequireTenant(missing)
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without tenant header, got %d", rec.Code)
	}
	// With header → passes through.
	req2 := httptest.NewRequest(http.MethodGet, "/x", nil)
	req2.Header.Set("X-Tenant-Id", "t-1")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTeapot {
		t.Fatalf("expected pass-through, got %d", rec2.Code)
	}
	// Health endpoint.
	rec3 := httptest.NewRecorder()
	v2Health(rec3, req)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec3.Code)
	}
}

func TestIsAddonInstalled(t *testing.T) {
	t.Parallel()
	deps := NewDefaultV2Deps()
	at := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	// Blank tenant.
	if isAddonInstalled(deps, "", "tms", at) {
		t.Fatal("blank tenant must not be installed")
	}
	// No subscription.
	if isAddonInstalled(deps, "t-1", "tms", at) {
		t.Fatal("no subscription must not be installed")
	}
	// Active.
	if _, err := deps.Subscriptions.Subscribe("t-1", "tms", "g-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !isAddonInstalled(deps, "t-1", "tms", at) {
		t.Fatal("active subscription must be installed")
	}
	// Pending activation.
	if isAddonInstalled(deps, "t-1", "cms", at) {
		t.Fatal("missing subscription should not be installed")
	}
	if _, err := deps.Subscriptions.SubscribePending("t-1", "cms", "g-1"); err != nil {
		t.Fatalf("SubscribePending: %v", err)
	}
	if !isAddonInstalled(deps, "t-1", "cms", at) {
		t.Fatal("pending-activation subscription must be installed")
	}
	// Grace period: expires in the future → installed; already expired → not.
	deps.Subscriptions.SetClock(func() time.Time { return at })
	if err := deps.Subscriptions.Unsubscribe("t-1", "cms", "g-1"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if !isAddonInstalled(deps, "t-1", "cms", at) {
		t.Fatal("in-grace subscription must be installed")
	}
	if isAddonInstalled(deps, "t-1", "cms", at.Add(30*24*time.Hour).Add(time.Hour)) {
		t.Fatal("past grace-end must not be installed")
	}
	// Deactivated (default branch).
	if _, _, err := deps.Subscriptions.AnchorDeactivation("t-1", "cms"); err != nil {
		t.Fatalf("AnchorDeactivation: %v", err)
	}
	if isAddonInstalled(deps, "t-1", "cms", at) {
		t.Fatal("deactivated subscription must not be installed")
	}
}

func TestV2TenantDTO_DeletedAtBranch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	ten := &tenant.Tenant{
		ID: "t-1", DisplayName: "X", OwnerGCID: "g", Status: tenant.StatusClosed,
		Branding:  tenant.BrandingConfig{PrimaryColorHex: "#000"},
		DeletedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	out := v2TenantDTO(ten)
	if out["deleted_at"] == "" {
		t.Fatal("expected deleted_at in DTO")
	}
	if out["status"] != string(tenant.StatusClosed) {
		t.Fatalf("unexpected status %v", out["status"])
	}
}
