// Package httpapi_test holds the RED-phase TDD specs for the HTTP/JSON
// adapter for chora-tenancy.
//
// We exercise the full server.Handler with `httptest` — no external server.
package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/inmem"
	domain "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

const tenantHeader = "01970000-0000-7000-8000-000000000001"

func newServer() http.Handler {
	deps := httpapi.Deps{
		Tenants:        inmem.NewTenantRepo(),
		AddOnCatalog:   domain.NewAddOnCatalog(),
		Entitlements:   domain.NewEntitlementRegistry(),
		Invoices:       inmem.NewInvoiceRepo(),
		PaymentMethods: inmem.NewPaymentMethodRepo(),
	}
	return httpapi.NewServer(deps)
}

func do(t *testing.T, srv http.Handler, method, path string, body interface{}, includeTenantHeader bool) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	// CHO-2077: root-tenant create/list (/api/tenants) is platform_operator-only;
	// stamp the operator mesh role so these tests pass the gate. Role-agnostic
	// endpoints this helper also drives (entitlements, addons) ignore it.
	req.Header.Set("x-mesh-user-roles", "platform_operator")
	if includeTenantHeader {
		req.Header.Set("X-Tenant-Id", tenantHeader)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

// -----------------------------------------------------------------------------
// Health + middleware
// -----------------------------------------------------------------------------

func TestHealth_NoTenantRequired(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "GET", "/healthz/", nil, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz/: expected 200, got %d", rec.Code)
	}
}

func TestReadyz_NoTenantRequired(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "GET", "/readyz", nil, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz: expected 200, got %d", rec.Code)
	}
}

func TestTenantMiddleware_RejectsMissingHeader(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "POST", "/api/tenants", map[string]interface{}{"display_name": "Acme"}, false)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing X-Tenant-Id, got %d", rec.Code)
	}
}

func TestHealth_TraceparentEcho(t *testing.T) {
	t.Parallel()
	srv := newServer()
	req := httptest.NewRequest("GET", "/healthz/", nil)
	req.Header.Set("traceparent", "00-aabbccddeeff00112233445566778899-0102030405060708-01")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if got := rec.Header().Get("traceparent"); !strings.Contains(got, "aabbccdd") {
		t.Fatalf("expected traceparent echo, got %q", got)
	}
}

// -----------------------------------------------------------------------------
// Tenants
// -----------------------------------------------------------------------------

func TestCreateTenant_Returns201(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, body := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme School",
		"self_hosted":  false,
	}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if _, ok := body["id"].(string); !ok {
		t.Fatalf("expected id in response")
	}
	if got, _ := body["display_name"].(string); got != "Acme School" {
		t.Fatalf("display_name mismatch: %q", got)
	}
}

func TestCreateTenant_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "",
	}, true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// Self-tenant cycle: parent_tenant_id cannot be self → 400. Per the brief.
func TestCreateTenant_RejectsSelfParent(t *testing.T) {
	t.Parallel()
	srv := newServer()
	// Create parent tenant first.
	_, body := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Parent",
	}, true)
	parentID, _ := body["id"].(string)
	// Try to create a tenant whose parent is itself — we cannot do that
	// at create time (id assigned by server) so instead PATCH would do it.
	// Instead: simulate by creating a tenant then setting its parent
	// via /api/tenants/{id}/parent -- but our route shape here is more
	// minimal. We test the behavior via two-step: create A, attempt to
	// set parent_tenant_id=A on tenant A.
	rec, _ := do(t, srv, "POST", "/api/tenants/"+parentID+"/parent", map[string]interface{}{
		"parent_tenant_id": parentID,
	}, true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for self-parent cycle, got %d", rec.Code)
	}
}

func TestGetTenant_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "GET", "/api/tenants/does-not-exist", nil, true)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestListTenants_PaginatedResponse(t *testing.T) {
	t.Parallel()
	srv := newServer()
	for i := 0; i < 3; i++ {
		do(t, srv, "POST", "/api/tenants", map[string]interface{}{
			"display_name": "T",
		}, true)
	}
	rec, body := do(t, srv, "GET", "/api/tenants", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) != 3 {
		t.Fatalf("expected 3 tenants, got %d", len(items))
	}
	if total, _ := body["total"].(float64); int(total) != 3 {
		t.Fatalf("expected total=3, got %v", body["total"])
	}
}

// -----------------------------------------------------------------------------
// AddOns catalog
// -----------------------------------------------------------------------------

func TestRegisterAddOn_Returns201(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, body := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name":                "Reward Vault",
		"description":         "Per-tenant reward economy",
		"monthly_price_cents": 4900,
		"included_capabilities": []string{
			"rewards", "marketplace",
		},
	}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if _, ok := body["id"].(string); !ok {
		t.Fatalf("expected id in response")
	}
}

func TestListAddOns(t *testing.T) {
	t.Parallel()
	srv := newServer()
	do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "AddOn1", "monthly_price_cents": 100,
	}, true)
	do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "AddOn2", "monthly_price_cents": 200,
	}, true)
	rec, body := do(t, srv, "GET", "/api/addons", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) != 2 {
		t.Fatalf("expected 2 addons, got %d", len(items))
	}
}

// -----------------------------------------------------------------------------
// Entitlements
// -----------------------------------------------------------------------------

func TestSubscribeEntitlement_Returns201(t *testing.T) {
	t.Parallel()
	srv := newServer()
	// Create tenant
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	// Create addon
	_, aBody := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name":                "Rewards",
		"monthly_price_cents": 4900,
	}, true)
	addOnID, _ := aBody["id"].(string)
	// Subscribe
	rec, body := do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{
		"addon_id": addOnID,
	}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "active" {
		t.Fatalf("expected active status, got %q", got)
	}
}

// Cancel idempotent: cancelling already-cancelled returns existing (not 404).
func TestCancelEntitlement_IsIdempotent(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	_, aBody := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "Rewards", "monthly_price_cents": 100,
	}, true)
	addOnID, _ := aBody["id"].(string)

	do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{
		"addon_id": addOnID,
	}, true)

	rec1, body1 := do(t, srv, "DELETE", "/api/tenants/"+tenantID+"/entitlements/"+addOnID, nil, true)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first cancel: expected 200, got %d", rec1.Code)
	}
	if got, _ := body1["status"].(string); got != "cancelled" {
		t.Fatalf("first cancel: expected cancelled, got %q", got)
	}
	rec2, body2 := do(t, srv, "DELETE", "/api/tenants/"+tenantID+"/entitlements/"+addOnID, nil, true)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second cancel: expected 200 (idempotent), got %d", rec2.Code)
	}
	if got, _ := body2["status"].(string); got != "cancelled" {
		t.Fatalf("second cancel: expected cancelled, got %q", got)
	}
}

func TestCancelEntitlement_UnknownReturns404(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	rec, _ := do(t, srv, "DELETE", "/api/tenants/"+tenantID+"/entitlements/no-such-addon", nil, true)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestListEntitlements(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	_, a1Body := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "A1", "monthly_price_cents": 100,
	}, true)
	a1ID, _ := a1Body["id"].(string)
	_, a2Body := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "A2", "monthly_price_cents": 200,
	}, true)
	a2ID, _ := a2Body["id"].(string)
	do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{"addon_id": a1ID}, true)
	do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{"addon_id": a2ID}, true)
	rec, body := do(t, srv, "GET", "/api/tenants/"+tenantID+"/entitlements", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) != 2 {
		t.Fatalf("expected 2 entitlements, got %d", len(items))
	}
	// CHO-1717 §4.6 contract pin — every entitlement DTO carries the
	// `addon_code` key (string; empty for legacy rows / the in-memory
	// registry which has no add-on catalog join).
	for i, raw := range items {
		item, _ := raw.(map[string]interface{})
		if _, present := item["addon_code"]; !present {
			t.Errorf("items[%d] missing addon_code key: %v", i, item)
		}
		if _, isString := item["addon_code"].(string); !isString {
			t.Errorf("items[%d].addon_code must be a string, got %T", i, item["addon_code"])
		}
	}
}

// fakeEntitlementStore satisfies the EntitlementStore port with canned
// rows so the DTO test can exercise a POPULATED AddOnCode (the in-memory
// registry never sets it; the pg adapter joins add_ons for it).
type fakeEntitlementStore struct {
	items []*domain.TenantEntitlement
}

func (f *fakeEntitlementStore) Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*domain.TenantEntitlement, error) {
	return nil, domain.ErrInvalidArgument
}
func (f *fakeEntitlementStore) Cancel(tenantID, addOnID string) (*domain.TenantEntitlement, error) {
	return nil, domain.ErrEntitlementNotFound
}
func (f *fakeEntitlementStore) ListActiveByTenant(tenantID string) []*domain.TenantEntitlement {
	return f.items
}

// CHO-1717 §4.6 — the entitlements read path surfaces the add-on `code`
// (migration 0021 added add_ons.code; the pg adapter JOINs it into
// TenantEntitlement.AddOnCode). Wire-contract pin for the parallel FE
// lane: JSON field name is exactly `addon_code`.
func TestListEntitlements_SurfacesAddonCode(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	deps := httpapi.Deps{
		Tenants:      inmem.NewTenantRepo(),
		AddOnCatalog: domain.NewAddOnCatalog(),
		Entitlements: &fakeEntitlementStore{items: []*domain.TenantEntitlement{{
			ID:                        "50b00001-0000-7000-8000-000000000001",
			TenantID:                  tenantHeader,
			AddOnID:                   "a7d00006-0000-7000-8000-000000000006",
			AddOnCode:                 "cplus_social",
			MonthlyPriceCentsSnapshot: 4900,
			Status:                    domain.EntitlementStatusActive,
			ActivatedAt:               now,
			UpdatedAt:                 now,
		}}},
		Invoices:       inmem.NewInvoiceRepo(),
		PaymentMethods: inmem.NewPaymentMethodRepo(),
	}
	srv := httpapi.NewServer(deps)

	rec, body := do(t, srv, "GET", "/api/tenants/"+tenantHeader+"/entitlements", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("expected 1 entitlement, got %d", len(items))
	}
	item, _ := items[0].(map[string]interface{})
	if got, _ := item["addon_code"].(string); got != "cplus_social" {
		t.Errorf("addon_code = %q, want %q (FE FeatureFlagService.isEnabled contract)", got, "cplus_social")
	}
}

// -----------------------------------------------------------------------------
// Invoices
// -----------------------------------------------------------------------------

// Invoice line item sum: sums all active entitlement prices for the period.
func TestGenerateInvoice_SumsActiveEntitlements(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	_, a1Body := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "A1", "monthly_price_cents": 4900,
	}, true)
	a1ID, _ := a1Body["id"].(string)
	_, a2Body := do(t, srv, "POST", "/api/addons", map[string]interface{}{
		"name": "A2", "monthly_price_cents": 2900,
	}, true)
	a2ID, _ := a2Body["id"].(string)
	do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{"addon_id": a1ID}, true)
	do(t, srv, "POST", "/api/tenants/"+tenantID+"/entitlements", map[string]interface{}{"addon_id": a2ID}, true)

	rec, body := do(t, srv, "POST", "/api/tenants/"+tenantID+"/invoices", map[string]interface{}{
		"period_start": "2026-05-01T00:00:00Z",
		"period_end":   "2026-06-01T00:00:00Z",
	}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if total, _ := body["total_cents"].(float64); int64(total) != 7800 {
		t.Fatalf("expected total 7800, got %v", body["total_cents"])
	}
}

func TestGenerateInvoice_RejectsInvertedPeriod(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	rec, _ := do(t, srv, "POST", "/api/tenants/"+tenantID+"/invoices", map[string]interface{}{
		"period_start": "2026-06-01T00:00:00Z",
		"period_end":   "2026-05-01T00:00:00Z",
	}, true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListInvoices(t *testing.T) {
	t.Parallel()
	srv := newServer()
	_, tBody := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme",
	}, true)
	tenantID, _ := tBody["id"].(string)
	do(t, srv, "POST", "/api/tenants/"+tenantID+"/invoices", map[string]interface{}{
		"period_start": "2026-05-01T00:00:00Z",
		"period_end":   "2026-06-01T00:00:00Z",
	}, true)
	rec, body := do(t, srv, "GET", "/api/tenants/"+tenantID+"/invoices", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("expected 1 invoice, got %d", len(items))
	}
}

// -----------------------------------------------------------------------------
// Method-not-allowed coverage
// -----------------------------------------------------------------------------

func TestTenants_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "PUT", "/api/tenants", nil, true)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestAddOns_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer()
	rec, _ := do(t, srv, "PUT", "/api/addons", nil, true)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}
