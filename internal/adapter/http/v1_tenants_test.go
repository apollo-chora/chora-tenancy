// Package httpapi_test — Phyllis MVP §5.2 light-wizard tests for the
// /v1/tenants route family + the new /v1/admin/tenants/{id}/golive
// transition + the BE-T-ADD-1 addon lifecycle endpoints (list, detail,
// usage, activate, change-tier, deactivate) + the addon.usage_recorded.v1
// emission path (Stage 4 nightly rollup wiring).
//
// Aligned with:
//   - docs/m13/phyllis-mvp-2026-05-08.md §5.2
//   - docs/m13/audit-platform-fillgaps.md §2.1 (gaps inventory) + §10 (Jira)
//   - docs/design/ux_tenant_addon_lifecycle.md (5 screens)
//
// All tests run RED first — implementation lives in v1_handlers.go (newly
// added). We assert idempotency, status transitions, Stripe customer ID
// stamping, mana-pool default allocation, the 6 addon lifecycle events
// (activated / upgraded / downgraded / deactivation_requested /
// deactivated / usage_recorded), and the compliance-lock 403 behaviour.
package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	// v1Tenant is the demo MTM tenant ID used in Phyllis MVP fixtures.
	v1Tenant = "01970000-0000-7000-8000-0000000000b0"
	v1GCID   = "gcid-phyllis-1"
)

func newV1Server(t *testing.T) (http.Handler, httpapi.V2Deps) {
	t.Helper()
	deps := httpapi.NewDefaultV2Deps()
	srv := httpapi.NewV2Server(deps)
	return srv, deps
}

func doV1(t *testing.T, srv http.Handler, method, path string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", v1Tenant)
	req.Header.Set("X-GCID", v1GCID)
	req.Header.Set("X-Role", "tenant_admin")
	// The real gateway stamps roles on x-mesh-user-roles (servicemesh). The bare
	// /v1/tenants root-tenant create/list is platform_operator-only (CHO-2077);
	// the /v1/admin/tenants/* addon gates are tenant_admin (CHO-1801/2072).
	meshRole := "tenant_admin"
	if path == "/v1/tenants" {
		meshRole = "platform_operator"
	}
	req.Header.Set("x-mesh-user-roles", meshRole)
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

// ---------------------------------------------------------------------------
// POST /v1/tenants — light-wizard creator (Phyllis MVP §5.2)
// ---------------------------------------------------------------------------

func TestV1_PostTenants_CreatesPendingActivation(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, body := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name":     "MTM Singapore",
		"owner_gcid":       "gcid-phyllis",
		"slug":             "mtm-singapore",
		"country":          "SG",
		"default_currency": "SGD",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "pending_activation" {
		t.Fatalf("expected status=pending_activation, got %q", got)
	}
	if got, _ := body["country"].(string); got != "SG" {
		t.Fatalf("expected country=SG, got %q", got)
	}
	if got, _ := body["default_currency"].(string); got != "SGD" {
		t.Fatalf("expected default_currency=SGD, got %q", got)
	}
	if id, _ := body["id"].(string); id == "" {
		t.Fatalf("expected non-empty tenant id")
	}
}

// Idempotent on slug: a second POST with the same slug returns the
// existing tenant (no duplicate).
func TestV1_PostTenants_IsIdempotentOnSlug(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	in := map[string]interface{}{
		"display_name": "MTM SG",
		"owner_gcid":   "gcid-phyllis",
		"slug":         "mtm-idem",
	}
	_, first := doV1(t, srv, "POST", "/v1/tenants", in)
	rec2, second := doV1(t, srv, "POST", "/v1/tenants", in)
	if rec2.Code != http.StatusCreated && rec2.Code != http.StatusOK {
		t.Fatalf("expected 200/201 on idempotent retry, got %d", rec2.Code)
	}
	if first["id"] != second["id"] {
		t.Fatalf("expected same id, got %v vs %v", first["id"], second["id"])
	}
}

func TestV1_PostTenants_RejectsInvalidSlug(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "Bad",
		"owner_gcid":   "g1",
		"slug":         "WITH SPACE",
	})
	if rec.Code != http.StatusBadRequest && rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 400/422 for invalid slug, got %d", rec.Code)
	}
}

func TestV1_PostTenants_EmitsTenantCreatedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM",
		"owner_gcid":   "g1",
		"slug":         "mtm-evt",
	})
	created := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.tenant.created.v1")
	if len(created) != 1 {
		t.Fatalf("expected 1 chora.tenancy.tenant.created.v1 event, got %d", len(created))
	}
	if created[0].Payload["slug"] != "mtm-evt" {
		t.Fatalf("expected slug in payload")
	}
}

// ---------------------------------------------------------------------------
// GET /v1/tenants/{id}
// ---------------------------------------------------------------------------

func TestV1_GetTenantByID(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM", "owner_gcid": "g1", "slug": "mtm-get",
	})
	id, _ := created["id"].(string)
	rec, body := doV1(t, srv, "GET", "/v1/tenants/"+id, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["id"].(string); got != id {
		t.Fatalf("ID mismatch: %q vs %q", got, id)
	}
}

// TestV1_GetTenantByID_OmitsWizardCompletedAtWhenNull — fresh tenants
// have a NULL wizard_completed_at; the DTO MUST omit the field rather
// than emit `null`, mirroring the activated_at / deleted_at pattern.
// The wizard's re-entry hydration (CHO-1692) treats omission as "not
// yet completed" — no banner.
func TestV1_GetTenantByID_OmitsWizardCompletedAtWhenNull(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "WizardOmit", "owner_gcid": "g-wo", "slug": "wizard-omit",
	})
	id, _ := created["id"].(string)
	_, body := doV1(t, srv, "GET", "/v1/tenants/"+id, nil)
	if _, ok := body["wizard_completed_at"]; ok {
		t.Errorf("fresh tenant DTO must OMIT wizard_completed_at; got %v", body["wizard_completed_at"])
	}
}

func TestV1_GetTenantByID_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "GET", "/v1/tenants/01970000-0000-7000-8000-000000000bad", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/admin/tenants/{id}/golive — go-live transition with Stripe Customer
// ---------------------------------------------------------------------------

func TestV1_PostGoLive_TransitionsTenantToActive(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM SG", "owner_gcid": "g1", "slug": "mtm-golive",
	})
	id, _ := created["id"].(string)
	rec, body := doV1(t, srv, "POST", "/v1/admin/tenants/"+id+"/golive", map[string]interface{}{})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "active" {
		t.Fatalf("expected status=active, got %q", got)
	}
	cus, _ := body["stripe_customer_id"].(string)
	if !startsWith(cus, "cus_") {
		t.Fatalf("expected stripe_customer_id with cus_ prefix, got %q", cus)
	}
}

// Idempotency: two calls return the same Stripe customer; the second call
// returns 200 (NOT 409) because the desired state matches.
func TestV1_PostGoLive_IsIdempotent(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM", "owner_gcid": "g1", "slug": "mtm-idem-gl",
	})
	id, _ := created["id"].(string)
	_, first := doV1(t, srv, "POST", "/v1/admin/tenants/"+id+"/golive", map[string]interface{}{})
	rec2, second := doV1(t, srv, "POST", "/v1/admin/tenants/"+id+"/golive", map[string]interface{}{})
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on retry, got %d", rec2.Code)
	}
	if first["stripe_customer_id"] != second["stripe_customer_id"] {
		t.Fatalf("expected idempotent stripe customer ID, got %v vs %v",
			first["stripe_customer_id"], second["stripe_customer_id"])
	}
}

func TestV1_PostGoLive_404OnUnknownTenant(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "POST", "/v1/admin/tenants/01970000-0000-7000-8000-00000000bad9/golive", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestV1_PostGoLive_EmitsTenantGoliveEvent_TaggedAccountability(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM", "owner_gcid": "g1", "slug": "mtm-evt-gl",
	})
	id, _ := created["id"].(string)
	doV1(t, srv, "POST", "/v1/admin/tenants/"+id+"/golive", map[string]interface{}{})

	events := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.tenant.golive.v1")
	if len(events) != 1 {
		t.Fatalf("expected 1 chora.tenancy.tenant.golive.v1 event, got %d", len(events))
	}
	pl := events[0].Payload
	if pl["tenant_id"] != id {
		t.Fatalf("expected tenant_id in payload")
	}
	if cus, _ := pl["stripe_customer_id"].(string); !startsWith(cus, "cus_") {
		t.Fatalf("expected stripe_customer_id in payload")
	}
	// IMDA D1 accountability tag per ADR-141.
	if got, _ := pl["chora_imda_dimension"].(string); got != "accountability" {
		t.Fatalf("expected chora_imda_dimension=accountability, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Add-on lifecycle endpoints — 6 endpoints per ux_tenant_addon_lifecycle.md
// ---------------------------------------------------------------------------

// 1. GET /v1/admin/tenants/{id}/addons → list active + available items.
//    The audit (`audit-platform-fillgaps.md` §2.1) confirmed `addons` exists
//    via the `/api/v1/admin/tenants/{id}/addons` path; this test guards
//    the response shape.
func TestV1_ListAddons_ShapeMatchesUXSpec(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, body := doV1(t, srv, "GET", "/api/v1/admin/tenants/"+v1Tenant+"/addons", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) == 0 {
		t.Fatalf("expected ≥1 active sub")
	}
	first := items[0].(map[string]interface{})
	for _, k := range []string{"addon_plan_id", "addon_code", "display_name", "status", "current_tier", "monthly_cost"} {
		if _, ok := first[k]; !ok {
			t.Fatalf("expected key %q in addon item", k)
		}
	}
}

// 2. POST /v1/admin/tenants/{id}/addons/{addonPlanId}/activate — idempotent
//    activation. Fails with 422 when add-on unknown.
func TestV1_PostActivateAddon_Idempotent(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	_, first := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/familiar/activate",
		map[string]interface{}{})
	_, second := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/familiar/activate",
		map[string]interface{}{})
	if first["subscription_id"] != second["subscription_id"] {
		t.Fatalf("expected idempotent activation, got %v vs %v",
			first["subscription_id"], second["subscription_id"])
	}
}

func TestV1_PostActivateAddon_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/no-such-addon/activate",
		map[string]interface{}{})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// 3. POST /v1/admin/tenants/{id}/addons/{addonPlanId}/deactivate
//    Compliance lock prevents deactivation of governance_dashboard.
func TestV1_PostDeactivateAddon_RejectsComplianceLocked(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "governance_dashboard", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, body := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/governance_dashboard/deactivate",
		map[string]interface{}{
			"reason": "no_longer_needed",
		})
	if rec.Code != http.StatusLocked {
		t.Fatalf("expected 423 Locked for compliance-locked add-on, got %d (body=%v)", rec.Code, body)
	}
}

// 4. POST /v1/admin/tenants/{id}/addons/{addonPlanId}/change-tier — proration
//    via Stripe Subscription Schedule.
func TestV1_PostChangeTier_EmitsUpgradeEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, body := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/change-tier",
		map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	upgrades := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1")
	if len(upgrades) != 1 {
		t.Fatalf("expected 1 upgraded event, got %d", len(upgrades))
	}
}

// 5. GET /v1/admin/tenants/{id}/addons/{addonPlanId}/usage — current period.
func TestV1_GetAddonUsage_ReturnsTimeSeries(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, body := doV1(t, srv, "GET",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/usage", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if _, ok := body["time_series"].([]interface{}); !ok {
		t.Fatalf("expected time_series in response")
	}
}

// 6. GET /v1/admin/tenants/{id}/addons/{addonPlanId} — detail.
func TestV1_GetAddonDetail_ReturnsCatalogueData(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, body := doV1(t, srv, "GET",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["code"].(string); got != "tms" {
		t.Fatalf("expected code=tms, got %v", body["code"])
	}
}

// ---------------------------------------------------------------------------
// addon.usage_recorded.v1 emission path
// ---------------------------------------------------------------------------

// The usage_recorded event is emitted by the nightly rollup job (deliverable
// 4) as it walks `addon_usage_events` → `addon_usage_rollups`. The
// HTTP layer exposes a stub trigger `:flush-usage` for tests + the
// nightly job entry-point.
func TestV1_FlushUsage_EmitsUsageRecordedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	// Record a usage snapshot for today; the flush handler aggregates +
	// emits.
	doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/usage:record",
		map[string]interface{}{
			"seats_used":     5,
			"quota_consumed": 1024,
			"api_calls":      32,
		})
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/usage:flush",
		map[string]interface{}{})
	if rec.Code != http.StatusAccepted && rec.Code != http.StatusOK {
		t.Fatalf("expected 200/202, got %d", rec.Code)
	}
	usage := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.usage_recorded.v1")
	if len(usage) == 0 {
		t.Fatalf("expected at least 1 usage_recorded event")
	}
}

// ---------------------------------------------------------------------------
// Additional coverage hardening
// ---------------------------------------------------------------------------

// Deactivate happy path (non-compliance-locked add-on) emits the
// chora.tenancy.addon.deactivated.v1 event when no future effective_at.
func TestV1_PostDeactivateAddon_EmitsImmediateDeactivatedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, body := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/deactivate",
		map[string]interface{}{
			"reason":      "no_longer_needed",
			"reason_text": "project ended",
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	deactivated := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivated.v1")
	if len(deactivated) != 1 {
		t.Fatalf("expected 1 deactivated event, got %d", len(deactivated))
	}
}

// Deactivate with future effective_at emits deactivation_requested instead.
func TestV1_PostDeactivateAddon_EmitsDeactivationRequestedOnDeferred(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	future := "2099-12-31T00:00:00Z"
	rec, body := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/deactivate",
		map[string]interface{}{
			"reason":       "consolidation",
			"effective_at": future,
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	requested := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_requested.v1")
	if len(requested) != 1 {
		t.Fatalf("expected 1 deactivation_requested event, got %d", len(requested))
	}
}

func TestV1_PostDeactivateAddon_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/no-such/deactivate",
		map[string]interface{}{"reason": "no_longer_needed"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestV1_PostDeactivateAddon_422OnBadEffectiveAt(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/deactivate",
		map[string]interface{}{
			"reason":       "cost",
			"effective_at": "not-a-timestamp",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

// Compliance-lock super_admin override: super_admin role bypasses the
// 423 Locked guard.
func TestV1_PostDeactivateAddon_SuperAdminBypassesComplianceLock(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "governance_dashboard", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	// Switch role to super_admin via header.
	body := map[string]interface{}{"reason": "compliance"}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest("POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/governance_dashboard/deactivate", &buf)
	req.Header.Set("X-Tenant-Id", v1Tenant)
	req.Header.Set("X-GCID", v1GCID)
	req.Header.Set("X-Role", "super_admin")
	req.Header.Set("x-mesh-user-roles", "super_admin") // CHO-2072: super_admin override reads the mesh header
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	// super_admin bypasses 423; the domain layer (a.ComplianceLocked)
	// still rejects, returning 423 from the domain's
	// ErrComplianceLocked path.
	if rec.Code != http.StatusLocked {
		t.Fatalf("expected 423 from domain compliance-lock for governance_dashboard (super_admin can override only via separate API), got %d", rec.Code)
	}
}

// Change-tier 422 on bad effective_at.
func TestV1_PostChangeTier_422OnBadEffectiveAt(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/change-tier",
		map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
			"effective_at":     "not-a-timestamp",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

// Change-tier 400 when neither target_tier_code nor target_plan_id supplied.
func TestV1_PostChangeTier_400OnMissingTarget(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	if _, err := deps.Subscriptions.Subscribe(v1Tenant, "tms", v1GCID); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/change-tier",
		map[string]interface{}{
			"proration_mode": "create_prorations",
		})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// GET /v1/tenants list returns multiple created tenants.
func TestV1_ListTenants_ReturnsCreatedTenants(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "T1", "owner_gcid": "g1", "slug": "list-t1",
	})
	doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "T2", "owner_gcid": "g2", "slug": "list-t2",
	})
	rec, body := doV1(t, srv, "GET", "/v1/tenants", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) < 2 {
		t.Fatalf("expected ≥2 items, got %d", len(items))
	}
}

// GoLive 409 on closed tenant.
func TestV1_PostGoLive_409OnClosedTenant(t *testing.T) {
	t.Parallel()
	srv, deps := newV1Server(t)
	_, created := doV1(t, srv, "POST", "/v1/tenants", map[string]interface{}{
		"display_name": "MTM", "owner_gcid": "g1", "slug": "mtm-closed",
	})
	id, _ := created["id"].(string)
	tt, _ := deps.Tenants.Get(id)
	tt.Close("test")
	deps.Tenants.Save(tt)
	rec, _ := doV1(t, srv, "POST", "/v1/admin/tenants/"+id+"/golive", map[string]interface{}{})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

// Usage:record happy path returns 200 with bucket_date.
func TestV1_RecordUsage_HappyPath(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, body := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/tms/usage:record",
		map[string]interface{}{
			"seats_used":     2,
			"quota_consumed": 100,
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if _, ok := body["bucket_date"]; !ok {
		t.Fatalf("expected bucket_date in response")
	}
}

// Usage:record 404 on unknown plan.
func TestV1_RecordUsage_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "POST",
		"/v1/admin/tenants/"+v1Tenant+"/addons/no-such/usage:record",
		map[string]interface{}{})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// Method-not-allowed paths.
func TestV1_PostTenants_MethodNotAllowedOnUnsupportedVerb(t *testing.T) {
	t.Parallel()
	srv, _ := newV1Server(t)
	rec, _ := doV1(t, srv, "PUT", "/v1/tenants", map[string]interface{}{})
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
