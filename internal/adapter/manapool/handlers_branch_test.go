// handlers_branch_test.go — error-branch coverage for the BE-USR-3
// manapool HTTP handlers (405/400/404/402/422 paths) + the
// RecorderPublisher envelope helpers. Reuses the harness from
// handlers_test.go.
package manapool_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	manapool "github.com/apollo-chora/chora-tenancy/internal/adapter/manapool"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// doReq issues a request with a custom body, method, path and tenant header.
func doReq(t *testing.T, h *harness, method, path, tenantID string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", tenantID)
	req.Header.Set("X-GCID", gcidAdmin)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

// doRaw sends a raw (possibly invalid) request body.
func doRaw(t *testing.T, h *harness, method, path string, raw string) int {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(raw))
	req.Header.Set("X-Tenant-Id", tenantA)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	return rec.Code
}

// createPool seeds a pool for tenantA via the API.
func createPool(t *testing.T, h *harness) {
	t.Helper()
	rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", tenantA,
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create pool: %d", rec.Code)
	}
}

func TestRouter_MissingTenantHeader_400(t *testing.T) {
	t.Parallel()
	h := newHarness()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without X-Tenant-Id, got %d", rec.Code)
	}
}

func TestRouter_UnknownRoutes_404(t *testing.T) {
	t.Parallel()
	h := newHarness()
	for _, path := range []string{
		"/api/v1/admin/tenants/" + tenantA + "/mana-poolx",
		"/api/v1/admin/tenants/" + tenantA + "/mana-pool/whatever",
		"/api/v1/admin/tenants/" + tenantA + "/mana-allocations/x/y",
		"/api/v1/admin/tenants/" + tenantA + "/mana-pool:unknown",
	} {
		rec, _ := doReq(t, h, http.MethodGet, path, tenantA, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("path %s: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestTopupHandler_ValidationAndErrorPaths(t *testing.T) {
	t.Parallel()
	h := newHarness()
	createPool(t, h)
	// Wrong method.
	if rec, _ := doReq(t, h, http.MethodPut, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	// Malformed body.
	if code := doRaw(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", "{bad"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
	// amount < 100.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 1, "units": 1, "currency": "SGD"}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for amount<100, got %d", rec.Code)
	}
	// units < 1.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 100, "units": 0, "currency": "SGD"}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for units<1, got %d", rec.Code)
	}
	// missing currency.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 100, "units": 1}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for missing currency, got %d", rec.Code)
	}
	// missing pool (tenantB has none).
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantB+"/mana-pool:topup", tenantB,
		map[string]interface{}{"amount_cents": 100, "units": 1, "currency": "SGD"}); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing pool, got %d", rec.Code)
	}
	// Stripe charge failure → 402.
	h.stripe.FailNext("card_declined")
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 100, "units": 1, "currency": "SGD"}); rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402 on stripe failure, got %d", rec.Code)
	}
	// Happy top-up.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 10000, "units": 1000, "currency": "SGD"}); rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAutoRenewHandler_Errors(t *testing.T) {
	t.Parallel()
	h := newHarness()
	createPool(t, h)
	if rec, _ := doReq(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew", tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if code := doRaw(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew", "{bad"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantB+"/mana-pool:auto-renew", tenantB,
		map[string]interface{}{"monthly_topup_units": 1000}); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	// Negative units → SetMonthlyTopupUnits error.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew", tenantA,
		map[string]interface{}{"monthly_topup_units": -5}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for negative units, got %d", rec.Code)
	}
	// Happy path.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew", tenantA,
		map[string]interface{}{"monthly_topup_units": 500}); rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPolicyHandler_Errors(t *testing.T) {
	t.Parallel()
	h := newHarness()
	createPool(t, h)
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool/policy", tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if code := doRaw(t, h, http.MethodPatch, "/api/v1/admin/tenants/"+tenantA+"/mana-pool/policy", "{bad"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
	// Unknown policy kind.
	if rec, _ := doReq(t, h, http.MethodPatch, "/api/v1/admin/tenants/"+tenantA+"/mana-pool/policy", tenantA,
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "garbage"}}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for bad policy, got %d", rec.Code)
	}
	// Missing pool.
	if rec, _ := doReq(t, h, http.MethodPatch, "/api/v1/admin/tenants/"+tenantB+"/mana-pool/policy", tenantB,
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}}); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing pool, got %d", rec.Code)
	}
}

func TestAllocationsHandler_ErrorAndHappyPaths(t *testing.T) {
	t.Parallel()
	h := newHarness()
	createPool(t, h)
	allocPath := "/api/v1/admin/tenants/" + tenantA + "/mana-allocations"
	if rec, _ := doReq(t, h, http.MethodPatch, allocPath, tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"gcid": "g-1", "units": 0}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for units<=0, got %d", rec.Code)
	}
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"units": 10}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for missing gcid, got %d", rec.Code)
	}
	if code := doRaw(t, h, http.MethodPost, allocPath, "{bad"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
	// Insufficient balance → 402.
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"gcid": "g-1", "units": 10}); rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402 for insufficient balance, got %d", rec.Code)
	}
	// Fund the pool, then invalid expires_at.
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 10000, "units": 1000, "currency": "SGD"}); rec.Code != http.StatusOK {
		t.Fatalf("topup setup: %d", rec.Code)
	}
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"gcid": "g-1", "units": 10, "expires_at": "not-a-time"}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for bad expires_at, got %d", rec.Code)
	}
	// Valid allocation.
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"gcid": "g-1", "units": 10}); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	// GET list with filters.
	if rec, _ := doReq(t, h, http.MethodGet, allocPath+"?status=pending&gcid=g-1&limit=5", tenantA, nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", rec.Code)
	}
}

func TestRevokeAllocationHandler(t *testing.T) {
	t.Parallel()
	h := newHarness()
	createPool(t, h)
	if rec, _ := doReq(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup", tenantA,
		map[string]interface{}{"amount_cents": 10000, "units": 1000, "currency": "SGD"}); rec.Code != http.StatusOK {
		t.Fatalf("topup setup: %d", rec.Code)
	}
	allocPath := "/api/v1/admin/tenants/" + tenantA + "/mana-allocations"
	if rec, _ := doReq(t, h, http.MethodPost, allocPath, tenantA,
		map[string]interface{}{"gcid": "g-1", "units": 10}); rec.Code != http.StatusCreated {
		t.Fatalf("alloc setup: %d", rec.Code)
	}
	// Wrong method.
	if rec, _ := doReq(t, h, http.MethodGet, allocPath+"/alloc-1", tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	// Missing allocation.
	if rec, _ := doReq(t, h, http.MethodDelete, allocPath+"/alloc-missing", tenantA,
		map[string]interface{}{"reason": "no longer needed"}); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if code := doRaw(t, h, http.MethodDelete, allocPath+"/alloc-missing", "{bad"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
	// Valid revoke.
	items, _, err := h.allocs.ListByTenant(t.Context(), tenantA, manapool.ListAllocationsFilter{Limit: 50})
	if err != nil || len(items) != 1 {
		t.Fatalf("setup list: %v len=%d", err, len(items))
	}
	if rec, _ := doReq(t, h, http.MethodDelete, allocPath+"/"+items[0].AllocationID, tenantA,
		map[string]interface{}{"reason": "no longer needed"}); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on revoke, got %d (id=%s)", rec.Code, items[0].AllocationID)
	}
}

func TestPoolDetailHandler_Errors(t *testing.T) {
	t.Parallel()
	h := newHarness()
	// Missing pool → 404.
	if rec, _ := doReq(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantB+"/mana-pool", tenantB, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing pool, got %d", rec.Code)
	}
	// Wrong method on existing pool.
	createPool(t, h)
	if rec, _ := doReq(t, h, http.MethodDelete, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", tenantA, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestRecorderPublisher_ResetAndSyntheticTraceparent(t *testing.T) {
	t.Parallel()
	pub := manapool.NewRecorderPublisher()
	pub.PublishPoolCreated(manapool.EnvelopeHeader{TenantID: "t-1", GCID: "g-1"}, pool.PoolCreated{PoolID: "p-1"})
	if len(pub.Recorded()) != 1 {
		t.Fatalf("expected 1 recorded event")
	}
	pub.Reset()
	if len(pub.Recorded()) != 0 {
		t.Fatalf("expected empty after Reset")
	}
	// No inbound traceparent → synthetic W3C traceparent minted.
	pub.PublishPoolCreated(manapool.EnvelopeHeader{TenantID: "t-2", GCID: "g-2"}, pool.PoolCreated{PoolID: "p-2"})
	ev := pub.Recorded()
	if len(ev) != 1 || len(ev[0].Envelope.Traceparent) < 2 {
		t.Fatalf("expected synthetic traceparent, got %+v", ev)
	}
}
