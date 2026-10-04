// Package httpapi_test — L1 Tenant lane (CHO-1705 WP-4) tests for the
// admin mana-pool surface backed by the REAL TenantManaPool aggregate.
//
// Contract: chora-contracts/openapi/tenancy-admin.yaml
//
//	GET  /api/v1/admin/tenants/{id}/mana-pool             → 200 pool | 404
//	POST /api/v1/admin/tenants/{id}/mana-pool             → 201 pool | 409 existing pool
//	POST /api/v1/admin/tenants/{id}/mana-pool:auto-renew  → 200 pool | 404 | 422
//	GET  /api/v1/admin/tenants/{id}/mana-pool/summary     → 200 dashboard cards
//	POST /api/v1/admin/tenants/{id}/mana-pool/allocations → 202 | 404 | 422
//
// The store behind V2Deps.PoolRepo is the SAME instance the ADR-164
// payments subscriber credits via manapool.PaymentsPoolApplier — the
// unification suite below pins that seam (checkout-paid top-ups must be
// visible in the H+ read).
package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	pooldom "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

// manaDo issues a request with explicit header control (adminDo always
// stamps X-GCID; the gcid-fallback tests below need to vary headers).
func manaDo(t *testing.T, srv http.Handler, method, path string, headers map[string]string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", adminTenantID)
	// CHO-2072: mana-pool routes are H+ admin gates (fail-closed mesh header).
	// Default an admin role so these wiring tests exercise the handler body,
	// not the 403; a test may override via headers.
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

func manaPoolPath(suffix string) string {
	return "/api/v1/admin/tenants/" + adminTenantID + "/mana-pool" + suffix
}

func errCode(body map[string]interface{}) string {
	e, _ := body["error"].(map[string]interface{})
	c, _ := e["code"].(string)
	return c
}

// createPool seeds a pool for adminTenantID via the public POST endpoint.
func createPool(t *testing.T, srv http.Handler) map[string]interface{} {
	t.Helper()
	rec, body := adminDo(t, srv, "POST", manaPoolPath(""), "tenant_admin",
		map[string]interface{}{})
	if rec.Code != http.StatusCreated {
		t.Fatalf("seed create pool: expected 201, got %d (body=%v)", rec.Code, body)
	}
	return body
}

// ---------------------------------------------------------------------------
// GET — contract: 404 before create, 200 real aggregate after
// ---------------------------------------------------------------------------

func TestManaPool_GET_NotFoundBeforeCreate(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 before create (no synthetic pool), got %d (body=%v)", rec.Code, body)
	}
	if errCode(body) != "pool_not_found" {
		t.Fatalf("expected error.code=pool_not_found, got %q (body=%v)", errCode(body), body)
	}
}

func TestManaPool_Create_Then_GET_ReturnsRealAggregate(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	created := createPool(t, srv)
	if got, _ := created["tenant_id"].(string); got != adminTenantID {
		t.Fatalf("create: tenant_id=%q, want %s", got, adminTenantID)
	}
	if id, _ := created["pool_id"].(string); id == "" {
		t.Fatalf("create: pool_id missing (body=%v)", created)
	}
	if bal, _ := created["balance_units"].(float64); bal != 0 {
		t.Fatalf("create: balance_units=%v, want 0 (no initial credit)", created["balance_units"])
	}
	rec, body := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET after create: expected 200, got %d (body=%v)", rec.Code, body)
	}
	if body["pool_id"] != created["pool_id"] {
		t.Fatalf("GET pool_id=%v, want created pool_id=%v", body["pool_id"], created["pool_id"])
	}
	for _, k := range []string{
		"pool_id", "tenant_id", "balance_units", "monthly_topup_units",
		"lifetime_topped_up_units", "lifetime_allocated_units", "version",
		"created_at", "low_balance_threshold", "is_low_balance",
	} {
		if _, ok := body[k]; !ok {
			t.Fatalf("pool DTO missing key %q (got keys=%v)", k, mapKeys(body))
		}
	}
}

// ---------------------------------------------------------------------------
// POST create — idempotent on tenant: second call 409 + existing pool body
// ---------------------------------------------------------------------------

func TestManaPool_Create_SecondCallConflictsWithExistingPool(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	created := createPool(t, srv)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(""), "tenant_admin",
		map[string]interface{}{})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on second create, got %d (body=%v)", rec.Code, body)
	}
	if body["pool_id"] != created["pool_id"] {
		t.Fatalf("409 body should carry the EXISTING pool (pool_id=%v, want %v)",
			body["pool_id"], created["pool_id"])
	}
}

func TestManaPool_Create_RequiresCallerGCID(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := manaDo(t, srv, "POST", manaPoolPath(""), nil,
		map[string]interface{}{})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 without caller gcid, got %d (body=%v)", rec.Code, body)
	}
}

// The gateway me-style proxy stamps the LOWERCASE `gcid` header (phyllis
// call()), not X-GCID — the handler must accept either.
func TestManaPool_Create_AcceptsLowercaseGcidHeader(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := manaDo(t, srv, "POST", manaPoolPath(""),
		map[string]string{"gcid": "gcid-gateway-caller"},
		map[string]interface{}{})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 with lowercase gcid header, got %d (body=%v)", rec.Code, body)
	}
}

// ---------------------------------------------------------------------------
// POST :auto-renew — SetMonthlyTopupUnits (the clean "set quota" op)
// ---------------------------------------------------------------------------

func TestManaPool_AutoRenew_SetsMonthlyTopupUnits(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	createPool(t, srv)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{"monthly_topup_units": 750})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["monthly_topup_units"].(float64); got != 750 {
		t.Fatalf("monthly_topup_units=%v, want 750", body["monthly_topup_units"])
	}
	// 0 disables auto-renew (contract: minimum 0).
	rec, body = adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{"monthly_topup_units": 0})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 setting 0 (disable), got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["monthly_topup_units"].(float64); got != 0 {
		t.Fatalf("monthly_topup_units=%v, want 0 after disable", body["monthly_topup_units"])
	}
}

func TestManaPool_AutoRenew_404WithoutPool(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{"monthly_topup_units": 100})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 without pool, got %d (body=%v)", rec.Code, body)
	}
	if errCode(body) != "pool_not_found" {
		t.Fatalf("expected error.code=pool_not_found, got %q", errCode(body))
	}
}

func TestManaPool_AutoRenew_RejectsNegativeUnits(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	createPool(t, srv)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{"monthly_topup_units": -5})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on negative units, got %d (body=%v)", rec.Code, body)
	}
}

func TestManaPool_AutoRenew_RejectsMissingField(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	createPool(t, srv)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 when monthly_topup_units absent, got %d (body=%v)", rec.Code, body)
	}
}

func TestManaPool_AutoRenew_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	createPool(t, srv)
	rec, _ := adminDo(t, srv, "GET", manaPoolPath(":auto-renew"), "tenant_admin", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET :auto-renew, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// UNIFICATION — the H+ read reflects what the ADR-164 payments subscriber
// credits (same PoolStore instance behind both).
// ---------------------------------------------------------------------------

func TestManaPool_GET_ReflectsPaymentsSubscriberCredits(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	createPool(t, srv)

	applier, err := manapool.NewPaymentsPoolApplier(deps.PoolRepo)
	if err != nil {
		t.Fatalf("applier over deps.PoolRepo: %v", err)
	}
	if err := applier.Credit(context.Background(), adminTenantID, 500, "tenant_mana_topup.payment_captured"); err != nil {
		t.Fatalf("applier credit: %v", err)
	}

	rec, body := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got, _ := body["balance_units"].(float64); got != 500 {
		t.Fatalf("balance_units=%v, want 500 (payments credit must be visible in H+ read)",
			body["balance_units"])
	}
}

// ---------------------------------------------------------------------------
// Summary — reads the real pool when present; zeros (not fabricated
// balances) when absent. Keeps the S6.2 dashboard card keys stable.
// ---------------------------------------------------------------------------

func TestManaPool_Summary_ZerosBeforeCreate(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "GET", manaPoolPath("/summary"), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["balance_units"].(float64); got != 0 {
		t.Fatalf("summary before create: balance_units=%v, want 0 (no fabricated 1000)", body["balance_units"])
	}
	if got, _ := body["pool_exists"].(bool); got {
		t.Fatalf("summary before create: pool_exists=true, want false")
	}
	for _, k := range []string{
		"tenant_id", "balance_units", "monthly_topup_units",
		"allocations_granted_count", "allocations_claimed_count",
		"low_balance_threshold", "is_low_balance", "pool_exists",
	} {
		if _, ok := body[k]; !ok {
			t.Fatalf("summary missing key %q (got keys=%v)", k, mapKeys(body))
		}
	}
}

func TestManaPool_Summary_ReflectsRealPool(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	createPool(t, srv)
	applier, err := manapool.NewPaymentsPoolApplier(deps.PoolRepo)
	if err != nil {
		t.Fatalf("applier: %v", err)
	}
	if err := applier.Credit(context.Background(), adminTenantID, 300, "seed"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	rec, body := adminDo(t, srv, "GET", manaPoolPath("/summary"), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got, _ := body["balance_units"].(float64); got != 300 {
		t.Fatalf("summary balance_units=%v, want 300", body["balance_units"])
	}
	if got, _ := body["pool_exists"].(bool); !got {
		t.Fatalf("summary pool_exists=false after create")
	}
}

// ---------------------------------------------------------------------------
// Allocations — now debit the REAL pool: 404 without pool, 422 on
// overdraw, balance visibly drops on grant.
// ---------------------------------------------------------------------------

func seedFundedPool(t *testing.T, srv http.Handler, deps httpapi.V2Deps, units int64) {
	t.Helper()
	createPool(t, srv)
	applier, err := manapool.NewPaymentsPoolApplier(deps.PoolRepo)
	if err != nil {
		t.Fatalf("applier: %v", err)
	}
	if err := applier.Credit(context.Background(), adminTenantID, units, "seed"); err != nil {
		t.Fatalf("seed credit: %v", err)
	}
}

func TestManaPool_AdjustAllocations_EmitsAdjustedEventAndDebitsPool(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	rec, body := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000001",
			"target_gcid":       "gcid-target-1",
			"units":             50,
			"monthly_cap_units": 200,
			"reason":            "manual_topup",
		})
	if rec.Code != http.StatusAccepted && rec.Code != http.StatusCreated {
		t.Fatalf("expected 201/202, got %d (body=%v)", rec.Code, body)
	}
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.mana_pool.adjusted.v1")
	if len(got) != 1 {
		t.Fatalf("expected mana_pool.adjusted.v1 event, got %d", len(got))
	}
	dim, _ := got[0].Payload["chora_imda_dimension"].(string)
	if dim != "accountability" {
		t.Fatalf("expected chora_imda_dimension=accountability, got %q", dim)
	}
	_, poolBody := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if bal, _ := poolBody["balance_units"].(float64); bal != 450 {
		t.Fatalf("pool balance after 50-unit grant = %v, want 450 (real debit)", poolBody["balance_units"])
	}
}

func TestManaPool_AdjustAllocations_IsIdempotentByAllocationID(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	body := map[string]interface{}{
		"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000002",
		"target_gcid":       "gcid-target-1",
		"units":             50,
		"monthly_cap_units": 200,
	}
	adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin", body)
	adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin", body)
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.mana_pool.adjusted.v1")
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 adjusted event for idempotent retry, got %d", len(got))
	}
	_, poolBody := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if bal, _ := poolBody["balance_units"].(float64); bal != 450 {
		t.Fatalf("idempotent retry must debit ONCE: balance=%v, want 450", poolBody["balance_units"])
	}
}

func TestManaPool_AdjustAllocations_404WithoutPool(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000005",
			"target_gcid":       "gcid-target-9",
			"units":             10,
			"monthly_cap_units": 100,
		})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 allocating from missing pool, got %d (body=%v)", rec.Code, body)
	}
}

func TestManaPool_AdjustAllocations_RejectsOverdraw(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 30)
	rec, body := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000006",
			"target_gcid":       "gcid-target-3",
			"units":             50,
			"monthly_cap_units": 100,
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on overdraw (balance 30 < grant 50), got %d (body=%v)", rec.Code, body)
	}
	if errCode(body) != "insufficient_balance" {
		t.Fatalf("expected error.code=insufficient_balance, got %q", errCode(body))
	}
}

func TestManaPool_AdjustAllocations_RequiresValidUnits(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	rec, _ := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000003",
			"target_gcid":       "gcid-target-2",
			"units":             0,
			"monthly_cap_units": 100,
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on units<=0, got %d", rec.Code)
	}
}

func TestManaPool_AdjustAllocations_ForbiddenForLearner(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	rec, _ := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "learner",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000004",
			"target_gcid":       "gcid-target",
			"units":             10,
			"monthly_cap_units": 100,
		})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Role gates + malformed bodies + repo failure paths
// ---------------------------------------------------------------------------

func TestManaPool_WriteOps_ForbiddenForLearner(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "POST", manaPoolPath(""), "learner",
		map[string]interface{}{})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("create as learner: expected 403, got %d", rec.Code)
	}
	rec, _ = adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "learner",
		map[string]interface{}{"monthly_topup_units": 10})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("auto-renew as learner: expected 403, got %d", rec.Code)
	}
}

func TestManaPool_MalformedJSONBodies(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	createPool(t, srv)
	for _, path := range []string{manaPoolPath(":auto-renew"), manaPoolPath("/allocations")} {
		req := httptest.NewRequest("POST", path, strings.NewReader("{not json"))
		req.Header.Set("X-Tenant-Id", adminTenantID)
		req.Header.Set("X-GCID", adminGCID)
		req.Header.Set("X-Role", "tenant_admin")
		req.Header.Set("x-mesh-user-roles", "tenant_admin") // CHO-2072 fail-closed gate
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s with malformed JSON: expected 400, got %d", path, rec.Code)
		}
	}
}

// DTO carries last_topped_up_at once a top-up has landed.
func TestManaPool_DTO_LastToppedUpAtAfterTopUp(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	createPool(t, srv)
	p, err := deps.PoolRepo.GetByTenant(context.Background(), adminTenantID)
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if err := p.TopUp(500, 1000, "SGD", "pi_test_1", false, adminGCID); err != nil {
		t.Fatalf("topup: %v", err)
	}
	if err := deps.PoolRepo.Save(context.Background(), p); err != nil {
		t.Fatalf("save: %v", err)
	}
	rec, body := adminDo(t, srv, "GET", manaPoolPath(""), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if s, _ := body["last_topped_up_at"].(string); s == "" {
		t.Fatalf("last_topped_up_at should be set after top-up (body=%v)", body)
	}
	if got, _ := body["lifetime_topped_up_units"].(float64); got != 500 {
		t.Fatalf("lifetime_topped_up_units=%v, want 500", body["lifetime_topped_up_units"])
	}
}

// failingPoolStore drives the 500 branches.
type failingPoolStore struct{ err error }

func (f failingPoolStore) GetByTenant(context.Context, string) (*pooldom.TenantManaPool, error) {
	return nil, f.err
}
func (f failingPoolStore) Save(context.Context, *pooldom.TenantManaPool) error { return f.err }

func TestManaPool_RepoFailuresSurfaceAs500(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	deps.PoolRepo = failingPoolStore{err: errors.New("store down")}
	srv := httpapi.NewV2Server(deps)
	cases := []struct {
		method, path string
		body         interface{}
	}{
		{"GET", manaPoolPath(""), nil},
		{"POST", manaPoolPath(""), map[string]interface{}{}},
		{"POST", manaPoolPath(":auto-renew"), map[string]interface{}{"monthly_topup_units": 5}},
		{"GET", manaPoolPath("/summary"), nil},
		{"POST", manaPoolPath("/allocations"), map[string]interface{}{
			"allocation_id": "01970000-aaaa-bbbb-cccc-00000000000e",
			"target_gcid":   "g", "units": 5, "monthly_cap_units": 10,
		}},
	}
	for _, c := range cases {
		rec, _ := adminDo(t, srv, c.method, c.path, "tenant_admin", c.body)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s %s with failing store: expected 500, got %d", c.method, c.path, rec.Code)
		}
	}
}

// racingPoolStore simulates a concurrent create: lookup misses, Save hits
// the unique constraint, the retry lookup wins.
type racingPoolStore struct {
	mu    sync.Mutex
	calls int
	won   *pooldom.TenantManaPool
}

func (rps *racingPoolStore) GetByTenant(_ context.Context, tenantID string) (*pooldom.TenantManaPool, error) {
	rps.mu.Lock()
	defer rps.mu.Unlock()
	rps.calls++
	if rps.calls == 1 {
		return nil, pooldom.ErrPoolNotFound
	}
	return rps.won, nil
}

func (rps *racingPoolStore) Save(context.Context, *pooldom.TenantManaPool) error {
	return pooldom.ErrPoolAlreadyExists
}

func TestManaPool_Create_RaceSurfacesWinnerAs409(t *testing.T) {
	t.Parallel()
	won, err := pooldom.NewPool(adminTenantID, "gcid-winner", pooldom.ManualPolicy{})
	if err != nil {
		t.Fatalf("seed winner: %v", err)
	}
	deps := httpapi.NewDefaultV2Deps()
	deps.PoolRepo = &racingPoolStore{won: won}
	srv := httpapi.NewV2Server(deps)
	rec, body := adminDo(t, srv, "POST", manaPoolPath(""), "tenant_admin",
		map[string]interface{}{})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on create race, got %d (body=%v)", rec.Code, body)
	}
	if body["pool_id"] != won.PoolID {
		t.Fatalf("race 409 should carry the winner pool (got %v, want %s)", body["pool_id"], won.PoolID)
	}
}

func TestManaPool_AdjustAllocations_FieldValidation(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	cases := []map[string]interface{}{
		{"allocation_id": "", "target_gcid": "g", "units": 5, "monthly_cap_units": 10},
		{"allocation_id": "01970000-aaaa-bbbb-cccc-00000000000f", "target_gcid": "", "units": 5, "monthly_cap_units": 10},
		{"allocation_id": "01970000-aaaa-bbbb-cccc-000000000010", "target_gcid": "g", "units": 50, "monthly_cap_units": 10},
	}
	for i, c := range cases {
		rec, _ := adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin", c)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("case %d: expected 422, got %d", i, rec.Code)
		}
	}
}

func TestManaPool_Summary_CountsGrantedAllocations(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedFundedPool(t, srv, deps, 500)
	adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000011",
			"target_gcid":       "gcid-target-7",
			"units":             25,
			"monthly_cap_units": 100,
		})
	rec, body := adminDo(t, srv, "GET", manaPoolPath("/summary"), "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got, _ := body["allocations_granted_count"].(float64); got != 1 {
		t.Fatalf("allocations_granted_count=%v, want 1", body["allocations_granted_count"])
	}
}

// saveFailPoolStore reads fine but fails persistence — drives the
// 500-after-debit branches.
type saveFailPoolStore struct {
	inner httpapi.V2Deps
	repo  interface {
		GetByTenant(context.Context, string) (*pooldom.TenantManaPool, error)
	}
}

func (s saveFailPoolStore) GetByTenant(ctx context.Context, tenantID string) (*pooldom.TenantManaPool, error) {
	return s.repo.GetByTenant(ctx, tenantID)
}
func (s saveFailPoolStore) Save(context.Context, *pooldom.TenantManaPool) error {
	return errors.New("persistence down")
}

func TestManaPool_SaveFailuresSurfaceAs500(t *testing.T) {
	t.Parallel()
	seeded := manapool.NewInmemPoolRepo()
	won, err := pooldom.NewPool(adminTenantID, "gcid-seed", pooldom.ManualPolicy{})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := won.Credit(100); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := seeded.Save(context.Background(), won); err != nil {
		t.Fatalf("save seed: %v", err)
	}
	deps := httpapi.NewDefaultV2Deps()
	deps.PoolRepo = saveFailPoolStore{repo: seeded}
	// The grant path no longer persists through PoolRepo.Save; it goes
	// through the atomic GrantStore. Fail that instead, so this test keeps
	// asserting what it was written to assert: a persistence failure on the
	// allocation path surfaces as 500, not a silent 202.
	deps.GrantStore = &fakeGrantStore{
		deps:     &deps,
		seen:     map[string]bool{},
		failWith: errors.New("grant persistence exploded"),
	}
	srv := httpapi.NewV2Server(deps)

	rec, _ := adminDo(t, srv, "POST", manaPoolPath(":auto-renew"), "tenant_admin",
		map[string]interface{}{"monthly_topup_units": 9})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("auto-renew save fail: expected 500, got %d", rec.Code)
	}
	rec, _ = adminDo(t, srv, "POST", manaPoolPath("/allocations"), "tenant_admin",
		map[string]interface{}{
			"allocation_id":     "01970000-aaaa-bbbb-cccc-000000000012",
			"target_gcid":       "g",
			"units":             5,
			"monthly_cap_units": 10,
		})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("allocation save fail: expected 500, got %d", rec.Code)
	}
}

// doubleRacePoolStore loses the create race AND the retry lookup — the
// handler must fall through to 500, not loop.
type doubleRacePoolStore struct{}

func (doubleRacePoolStore) GetByTenant(context.Context, string) (*pooldom.TenantManaPool, error) {
	return nil, pooldom.ErrPoolNotFound
}
func (doubleRacePoolStore) Save(context.Context, *pooldom.TenantManaPool) error {
	return pooldom.ErrPoolAlreadyExists
}

func TestManaPool_Create_RaceRetryLookupFailureIs500(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	deps.PoolRepo = doubleRacePoolStore{}
	srv := httpapi.NewV2Server(deps)
	rec, _ := adminDo(t, srv, "POST", manaPoolPath(""), "tenant_admin",
		map[string]interface{}{})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when race retry lookup also misses, got %d", rec.Code)
	}
}

// helper to surface map keys when test assertions fail
func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Sanity: NewDefaultV2Deps wires the real PoolStore (the unification seam
// main.go overrides with the payments-subscriber-shared instance).
func TestNewDefaultV2Deps_HasManaPoolWiring(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	if deps.Subscriptions == nil {
		t.Fatalf("subscriptions wiring missing")
	}
	if deps.Events == nil {
		t.Fatalf("events wiring missing")
	}
	if deps.PoolRepo == nil {
		t.Fatalf("PoolRepo wiring missing (real TenantManaPool store)")
	}
}
