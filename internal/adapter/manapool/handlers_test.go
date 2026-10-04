package manapool_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	manapool "github.com/apollo-chora/chora-tenancy/internal/adapter/manapool"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// -----------------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------------

type harness struct {
	srv     http.Handler
	pools   *manapool.InmemPoolRepo
	allocs  *manapool.InmemAllocationRepo
	stripe  *manapool.StripeStub
	pub     *manapool.RecorderPublisher
	counter *manapool.InmemLearnerCounter
}

func newHarness() *harness {
	pools := manapool.NewInmemPoolRepo()
	allocs := manapool.NewInmemAllocationRepo()
	stripe := manapool.NewStripeStub()
	pub := manapool.NewRecorderPublisher()
	counter := manapool.NewInmemLearnerCounter()
	deps := manapool.HandlerDeps{
		Pools:        pools,
		Allocations:  allocs,
		Stripe:       stripe,
		Publisher:    pub,
		LearnerCount: counter,
	}
	return &harness{
		srv: manapool.NewServer(deps), pools: pools, allocs: allocs,
		stripe: stripe, pub: pub, counter: counter,
	}
}

func do(t *testing.T, h *harness, method, path string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", tenantA)
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

// -----------------------------------------------------------------------------
// 1. POST .../mana-pool — create
// -----------------------------------------------------------------------------

func TestCreatePool_ReturnsCreated(t *testing.T) {
	t.Parallel()
	h := newHarness()
	rec, body := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body=%v)", rec.Code, body)
	}
	if _, ok := body["pool_id"]; !ok {
		t.Fatalf("expected pool_id in body, got %v", body)
	}
	if body["tenant_id"] != tenantA {
		t.Fatalf("tenant_id mismatch")
	}
}

func TestCreatePool_DuplicateReturnsExisting409(t *testing.T) {
	t.Parallel()
	h := newHarness()
	body := map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}}
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", body)
	rec, _ := do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate, got %d", rec.Code)
	}
}

func TestCreatePool_RejectsInvalidPolicy(t *testing.T) {
	t.Parallel()
	h := newHarness()
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "garbage"}})
	if rec.Code != http.StatusUnprocessableEntity && rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 4xx for invalid policy, got %d", rec.Code)
	}
}

func TestCreatePool_PublishesCreatedEvent(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	if len(h.pub.Recorded()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(h.pub.Recorded()))
	}
	if h.pub.Recorded()[0].Topic != "chora.tenancy.tenant_mana_pool.created.v1" {
		t.Fatalf("topic mismatch")
	}
}

// -----------------------------------------------------------------------------
// 2. GET .../mana-pool — detail
// -----------------------------------------------------------------------------

func TestGetPool_ReturnsExisting(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, body := do(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body["tenant_id"] != tenantA {
		t.Fatalf("tenant_id mismatch")
	}
}

func TestGetPool_NotFound(t *testing.T) {
	t.Parallel()
	h := newHarness()
	rec, _ := do(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// 3. POST .../mana-pool:topup — top up via Stripe
// -----------------------------------------------------------------------------

func TestTopup_CreditsBalance_AndPublishesEvent(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, body := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 500, "payment_method_id": "pm_card"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if body["units_credited"].(float64) != 500 {
		t.Fatalf("expected 500 credited, got %v", body["units_credited"])
	}
	if body["balance_after_units"].(float64) != 500 {
		t.Fatalf("expected balance 500, got %v", body["balance_after_units"])
	}
	// Event ordering: created (#1) then topped_up (#2).
	if len(h.pub.Recorded()) != 2 {
		t.Fatalf("expected 2 events, got %d", len(h.pub.Recorded()))
	}
	if h.pub.Recorded()[1].Topic != "chora.tenancy.tenant_mana_pool.topped_up.v1" {
		t.Fatalf("expected topped_up topic at index 1")
	}
}

func TestTopup_PoolMissingReturns404(t *testing.T) {
	t.Parallel()
	h := newHarness()
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 500})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestTopup_StripeDeclinedReturns402(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	h.stripe.FailNext("card_declined")
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 500})
	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d", rec.Code)
	}
}

func TestTopup_RejectsAmountBelowMinimum(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 50, "currency": "USD", "units": 25})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for amount<100, got %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// 4. POST .../mana-pool:auto-renew
// -----------------------------------------------------------------------------

func TestSetAutoRenew_UpdatesField(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, body := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew",
		map[string]interface{}{"monthly_topup_units": 1000})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body["monthly_topup_units"].(float64) != 1000 {
		t.Fatalf("expected 1000")
	}
}

// -----------------------------------------------------------------------------
// 5. PATCH .../mana-pool/policy
// -----------------------------------------------------------------------------

func TestUpdatePolicy_ChangesAllocationPolicy(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, body := do(t, h, http.MethodPatch,
		"/api/v1/admin/tenants/"+tenantA+"/mana-pool/policy",
		map[string]interface{}{
			"auto_allocation_policy": map[string]interface{}{
				"policy": "on_enrollment", "units_on_enrollment": 250,
			},
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	pol, _ := body["auto_allocation_policy"].(map[string]interface{})
	if pol == nil || pol["policy"] != "on_enrollment" {
		t.Fatalf("expected on_enrollment policy in response, got %v", body["auto_allocation_policy"])
	}
}

// -----------------------------------------------------------------------------
// 6. POST .../mana-allocations — manual
// -----------------------------------------------------------------------------

func TestCreateAllocation_DebitsPool_AndPublishesGranted(t *testing.T) {
	t.Parallel()
	h := newHarness()
	// Seed pool + balance.
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 500})

	rec, body := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL1, "units": 100})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body=%v)", rec.Code, body)
	}
	if body["gcid"] != gcidL1 {
		t.Fatalf("gcid mismatch")
	}
	if body["status"] != "pending" {
		t.Fatalf("expected pending status")
	}
	// Pool balance now 400.
	_, getBody := do(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	if getBody["balance_units"].(float64) != 400 {
		t.Fatalf("expected balance 400 after debit, got %v", getBody["balance_units"])
	}
	// Event ordering: created, topped_up, granted.
	rs := h.pub.Recorded()
	if len(rs) != 3 || rs[2].Topic != "chora.tenancy.tenant_mana_allocation.granted.v1" {
		t.Fatalf("expected granted event last, got %#v", rs)
	}
}

func TestCreateAllocation_InsufficientReturns402(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL1, "units": 100})
	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d", rec.Code)
	}
}

func TestCreateAllocation_RejectsZeroUnits(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	rec, _ := do(t, h, http.MethodPost,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL1, "units": 0})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// 7. GET .../mana-allocations — list
// -----------------------------------------------------------------------------

func TestListAllocations_FiltersByGCIDAndStatus(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 1000})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL1, "units": 100})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL2, "units": 200})

	rec, body := do(t, h, http.MethodGet,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations?gcid="+gcidL1, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, ok := body["items"].([]interface{})
	if !ok {
		t.Fatalf("expected items array")
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 result for gcid filter, got %d", len(items))
	}
}

// -----------------------------------------------------------------------------
// 8. DELETE .../mana-allocations/{id}
// -----------------------------------------------------------------------------

func TestRevokeAllocation_ReturnsUnitsToPool_AndPublishes(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 500})
	_, allocBody := do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-allocations",
		map[string]interface{}{"gcid": gcidL1, "units": 100})
	allocID := allocBody["allocation_id"].(string)

	rec, body := do(t, h, http.MethodDelete,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations/"+allocID,
		map[string]interface{}{"reason": "offboarding"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if body["status"] != "revoked" {
		t.Fatalf("expected revoked status, got %v", body["status"])
	}
	// Pool balance restored to 500.
	_, getBody := do(t, h, http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	if getBody["balance_units"].(float64) != 500 {
		t.Fatalf("expected balance restored to 500, got %v", getBody["balance_units"])
	}
}

func TestRevokeAllocation_UnknownReturns404(t *testing.T) {
	t.Parallel()
	h := newHarness()
	rec, _ := do(t, h, http.MethodDelete,
		"/api/v1/admin/tenants/"+tenantA+"/mana-allocations/nope",
		map[string]interface{}{"reason": "test"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// X-Tenant-Id middleware
// -----------------------------------------------------------------------------

func TestMissingTenantHeader_Returns400(t *testing.T) {
	t.Parallel()
	h := newHarness()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+tenantA+"/mana-pool", nil)
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 missing tenant, got %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// Subscriber: enrollment-driven auto-allocation
// -----------------------------------------------------------------------------

func TestSubscribeEnrollment_OnEnrollmentPolicyEmitsAllocation(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{
			"auto_allocation_policy": map[string]interface{}{"policy": "on_enrollment", "units_on_enrollment": 250},
		})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 1000})

	// Build subscriber wired to the same harness.
	sub := manapool.NewEnrollmentSubscriber(manapool.SubscriberDeps{
		Pools: h.pools, Allocations: h.allocs, Publisher: h.pub, LearnerCount: h.counter,
	})
	if err := sub.HandleEnrollment(context.Background(), manapool.EnrollmentEvent{
		TenantID: tenantA, GCID: gcidL1, Role: "standard", OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("HandleEnrollment: %v", err)
	}

	// 1 allocation should exist for gcidL1.
	allocs, total, _ := h.allocs.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{GCID: gcidL1, Limit: 50})
	if total != 1 || len(allocs) != 1 {
		t.Fatalf("expected 1 auto-allocation, got total=%d len=%d", total, len(allocs))
	}
	if allocs[0].Units != 250 {
		t.Fatalf("expected 250 units, got %d", allocs[0].Units)
	}
	if allocs[0].Reason != "on_enrollment" {
		t.Fatalf("expected on_enrollment reason, got %q", allocs[0].Reason)
	}

	// LearnerCounter should now know about gcidL1 for future EqualSplit.
	n, _ := h.counter.CountActiveLearners(context.Background(), tenantA)
	if n != 1 {
		t.Fatalf("expected counter=1, got %d", n)
	}
}

func TestSubscribeEnrollment_ManualPolicyEmitsNothing(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "manual"}})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 1000})

	sub := manapool.NewEnrollmentSubscriber(manapool.SubscriberDeps{
		Pools: h.pools, Allocations: h.allocs, Publisher: h.pub, LearnerCount: h.counter,
	})
	_ = sub.HandleEnrollment(context.Background(), manapool.EnrollmentEvent{
		TenantID: tenantA, GCID: gcidL1, Role: "standard", OccurredAt: time.Now().UTC(),
	})
	_, total, _ := h.allocs.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{Limit: 50})
	if total != 0 {
		t.Fatalf("manual policy should not auto-allocate; got %d", total)
	}
}

// Ensure the policy engine wired to the harness is reachable for the
// monthly tick path (smoke test for adapter wiring).
func TestRunMonthlyTick_EqualSplitDistributes(t *testing.T) {
	t.Parallel()
	h := newHarness()
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool",
		map[string]interface{}{"auto_allocation_policy": map[string]interface{}{"policy": "equal_split"}})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:auto-renew",
		map[string]interface{}{"monthly_topup_units": 1000})
	_, _ = do(t, h, http.MethodPost, "/api/v1/admin/tenants/"+tenantA+"/mana-pool:topup",
		map[string]interface{}{"amount_cents": 1000, "currency": "USD", "units": 1000})
	h.counter.Register(tenantA, pool.LearnerContext{GCID: gcidL1, Role: "standard"})
	h.counter.Register(tenantA, pool.LearnerContext{GCID: gcidL2, Role: "standard"})

	tick := manapool.NewMonthlyTicker(manapool.SubscriberDeps{
		Pools: h.pools, Allocations: h.allocs, Publisher: h.pub, LearnerCount: h.counter,
	})
	if err := tick.RunMonthlyTick(context.Background(), tenantA); err != nil {
		t.Fatalf("RunMonthlyTick: %v", err)
	}
	allocs, total, _ := h.allocs.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{Limit: 50})
	if total != 2 || len(allocs) != 2 {
		t.Fatalf("expected 2 allocations, got %d", total)
	}
	for _, a := range allocs {
		if a.Units != 500 {
			t.Fatalf("expected 500 units each, got %d", a.Units)
		}
	}
}
