// Package pool_test — RED-phase tests for the PolicyEngine.
//
// PolicyEngine drives auto-allocation events:
//   - OnLearnerEnrolled — invoked from chora.identity.user_membership.created.v1
//   - OnMonthlyTick     — invoked from monthly cron
//   - RecordClaim       — invoked from chora.identity.user_mana.debited.v1
//
// The engine returns an "intent" describing what allocations to issue + what
// debits to apply to the pool. The HTTP/event adapters wrap this into the
// actual aggregate updates and Pub/Sub publishes.
package pool_test

import (
	"context"
	"testing"

	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// fakePoolStore implements pool.PoolStore for engine unit tests.
type fakePoolStore struct {
	pools map[string]*pool.TenantManaPool
}

func newFakePoolStore() *fakePoolStore {
	return &fakePoolStore{pools: make(map[string]*pool.TenantManaPool)}
}

func (s *fakePoolStore) GetByTenant(_ context.Context, tenantID string) (*pool.TenantManaPool, error) {
	p, ok := s.pools[tenantID]
	if !ok {
		return nil, pool.ErrPoolNotFound
	}
	return p, nil
}

func (s *fakePoolStore) Save(_ context.Context, p *pool.TenantManaPool) error {
	s.pools[p.TenantID] = p
	return nil
}

// fakeLearnerCounter satisfies the LearnerCounter port for engine tests.
type fakeLearnerCounter struct {
	active map[string]int // tenantID -> active count
	list   map[string][]pool.LearnerContext
}

func (c *fakeLearnerCounter) CountActiveLearners(_ context.Context, tenantID string) (int, error) {
	return c.active[tenantID], nil
}

func (c *fakeLearnerCounter) ListActiveLearners(_ context.Context, tenantID string) ([]pool.LearnerContext, error) {
	return c.list[tenantID], nil
}

// -----------------------------------------------------------------------------
// OnLearnerEnrolled
// -----------------------------------------------------------------------------

func TestEngine_OnLearnerEnrolled_ManualPolicyEmitsNothing(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	eng := pool.NewPolicyEngine(store, nil)
	intents, err := eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-x", pool.LearnerContext{Role: "standard"})
	if err != nil {
		t.Fatalf("OnLearnerEnrolled: unexpected error: %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("expected 0 intents under manual policy, got %d", len(intents))
	}
}

func TestEngine_OnLearnerEnrolled_OnEnrollmentEmitsIntent(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 250})
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	eng := pool.NewPolicyEngine(store, nil)
	intents, err := eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-x", pool.LearnerContext{Role: "standard"})
	if err != nil {
		t.Fatalf("OnLearnerEnrolled: unexpected error: %v", err)
	}
	if len(intents) != 1 {
		t.Fatalf("expected 1 intent, got %d", len(intents))
	}
	got := intents[0]
	if got.GCID != "gcid-x" {
		t.Fatalf("expected gcid-x, got %s", got.GCID)
	}
	if got.Units != 250 {
		t.Fatalf("expected 250 units, got %d", got.Units)
	}
	if got.Reason != pool.ReasonOnEnrollment {
		t.Fatalf("reason mismatch")
	}
}

func TestEngine_OnLearnerEnrolled_InsufficientPoolNoIntent(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 250})
	// Pool has only 100 units; cannot satisfy 250.
	_ = p.TopUp(100, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	eng := pool.NewPolicyEngine(store, nil)
	intents, err := eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-x", pool.LearnerContext{Role: "standard"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("expected no intents when pool insufficient, got %d", len(intents))
	}
}

func TestEngine_OnLearnerEnrolled_PoolNotFoundIsNoOp(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	eng := pool.NewPolicyEngine(store, nil)
	intents, err := eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-x", pool.LearnerContext{Role: "standard"})
	if err != nil {
		t.Fatalf("expected no-op no-error when no pool, got %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("expected 0 intents")
	}
}

func TestEngine_OnLearnerEnrolled_TierBasedHonoursRole(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.TierBasedPolicy{
		TierToUnits: map[string]int64{"power_user": 1000, "standard": 200},
	})
	_ = p.TopUp(5000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	eng := pool.NewPolicyEngine(store, nil)
	intents, _ := eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-pu", pool.LearnerContext{Role: "power_user"})
	if len(intents) != 1 || intents[0].Units != 1000 {
		t.Fatalf("expected 1000 units for power_user, got %#v", intents)
	}
	intents, _ = eng.OnLearnerEnrolled(context.Background(), tenantA, "gcid-st", pool.LearnerContext{Role: "standard"})
	if len(intents) != 1 || intents[0].Units != 200 {
		t.Fatalf("expected 200 units for standard, got %#v", intents)
	}
}

// -----------------------------------------------------------------------------
// OnMonthlyTick
// -----------------------------------------------------------------------------

func TestEngine_OnMonthlyTick_EqualSplitDistributes(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.EqualSplitPolicy{})
	_ = p.SetMonthlyTopupUnits(1000)
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	counter := &fakeLearnerCounter{
		active: map[string]int{tenantA: 4},
		list: map[string][]pool.LearnerContext{
			tenantA: {
				{GCID: "gcid-1", Role: "standard"},
				{GCID: "gcid-2", Role: "standard"},
				{GCID: "gcid-3", Role: "standard"},
				{GCID: "gcid-4", Role: "standard"},
			},
		},
	}
	eng := pool.NewPolicyEngine(store, counter)

	intents, err := eng.OnMonthlyTick(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("OnMonthlyTick: %v", err)
	}
	if len(intents) != 4 {
		t.Fatalf("expected 4 intents, got %d", len(intents))
	}
	for _, in := range intents {
		if in.Units != 250 {
			t.Fatalf("expected 250 units each, got %d", in.Units)
		}
		if in.Reason != pool.ReasonEqualSplit {
			t.Fatalf("reason mismatch")
		}
	}
}

func TestEngine_OnMonthlyTick_ManualPolicyNoOp(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)
	eng := pool.NewPolicyEngine(store, &fakeLearnerCounter{})
	intents, err := eng.OnMonthlyTick(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("manual policy should not auto-distribute on tick")
	}
}

func TestEngine_OnMonthlyTick_NoLearnersNoOp(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.EqualSplitPolicy{})
	_ = p.SetMonthlyTopupUnits(1000)
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)
	counter := &fakeLearnerCounter{active: map[string]int{tenantA: 0}, list: map[string][]pool.LearnerContext{}}
	eng := pool.NewPolicyEngine(store, counter)
	intents, _ := eng.OnMonthlyTick(context.Background(), tenantA)
	if len(intents) != 0 {
		t.Fatalf("expected 0 intents with 0 learners, got %d", len(intents))
	}
}

func TestEngine_OnMonthlyTick_PoolNotFoundIsNoOp(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	eng := pool.NewPolicyEngine(store, &fakeLearnerCounter{})
	intents, err := eng.OnMonthlyTick(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("expected no-op when no pool, got %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("expected 0 intents, got %d", len(intents))
	}
}

// -----------------------------------------------------------------------------
// RecordClaim
// -----------------------------------------------------------------------------

func TestEngine_RecordClaim_AcceptsValidInput(t *testing.T) {
	t.Parallel()
	eng := pool.NewPolicyEngine(newFakePoolStore(), nil)
	if err := eng.RecordClaim(context.Background(), "alloc-1", 50); err != nil {
		t.Fatalf("RecordClaim: %v", err)
	}
}

func TestEngine_RecordClaim_RejectsEmptyAllocation(t *testing.T) {
	t.Parallel()
	eng := pool.NewPolicyEngine(newFakePoolStore(), nil)
	if err := eng.RecordClaim(context.Background(), "", 50); err == nil {
		t.Fatalf("expected error for empty allocation_id")
	}
}

func TestEngine_RecordClaim_RejectsNonPositiveUnits(t *testing.T) {
	t.Parallel()
	eng := pool.NewPolicyEngine(newFakePoolStore(), nil)
	if err := eng.RecordClaim(context.Background(), "alloc-1", 0); err == nil {
		t.Fatalf("expected error for zero units")
	}
}

func TestEngine_OnMonthlyTick_PoolDepletedSkips(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.EqualSplitPolicy{})
	_ = p.SetMonthlyTopupUnits(0) // disabled
	_ = store.Save(context.Background(), p)
	counter := &fakeLearnerCounter{active: map[string]int{tenantA: 5}}
	eng := pool.NewPolicyEngine(store, counter)
	intents, _ := eng.OnMonthlyTick(context.Background(), tenantA)
	if len(intents) != 0 {
		t.Fatalf("expected 0 intents when monthly_topup=0, got %d", len(intents))
	}
}
