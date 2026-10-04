// policy_engine_branch_test.go — branch coverage additions to
// policy_engine_test.go: the error + no-op cases of OnLearnerEnrolled /
// OnMonthlyTick and the allocationReason mapping across policy kinds.
package pool_test

import (
	"context"
	"errors"
	"testing"

	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

func TestEngine_OnLearnerEnrolled_Branches(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 250})
	_ = p.TopUp(1000, 500, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)

	eng := pool.NewPolicyEngine(store, nil)
	ctx := context.Background()

	// Blank tenant/gcid → no-op.
	if intents, err := eng.OnLearnerEnrolled(ctx, "  ", "g", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("blank tenant should no-op, got %v %v", intents, err)
	}
	if intents, err := eng.OnLearnerEnrolled(ctx, tenantA, "", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("blank gcid should no-op, got %v %v", intents, err)
	}
	// Missing pool → silent no-op.
	if intents, err := eng.OnLearnerEnrolled(ctx, "no-pool", "g", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("missing pool should no-op, got %v %v", intents, err)
	}
	// Store infrastructure failure propagates.
	bad := &failingPoolStore{err: errors.New("db down")}
	engBad := pool.NewPolicyEngine(bad, nil)
	if _, err := engBad.OnLearnerEnrolled(ctx, tenantA, "g", pool.LearnerContext{}); err == nil {
		t.Fatalf("expected store error to propagate")
	}
	// Deleted pool → no-op.
	p2, _ := pool.NewPool(tenantB, gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 100})
	p2.SoftDelete()
	_ = store.Save(context.Background(), p2)
	if intents, err := eng.OnLearnerEnrolled(ctx, tenantB, "g", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("deleted pool should no-op, got %v %v", intents, err)
	}
	// Insufficient balance → silent no-op.
	p3, _ := pool.NewPool("tenant-c", gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 999999})
	_ = store.Save(context.Background(), p3)
	if intents, err := eng.OnLearnerEnrolled(ctx, "tenant-c", "g", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("insufficient balance should no-op, got %v %v", intents, err)
	}
	// Policy with no units for the learner (manual) → no-op.
	p4, _ := pool.NewPool("tenant-d", gcidAdmin, pool.ManualPolicy{})
	_ = p4.TopUp(100, 9, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p4)
	if intents, err := eng.OnLearnerEnrolled(ctx, "tenant-d", "g", pool.LearnerContext{}); err != nil || len(intents) != 0 {
		t.Fatalf("manual policy should no-op, got %v %v", intents, err)
	}
	// lctx carries its own GCID; enrolling gcid differs.
	p5, _ := pool.NewPool("tenant-e", gcidAdmin, pool.OnEnrollmentPolicy{DefaultUnits: 10})
	_ = p5.TopUp(100, 9, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p5)
	intents, err := eng.OnLearnerEnrolled(ctx, "tenant-e", "caller-gcid", pool.LearnerContext{GCID: "ctx-gcid"})
	if err != nil || len(intents) != 1 || intents[0].GCID != "caller-gcid" {
		t.Fatalf("expected intent with caller gcid, got %v %v", intents, err)
	}
}

func TestEngine_OnMonthlyTick_Branches(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	counter := &fakeLearnerCounter{list: map[string][]pool.LearnerContext{
		tenantA: {{GCID: "g-1"}, {GCID: "g-2"}},
	}}
	eng := pool.NewPolicyEngine(store, counter)
	ctx := context.Background()

	// Blank tenant → no-op.
	if intents, err := eng.OnMonthlyTick(ctx, "  "); err != nil || len(intents) != 0 {
		t.Fatalf("blank tenant should no-op, got %v %v", intents, err)
	}
	// Missing pool → no-op.
	if intents, err := eng.OnMonthlyTick(ctx, "no-pool"); err != nil || len(intents) != 0 {
		t.Fatalf("missing pool should no-op, got %v %v", intents, err)
	}
	// Store failure propagates.
	if _, err := pool.NewPolicyEngine(&failingPoolStore{err: errors.New("boom")}, counter).OnMonthlyTick(ctx, tenantA); err == nil {
		t.Fatalf("expected store error to propagate")
	}
	// Non-EqualSplit policy → no intents.
	p, _ := pool.NewPool("tenant-f", gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(100, 9, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)
	if intents, err := eng.OnMonthlyTick(ctx, "tenant-f"); err != nil || len(intents) != 0 {
		t.Fatalf("manual pool should no-op, got %v %v", intents, err)
	}
	// Zero monthly topup → no-op.
	p2, _ := pool.NewPool("tenant-g", gcidAdmin, pool.EqualSplitPolicy{})
	_ = store.Save(context.Background(), p2)
	if intents, err := eng.OnMonthlyTick(ctx, "tenant-g"); err != nil || len(intents) != 0 {
		t.Fatalf("zero monthly topup should no-op, got %v %v", intents, err)
	}
	// Missing counter → error.
	p3, _ := pool.NewPool("tenant-h", gcidAdmin, pool.EqualSplitPolicy{})
	_ = p3.SetMonthlyTopupUnits(100)
	_ = store.Save(context.Background(), p3)
	noCounter := pool.NewPolicyEngine(store, nil)
	if _, err := noCounter.OnMonthlyTick(ctx, "tenant-h"); err == nil {
		t.Fatalf("expected missing-counter error")
	}
	// Counter failure propagates.
	failCounter := &fakeLearnerCounterErr{listErr: errors.New("counter boom")}
	if _, err := pool.NewPolicyEngine(store, failCounter).OnMonthlyTick(ctx, "tenant-h"); err == nil {
		t.Fatalf("expected counter error to propagate")
	}
	// No learners → no-op.
	emptyCounter := &fakeLearnerCounter{list: map[string][]pool.LearnerContext{"tenant-h": {}}}
	if intents, err := pool.NewPolicyEngine(store, emptyCounter).OnMonthlyTick(ctx, "tenant-h"); err != nil || len(intents) != 0 {
		t.Fatalf("no learners should no-op, got %v %v", intents, err)
	}
	// Equal-split distribution with pool exhaustion mid-way.
	p4, _ := pool.NewPool("tenant-i", gcidAdmin, pool.EqualSplitPolicy{})
	_ = p4.SetMonthlyTopupUnits(10)
	_ = p4.TopUp(5, 9, "USD", "pi_x", false, gcidAdmin) // balance 5 < per (10/2=5 → equal)
	_ = store.Save(context.Background(), p4)
	// balance=5 → first learner gets 5, second exhausts → stop.
	some := &fakeLearnerCounter{list: map[string][]pool.LearnerContext{
		"tenant-i": {{GCID: "g-1"}, {GCID: "g-2"}},
	}}
	intents, err := pool.NewPolicyEngine(store, some).OnMonthlyTick(ctx, "tenant-i")
	if err != nil {
		t.Fatalf("OnMonthlyTick: %v", err)
	}
	if len(intents) != 1 {
		t.Fatalf("expected distribution to stop at pool exhaustion, got %d", len(intents))
	}
	// Deleted pool → no-op.
	p5, _ := pool.NewPool("tenant-j", gcidAdmin, pool.EqualSplitPolicy{})
	_ = p5.SetMonthlyTopupUnits(100)
	_ = p5.TopUp(100, 9, "USD", "pi_x", false, gcidAdmin)
	p5.SoftDelete()
	_ = store.Save(context.Background(), p5)
	if intents, err := eng.OnMonthlyTick(ctx, "tenant-j"); err != nil || len(intents) != 0 {
		t.Fatalf("deleted pool should no-op, got %v %v", intents, err)
	}
}

func TestEngine_AllocationReasonMapping(t *testing.T) {
	t.Parallel()
	store := newFakePoolStore()
	// Tier-based policy → tier_based reason (exercises allocationReasonFor TierBased case).
	p, _ := pool.NewPool("tenant-k", gcidAdmin, pool.TierBasedPolicy{TierToUnits: map[string]int64{"standard": 50}})
	_ = p.TopUp(100, 9, "USD", "pi_x", false, gcidAdmin)
	_ = store.Save(context.Background(), p)
	eng := pool.NewPolicyEngine(store, nil)
	intents, err := eng.OnLearnerEnrolled(context.Background(), "tenant-k", "g", pool.LearnerContext{Role: "standard"})
	if err != nil || len(intents) != 1 {
		t.Fatalf("expected tier-based intent, got %v %v", intents, err)
	}
	if intents[0].Reason != pool.ReasonTierBased {
		t.Fatalf("expected tier_based reason, got %s", intents[0].Reason)
	}
	// Tier-based policy with unknown role → no-op.
	if intents, err := eng.OnLearnerEnrolled(context.Background(), "tenant-k", "g", pool.LearnerContext{Role: "guest"}); err != nil || len(intents) != 0 {
		t.Fatalf("unknown tier should no-op, got %v %v", intents, err)
	}
}

// failingPoolStore errors on every read.
type failingPoolStore struct{ err error }

func (f *failingPoolStore) GetByTenant(context.Context, string) (*pool.TenantManaPool, error) {
	return nil, f.err
}
func (f *failingPoolStore) Save(context.Context, *pool.TenantManaPool) error { return nil }

// fakeLearnerCounter with an injectable list error.
type fakeLearnerCounterErr struct {
	list    map[string][]pool.LearnerContext
	listErr error
}

func (c *fakeLearnerCounterErr) CountActiveLearners(context.Context, string) (int, error) {
	return 0, nil
}
func (c *fakeLearnerCounterErr) ListActiveLearners(_ context.Context, tenantID string) ([]pool.LearnerContext, error) {
	if c.listErr != nil {
		return nil, c.listErr
	}
	return c.list[tenantID], nil
}
