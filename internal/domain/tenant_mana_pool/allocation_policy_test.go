// Package pool_test — RED-phase TDD specs for AllocationPolicy variants.
//
// 4 policy variants per ADR-142:
//   - ManualPolicy{}            — no auto-allocation
//   - OnEnrollmentPolicy{N}     — N units when learner enrolls
//   - EqualSplitPolicy{}        — pool monthly drip / active learner count
//   - TierBasedPolicy{tier→N}   — different units per learner role
package pool_test

import (
	"testing"

	pool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

func TestManualPolicy_KindAndComputeUnitsZero(t *testing.T) {
	t.Parallel()
	p := pool.ManualPolicy{}
	if p.Kind() != pool.PolicyKindManual {
		t.Fatalf("expected PolicyKindManual")
	}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if ok {
		t.Fatalf("ManualPolicy should NOT auto-allocate")
	}
	if got != 0 {
		t.Fatalf("expected 0 units for manual, got %d", got)
	}
}

func TestOnEnrollmentPolicy_GrantsConfiguredUnits(t *testing.T) {
	t.Parallel()
	p := pool.OnEnrollmentPolicy{DefaultUnits: 250}
	if p.Kind() != pool.PolicyKindOnEnrollment {
		t.Fatalf("kind mismatch")
	}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if !ok {
		t.Fatalf("OnEnrollmentPolicy should auto-allocate")
	}
	if got != 250 {
		t.Fatalf("expected 250, got %d", got)
	}
}

func TestOnEnrollmentPolicy_RejectsNonPositive(t *testing.T) {
	t.Parallel()
	p := pool.OnEnrollmentPolicy{DefaultUnits: 0}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if ok {
		t.Fatalf("zero default units → no allocation")
	}
	if got != 0 {
		t.Fatalf("zero")
	}
}

func TestEqualSplitPolicy_OnEnrollmentNoOp(t *testing.T) {
	t.Parallel()
	p := pool.EqualSplitPolicy{}
	if p.Kind() != pool.PolicyKindEqualSplit {
		t.Fatalf("kind mismatch")
	}
	// EqualSplit only fires on monthly tick — never on enrollment.
	_, ok := p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if ok {
		t.Fatalf("EqualSplit should NOT fire on enrollment")
	}
}

func TestEqualSplitPolicy_ComputeUnitsPerLearner(t *testing.T) {
	t.Parallel()
	p := pool.EqualSplitPolicy{}
	per := p.ComputeMonthlyUnits(1000, 4)
	if per != 250 {
		t.Fatalf("expected 250 per learner, got %d", per)
	}
	// rounding: 1000 / 3 = 333 (truncation; we don't lose units to rounding above 333)
	per = p.ComputeMonthlyUnits(1000, 3)
	if per != 333 {
		t.Fatalf("expected 333 (floor), got %d", per)
	}
}

func TestEqualSplitPolicy_ZeroLearnersOrPoolReturnsZero(t *testing.T) {
	t.Parallel()
	p := pool.EqualSplitPolicy{}
	if got := p.ComputeMonthlyUnits(1000, 0); got != 0 {
		t.Fatalf("expected 0 for zero learners, got %d", got)
	}
	if got := p.ComputeMonthlyUnits(0, 5); got != 0 {
		t.Fatalf("expected 0 for empty pool, got %d", got)
	}
}

func TestTierBasedPolicy_LooksUpRole(t *testing.T) {
	t.Parallel()
	p := pool.TierBasedPolicy{TierToUnits: map[string]int64{
		"power_user": 1000,
		"standard":   200,
	}}
	if p.Kind() != pool.PolicyKindTierBased {
		t.Fatalf("kind mismatch")
	}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "power_user"})
	if !ok {
		t.Fatalf("expected hit")
	}
	if got != 1000 {
		t.Fatalf("expected 1000 for power_user, got %d", got)
	}
	got, ok = p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if !ok || got != 200 {
		t.Fatalf("expected 200 for standard, got %d (ok=%v)", got, ok)
	}
}

func TestTierBasedPolicy_UnknownRoleNoAllocation(t *testing.T) {
	t.Parallel()
	p := pool.TierBasedPolicy{TierToUnits: map[string]int64{"power_user": 1000}}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "guest"})
	if ok {
		t.Fatalf("unknown role should NOT auto-allocate")
	}
	if got != 0 {
		t.Fatalf("expected 0 for unknown role, got %d", got)
	}
}

func TestTierBasedPolicy_NilMapNoAllocation(t *testing.T) {
	t.Parallel()
	p := pool.TierBasedPolicy{TierToUnits: nil}
	got, ok := p.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if ok || got != 0 {
		t.Fatalf("nil map → no allocation")
	}
}

// PolicyFromConfig — round-trip from API config block (admin update endpoint
// receives this map; domain materialises the AllocationPolicy variant).
func TestPolicyFromConfig_Manual(t *testing.T) {
	t.Parallel()
	pol, err := pool.PolicyFromConfig(pool.PolicyConfig{Policy: "manual"})
	if err != nil {
		t.Fatalf("PolicyFromConfig manual: %v", err)
	}
	if pol.Kind() != pool.PolicyKindManual {
		t.Fatalf("expected manual kind")
	}
}

func TestPolicyFromConfig_OnEnrollment(t *testing.T) {
	t.Parallel()
	cfg := pool.PolicyConfig{Policy: "on_enrollment", UnitsOnEnrollment: 100}
	pol, err := pool.PolicyFromConfig(cfg)
	if err != nil {
		t.Fatalf("PolicyFromConfig: %v", err)
	}
	if pol.Kind() != pool.PolicyKindOnEnrollment {
		t.Fatalf("kind mismatch")
	}
	got, ok := pol.UnitsForLearner(pool.LearnerContext{Role: "standard"})
	if !ok || got != 100 {
		t.Fatalf("expected 100 units")
	}
}

func TestPolicyFromConfig_EqualSplit(t *testing.T) {
	t.Parallel()
	pol, err := pool.PolicyFromConfig(pool.PolicyConfig{Policy: "equal_split"})
	if err != nil {
		t.Fatalf("PolicyFromConfig: %v", err)
	}
	if pol.Kind() != pool.PolicyKindEqualSplit {
		t.Fatalf("kind mismatch")
	}
}

func TestPolicyFromConfig_TierBased(t *testing.T) {
	t.Parallel()
	cfg := pool.PolicyConfig{
		Policy:      "tier_based",
		TierAmounts: map[string]int64{"power_user": 1000, "standard": 200},
	}
	pol, err := pool.PolicyFromConfig(cfg)
	if err != nil {
		t.Fatalf("PolicyFromConfig: %v", err)
	}
	if pol.Kind() != pool.PolicyKindTierBased {
		t.Fatalf("kind mismatch")
	}
	got, ok := pol.UnitsForLearner(pool.LearnerContext{Role: "power_user"})
	if !ok || got != 1000 {
		t.Fatalf("expected 1000")
	}
}

func TestPolicyFromConfig_RejectsUnknownPolicy(t *testing.T) {
	t.Parallel()
	if _, err := pool.PolicyFromConfig(pool.PolicyConfig{Policy: "garbage"}); err == nil {
		t.Fatalf("expected error for unknown policy")
	}
}

func TestPolicyFromConfig_RejectsOnEnrollmentNegative(t *testing.T) {
	t.Parallel()
	cfg := pool.PolicyConfig{Policy: "on_enrollment", UnitsOnEnrollment: -5}
	if _, err := pool.PolicyFromConfig(cfg); err == nil {
		t.Fatalf("expected error for negative units")
	}
}

func TestPolicyFromConfig_RejectsTierBasedNegativeUnits(t *testing.T) {
	t.Parallel()
	cfg := pool.PolicyConfig{
		Policy:      "tier_based",
		TierAmounts: map[string]int64{"x": -1},
	}
	if _, err := pool.PolicyFromConfig(cfg); err == nil {
		t.Fatalf("expected error for negative tier units")
	}
}
