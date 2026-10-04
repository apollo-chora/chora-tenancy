// Package pool_test holds the RED-phase TDD specs for the TenantManaPool
// aggregate per BE-USR-3 (Wave 3 of the per-user economy / Q2=v1 tenant
// subsidy track).
//
// Aligned with:
//   - docs/architecture/adrs/adr-142-per-user-economy-mana.md
//   - chora-contracts/openapi/tenancy-admin.yaml v1.2 (mana-pool routes)
//   - chora-contracts/proto/events/tenancy/tenant_mana_pool.proto
//
// Aggregate invariants:
//   - One TenantManaPool per tenant (idempotent on tenant_id)
//   - balance_units >= 0 always (negative balance forbidden)
//   - Append-only audit on every Topup (lifetime_topped_up monotonic)
//   - OCC version bumps on every state mutation
//   - Soft-delete via DeletedAt (never hard-delete per ddd-enforcement)
package pool_test

import (
	"errors"
	"testing"
	"time"

	pool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

const (
	tenantA   = "01970000-0000-7000-8000-0000000000a1"
	tenantB   = "01970000-0000-7000-8000-0000000000a2"
	gcidAdmin = "01970000-0000-7000-8000-0000000000bb"
)

// -----------------------------------------------------------------------------
// TenantManaPool — construction
// -----------------------------------------------------------------------------

func TestNewPool_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()
	p, err := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: unexpected error: %v", err)
	}
	if p.PoolID == "" {
		t.Fatalf("expected non-empty pool_id")
	}
	if p.TenantID != tenantA {
		t.Fatalf("tenant_id mismatch")
	}
	if p.BalanceUnits != 0 {
		t.Fatalf("expected initial balance_units = 0, got %d", p.BalanceUnits)
	}
	if p.MonthlyTopupUnits != 0 {
		t.Fatalf("expected default monthly_topup_units = 0, got %d", p.MonthlyTopupUnits)
	}
	if p.Version != 1 {
		t.Fatalf("expected initial version = 1, got %d", p.Version)
	}
	if p.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt set")
	}
	if p.AutoAllocationPolicy.Kind() != pool.PolicyKindManual {
		t.Fatalf("expected manual policy default")
	}
}

func TestNewPool_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	if _, err := pool.NewPool("", gcidAdmin, pool.ManualPolicy{}); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
	if _, err := pool.NewPool("   ", gcidAdmin, pool.ManualPolicy{}); err == nil {
		t.Fatalf("expected error for whitespace tenant_id")
	}
}

func TestNewPool_RejectsEmptyAdminGCID(t *testing.T) {
	t.Parallel()
	if _, err := pool.NewPool(tenantA, "", pool.ManualPolicy{}); err == nil {
		t.Fatalf("expected error for empty admin gcid")
	}
}

// -----------------------------------------------------------------------------
// TopUp
// -----------------------------------------------------------------------------

func TestTopUp_CreditsBalance_AndBumpsVersion(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	prev := p.Version
	if err := p.TopUp(1000, 500, "USD", "pi_stub_1", false, gcidAdmin); err != nil {
		t.Fatalf("TopUp: unexpected error: %v", err)
	}
	if p.BalanceUnits != 1000 {
		t.Fatalf("expected balance 1000, got %d", p.BalanceUnits)
	}
	if p.LifetimeToppedUpUnits != 1000 {
		t.Fatalf("expected lifetime topped_up 1000, got %d", p.LifetimeToppedUpUnits)
	}
	if p.Version != prev+1 {
		t.Fatalf("expected version bump to %d, got %d", prev+1, p.Version)
	}
	if p.LastToppedUpAt == nil {
		t.Fatalf("expected last_topped_up_at set")
	}
}

func TestTopUp_RejectsZeroOrNegativeUnits(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.TopUp(0, 500, "USD", "pi_x", false, gcidAdmin); err == nil {
		t.Fatalf("expected error for zero units")
	}
	if err := p.TopUp(-5, 500, "USD", "pi_x", false, gcidAdmin); err == nil {
		t.Fatalf("expected error for negative units")
	}
}

func TestTopUp_RejectsZeroOrNegativeChargedCents(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.TopUp(100, 0, "USD", "pi_x", false, gcidAdmin); err == nil {
		t.Fatalf("expected error for zero charged_cents")
	}
}

func TestTopUp_AccumulatesLifetimeUnits(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(1000, 500, "USD", "pi_1", false, gcidAdmin)
	_ = p.TopUp(2000, 1000, "USD", "pi_2", true, "")
	if p.LifetimeToppedUpUnits != 3000 {
		t.Fatalf("expected lifetime 3000, got %d", p.LifetimeToppedUpUnits)
	}
	if p.BalanceUnits != 3000 {
		t.Fatalf("expected balance 3000, got %d", p.BalanceUnits)
	}
}

// -----------------------------------------------------------------------------
// Debit (allocation drives this)
// -----------------------------------------------------------------------------

func TestDebit_DecreasesBalance(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(1000, 500, "USD", "pi_1", false, gcidAdmin)
	if err := p.Debit(300); err != nil {
		t.Fatalf("Debit: unexpected error: %v", err)
	}
	if p.BalanceUnits != 700 {
		t.Fatalf("expected balance 700, got %d", p.BalanceUnits)
	}
	if p.LifetimeAllocatedUnits != 300 {
		t.Fatalf("expected lifetime_allocated 300, got %d", p.LifetimeAllocatedUnits)
	}
}

func TestDebit_RejectsInsufficientBalance(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(100, 500, "USD", "pi_1", false, gcidAdmin)
	err := p.Debit(101)
	if !errors.Is(err, pool.ErrInsufficientBalance) {
		t.Fatalf("expected ErrInsufficientBalance, got %v", err)
	}
	if p.BalanceUnits != 100 {
		t.Fatalf("balance should remain 100 after rejected debit, got %d", p.BalanceUnits)
	}
}

func TestDebit_RejectsZeroOrNegative(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(100, 500, "USD", "pi_1", false, gcidAdmin)
	if err := p.Debit(0); err == nil {
		t.Fatalf("expected error for zero debit")
	}
	if err := p.Debit(-5); err == nil {
		t.Fatalf("expected error for negative debit")
	}
}

// -----------------------------------------------------------------------------
// Credit (revoke / expire returns)
// -----------------------------------------------------------------------------

func TestCredit_ReturnsUnitsToBalance(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(1000, 500, "USD", "pi_1", false, gcidAdmin)
	_ = p.Debit(300)
	if err := p.Credit(100); err != nil {
		t.Fatalf("Credit: unexpected error: %v", err)
	}
	if p.BalanceUnits != 800 {
		t.Fatalf("expected balance 800 after revoke-credit, got %d", p.BalanceUnits)
	}
	// LifetimeAllocatedUnits represents NET allocation; revocations should
	// reduce it so the dashboard reflects current outstanding.
	if p.LifetimeAllocatedUnits != 200 {
		t.Fatalf("expected lifetime_allocated 200 after credit-back, got %d", p.LifetimeAllocatedUnits)
	}
}

func TestCredit_RejectsZeroOrNegative(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.Credit(0); err == nil {
		t.Fatalf("expected error for zero credit")
	}
}

// -----------------------------------------------------------------------------
// Auto-renew config
// -----------------------------------------------------------------------------

func TestSetMonthlyTopup_UpdatesField(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	prevVer := p.Version
	if err := p.SetMonthlyTopupUnits(500); err != nil {
		t.Fatalf("SetMonthlyTopupUnits: unexpected error: %v", err)
	}
	if p.MonthlyTopupUnits != 500 {
		t.Fatalf("expected 500, got %d", p.MonthlyTopupUnits)
	}
	if p.Version != prevVer+1 {
		t.Fatalf("expected version bump on policy change")
	}
}

func TestSetMonthlyTopup_AcceptsZeroToDisable(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.SetMonthlyTopupUnits(0); err != nil {
		t.Fatalf("expected zero (disable) to be accepted: %v", err)
	}
}

func TestSetMonthlyTopup_RejectsNegative(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.SetMonthlyTopupUnits(-1); err == nil {
		t.Fatalf("expected error for negative monthly topup")
	}
}

// -----------------------------------------------------------------------------
// Set policy
// -----------------------------------------------------------------------------

func TestSetPolicy_UpdatesAndBumpsVersion(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	prev := p.Version
	policy := pool.OnEnrollmentPolicy{DefaultUnits: 500}
	if err := p.SetPolicy(policy); err != nil {
		t.Fatalf("SetPolicy: unexpected error: %v", err)
	}
	if p.AutoAllocationPolicy.Kind() != pool.PolicyKindOnEnrollment {
		t.Fatalf("policy kind mismatch")
	}
	if p.Version != prev+1 {
		t.Fatalf("expected version bump")
	}
}

func TestSetPolicy_RejectsNil(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := p.SetPolicy(nil); err == nil {
		t.Fatalf("expected error for nil policy")
	}
}

// -----------------------------------------------------------------------------
// Depletion detection (low balance + zero)
// -----------------------------------------------------------------------------

func TestIsDepleted_FullyDepletedAtZero(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.TopUp(100, 500, "USD", "pi_1", false, gcidAdmin)
	_ = p.Debit(100)
	if !p.IsFullyDepleted() {
		t.Fatalf("expected fully depleted at balance=0")
	}
}

func TestIsDepleted_LowBalanceThreshold(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.SetMonthlyTopupUnits(1000)
	_ = p.TopUp(1000, 500, "USD", "pi_1", false, gcidAdmin)
	// 10% threshold of monthly_topup_units = 100. Drain to 50.
	_ = p.Debit(950)
	threshold := p.LowBalanceThreshold()
	if threshold != 100 {
		t.Fatalf("expected default threshold = 100 (10%% of 1000), got %d", threshold)
	}
	if !p.IsLowBalance() {
		t.Fatalf("expected IsLowBalance true at 50/threshold=100")
	}
}

func TestIsDepleted_NotLowWhenAboveThreshold(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = p.SetMonthlyTopupUnits(1000)
	_ = p.TopUp(1000, 500, "USD", "pi_1", false, gcidAdmin)
	if p.IsLowBalance() {
		t.Fatalf("not low at full balance")
	}
	if p.IsFullyDepleted() {
		t.Fatalf("not depleted at full balance")
	}
}

// -----------------------------------------------------------------------------
// Soft delete
// -----------------------------------------------------------------------------

func TestPool_SoftDelete_StampsDeletedAt(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if p.IsDeleted() {
		t.Fatalf("expected not deleted on construction")
	}
	p.SoftDelete()
	if !p.IsDeleted() {
		t.Fatalf("expected IsDeleted true after SoftDelete")
	}
	if p.DeletedAt == nil {
		t.Fatalf("expected DeletedAt set")
	}
	first := *p.DeletedAt
	p.SoftDelete() // idempotent
	if !p.DeletedAt.Equal(first) {
		t.Fatalf("SoftDelete should be idempotent")
	}
}

// -----------------------------------------------------------------------------
// UUIDv7 sanity
// -----------------------------------------------------------------------------

func TestNewUUIDv7_DistinctValues(t *testing.T) {
	t.Parallel()
	id1 := pool.NewUUIDv7()
	id2 := pool.NewUUIDv7()
	if id1 == "" || id2 == "" {
		t.Fatalf("expected non-empty UUIDv7")
	}
	if id1 == id2 {
		t.Fatalf("expected distinct UUIDv7 values")
	}
}

// -----------------------------------------------------------------------------
// Time monotonicity guard
// -----------------------------------------------------------------------------

func TestPool_UpdatedAt_AdvancesOnMutation(t *testing.T) {
	t.Parallel()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	first := p.UpdatedAt
	time.Sleep(2 * time.Millisecond)
	_ = p.TopUp(100, 500, "USD", "pi_x", false, gcidAdmin)
	if !p.UpdatedAt.After(first) {
		t.Fatalf("expected UpdatedAt to advance after mutation")
	}
}

// -----------------------------------------------------------------------------
// Independence: new pool for tenantB does not collide with tenantA
// -----------------------------------------------------------------------------

func TestNewPool_DistinctTenantsHaveDistinctIDs(t *testing.T) {
	t.Parallel()
	a, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	b, _ := pool.NewPool(tenantB, gcidAdmin, pool.ManualPolicy{})
	if a.PoolID == b.PoolID {
		t.Fatalf("expected distinct pool IDs across tenants")
	}
}

func TestNewPool_ValidationBranches(t *testing.T) {
	t.Parallel()
	if _, err := pool.NewPool("", "g", pool.ManualPolicy{}); err == nil {
		t.Fatal("expected error for blank tenant")
	}
	if _, err := pool.NewPool("t", "", pool.ManualPolicy{}); err == nil {
		t.Fatal("expected error for blank creator")
	}
	if _, err := pool.NewPool("t", "g", nil); err == nil {
		t.Fatal("expected error for nil policy")
	}
}

func TestTopUp_ValidationBranches(t *testing.T) {
	t.Parallel()
	p, err := pool.NewPool("t-1", "g-1", pool.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := p.TopUp(0, 100, "SGD", "pi_1", false, "g-1"); err == nil {
		t.Fatal("expected error for zero units")
	}
	if err := p.TopUp(10, 0, "SGD", "pi_1", false, "g-1"); err == nil {
		t.Fatal("expected error for zero charged cents")
	}
	if err := p.TopUp(10, 100, "SGD", "pi_1", false, ""); err == nil {
		t.Fatal("expected error for manual top-up without gcid")
	}
	if err := p.TopUp(10, 100, "", "pi_1", true, ""); err == nil {
		t.Fatal("expected error for blank currency")
	}
	if err := p.TopUp(10, 100, "SGD", "", true, ""); err == nil {
		t.Fatal("expected error for blank payment intent")
	}
	if err := p.TopUp(10, 100, "SGD", "pi_1", true, ""); err != nil {
		t.Fatalf("auto-renew top-up should succeed: %v", err)
	}
}

func TestCredit_LifetimeClamp(t *testing.T) {
	t.Parallel()
	p, err := pool.NewPool("t-1", "g-1", pool.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	_ = p.TopUp(50, 100, "SGD", "pi_1", true, "")
	// Credit more than was ever allocated → clamps LifetimeAllocatedUnits at 0.
	if err := p.Credit(500); err != nil {
		t.Fatalf("Credit: %v", err)
	}
	if p.LifetimeAllocatedUnits != 0 || p.BalanceUnits != 550 {
		t.Fatalf("unexpected credit result balance=%d lifetime=%d", p.BalanceUnits, p.LifetimeAllocatedUnits)
	}
	if err := p.Credit(0); err == nil {
		t.Fatal("expected error for zero credit")
	}
	if err := p.Credit(-5); err == nil {
		t.Fatal("expected error for negative credit")
	}
}

func TestDebit_Validation(t *testing.T) {
	t.Parallel()
	p, err := pool.NewPool("t-1", "g-1", pool.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := p.Debit(0); err == nil {
		t.Fatal("expected error for zero debit")
	}
}
