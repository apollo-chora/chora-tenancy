// Package allocation_test holds the RED-phase TDD specs for the
// TenantManaAllocation aggregate per BE-USR-3.
//
// Aligned with:
//   - chora-contracts/openapi/tenancy-admin.yaml v1.2 (mana-allocations routes)
//   - chora-contracts/proto/events/tenancy/tenant_mana_allocation.proto
//
// Aggregate is APPEND-ONLY: rows are written once and only mutate via
// status transitions (pending → claimed | revoked | expired). They are
// never UPDATEd in place beyond status + tracking timestamps.
package allocation_test

import (
	"errors"
	"testing"
	"time"

	allocation "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_allocation"
)

const (
	tenantA   = "01970000-0000-7000-8000-0000000000a1"
	poolA     = "01970000-0000-7000-8000-0000000000aa"
	gcidL1    = "01970000-0000-7000-8000-0000000000c1"
	gcidL2    = "01970000-0000-7000-8000-0000000000c2"
	gcidAdmin = "01970000-0000-7000-8000-0000000000bb"
)

func TestNewAllocation_AssignsUUIDAndDefaults(t *testing.T) {
	t.Parallel()
	a, err := allocation.New(allocation.NewParams{
		TenantID:        tenantA,
		GCID:            gcidL1,
		SourcePoolID:    poolA,
		Units:           250,
		Reason:          allocation.ReasonManual,
		AllocatedByGCID: gcidAdmin,
	})
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	if a.AllocationID == "" {
		t.Fatalf("expected non-empty allocation_id")
	}
	if a.TenantID != tenantA {
		t.Fatalf("tenant mismatch")
	}
	if a.GCID != gcidL1 {
		t.Fatalf("gcid mismatch")
	}
	if a.Units != 250 {
		t.Fatalf("units mismatch")
	}
	if a.Status != allocation.StatusPending {
		t.Fatalf("expected pending, got %q", a.Status)
	}
	if a.Reason != allocation.ReasonManual {
		t.Fatalf("reason mismatch")
	}
	if a.AllocatedByGCID != gcidAdmin {
		t.Fatalf("allocated_by mismatch")
	}
	if a.AllocatedAt.IsZero() {
		t.Fatalf("expected allocated_at set")
	}
	if a.ExpiresAt != nil {
		t.Fatalf("default expires_at should be nil")
	}
}

func TestNew_RejectsZeroOrNegativeUnits(t *testing.T) {
	t.Parallel()
	for _, u := range []int64{0, -1, -100} {
		if _, err := allocation.New(allocation.NewParams{
			TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA,
			Units: u, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		}); err == nil {
			t.Fatalf("expected error for units=%d", u)
		}
	}
}

func TestNew_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := allocation.New(allocation.NewParams{
		TenantID: "", GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	}); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
}

func TestNew_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	if _, err := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: "", SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	}); err == nil {
		t.Fatalf("expected error for empty gcid")
	}
}

func TestNew_RejectsEmptySourcePool(t *testing.T) {
	t.Parallel()
	if _, err := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: "", Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	}); err == nil {
		t.Fatalf("expected error for empty pool id")
	}
}

func TestNew_RejectsUnknownReason(t *testing.T) {
	t.Parallel()
	if _, err := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.Reason("garbage"), AllocatedByGCID: gcidAdmin,
	}); err == nil {
		t.Fatalf("expected error for invalid reason")
	}
}

// AllocatedByGCID is REQUIRED for manual but optional for auto-policy reasons.
func TestNew_ManualRequiresAllocator(t *testing.T) {
	t.Parallel()
	if _, err := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: "",
	}); err == nil {
		t.Fatalf("expected error for missing allocator on manual")
	}
}

func TestNew_AutoPolicyAllocatorOptional(t *testing.T) {
	t.Parallel()
	a, err := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonOnEnrollment, AllocatedByGCID: "",
	})
	if err != nil {
		t.Fatalf("on_enrollment without allocator should be allowed: %v", err)
	}
	if a.AllocatedByGCID != "" {
		t.Fatalf("expected empty allocator")
	}
}

// -----------------------------------------------------------------------------
// Status transitions
// -----------------------------------------------------------------------------

func TestClaim_TransitionsToClaimed(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	if err := a.Claim("entry-1", 50); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if a.Status != allocation.StatusClaimed {
		t.Fatalf("expected claimed, got %q", a.Status)
	}
	if a.FirstDebitEntryID != "entry-1" {
		t.Fatalf("expected first_debit_entry_id set")
	}
	if a.RemainingUnits != 50 {
		t.Fatalf("expected remaining 50, got %d", a.RemainingUnits)
	}
	if a.ClaimedAt == nil {
		t.Fatalf("expected claimed_at set")
	}
}

func TestClaim_IsIdempotent(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_ = a.Claim("entry-1", 50)
	first := *a.ClaimedAt
	// Re-claim with different entry should NOT change the recorded first debit.
	if err := a.Claim("entry-2", 30); err != nil {
		t.Fatalf("Claim idempotent should not error: %v", err)
	}
	if a.FirstDebitEntryID != "entry-1" {
		t.Fatalf("first_debit_entry_id should remain entry-1")
	}
	if !a.ClaimedAt.Equal(first) {
		t.Fatalf("claimed_at should not be rewritten")
	}
}

func TestClaim_RejectsAfterRevoke(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_, _ = a.Revoke("offboarding", gcidAdmin)
	err := a.Claim("entry-1", 50)
	if !errors.Is(err, allocation.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestClaim_RejectsAfterExpire(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_, _ = a.Expire()
	if err := a.Claim("entry-1", 50); !errors.Is(err, allocation.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestRevoke_PendingReturnsUnitsToPool(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	res, err := a.Revoke("admin override", gcidAdmin)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !res.UnitsReturnedToPool {
		t.Fatalf("expected units returned to pool")
	}
	if res.ReturnedUnits != 100 {
		t.Fatalf("expected 100 returned, got %d", res.ReturnedUnits)
	}
	if a.Status != allocation.StatusRevoked {
		t.Fatalf("expected revoked, got %q", a.Status)
	}
	if a.RevokedReason != "admin override" {
		t.Fatalf("revoked_reason mismatch")
	}
	if a.RevokedAt == nil {
		t.Fatalf("expected revoked_at set")
	}
}

func TestRevoke_ClaimedKeepsUnitsWithLearner(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_ = a.Claim("entry-1", 50)
	res, err := a.Revoke("audit", gcidAdmin)
	if err != nil {
		t.Fatalf("Revoke claimed: %v", err)
	}
	if res.UnitsReturnedToPool {
		t.Fatalf("claimed allocation revocation should NOT return units")
	}
	if res.ReturnedUnits != 0 {
		t.Fatalf("expected 0 returned, got %d", res.ReturnedUnits)
	}
}

func TestRevoke_AlreadyRevokedReturnsInvalidTransition(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_, _ = a.Revoke("first", gcidAdmin)
	if _, err := a.Revoke("second", gcidAdmin); !errors.Is(err, allocation.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition on double revoke, got %v", err)
	}
}

func TestRevoke_RequiresReasonAndActor(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	if _, err := a.Revoke("", gcidAdmin); err == nil {
		t.Fatalf("expected error for empty reason")
	}
	if _, err := a.Revoke("ok", ""); err == nil {
		t.Fatalf("expected error for empty actor gcid")
	}
}

func TestRevoke_ReasonTooLongRejected(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	long := make([]byte, 600)
	for i := range long {
		long[i] = 'x'
	}
	if _, err := a.Revoke(string(long), gcidAdmin); err == nil {
		t.Fatalf("expected error for reason >512 chars")
	}
}

func TestExpire_PendingReturnsUnits(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonOnEnrollment,
	})
	res, err := a.Expire()
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if !res.UnitsReturnedToPool {
		t.Fatalf("pending → expire returns units")
	}
	if res.ReturnedUnits != 100 {
		t.Fatalf("expected 100 returned, got %d", res.ReturnedUnits)
	}
	if a.Status != allocation.StatusExpired {
		t.Fatalf("expected expired status")
	}
}

func TestExpire_ClaimedKeepsUnits(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonOnEnrollment,
	})
	_ = a.Claim("e", 30)
	res, err := a.Expire()
	if err != nil {
		t.Fatalf("Expire claimed: %v", err)
	}
	if res.UnitsReturnedToPool {
		t.Fatalf("claimed expiry must not return units")
	}
}

func TestExpire_DoubleExpireInvalid(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonOnEnrollment,
	})
	_, _ = a.Expire()
	if _, err := a.Expire(); !errors.Is(err, allocation.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// IsExpired (clock-based check)
// -----------------------------------------------------------------------------

func TestIsExpired_TrueWhenPastExpiresAt(t *testing.T) {
	t.Parallel()
	expiry := time.Now().UTC().Add(-1 * time.Hour)
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		ExpiresAt: &expiry,
	})
	if !a.IsExpired(time.Now().UTC()) {
		t.Fatalf("expected expired with past expires_at")
	}
}

func TestIsExpired_FalseWhenNoExpiry(t *testing.T) {
	t.Parallel()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	if a.IsExpired(time.Now().UTC()) {
		t.Fatalf("nil expires_at = never expires")
	}
}

func TestIsExpired_FalseWhenFutureExpiry(t *testing.T) {
	t.Parallel()
	expiry := time.Now().UTC().Add(48 * time.Hour)
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: poolA, Units: 100,
		Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		ExpiresAt: &expiry,
	})
	if a.IsExpired(time.Now().UTC()) {
		t.Fatalf("future expires_at = not yet expired")
	}
}

// -----------------------------------------------------------------------------
// Reason validation
// -----------------------------------------------------------------------------

func TestReasonIsValid(t *testing.T) {
	t.Parallel()
	for _, r := range []allocation.Reason{
		allocation.ReasonManual,
		allocation.ReasonOnEnrollment,
		allocation.ReasonEqualSplit,
		allocation.ReasonTierBased,
		allocation.ReasonPromo,
	} {
		if !r.IsValid() {
			t.Fatalf("expected reason %q valid", r)
		}
	}
	if allocation.Reason("nope").IsValid() {
		t.Fatalf("expected garbage reason invalid")
	}
}

func TestAllocation_Claim_Branches(t *testing.T) {
	t.Parallel()
	// Claim on a pending allocation with empty debit entry → error.
	a, err := allocation.New(allocation.NewParams{TenantID: "t-1", GCID: "g-1", SourcePoolID: "p-1", Units: 10, Reason: allocation.ReasonManual, AllocatedByGCID: "g-0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Claim("", 10); err == nil {
		t.Fatal("expected error for empty first debit entry")
	}
	// Claim again after claiming → idempotent nil.
	if err := a.Claim("entry-1", 10); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := a.Claim("entry-1", 10); err != nil {
		t.Fatalf("Claim(2nd) should be idempotent: %v", err)
	}
	// Claim on revoked allocation → ErrInvalidTransition.
	a2, _ := allocation.New(allocation.NewParams{TenantID: "t-2", GCID: "g-2", SourcePoolID: "p-1", Units: 5, Reason: allocation.ReasonManual, AllocatedByGCID: "g-0"})
	_, _ = a2.Revoke("no longer needed", "g-9")
	if err := a2.Claim("entry-2", 5); err == nil {
		t.Fatal("expected invalid-transition error on revoked claim")
	}
	// Claim on expired allocation.
	a3, _ := allocation.New(allocation.NewParams{TenantID: "t-3", GCID: "g-3", SourcePoolID: "p-1", Units: 5, Reason: allocation.ReasonManual, AllocatedByGCID: "g-0"})
	a3.Expire()
	if err := a3.Claim("entry-3", 5); err == nil {
		t.Fatal("expected invalid-transition error on expired claim")
	}
}
