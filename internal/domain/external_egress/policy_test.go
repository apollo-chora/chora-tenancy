// Package external_egress_test holds the RED-phase TDD specs for the
// TenantExternalEgressPolicy aggregate (CHO-2148).
//
// Aggregate invariants:
//
//  1. One policy per tenant (tenant_id is the identity).
//
//  2. DEFAULT-DENY. A tenant that has never opted in has NO row, and reading
//     one MUST yield a fail-closed default (egress_enabled = false) without
//     fabricating a persisted row. This is the ADR-220 D4 "franchise default
//     OFF" posture expressed in the domain: absence means OFF, never "unknown".
//
//  3. Every mutation names its actor (GCID). External web egress is a
//     governance-sensitive decision — an unattributed change is refused
//     (PLAN.md §4.2.4 safety net b; IMDA D1 accountability).
//
//  4. Version is monotonic and increments ONLY on a real change. It is the
//     stale-event guard the chora-observability projection uses to reject an
//     out-of-order delivery, so it must never move backwards and must never
//     move without a corresponding change.
//
//  5. An identical write is a NO-OP — no version bump, no event, no audit row.
//     The audit trail records real decisions only.
package external_egress_test

import (
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

const (
	testTenantID = "11111111-1111-7111-8111-111111111111"
	testActor    = "00000000-0000-7000-8000-000000001999"
)

func testNow() time.Time { return time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC) }

// -----------------------------------------------------------------------------
// DefaultPolicy — the fail-closed default (invariant 2)
// -----------------------------------------------------------------------------

func TestDefaultPolicy_IsEgressDisabled(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	if p.EgressEnabled {
		t.Fatal("DefaultPolicy must be egress-DISABLED (ADR-220 D4: no row => OFF, fail-closed)")
	}
	if p.TenantID != testTenantID {
		t.Errorf("TenantID = %q, want %q", p.TenantID, testTenantID)
	}
}

// A never-opted-in tenant must be distinguishable from a persisted, explicitly
// disabled one — the projection's monotonic guard depends on version 0 meaning
// "no row yet".
func TestDefaultPolicy_IsUnpersisted_VersionZero(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	if p.Version != 0 {
		t.Errorf("DefaultPolicy Version = %d, want 0 (0 signals 'never persisted')", p.Version)
	}
	if p.Persisted() {
		t.Error("DefaultPolicy must report Persisted() == false")
	}
}

func TestDefaultPolicy_CarriesDefaultCeiling(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	if p.DailyCallCeiling != external_egress.DefaultDailyCallCeiling {
		t.Errorf("DailyCallCeiling = %d, want %d",
			p.DailyCallCeiling, external_egress.DefaultDailyCallCeiling)
	}
}

// -----------------------------------------------------------------------------
// Update — validation (invariants 3, 4)
// -----------------------------------------------------------------------------

func TestUpdate_OptIn_SetsFieldsAndBumpsVersion(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	changed, err := p.Update(true, 50, testActor, testNow())
	if err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("opting a default-OFF tenant IN must report changed == true")
	}
	if !p.EgressEnabled {
		t.Error("EgressEnabled = false, want true after opt-in")
	}
	if p.DailyCallCeiling != 50 {
		t.Errorf("DailyCallCeiling = %d, want 50", p.DailyCallCeiling)
	}
	if p.Version != 1 {
		t.Errorf("Version = %d, want 1 (first persisted version)", p.Version)
	}
	if p.UpdatedByGCID != testActor {
		t.Errorf("UpdatedByGCID = %q, want %q (every change names its actor)", p.UpdatedByGCID, testActor)
	}
	if !p.UpdatedAt.Equal(testNow()) {
		t.Errorf("UpdatedAt = %v, want %v", p.UpdatedAt, testNow())
	}
	if !p.Persisted() {
		t.Error("after a real change the policy must report Persisted() == true")
	}
}

func TestUpdate_RejectsEmptyActor(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	_, err := p.Update(true, 50, "", testNow())
	if !errors.Is(err, external_egress.ErrEmptyActor) {
		t.Fatalf("Update with empty actor: err = %v, want ErrEmptyActor "+
			"(an unattributed egress change is refused — IMDA D1)", err)
	}
	if p.EgressEnabled {
		t.Error("a rejected Update must not mutate the aggregate")
	}
	if p.Version != 0 {
		t.Errorf("a rejected Update must not bump Version (got %d)", p.Version)
	}
}

func TestUpdate_RejectsNegativeCeiling(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	_, err := p.Update(true, -1, testActor, testNow())
	if !errors.Is(err, external_egress.ErrCeilingOutOfRange) {
		t.Fatalf("Update with ceiling -1: err = %v, want ErrCeilingOutOfRange", err)
	}
}

func TestUpdate_RejectsCeilingAboveHardMax(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	_, err := p.Update(true, external_egress.MaxDailyCallCeiling+1, testActor, testNow())
	if !errors.Is(err, external_egress.ErrCeilingOutOfRange) {
		t.Fatalf("Update with ceiling above the platform hard max: err = %v, want ErrCeilingOutOfRange "+
			"(the ceiling caps a PRICED external-egress call — an unbounded ceiling is a cost hole)", err)
	}
}

func TestUpdate_AllowsZeroCeiling(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	// A zero ceiling is a legitimate "enabled but budgeted to nothing today"
	// state — the gateway's per-day counter denies at 0. It is NOT an error.
	if _, err := p.Update(true, 0, testActor, testNow()); err != nil {
		t.Fatalf("Update with ceiling 0: unexpected error: %v", err)
	}
	if p.DailyCallCeiling != 0 {
		t.Errorf("DailyCallCeiling = %d, want 0", p.DailyCallCeiling)
	}
}

func TestUpdate_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy("")

	_, err := p.Update(true, 50, testActor, testNow())
	if !errors.Is(err, external_egress.ErrEmptyTenantID) {
		t.Fatalf("Update on a policy with no tenant: err = %v, want ErrEmptyTenantID", err)
	}
}

// -----------------------------------------------------------------------------
// Update — no-op semantics (invariant 5)
// -----------------------------------------------------------------------------

func TestUpdate_IdenticalWrite_IsNoOp(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)
	if _, err := p.Update(true, 50, testActor, testNow()); err != nil {
		t.Fatalf("seed Update: %v", err)
	}
	versionAfterSeed := p.Version

	later := testNow().Add(time.Hour)
	changed, err := p.Update(true, 50, testActor, later)
	if err != nil {
		t.Fatalf("idempotent Update: unexpected error: %v", err)
	}
	if changed {
		t.Fatal("an identical write must report changed == false (no event, no audit row)")
	}
	if p.Version != versionAfterSeed {
		t.Errorf("Version = %d, want %d — a no-op must NOT bump the version "+
			"(it is the projection's stale-event guard)", p.Version, versionAfterSeed)
	}
	if !p.UpdatedAt.Equal(testNow()) {
		t.Errorf("UpdatedAt = %v, want it unchanged at %v on a no-op", p.UpdatedAt, testNow())
	}
}

func TestUpdate_CeilingOnlyChange_IsAChange(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)
	if _, err := p.Update(true, 50, testActor, testNow()); err != nil {
		t.Fatalf("seed Update: %v", err)
	}

	changed, err := p.Update(true, 25, testActor, testNow().Add(time.Hour))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !changed {
		t.Fatal("a ceiling-only change must report changed == true")
	}
	if p.Version != 2 {
		t.Errorf("Version = %d, want 2", p.Version)
	}
}

// Opting OUT is the safety-critical direction — it must always be recorded.
func TestUpdate_OptOut_IsAChangeAndBumpsVersion(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)
	if _, err := p.Update(true, 50, testActor, testNow()); err != nil {
		t.Fatalf("seed Update: %v", err)
	}

	changed, err := p.Update(false, 50, testActor, testNow().Add(time.Hour))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !changed {
		t.Fatal("opting OUT must report changed == true")
	}
	if p.EgressEnabled {
		t.Error("EgressEnabled = true, want false after opt-out")
	}
	if p.Version != 2 {
		t.Errorf("Version = %d, want 2 (monotonic)", p.Version)
	}
}

// -----------------------------------------------------------------------------
// Version monotonicity (invariant 4) — the projection depends on this
// -----------------------------------------------------------------------------

func TestUpdate_VersionIsStrictlyMonotonicAcrossChanges(t *testing.T) {
	t.Parallel()
	p := external_egress.DefaultPolicy(testTenantID)

	prev := p.Version
	for i, enabled := range []bool{true, false, true, false} {
		changed, err := p.Update(enabled, int32(10+i), testActor, testNow())
		if err != nil {
			t.Fatalf("Update #%d: %v", i, err)
		}
		if !changed {
			t.Fatalf("Update #%d should have changed", i)
		}
		if p.Version <= prev {
			t.Fatalf("Update #%d: Version = %d, must be strictly greater than previous %d",
				i, p.Version, prev)
		}
		prev = p.Version
	}
}
