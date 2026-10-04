// Package addon_test (lifecycle_test.go) holds the BE-T-ADD-1 unit tests
// for the Subscription state machine, Tier value-object, deactivation
// reasons, lifecycle events, usage snapshots, and pseudonymization helpers.
package addon_test

import (
	"strings"
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// Tier.Validate -------------------------------------------------------------

func TestTier_Validate_RejectsEmptyCode(t *testing.T) {
	t.Parallel()
	tier := addon.Tier{Code: "", MonthlyPriceCents: 1000, Currency: "USD"}
	if err := tier.Validate(); err == nil {
		t.Fatalf("expected error for empty code")
	}
}

func TestTier_Validate_RejectsNegativePrice(t *testing.T) {
	t.Parallel()
	tier := addon.Tier{Code: "starter", MonthlyPriceCents: -1, Currency: "USD"}
	if err := tier.Validate(); err == nil {
		t.Fatalf("expected error for negative price")
	}
}

func TestTier_Validate_RejectsBadCurrency(t *testing.T) {
	t.Parallel()
	tier := addon.Tier{Code: "starter", MonthlyPriceCents: 1000, Currency: "ZZ"}
	if err := tier.Validate(); err == nil {
		t.Fatalf("expected error for non-3-letter currency")
	}
}

func TestTier_Validate_AcceptsValid(t *testing.T) {
	t.Parallel()
	tier := addon.Tier{Code: "starter", MonthlyPriceCents: 1000, Currency: "USD"}
	if err := tier.Validate(); err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
}

// IsValidReason / IsValidProration ------------------------------------------

func TestIsValidReason(t *testing.T) {
	t.Parallel()
	for _, r := range []string{"no_longer_needed", "cost", "consolidation", "migration", "compliance", "other"} {
		if !addon.IsValidReason(r) {
			t.Fatalf("expected %q valid", r)
		}
	}
	if addon.IsValidReason("nope") {
		t.Fatalf("expected nope invalid")
	}
}

func TestIsValidProration(t *testing.T) {
	t.Parallel()
	if !addon.IsValidProration("create_prorations") {
		t.Fatalf("expected create_prorations valid")
	}
	if !addon.IsValidProration("none") {
		t.Fatalf("expected none valid")
	}
	// CHO-1786 — the H+ "Immediately" radio now sends always_invoice
	// (PR #101 widened chora-payments). The chora-tenancy proxy at
	// addon_screens_handlers.go::72 gates via this helper before
	// forwarding to chora-payments — without this entry the FE 422s.
	if !addon.IsValidProration("always_invoice") {
		t.Fatalf("expected always_invoice valid")
	}
	if addon.IsValidProration("nope") {
		t.Fatalf("expected nope invalid")
	}
}

// DeactivationRequest.Validate ----------------------------------------------

func TestDeactivationRequest_Validate_RejectsUnknownReason(t *testing.T) {
	t.Parallel()
	req := addon.DeactivationRequest{Reason: "nope"}
	if err := req.Validate(); err == nil {
		t.Fatalf("expected error for unknown reason")
	}
}

func TestDeactivationRequest_Validate_RequiresTextWhenOther(t *testing.T) {
	t.Parallel()
	req := addon.DeactivationRequest{Reason: addon.ReasonOther, ReasonText: ""}
	if err := req.Validate(); err == nil {
		t.Fatalf("expected error for other without text")
	}
	if err := req.Validate(); err != addon.ErrReasonTextRequired {
		t.Fatalf("expected ErrReasonTextRequired, got %v", err)
	}
}

func TestDeactivationRequest_Validate_AcceptsOtherWithText(t *testing.T) {
	t.Parallel()
	req := addon.DeactivationRequest{Reason: addon.ReasonOther, ReasonText: "free-text reason"}
	if err := req.Validate(); err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
}

// Subscription.IsActive -----------------------------------------------------

func TestSubscription_IsActive_PendingDeactivationStillActive(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	future := time.Now().UTC().Add(24 * time.Hour)
	if _, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason:      addon.ReasonNoLongerNeeded,
		EffectiveAt: &future,
	}); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	got, _ := reg.GetSubscription(tenantA, "tms")
	if !got.IsActive(time.Now().UTC()) {
		t.Fatalf("pending_deactivation must still count as active for entitlements")
	}
}

func TestSubscription_IsActive_DeactivatedFalse(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	if _, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason: addon.ReasonNoLongerNeeded,
	}); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	got, _ := reg.GetSubscription(tenantA, "tms")
	if got.IsActive(time.Now().UTC()) {
		t.Fatalf("deactivated subscription must not be active")
	}
}

// Deactivate error paths ----------------------------------------------------

func TestDeactivate_RejectsBaseAlwaysOn(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "base", "owner-1")
	_, _, _, err := reg.Deactivate(tenantA, "base", addon.DeactivationRequest{
		Reason: addon.ReasonNoLongerNeeded,
	})
	if err != addon.ErrCannotUnsubscribeBase {
		t.Fatalf("expected ErrCannotUnsubscribeBase, got %v", err)
	}
}

func TestDeactivate_RejectsUnknownAddOn(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, _, err := reg.Deactivate(tenantA, "no-such", addon.DeactivationRequest{
		Reason: addon.ReasonNoLongerNeeded,
	})
	if err != addon.ErrAddOnNotFound {
		t.Fatalf("expected ErrAddOnNotFound, got %v", err)
	}
}

func TestDeactivate_RejectsMissingSubscription(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason: addon.ReasonNoLongerNeeded,
	})
	if err != addon.ErrSubscriptionNotFound {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestDeactivate_AlreadyDeactivatedReturnsConflict(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{Reason: addon.ReasonCost})
	_, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{Reason: addon.ReasonCost})
	if err != addon.ErrAlreadyDeactivated {
		t.Fatalf("expected ErrAlreadyDeactivated, got %v", err)
	}
}

func TestDeactivate_ComplianceLocked(t *testing.T) {
	t.Parallel()
	// Per audit-platform-fillgaps.md §9.6 + ADR-141 the
	// governance_dashboard add-on (O+ Observability+ baseline) is
	// compliance-locked: deactivation requires super_admin override.
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "governance_dashboard", "owner-1"); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	_, _, _, err := reg.Deactivate(tenantA, "governance_dashboard", addon.DeactivationRequest{
		Reason: addon.ReasonCompliance,
	})
	if err != addon.ErrComplianceLocked {
		t.Fatalf("expected ErrComplianceLocked, got %v", err)
	}
}

func TestDeactivate_ImmediateEmitsDeactivatedEvent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	_, status, ev, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason: addon.ReasonNoLongerNeeded,
	})
	if err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	if status != "DEACTIVATED" {
		t.Fatalf("expected DEACTIVATED, got %q", status)
	}
	if ev == nil || ev.Kind != addon.EventDeactivated {
		t.Fatalf("expected lifecycle event kind=deactivated, got %+v", ev)
	}
}

func TestDeactivate_DeferredEmitsDeactivationRequestedEvent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	future := time.Now().UTC().Add(24 * time.Hour)
	_, status, ev, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason:      addon.ReasonCost,
		EffectiveAt: &future,
	})
	if err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	if status != "DEACTIVATION_SCHEDULED" {
		t.Fatalf("expected DEACTIVATION_SCHEDULED, got %q", status)
	}
	if ev == nil || ev.Kind != addon.EventDeactivationRequested {
		t.Fatalf("expected event kind=deactivation_requested, got %+v", ev)
	}
}

// ChangeTier ----------------------------------------------------------------

func TestChangeTier_UpgradePositiveDelta(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	sub, delta, kind, err := reg.ChangeTier(tenantA, "tms", "pro", "create_prorations", nil, "owner-1", 0)
	if err != nil {
		t.Fatalf("ChangeTier: %v", err)
	}
	if delta <= 0 {
		t.Fatalf("expected positive delta, got %d", delta)
	}
	if kind != addon.EventUpgraded {
		t.Fatalf("expected EventUpgraded, got %q", kind)
	}
	if sub.CurrentTier != "pro" {
		t.Fatalf("expected current_tier=pro, got %q", sub.CurrentTier)
	}
}

func TestChangeTier_DowngradeNegativeDelta(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.ChangeTier(tenantA, "tms", "enterprise", "create_prorations", nil, "owner-1", 0)
	_, delta, kind, err := reg.ChangeTier(tenantA, "tms", "starter", "create_prorations", nil, "owner-1", 0)
	if err != nil {
		t.Fatalf("ChangeTier downgrade: %v", err)
	}
	if delta >= 0 {
		t.Fatalf("expected negative delta, got %d", delta)
	}
	if kind != addon.EventDowngraded {
		t.Fatalf("expected EventDowngraded, got %q", kind)
	}
}

func TestChangeTier_NoneProrationZeroDelta(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	_, delta, _, err := reg.ChangeTier(tenantA, "tms", "pro", "none", nil, "owner-1", 0)
	if err != nil {
		t.Fatalf("ChangeTier: %v", err)
	}
	if delta != 0 {
		t.Fatalf("expected 0 delta, got %d", delta)
	}
}

func TestChangeTier_RejectsInvalidProration(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	_, _, _, err := reg.ChangeTier(tenantA, "tms", "pro", "wrong", nil, "owner-1", 0)
	if err == nil {
		t.Fatalf("expected error for invalid proration_mode")
	}
}

func TestChangeTier_RejectsUnknownTier(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	_, _, _, err := reg.ChangeTier(tenantA, "tms", "no-such-tier", "create_prorations", nil, "owner-1", 0)
	if err == nil {
		t.Fatalf("expected error for unknown tier")
	}
}

func TestChangeTier_RejectsMissingAddOn(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, _, err := reg.ChangeTier(tenantA, "no-such", "pro", "create_prorations", nil, "owner-1", 0)
	if err != addon.ErrAddOnNotFound {
		t.Fatalf("expected ErrAddOnNotFound, got %v", err)
	}
}

func TestChangeTier_RejectsMissingSubscription(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, _, err := reg.ChangeTier(tenantA, "tms", "pro", "create_prorations", nil, "owner-1", 0)
	if err != addon.ErrSubscriptionNotFound {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestChangeTier_RejectsConcurrencyMismatch(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	sub, _ := reg.Subscribe(tenantA, "tms", "owner-1")
	_, _, _, err := reg.ChangeTier(tenantA, "tms", "pro", "create_prorations", nil, "owner-1", sub.Version+99)
	if err != addon.ErrConcurrencyConflict {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestChangeTier_RejectsIneligibleStatus(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	future := time.Now().UTC().Add(24 * time.Hour)
	reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason:      addon.ReasonCost,
		EffectiveAt: &future,
	})
	_, _, _, err := reg.ChangeTier(tenantA, "tms", "pro", "create_prorations", nil, "owner-1", 0)
	if err != addon.ErrIneligibleForChange {
		t.Fatalf("expected ErrIneligibleForChange, got %v", err)
	}
}

func TestChangeTier_RecordsLifecycleEvent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.ChangeTier(tenantA, "tms", "pro", "create_prorations", nil, "owner-1", 0)
	events := reg.ListEvents(tenantA, "tms")
	hasUpgrade := false
	for _, ev := range events {
		if ev.Kind == addon.EventUpgraded {
			hasUpgrade = true
			if ev.FromTier != "starter" || ev.ToTier != "pro" {
				t.Fatalf("expected from=starter to=pro, got %q→%q", ev.FromTier, ev.ToTier)
			}
		}
	}
	if !hasUpgrade {
		t.Fatalf("expected EventUpgraded recorded")
	}
}

// ListEvents / GetSubscription / RecordUsage --------------------------------

func TestListEvents_AllForTenant(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.Subscribe(tenantA, "cms", "owner-1")
	all := reg.ListEvents(tenantA, "")
	if len(all) < 2 {
		t.Fatalf("expected ≥2 events, got %d", len(all))
	}
}

func TestGetSubscription_HitAndMiss(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	if _, ok := reg.GetSubscription(tenantA, "tms"); !ok {
		t.Fatalf("expected hit")
	}
	if _, ok := reg.GetSubscription(tenantA, "no-such"); ok {
		t.Fatalf("expected miss")
	}
}

func TestRecordUsage_AndListUsageInRange(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	a, _ := cat.Get("tms")
	d1 := time.Now().UTC().Add(-3 * 24 * time.Hour)
	d2 := time.Now().UTC().Add(-2 * 24 * time.Hour)
	d3 := time.Now().UTC().Add(-1 * 24 * time.Hour)
	for _, d := range []time.Time{d1, d2, d3} {
		reg.RecordUsage(&addon.UsageSnapshot{
			TenantID: tenantA, AddOnID: a.ID, BucketDate: d,
			SeatsUsed: 5, SeatsUsedPeak: 7, QuotaConsumed: 100,
		})
	}
	got := reg.ListUsage(tenantA, a.ID, d1.Add(-time.Hour), d3.Add(time.Hour))
	if len(got) != 3 {
		t.Fatalf("expected 3 snapshots in range, got %d", len(got))
	}
	got2 := reg.ListUsage(tenantA, a.ID, d1.Add(-time.Hour), d3)
	if len(got2) != 2 {
		t.Fatalf("expected 2 snapshots with d3 excluded, got %d", len(got2))
	}
}

func TestRecordUsage_NilNoOp(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.RecordUsage(nil)
}

// PseudonymiseGCID ----------------------------------------------------------

func TestPseudonymiseGCID_DeterministicWithinWindow(t *testing.T) {
	t.Parallel()
	a := addon.PseudonymiseGCID("tenantA", "2026-05-01_2026-06-01", "gcid-1")
	b := addon.PseudonymiseGCID("tenantA", "2026-05-01_2026-06-01", "gcid-1")
	if a != b {
		t.Fatalf("expected deterministic pseudonym within window, got %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "u_") {
		t.Fatalf("expected u_-prefixed pseudonym, got %q", a)
	}
}

func TestPseudonymiseGCID_DiffersAcrossTenants(t *testing.T) {
	t.Parallel()
	a := addon.PseudonymiseGCID("tenantA", "w1", "gcid-1")
	b := addon.PseudonymiseGCID("tenantB", "w1", "gcid-1")
	if a == b {
		t.Fatalf("expected different pseudonyms for different tenants")
	}
}

func TestPseudonymiseGCID_DiffersAcrossWindows(t *testing.T) {
	t.Parallel()
	a := addon.PseudonymiseGCID("tenantA", "w1", "gcid-1")
	b := addon.PseudonymiseGCID("tenantA", "w2", "gcid-1")
	if a == b {
		t.Fatalf("expected different pseudonyms for different windows (rotation)")
	}
}

func TestPseudonymiseGCID_NeverReturnsRawGCID(t *testing.T) {
	t.Parallel()
	gcid := "gcid-very-secret-12345"
	got := addon.PseudonymiseGCID("tenantA", "w1", gcid)
	if strings.Contains(got, gcid) {
		t.Fatalf("pseudonym leaked raw GCID: %q", got)
	}
}
