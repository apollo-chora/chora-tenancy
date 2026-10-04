// Package addon_test holds the RED-phase TDD specs for the Composable
// Add-On Plans sub-domain inside chora-tenancy.
//
// Aligned with:
//   - CLAUDE.md §1 — Composable Add-On Plans (NOT tier pricing)
//   - Sequel Act 4 P2 — "☑ Base ☑ TMS ☑ CMS ☐ PvP Arena ☐ Familiar ☑ Campus Ops"
//
// Catalogue contract (post-Phyllis-MVP refresh, audit 2026-05-09 §9):
//   - 13 seeded add-ons. Twelve are the Phyllis MVP scope: base, tms, cms,
//     pvp-arena, familiar, campus-ops, marketplace, ai_assist, daily_dose,
//     kg_hexagonal, course_application, governance_dashboard. The thirteenth,
//     `cplus_social`, was added later by CHO-1812 under ADR-182 D5.
//   - `singpass-federation` REMOVED from seed (Premature; deferred to
//     post-MVP catalog refresh per audit-platform-fillgaps.md §9.6)
//   - TWO are always-on at monthly_price_cents=0 and cannot be unsubscribed:
//     `base`, and `cplus_social` (entitled to the public chora-master tenant
//     by default). The other 11 each have a non-zero monthly price
//   - `governance_dashboard` is compliance-locked (cannot be deactivated
//     except via super_admin override per ADR-141)
//   - Subscribe is idempotent
//   - Unsubscribe sets a 30-day grace window (does not immediately revoke
//     access; downstream Stripe + access decisions consult IsActive())
package addon_test

import (
	"errors"
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

const (
	tenantA = "01970000-0000-7000-8000-0000000000aa"
)

// -----------------------------------------------------------------------------
// Catalogue (in-memory seed)
// -----------------------------------------------------------------------------

// The seed catalogue is a SCOPE guard, not a counter. Its whole reason for
// existing is the singpass-federation case below: an add-on seeded ahead of the
// milestone that ships it. A bare length assertion catches that, but it reports
// "got 13, want 12" and names nothing, so the reader cannot tell a lawful
// addition from a premature seed without diffing the catalogue by hand. That is
// how it came to be red on main: cplus_social was added lawfully by CHO-1812
// under ADR-182 D5 (entitled to the public chora-master tenant by default), and
// the count was never moved with it.
//
// Asserting the SET instead makes both directions name the add-on: a new one fails
// as "unexpected: <code>", a dropped one as "missing: <code>". Extending this
// list is the deliberate act of ratifying a catalogue change.
func TestSeedCatalogue_SeedsExactlyTheRatifiedAddOns(t *testing.T) {
	t.Parallel()
	want := map[string]bool{
		// Original Phyllis MVP scope (audit-platform-fillgaps.md §9.6).
		"base": true, "tms": true, "cms": true, "pvp-arena": true,
		"familiar": true, "campus-ops": true, "marketplace": true,
		"ai_assist": true, "daily_dose": true, "kg_hexagonal": true,
		"course_application": true, "governance_dashboard": true,
		// Added by CHO-1812 under ADR-182 D5.
		"cplus_social": true,
	}

	got := map[string]bool{}
	for _, a := range addon.NewSeedCatalogue().List() {
		got[a.Code] = true
	}

	for code := range want {
		if !got[code] {
			t.Errorf("missing seeded add-on %q: it was ratified and the catalogue dropped it", code)
		}
	}
	for code := range got {
		if !want[code] {
			t.Errorf("unexpected seeded add-on %q: either it is premature (the singpass-federation "+
				"case) or it is lawful and this list must be extended in the same commit", code)
		}
	}
	if len(got) != len(want) {
		t.Errorf("seeded add-on count = %d, want %d", len(got), len(want))
	}
}

// Phyllis MVP scope per audit-platform-fillgaps.md §9.6 — the 5 new MVP
// codes must exist in the seed catalogue (replacing prematurely-seeded
// singpass-federation).
func TestSeedCatalogue_PhyllisMVPCodesPresent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	for _, code := range []string{
		"ai_assist", "daily_dose", "kg_hexagonal",
		"course_application", "governance_dashboard",
	} {
		if _, ok := cat.Get(code); !ok {
			t.Fatalf("expected Phyllis MVP add-on %q in seed catalogue", code)
		}
	}
}

// Inverse — singpass-federation removed per audit-platform-fillgaps.md §9.6.
func TestSeedCatalogue_SingpassFederationRemoved(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	if _, ok := cat.Get("singpass-federation"); ok {
		t.Fatalf("singpass-federation should not be in seed catalogue (deferred to post-MVP refresh)")
	}
}

// governance_dashboard is the O+ baseline IMDA evidence dashboard add-on.
// Per ADR-141 it is compliance-locked: cannot be deactivated except via
// super_admin override.
func TestSeedCatalogue_GovernanceDashboardComplianceLocked(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	gd, ok := cat.Get("governance_dashboard")
	if !ok {
		t.Fatalf("expected governance_dashboard in seed catalogue")
	}
	if !gd.ComplianceLocked {
		t.Fatalf("expected governance_dashboard.ComplianceLocked=true")
	}
}

func TestSeedCatalogue_HasBaseAlwaysOnAtZero(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	base, ok := cat.Get("base")
	if !ok {
		t.Fatalf("expected base add-on in seed catalogue")
	}
	if base.MonthlyPriceCents != 0 {
		t.Fatalf("expected base price=0, got %d", base.MonthlyPriceCents)
	}
	if !base.AlwaysOn {
		t.Fatalf("expected base AlwaysOn=true")
	}
}

func TestSeedCatalogue_OtherAddOnsHavePositivePrices(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	for _, id := range []string{
		"tms", "cms", "pvp-arena", "familiar", "campus-ops", "marketplace",
		"ai_assist", "daily_dose", "kg_hexagonal",
		"course_application", "governance_dashboard",
	} {
		a, ok := cat.Get(id)
		if !ok {
			t.Fatalf("expected add-on %q in seed catalogue", id)
		}
		if a.MonthlyPriceCents <= 0 {
			t.Fatalf("expected positive price for %q, got %d", id, a.MonthlyPriceCents)
		}
		if a.AlwaysOn {
			t.Fatalf("only base should be AlwaysOn; %q is not", id)
		}
	}
}

func TestSeedCatalogue_GetByID(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	if _, ok := cat.Get("does-not-exist"); ok {
		t.Fatalf("expected miss for unknown add-on")
	}
}

// -----------------------------------------------------------------------------
// Subscription registry — Composability + idempotency + base-protection
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// SubscribePending — Setup Wizard step 2 (CHO-1664)
//
// The wizard records the tenant's "declared intent" — the user has
// ticked the add-ons they want, but no billing/activation has happened
// yet. SubscribePending creates rows in StatusPendingActivation, so a
// later activation flow (billing webhook, admin approval, etc.) can
// transition them to StatusActive via the existing Subscribe call. The
// `pending_activation` enum value was already defined on the Status
// type (and in the SQL migration) but had no domain emitter until now.
// -----------------------------------------------------------------------------

func TestSubscriptionRegistry_SubscribePending_CreatesRowInPendingActivation(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	sub, err := reg.SubscribePending(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("SubscribePending: unexpected error: %v", err)
	}
	if sub.Status != addon.StatusPendingActivation {
		t.Errorf("Status = %q; want %q", sub.Status, addon.StatusPendingActivation)
	}
	if sub.TenantID != tenantA {
		t.Errorf("TenantID = %q; want %q", sub.TenantID, tenantA)
	}
	if sub.AddOnCode != "tms" {
		t.Errorf("AddOnCode = %q; want tms", sub.AddOnCode)
	}
	if sub.OwnerGCID != "owner-1" {
		t.Errorf("OwnerGCID = %q; want owner-1", sub.OwnerGCID)
	}
}

func TestSubscriptionRegistry_SubscribePending_IsIdempotent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	first, err := reg.SubscribePending(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("SubscribePending #1: %v", err)
	}
	second, err := reg.SubscribePending(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("SubscribePending #2 (idempotent): %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected same subscription ID on idempotent SubscribePending; got %s vs %s", first.ID, second.ID)
	}
	if second.Status != addon.StatusPendingActivation {
		t.Errorf("idempotent call must NOT silently activate; Status = %q", second.Status)
	}
}

func TestSubscriptionRegistry_SubscribePending_DoesNotDemoteActive(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	// Pre-existing active subscription via the regular Subscribe path.
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("seed Subscribe: %v", err)
	}
	// Wizard re-runs and calls SubscribePending for the same code. The
	// already-active subscription MUST stay active — pending is a
	// declared-intent state for net-new subscriptions only, never a
	// rollback for active ones.
	sub, err := reg.SubscribePending(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("SubscribePending on already-active: %v", err)
	}
	if sub.Status != addon.StatusActive {
		t.Errorf("SubscribePending on Active must return the active row unchanged; Status = %q", sub.Status)
	}
}

func TestSubscriptionRegistry_SubscribePending_RejectsUnknownAddOn(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, err := reg.SubscribePending(tenantA, "definitely_not_a_code", "owner-1")
	if err != addon.ErrAddOnNotFound {
		t.Errorf("expected ErrAddOnNotFound; got %v", err)
	}
}

func TestSubscriptionRegistry_SubscribePending_RejectsEmptyTenantOrCode(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.SubscribePending("", "tms", "owner-1"); err == nil {
		t.Errorf("expected error for empty tenant")
	}
	if _, err := reg.SubscribePending(tenantA, "", "owner-1"); err == nil {
		t.Errorf("expected error for empty code")
	}
}

func TestSubscriptionRegistry_Subscribe_IsIdempotent(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	first, err := reg.Subscribe(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("Subscribe #1: unexpected error: %v", err)
	}
	second, err := reg.Subscribe(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("Subscribe #2 (idempotent): unexpected error: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected same subscription ID on idempotent subscribe")
	}
}

// Composability: tenant can hold ALL 8 add-ons simultaneously (no tier).
func TestSubscriptionRegistry_Composability_TenantCanHoldAll(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	// Phyllis MVP scope: 12 seeded add-ons (post-2026-05-09 catalog refresh).
	ids := []string{
		"base", "tms", "cms", "pvp-arena", "familiar", "campus-ops", "marketplace",
		"ai_assist", "daily_dose", "kg_hexagonal",
		"course_application", "governance_dashboard",
	}
	for _, id := range ids {
		if _, err := reg.Subscribe(tenantA, id, "owner-1"); err != nil {
			t.Fatalf("Subscribe %q: unexpected error: %v", id, err)
		}
	}
	active := reg.ListActiveByTenant(tenantA)
	if len(active) != 12 {
		t.Fatalf("expected 12 active subs (composability), got %d", len(active))
	}
}

func TestSubscriptionRegistry_Subscribe_RejectsUnknownAddOn(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "nope", "owner-1"); err == nil {
		t.Fatalf("expected error for unknown add-on")
	}
	if _, err := reg.Subscribe(tenantA, "nope", "owner-1"); err != addon.ErrAddOnNotFound {
		t.Fatalf("expected ErrAddOnNotFound")
	}
}

// Base is always-on; cannot be unsubscribed.
func TestSubscriptionRegistry_Unsubscribe_BaseRejected(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "base", "owner-1")
	if err := reg.Unsubscribe(tenantA, "base", "owner-1"); err == nil {
		t.Fatalf("expected error for unsubscribe of base")
	}
	if err := reg.Unsubscribe(tenantA, "base", "owner-1"); err != addon.ErrCannotUnsubscribeBase {
		t.Fatalf("expected ErrCannotUnsubscribeBase")
	}
}

// Unsubscribe sets a grace window — IsActive() remains true until grace expires.
func TestSubscriptionRegistry_Unsubscribe_GracePeriod(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	if err := reg.Unsubscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Unsubscribe: unexpected error: %v", err)
	}
	subs := reg.ListAllByTenant(tenantA)
	if len(subs) != 1 {
		t.Fatalf("expected 1 sub still on record (grace), got %d", len(subs))
	}
	sub := subs[0]
	if sub.Status != addon.StatusGracePeriod {
		t.Fatalf("expected grace_period status, got %q", sub.Status)
	}
	if sub.GraceEndsAt == nil {
		t.Fatalf("expected grace_ends_at set")
	}
	// Grace window is exactly 30 days from cancellation.
	if sub.GraceEndsAt.Sub(*sub.CancelledAt) < 29*24*time.Hour {
		t.Fatalf("expected ~30 day grace, got %v", sub.GraceEndsAt.Sub(*sub.CancelledAt))
	}
	if !sub.IsActive(time.Now().UTC()) {
		t.Fatalf("expected sub still active during grace window")
	}
}

// After grace expires, IsActive() returns false.
func TestSubscription_IsActive_FalseAfterGrace(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.Unsubscribe(tenantA, "tms", "owner-1")
	subs := reg.ListAllByTenant(tenantA)
	if len(subs) != 1 {
		t.Fatalf("expected 1 sub, got %d", len(subs))
	}
	future := subs[0].GraceEndsAt.Add(1 * time.Hour)
	if subs[0].IsActive(future) {
		t.Fatalf("expected sub inactive after grace expiry")
	}
}

// Re-subscribing during grace returns to active.
func TestSubscriptionRegistry_Subscribe_ReactivatesDuringGrace(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	first, _ := reg.Subscribe(tenantA, "tms", "owner-1")
	reg.Unsubscribe(tenantA, "tms", "owner-1")
	second, err := reg.Subscribe(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("Re-subscribe: unexpected error: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected same sub ID on grace-period reactivation")
	}
	if second.Status != addon.StatusActive {
		t.Fatalf("expected active status after re-subscribe, got %q", second.Status)
	}
	if second.CancelledAt != nil {
		t.Fatalf("expected cancelled_at cleared on reactivate")
	}
	if second.GraceEndsAt != nil {
		t.Fatalf("expected grace_ends_at cleared on reactivate")
	}
}

// Unsubscribe of a non-existent subscription is a 404.
func TestSubscriptionRegistry_Unsubscribe_UnknownReturns404(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if err := reg.Unsubscribe(tenantA, "tms", "owner-1"); err != addon.ErrSubscriptionNotFound {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

// Subscribe rejects empty inputs.
func TestSubscriptionRegistry_Subscribe_RejectsEmptyTenantOrAddon(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe("", "tms", "owner-1"); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
	if _, err := reg.Subscribe(tenantA, "", "owner-1"); err == nil {
		t.Fatalf("expected error for empty add-on id")
	}
}

// ListActiveByTenant filters out grace-expired subscriptions.
func TestSubscriptionRegistry_ListActive_FiltersGraceExpired(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	reg.Subscribe(tenantA, "tms", "owner-1")
	reg.Subscribe(tenantA, "cms", "owner-1")
	reg.Unsubscribe(tenantA, "cms", "owner-1") // still in grace
	active := reg.ListActiveByTenant(tenantA)
	if len(active) != 2 {
		t.Fatalf("expected 2 active subs (cms in grace still counts), got %d", len(active))
	}
	// Now jump time forward via SetClock and re-list.
	reg.SetClock(func() time.Time { return time.Now().UTC().Add(60 * 24 * time.Hour) })
	active = reg.ListActiveByTenant(tenantA)
	if len(active) != 1 {
		t.Fatalf("expected 1 active sub after grace expiry, got %d", len(active))
	}
}

// -----------------------------------------------------------------------------
// CHO-1776 — MarkPastDue / MarkRecovered dunning-state mutators.
// -----------------------------------------------------------------------------

func TestSubscriptionRegistry_MarkPastDue_FlipsOnActive(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.Subscribe("t1", "kg_hexagonal", "g1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub, err := reg.MarkPastDue("t1", "kg_hexagonal")
	if err != nil {
		t.Fatalf("MarkPastDue: %v", err)
	}
	if !sub.PastDue {
		t.Errorf("PastDue = false; want true")
	}
}

func TestSubscriptionRegistry_MarkPastDue_IdempotentOnAlreadyPastDue(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.Subscribe("t1", "kg_hexagonal", "g1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := reg.MarkPastDue("t1", "kg_hexagonal"); err != nil {
		t.Fatalf("first MarkPastDue: %v", err)
	}
	sub, err := reg.MarkPastDue("t1", "kg_hexagonal")
	if err != nil {
		t.Errorf("second MarkPastDue should be idempotent no-op: %v", err)
	}
	if !sub.PastDue {
		t.Errorf("PastDue regressed to false")
	}
}

func TestSubscriptionRegistry_MarkPastDue_RejectsUnknownTenant(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.MarkPastDue("missing", "kg_hexagonal"); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Errorf("err = %v; want ErrSubscriptionNotFound", err)
	}
}

func TestSubscriptionRegistry_MarkRecovered_ClearsPastDue(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.Subscribe("t1", "kg_hexagonal", "g1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := reg.MarkPastDue("t1", "kg_hexagonal"); err != nil {
		t.Fatalf("MarkPastDue: %v", err)
	}
	sub, err := reg.MarkRecovered("t1", "kg_hexagonal")
	if err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}
	if sub.PastDue {
		t.Errorf("PastDue = true; want false after recovery")
	}
}

func TestSubscriptionRegistry_MarkRecovered_IdempotentOnAlreadyClear(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.Subscribe("t1", "kg_hexagonal", "g1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub, err := reg.MarkRecovered("t1", "kg_hexagonal")
	if err != nil {
		t.Errorf("MarkRecovered on never-past_due should be a no-op: %v", err)
	}
	if sub.PastDue {
		t.Errorf("PastDue spuriously flipped to true")
	}
}

func TestSubscriptionRegistry_MarkRecovered_RejectsUnknownTenant(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if _, err := reg.MarkRecovered("missing", "kg_hexagonal"); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Errorf("err = %v; want ErrSubscriptionNotFound", err)
	}
}
