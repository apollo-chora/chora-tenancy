// promote_scheduled_tier_test.go — CHO-1779.
//
// Pins SubscriptionRegistry.PromoteScheduledTier — the registry hook
// chora-tenancy's payments_subscriber calls when Stripe fires
// `subscription_schedule.released` at cycle anchor. Flips CurrentTier
// ← ScheduledTier + clears the schedule fields + records the
// upgraded/downgraded lifecycle event.
package addon_test

import (
	"errors"
	"testing"
	"time"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

func seedWithSchedule(t *testing.T, scheduledTier string) *addon.SubscriptionRegistry {
	t.Helper()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// The seed catalogue ships TMS with `starter` as the default tier
	// — that's what Subscribe pins. Stage a pending end-of-cycle change.
	when := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	if _, err := reg.SetScheduledTier(tenantA, "tms", scheduledTier, when); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}
	return reg
}

func TestPromoteScheduledTier_UpgradePath(t *testing.T) {
	t.Parallel()
	reg := seedWithSchedule(t, "pro")
	before, _ := reg.GetSubscription(tenantA, "tms")
	if before.CurrentTier != "starter" {
		t.Fatalf("pre-condition: CurrentTier=%q, want starter", before.CurrentTier)
	}
	// Capture pre-state by value — registry returns a pointer that the
	// promote call will mutate in place.
	preVersion := before.Version
	prePriceSnapshot := before.MonthlyPriceCentsSnapshot

	sub, kind, err := reg.PromoteScheduledTier(tenantA, "tms")
	if err != nil {
		t.Fatalf("PromoteScheduledTier: %v", err)
	}
	if kind != addon.EventUpgraded {
		t.Errorf("kind = %q, want upgraded", kind)
	}
	if sub.CurrentTier != "pro" {
		t.Errorf("CurrentTier = %q, want pro", sub.CurrentTier)
	}
	if sub.ScheduledTier != "" {
		t.Errorf("ScheduledTier = %q, want empty after promote", sub.ScheduledTier)
	}
	if sub.ScheduledEffectiveAt != nil {
		t.Errorf("ScheduledEffectiveAt = %v, want nil after promote", sub.ScheduledEffectiveAt)
	}
	if sub.Version <= preVersion {
		t.Errorf("Version = %d, want > %d (promotion should bump version)", sub.Version, preVersion)
	}
	if sub.MonthlyPriceCentsSnapshot == prePriceSnapshot {
		t.Errorf("MonthlyPriceCentsSnapshot = %d, want != %d (pro tier has different price)",
			sub.MonthlyPriceCentsSnapshot, prePriceSnapshot)
	}
}

func TestPromoteScheduledTier_DowngradePath(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Immediate-upgrade to pro first so the downgrade has somewhere to go.
	if _, _, _, err := reg.ChangeTier(tenantA, "tms", "pro", "none", nil, "owner-1", 0); err != nil {
		t.Fatalf("ChangeTier: %v", err)
	}
	// Now schedule a downgrade back to starter.
	when := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	if _, err := reg.SetScheduledTier(tenantA, "tms", "starter", when); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}

	sub, kind, err := reg.PromoteScheduledTier(tenantA, "tms")
	if err != nil {
		t.Fatalf("PromoteScheduledTier: %v", err)
	}
	if kind != addon.EventDowngraded {
		t.Errorf("kind = %q, want downgraded", kind)
	}
	if sub.CurrentTier != "starter" {
		t.Errorf("CurrentTier = %q, want starter", sub.CurrentTier)
	}
}

func TestPromoteScheduledTier_RecordsLifecycleEvent(t *testing.T) {
	t.Parallel()
	reg := seedWithSchedule(t, "pro")
	preCount := len(reg.ListEvents(tenantA, "tms"))
	if _, _, err := reg.PromoteScheduledTier(tenantA, "tms"); err != nil {
		t.Fatalf("PromoteScheduledTier: %v", err)
	}
	events := reg.ListEvents(tenantA, "tms")
	if len(events) != preCount+1 {
		t.Fatalf("event count = %d, want %d (one new event from promote)", len(events), preCount+1)
	}
	// Find the upgrade event — ListEvents sorts newest-first but
	// activation + promote can land in the same nano-second in tests
	// (sort.Slice isn't stable), so locate by Kind rather than position.
	var upgrade *addon.LifecycleEvent
	for _, ev := range events {
		if ev.Kind == addon.EventUpgraded {
			upgrade = ev
			break
		}
	}
	if upgrade == nil {
		t.Fatal("no upgraded event in the lifecycle log")
	}
	if upgrade.FromTier != "starter" {
		t.Errorf("FromTier = %q, want starter", upgrade.FromTier)
	}
	if upgrade.ToTier != "pro" {
		t.Errorf("ToTier = %q, want pro", upgrade.ToTier)
	}
}

func TestPromoteScheduledTier_NoOpWhenNoSchedule(t *testing.T) {
	// Idempotent — duplicate Stripe webhook delivery should be safe.
	// First call promotes + clears; second call sees no pending schedule
	// and returns successfully without touching CurrentTier or the
	// lifecycle event log.
	t.Parallel()
	reg := seedWithSchedule(t, "pro")
	if _, _, err := reg.PromoteScheduledTier(tenantA, "tms"); err != nil {
		t.Fatalf("first PromoteScheduledTier: %v", err)
	}
	preEvents := len(reg.ListEvents(tenantA, "tms"))

	sub, kind, err := reg.PromoteScheduledTier(tenantA, "tms")
	if err != nil {
		t.Errorf("duplicate PromoteScheduledTier returned err: %v", err)
	}
	if sub.CurrentTier != "pro" {
		t.Errorf("CurrentTier mutated on duplicate; got %q", sub.CurrentTier)
	}
	if kind != "" {
		t.Errorf("duplicate kind = %q, want empty (no event recorded)", kind)
	}
	postEvents := len(reg.ListEvents(tenantA, "tms"))
	if postEvents != preEvents {
		t.Errorf("duplicate Promote recorded a new lifecycle event (was %d, now %d)", preEvents, postEvents)
	}
}

func TestPromoteScheduledTier_MissingSubscription(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	// No Subscribe call.
	_, _, err := reg.PromoteScheduledTier(tenantA, "tms")
	if !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Errorf("err = %v, want ErrSubscriptionNotFound", err)
	}
}

func TestPromoteScheduledTier_InvalidScheduledTierFamily(t *testing.T) {
	// Defensive: if SetScheduledTier somehow stashed a tier code that
	// isn't in the addon's catalogue (only possible via a buggy direct
	// write or a catalogue change between schedule + release), promote
	// fails-loud rather than silently picking a random snapshot price.
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	when := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	if _, err := reg.SetScheduledTier(tenantA, "tms", "bogus-tier", when); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}
	_, _, err := reg.PromoteScheduledTier(tenantA, "tms")
	if !errors.Is(err, addon.ErrTierFamilyMismatch) {
		t.Errorf("err = %v, want ErrTierFamilyMismatch", err)
	}
}
