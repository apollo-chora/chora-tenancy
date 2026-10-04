// anchor_deactivation_test.go — CHO-1783.
//
// Pins SubscriptionRegistry.AnchorDeactivation — the registry hook
// chora-tenancy's payments_subscriber calls when Stripe fires
// `customer.subscription.deleted` at cycle anchor on a subscription
// that was previously placed in PENDING_DEACTIVATION via the FE
// "End of billing cycle" deactivate modal. Flips Status → DEACTIVATED,
// sets DeactivatedAt, clears DeactivationEffectiveAt, records an
// EventDeactivated lifecycle entry.
//
// Distinct from the legacy `Unsubscribe` path (30-day grace) which is
// still used for the FE-initiated unsubscribe flow on non-scheduled
// subscriptions. The anchor path is the destructive parallel of
// PromoteScheduledTier (CHO-1779).
package addon_test

import (
	"errors"
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

func seedPendingDeactivation(t *testing.T) *addon.SubscriptionRegistry {
	t.Helper()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	effective := time.Now().UTC().Add(30 * 24 * time.Hour)
	if _, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason:          addon.ReasonCost,
		EffectiveAt:     &effective,
		RequestedByGCID: "owner-1",
	}); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	return reg
}

func TestAnchorDeactivation_HappyPath_FromPendingDeactivation(t *testing.T) {
	t.Parallel()
	reg := seedPendingDeactivation(t)
	before, _ := reg.GetSubscription(tenantA, "tms")
	if before.Status != addon.StatusPendingDeactivation {
		t.Fatalf("pre-condition: Status=%q, want pending_deactivation", before.Status)
	}
	preVersion := before.Version

	sub, ev, err := reg.AnchorDeactivation(tenantA, "tms")
	if err != nil {
		t.Fatalf("AnchorDeactivation: %v", err)
	}
	if sub.Status != addon.StatusDeactivated {
		t.Errorf("Status = %q, want deactivated", sub.Status)
	}
	if sub.DeactivatedAt == nil {
		t.Errorf("DeactivatedAt is nil, want populated")
	}
	if sub.DeactivationEffectiveAt != nil {
		t.Errorf("DeactivationEffectiveAt = %v, want nil after anchor", sub.DeactivationEffectiveAt)
	}
	if sub.Version <= preVersion {
		t.Errorf("Version = %d, want > %d", sub.Version, preVersion)
	}
	if ev == nil {
		t.Fatal("expected a LifecycleEvent on the happy path, got nil")
	}
	if ev.Kind != addon.EventDeactivated {
		t.Errorf("event kind = %q, want deactivated", ev.Kind)
	}
}

func TestAnchorDeactivation_FromActiveAlsoFlipsToDeactivated(t *testing.T) {
	// Stripe Dashboard cancel arrives without going through the FE
	// deactivate modal — the row is still ACTIVE. The anchor path
	// still flips it to DEACTIVATED (Stripe is the source of truth on
	// subscription lifecycle once the webhook fires).
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	sub, ev, err := reg.AnchorDeactivation(tenantA, "tms")
	if err != nil {
		t.Fatalf("AnchorDeactivation: %v", err)
	}
	if sub.Status != addon.StatusDeactivated {
		t.Errorf("Status = %q, want deactivated", sub.Status)
	}
	if sub.DeactivatedAt == nil {
		t.Errorf("DeactivatedAt is nil, want populated")
	}
	if ev == nil || ev.Kind != addon.EventDeactivated {
		t.Errorf("event = %+v, want EventDeactivated", ev)
	}
}

func TestAnchorDeactivation_IdempotentOnAlreadyDeactivated(t *testing.T) {
	// Duplicate Stripe webhook delivery: first call deactivates,
	// second call is a no-op (returns the sub, nil event, nil error).
	t.Parallel()
	reg := seedPendingDeactivation(t)
	if _, _, err := reg.AnchorDeactivation(tenantA, "tms"); err != nil {
		t.Fatalf("first AnchorDeactivation: %v", err)
	}
	preEvents := len(reg.ListEvents(tenantA, "tms"))

	sub, ev, err := reg.AnchorDeactivation(tenantA, "tms")
	if err != nil {
		t.Errorf("duplicate AnchorDeactivation returned err: %v", err)
	}
	if sub.Status != addon.StatusDeactivated {
		t.Errorf("Status mutated on duplicate; got %q", sub.Status)
	}
	if ev != nil {
		t.Errorf("duplicate event = %+v, want nil (no event recorded)", ev)
	}
	postEvents := len(reg.ListEvents(tenantA, "tms"))
	if postEvents != preEvents {
		t.Errorf("duplicate AnchorDeactivation recorded a new lifecycle event (was %d, now %d)", preEvents, postEvents)
	}
}

func TestAnchorDeactivation_NotFound(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, err := reg.AnchorDeactivation(tenantA, "tms")
	if !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Errorf("err = %v, want ErrSubscriptionNotFound", err)
	}
}
