// cancel_scheduled_deactivation_test.go — CHO-1785.
//
// Pins SubscriptionRegistry.CancelScheduledDeactivation — the registry
// hook chora-tenancy's :cancel-deactivation HTTP endpoint calls when the
// admin clicks "(undo)" on the amber "Deactivating <date>" pill (CHO-1782).
//
// Reverses CHO-1782's end-of-cycle deactivation: PENDING_DEACTIVATION →
// ACTIVE, clears the deactivation_* fields, records EventActivated.
//
// Distinct from CHO-1783's AnchorDeactivation (which closes the row at
// the Stripe cycle anchor); this is the opposite direction.
package addon_test

import (
	"errors"
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

func seedPendingDeactivationForUndo(t *testing.T) *addon.SubscriptionRegistry {
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

func TestCancelScheduledDeactivation_HappyPath(t *testing.T) {
	t.Parallel()
	reg := seedPendingDeactivationForUndo(t)
	before, _ := reg.GetSubscription(tenantA, "tms")
	if before.Status != addon.StatusPendingDeactivation {
		t.Fatalf("pre-condition: Status=%q, want pending_deactivation", before.Status)
	}
	if before.DeactivationEffectiveAt == nil {
		t.Fatalf("pre-condition: DeactivationEffectiveAt nil; want populated")
	}
	preVersion := before.Version

	sub, ev, err := reg.CancelScheduledDeactivation(tenantA, "tms", "owner-1")
	if err != nil {
		t.Fatalf("CancelScheduledDeactivation: %v", err)
	}
	if sub.Status != addon.StatusActive {
		t.Errorf("Status = %q, want active", sub.Status)
	}
	if sub.DeactivationEffectiveAt != nil {
		t.Errorf("DeactivationEffectiveAt = %v, want nil", sub.DeactivationEffectiveAt)
	}
	if sub.DeactivationRequestedAt != nil {
		t.Errorf("DeactivationRequestedAt = %v, want nil", sub.DeactivationRequestedAt)
	}
	if sub.DeactivationRequestedBy != "" {
		t.Errorf("DeactivationRequestedBy = %q, want empty", sub.DeactivationRequestedBy)
	}
	if sub.DeactivationReason != "" {
		t.Errorf("DeactivationReason = %q, want empty", sub.DeactivationReason)
	}
	if sub.DeactivationReasonText != "" {
		t.Errorf("DeactivationReasonText = %q, want empty", sub.DeactivationReasonText)
	}
	if sub.Version <= preVersion {
		t.Errorf("Version = %d, want > %d", sub.Version, preVersion)
	}
	if ev == nil {
		t.Fatal("expected a LifecycleEvent on the happy path")
	}
	if ev.Kind != addon.EventActivated {
		t.Errorf("event kind = %q, want activated", ev.Kind)
	}
	if ev.RequestedByGCID != "owner-1" {
		t.Errorf("event RequestedByGCID = %q, want owner-1", ev.RequestedByGCID)
	}
}

func TestCancelScheduledDeactivation_IdempotentOnActive(t *testing.T) {
	// Duplicate click / hot-reload race: row already ACTIVE → no-op.
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	preEvents := len(reg.ListEvents(tenantA, "tms"))

	sub, ev, err := reg.CancelScheduledDeactivation(tenantA, "tms", "owner-1")
	if err != nil {
		t.Errorf("idempotent call returned err: %v", err)
	}
	if sub.Status != addon.StatusActive {
		t.Errorf("Status mutated; got %q", sub.Status)
	}
	if ev != nil {
		t.Errorf("idempotent call recorded an event: %+v", ev)
	}
	if len(reg.ListEvents(tenantA, "tms")) != preEvents {
		t.Errorf("idempotent call appended a lifecycle event")
	}
}

func TestCancelScheduledDeactivation_NotFound(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	_, _, err := reg.CancelScheduledDeactivation(tenantA, "tms", "owner-1")
	if !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Errorf("err = %v, want ErrSubscriptionNotFound", err)
	}
}

func TestCancelScheduledDeactivation_RejectsDeactivated(t *testing.T) {
	// Already DEACTIVATED → can't be undone via this endpoint (the row is
	// closed; admin must re-subscribe). Distinct from ACTIVE-idempotent.
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(tenantA, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, _, _, err := reg.Deactivate(tenantA, "tms", addon.DeactivationRequest{
		Reason:          addon.ReasonCost,
		RequestedByGCID: "owner-1",
	}); err != nil {
		t.Fatalf("Deactivate (immediate): %v", err)
	}

	_, _, err := reg.CancelScheduledDeactivation(tenantA, "tms", "owner-1")
	if !errors.Is(err, addon.ErrAlreadyDeactivated) {
		t.Errorf("err = %v, want ErrAlreadyDeactivated", err)
	}
}
