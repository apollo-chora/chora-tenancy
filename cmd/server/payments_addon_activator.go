// payments_addon_activator.go — CHO-1740 H+ Marketplace Subscribe saga.
//
// Implements events.AddOnActivator by wrapping the existing
// addon.SubscriptionRegistry. Activate → Subscribe + (optional)
// ChangeTier; Deactivate → Unsubscribe (the canonical 30-day-grace
// flow). This adapter is the only chora-tenancy-side change to the
// AddOn domain — the registry's primitives compose into the
// activate-after-paid + deactivate-on-refund semantics cleanly.
package main

import (
	"context"
	"errors"
	"strings"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// registryAddOnActivator adapts *addon.SubscriptionRegistry to the
// events.AddOnActivator port. Defaults to the proration mode
// `ProrationImmediate` on ChangeTier post-Subscribe, matching the
// chora-payments invoice-immediate behaviour.
type registryAddOnActivator struct {
	reg *addon.SubscriptionRegistry
}

func newRegistryAddOnActivator(reg *addon.SubscriptionRegistry) *registryAddOnActivator { //nolint:unused
	return &registryAddOnActivator{reg: reg}
}

// Activate satisfies events.AddOnActivator. Subscribe creates or
// reactivates the (tenant, addon) subscription idempotently; when
// the event carries a non-empty tierCode that differs from the
// registry-assigned default, ChangeTier shifts the live row.
func (r *registryAddOnActivator) Activate(_ context.Context, tenantID, addonCode, adminGCID, tierCode string) error {
	if r == nil || r.reg == nil {
		return errors.New("payments_addon_activator: nil registry")
	}
	sub, err := r.reg.Subscribe(tenantID, addonCode, adminGCID)
	if err != nil {
		return err
	}
	tier := strings.TrimSpace(tierCode)
	if tier == "" || sub == nil || sub.CurrentTier == tier {
		return nil
	}
	if _, _, _, cerr := r.reg.ChangeTier(
		tenantID,
		addonCode,
		tier,
		string(addon.ProrationCreate),
		nil, // effectiveAt — nil = now
		adminGCID,
		sub.Version,
	); cerr != nil {
		return cerr
	}
	return nil
}

// Deactivate satisfies events.AddOnActivator. Maps onto the existing
// 30-day-grace unsubscribe path. Reason flows through to the
// LifecycleEvent for IMDA audit.
func (r *registryAddOnActivator) Deactivate(_ context.Context, tenantID, addonCode, requesterGCID, _ string) error {
	if r == nil || r.reg == nil {
		return errors.New("payments_addon_activator: nil registry")
	}
	return r.reg.Unsubscribe(tenantID, addonCode, requesterGCID)
}

// MarkPastDue + MarkRecovered satisfy events.RegistryPastDueUpdater
// (CHO-1776). Thin adapter — the registry's own methods return the
// updated *Subscription which the dunning subscribers don't need.
func (r *registryAddOnActivator) MarkPastDue(tenantID, addonCode string) error {
	if r == nil || r.reg == nil {
		return errors.New("payments_addon_activator: nil registry")
	}
	_, err := r.reg.MarkPastDue(tenantID, addonCode)
	return err
}

func (r *registryAddOnActivator) MarkRecovered(tenantID, addonCode string) error {
	if r == nil || r.reg == nil {
		return errors.New("payments_addon_activator: nil registry")
	}
	_, err := r.reg.MarkRecovered(tenantID, addonCode)
	return err
}

// AnchorDeactivation satisfies events.RegistryDeactivationAnchorer
// (CHO-1783). Returns ok=true when the row actually transitioned to
// DEACTIVATED (so the subscriber may emit downstream), ok=false on the
// idempotent no-op (row was already DEACTIVATED) so duplicate Stripe
// webhook delivery doesn't spam events. Errors propagate.
func (r *registryAddOnActivator) AnchorDeactivation(tenantID, addonCode string) (bool, error) {
	if r == nil || r.reg == nil {
		return false, errors.New("payments_addon_activator: nil registry")
	}
	_, ev, err := r.reg.AnchorDeactivation(tenantID, addonCode)
	if err != nil {
		return false, err
	}
	return ev != nil, nil
}

// PromoteScheduledTier satisfies events.RegistryScheduledTierPromoter
// (CHO-1779). Returns the registry-recorded kind as a string ("upgraded"
// / "downgraded") + the from/to tiers so the subscriber can pick the
// right downstream topic + populate the event payload without the
// LifecycleEvent type leaking out of the registry.
func (r *registryAddOnActivator) PromoteScheduledTier(tenantID, addonCode string) (string, string, string, error) {
	if r == nil || r.reg == nil {
		return "", "", "", errors.New("payments_addon_activator: nil registry")
	}
	// Snapshot the pre-promotion tier so we can populate from_tier on the
	// downstream payload; the registry call mutates CurrentTier in place.
	var fromTier string
	if sub, ok := r.reg.GetSubscription(tenantID, addonCode); ok {
		fromTier = sub.CurrentTier
	}
	sub, kind, err := r.reg.PromoteScheduledTier(tenantID, addonCode)
	if err != nil {
		return "", "", "", err
	}
	if kind == "" {
		// Idempotent no-op (no pending schedule). Return empty kind so
		// the subscriber treats it as a duplicate delivery.
		return "", "", "", nil
	}
	return string(kind), fromTier, sub.CurrentTier, nil
}

// Ensure compile-time satisfaction of the ports.
var _ events.AddOnActivator = (*registryAddOnActivator)(nil)
var _ events.RegistryPastDueUpdater = (*registryAddOnActivator)(nil)
var _ events.RegistryScheduledTierPromoter = (*registryAddOnActivator)(nil)
