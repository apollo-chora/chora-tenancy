// bootstrap_subscriber.go — CHO-1751 Phase 1 wiring.
//
// Adapts `*addon.SubscriptionRegistry` to the narrow
// `bootstrap.AddOnSubscriber` port the orchestrator depends on.
// The registry's `Subscribe` returns `(*Subscription, error)`; the
// bootstrap orchestrator only cares about the error, so this wrapper
// drops the subscription value.
//
// Lives alongside `payments_addon_activator.go` (same wrap-the-registry
// pattern) but stays a separate adapter so a future tweak to the payments
// activator surface doesn't ripple into bootstrap tests.
package main

import (
	"errors"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

type registryBootstrapSubscriber struct {
	reg *addon.SubscriptionRegistry
}

func newRegistryBootstrapSubscriber(reg *addon.SubscriptionRegistry) *registryBootstrapSubscriber {
	return &registryBootstrapSubscriber{reg: reg}
}

// Subscribe satisfies bootstrap.AddOnSubscriber. The registry's
// `Subscribe` is idempotent — re-subscribing an existing (tenant,
// addon) reactivates instead of erroring — which matches the
// orchestrator's idempotency expectation.
func (s *registryBootstrapSubscriber) Subscribe(tenantID, addOnCode, ownerGCID string) error {
	if s == nil || s.reg == nil {
		return errors.New("bootstrap_subscriber: nil registry")
	}
	_, err := s.reg.Subscribe(tenantID, addOnCode, ownerGCID)
	return err
}

// Compile-time satisfaction of the port.
var _ bootstrap.AddOnSubscriber = (*registryBootstrapSubscriber)(nil)
