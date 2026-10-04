// knows_tenant_test.go: the registry's "have I ever seen this tenant"
// predicate (UX Track U, package E1).
//
// The entitlement read wrapper in cmd/server falls through to the durable
// add_on_subscriptions table when the registry has never heard of a tenant, so
// a tenant created after the boot hydrator ran is still readable. That fix is
// only safe while KnowsTenant stays true for a tenant whose subscriptions have
// all been deactivated: a registry that forgot such a tenant would send the
// read to PG, where the row still says active, and every runtime deactivation
// would come back. These tests pin that invariant here, at the structure that
// guarantees it, rather than leaving it as a comment in the composition root.
package addon_test

import (
	"testing"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

func TestSubscriptionRegistry_KnowsTenant_FalseUntilFirstSubscription(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())

	if reg.KnowsTenant("tenant-1") {
		t.Fatalf("an empty registry must not claim to know a tenant")
	}
	if _, err := reg.Subscribe("tenant-1", "tms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !reg.KnowsTenant("tenant-1") {
		t.Fatalf("a subscribed tenant must be known")
	}
	if reg.KnowsTenant("tenant-2") {
		t.Fatalf("knowing one tenant must not leak into another")
	}
}

// A pending subscription is intent, not activation, but the registry has still
// seen the tenant: the wrapper must serve its (empty) active list rather than
// reaching for PG.
func TestSubscriptionRegistry_KnowsTenant_TrueForPendingOnlyTenant(t *testing.T) {
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())

	if _, err := reg.SubscribePending("tenant-1", "tms", "gcid-1"); err != nil {
		t.Fatalf("SubscribePending: %v", err)
	}
	if !reg.KnowsTenant("tenant-1") {
		t.Fatalf("a tenant with declared intent must be known")
	}
	if len(reg.ListActiveByTenant("tenant-1")) != 0 {
		t.Fatalf("a pending subscription must not read as active")
	}
}

// The load-bearing case. Deactivate and Unsubscribe flip Status; they must
// never make the registry forget the tenant, or the durable fall-through
// resurrects what the operator just switched off.
func TestSubscriptionRegistry_KnowsTenant_SurvivesDeactivationAndUnsubscribe(t *testing.T) {
	for _, tc := range []struct {
		name  string
		empty func(t *testing.T, reg *addon.SubscriptionRegistry)
	}{
		{
			name: "deactivate",
			empty: func(t *testing.T, reg *addon.SubscriptionRegistry) {
				t.Helper()
				if _, _, _, err := reg.Deactivate("tenant-1", "tms", addon.DeactivationRequest{
					Reason:          "cost",
					RequestedByGCID: "gcid-1",
				}); err != nil {
					t.Fatalf("Deactivate: %v", err)
				}
			},
		},
		{
			name: "unsubscribe",
			empty: func(t *testing.T, reg *addon.SubscriptionRegistry) {
				t.Helper()
				if err := reg.Unsubscribe("tenant-1", "tms", "gcid-1"); err != nil {
					t.Fatalf("Unsubscribe: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
			if _, err := reg.Subscribe("tenant-1", "tms", "gcid-1"); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			tc.empty(t, reg)
			if !reg.KnowsTenant("tenant-1") {
				t.Fatalf("the registry must still know a tenant it has emptied")
			}
		})
	}
}
