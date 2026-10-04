// registry_entitlement_store.go — CHO-1811 demo unblock.
//
// The legacy httpapi.EntitlementStore impl (pg.LegacyEntitlementStore) reads
// directly from chora_tenancy.add_on_subscriptions. The runtime
// activate/deactivate handlers (v2_handlers.go and the payments_subscriber
// activator) mutate the in-memory *addon.SubscriptionRegistry only — they
// never write back to the PG row. The registry is hydrated from PG on boot
// so initial state is correct, but every runtime mutation diverges the two
// stores: /h/addons (reads registry) flips state immediately, while
// /api/feature-flags (reads PG via LegacyEntitlementStore) stays stuck on
// the pre-mutation snapshot.
//
// The fix used here for the demo (and recommended as the production fix
// for single-pod deployments): treat the registry as the runtime source
// of truth for entitlement READS too. Hydrator still seeds it from PG on
// boot. Subscribe / Cancel on the legacy v1 path delegate to the PG-backed
// store (so v1 writes continue to land) — but Subscribe/Cancel aren't on
// the v2 demo path; the v2 deactivate handler calls Subscriptions.Deactivate
// on the registry directly, and that mutation is now visible to the
// entitlement endpoint without parallel PG write.
//
// Production note: multi-pod deployments lose this property because the
// registry is per-pod. The real fix is event-sourced PG writes from every
// runtime mutation path. Documented in CHO-1811 Breakage 4/5.
//
// UX Track U, package E1 amends the READ half of this. Serving the registry
// unconditionally made the boot snapshot the whole truth, so a tenant created
// after boot held durable add_on_subscriptions rows that no reader could see
// until the next restart. E1 grants add-ons inside the creation transaction,
// which made that gap the difference between "durable" and "durable and
// readable". ListActiveByTenant now falls through to the durable table when
// the registry has never seen the tenant.
//
// The WRITE half is unchanged and still carries breakage 4/5: a v2 runtime
// deactivate lives only in the registry, so the next pod restart re-hydrates
// the PG row and the add-on comes back active. Closing that means writing
// through from the v2 handlers, the payments subscriber activator and the
// wizard SubscribePending path, which is a separate package.
package main

import (
	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	"github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

// registryEntitlementStore wraps the in-memory SubscriptionRegistry to
// satisfy httpapi.EntitlementStore. Reads come from the registry; Subscribe
// / Cancel from the legacy v1 path delegate to the supplied store so the
// PG row still gets stamped for the v1 flow that consumers may depend on.
type registryEntitlementStore struct {
	reg      *addon.SubscriptionRegistry
	delegate httpapi.EntitlementStore // PG-backed; non-nil
}

func newRegistryEntitlementStore(reg *addon.SubscriptionRegistry, delegate httpapi.EntitlementStore) *registryEntitlementStore {
	return &registryEntitlementStore{reg: reg, delegate: delegate}
}

// Subscribe delegates to the PG-backed store. The legacy v1 endpoint uses
// addon_id (UUID) keying which the registry doesn't support directly;
// delegation preserves v1 semantics. v2 subscribe is independent (it goes
// through the SubscriptionRegistry directly via Subscriptions.Subscribe).
func (s *registryEntitlementStore) Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*tenancy.TenantEntitlement, error) {
	return s.delegate.Subscribe(tenantID, addOnID, monthlyPriceCents)
}

// Cancel delegates to the PG-backed store, same rationale as Subscribe.
func (s *registryEntitlementStore) Cancel(tenantID, addOnID string) (*tenancy.TenantEntitlement, error) {
	return s.delegate.Cancel(tenantID, addOnID)
}

// ListActiveByTenant returns the registry's view of active subscriptions
// mapped onto the TenantEntitlement DTO. This is the demo-critical method:
// /api/feature-flags / GET /api/tenants/{id}/entitlements both flow through
// here, and the FE's addOnGuard / FeatureFlagService consume the result.
func (s *registryEntitlementStore) ListActiveByTenant(tenantID string) []*tenancy.TenantEntitlement {
	if !s.reg.KnowsTenant(tenantID) {
		// The registry holds no opinion about this tenant to serve: the boot
		// hydrator ran before it existed, or another pod created it. Read the
		// durable table rather than reporting the snapshot's silence as "no
		// entitlements". KnowsTenant, not an empty active list, is the test:
		// a tenant whose add-ons were all deactivated at runtime stays known
		// and must keep reading empty, because its PG rows still say active.
		//
		// Deliberately no cache fill. TenantEntitlement carries none of the
		// tier, owner or lifecycle state a Subscription needs, so seeding the
		// registry from it would install a lossy row as the runtime source of
		// truth for the next tier change.
		//
		// The codes are translated on the way out. The bootstrap saga still
		// writes the legacy "core" into add_on_subscriptions and the boot
		// hydrator maps it to the canonical "base" on the way INTO the
		// registry, so the durable table never had that applied. Without this
		// the endpoint would answer "base" or "core" depending on whether the
		// serving pod had hydrated the tenant, and the FE gate keys on the
		// code. Rows are copied rather than rewritten in place: the durable
		// store hands out fresh structs, but this wrapper is not entitled to
		// assume that. The copy also gives the handler a non-nil slice, which
		// it encodes straight into {"items":[...]} where a nil is null.
		durable := s.delegate.ListActiveByTenant(tenantID)
		out := make([]*tenancy.TenantEntitlement, 0, len(durable))
		for _, e := range durable {
			if e == nil {
				continue
			}
			row := *e
			row.AddOnCode = pg.TranslateAddOnCode(row.AddOnCode)
			out = append(out, &row)
		}
		return out
	}
	subs := s.reg.ListActiveByTenant(tenantID)
	out := make([]*tenancy.TenantEntitlement, 0, len(subs))
	for _, sub := range subs {
		out = append(out, &tenancy.TenantEntitlement{
			ID:                        sub.ID,
			TenantID:                  sub.TenantID,
			AddOnID:                   sub.AddOnID,
			AddOnCode:                 sub.AddOnCode,
			MonthlyPriceCentsSnapshot: sub.MonthlyPriceCentsSnapshot,
			Status:                    tenancy.EntitlementStatusActive,
			ActivatedAt:               sub.ActivatedAt,
			CancelledAt:               sub.CancelledAt,
			UpdatedAt:                 sub.UpdatedAt,
		})
	}
	return out
}

// Compile-time port satisfaction.
var _ httpapi.EntitlementStore = (*registryEntitlementStore)(nil)
