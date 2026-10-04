// service.go — bootstrap orchestrator + Repository port
// (CHO-1628 Phase 1).
//
// The Service composes Input validation, idempotency lookup, and the
// atomic three-row persist. It depends only on the Repository
// interface; adapters (postgres, in-memory) implement it.
package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

// Repository is the persistence port for the bootstrap orchestration.
//
// HasAnyMembership backs the idempotency rule: caller is allowed to
// bootstrap exactly one tenant per GCID in Phase 1. A second attempt
// after a successful first call MUST return ErrAlreadyMember from the
// service layer.
//
// Persist commits the Bundle's three rows (tenants + members +
// tenant_entitlements) in a SINGLE Postgres transaction. The adapter
// owns the BEGIN / COMMIT and is responsible for failing the whole
// transaction if any of the three INSERTs fail (no partial state).
type Repository interface {
	HasAnyMembership(ctx context.Context, gcid string) (bool, error)
	Persist(ctx context.Context, b *Bundle) error
}

// AddOnSubscriber is the narrow port the bootstrap orchestrator uses to
// register the always-on baseline plan in the v2 SubscriptionRegistry
// after a successful tenant + member + entitlement persist (CHO-1751
// Phase 1). Production wires `*addon.SubscriptionRegistry`; tests wire
// a recording fake.
//
// Keeping this port distinct from the existing payments AddOnActivator
// (cmd/server/payments_addon_activator.go) lets the orchestrator depend
// on the minimum surface — Subscribe only, no ChangeTier or Deactivate —
// so a future activator refactor doesn't ripple into bootstrap tests.
type AddOnSubscriber interface {
	// Subscribe registers (tenantID, addOnCode) in the v2 registry, in
	// `StatusActive`. Implementations MUST be idempotent — a second call
	// with the same (tenantID, addOnCode) MUST NOT fail (the registry's
	// existing Subscribe already satisfies this).
	Subscribe(tenantID, addOnCode, ownerGCID string) error
}

// BaseAddOnCode is the canonical addon code for the always-on tenant
// baseline plan. The catalogue row at `addon.NewSeedCatalogue` line ~185
// declares it with `AlwaysOn: true` and the bootstrap orchestrator
// subscribes it immediately after persisting the tenant + entitlement.
// Defense-in-depth: `ErrCannotUnsubscribeBase` rejects any later attempt
// to deactivate it.
const BaseAddOnCode = "base"

// Service is the bootstrap orchestrator.
type Service struct {
	repo Repository
	sub  AddOnSubscriber
}

// NewService constructs a Service. Panics on nil dependencies —
// production wiring is at composition root (cmd/server) so a nil here is
// a wiring bug that should fail loud at boot, not silently at request
// time. (Subscriber added in CHO-1751 Phase 1.)
func NewService(repo Repository, sub AddOnSubscriber) *Service {
	if repo == nil {
		panic("bootstrap: NewService requires a non-nil Repository")
	}
	if sub == nil {
		panic("bootstrap: NewService requires a non-nil AddOnSubscriber")
	}
	return &Service{repo: repo, sub: sub}
}

// Bootstrap orchestrates the three-step self-onboard:
//
//  1. validate Input (pure, no I/O)
//  2. check that the caller has no existing TenantMembership
//  3. assemble + persist the three-row Bundle in one transaction
//
// Returns ErrInvalidArgument on bad input, ErrAlreadyMember when the
// caller already owns/joined a tenant, or a wrapped repository error
// on read/write failures.
func (s *Service) Bootstrap(ctx context.Context, in Input) (*Output, error) {
	// ADR-217 Phase 2.4 (CHO-2002): a self-onboarded tenant is a direct child
	// of the franchisor ROOT (chora-master) so it joins the managed-tenant
	// hierarchy at creation. Without this it orphans as its own root and drops
	// out of the operator "managed tenants" subtree filter. hosting_mode is
	// left unset here (New defaults it to PLATFORM_HOSTED); only the operator
	// CreateSubTenant path classifies FRANCHISE. The tenant ID New() mints is a
	// fresh UUIDv7 (never the chora-master sentinel), so this never re-parents
	// chora-master onto itself.
	in.ParentTenantID = tenant.ChoraMasterTenantID

	// Domain validation MUST run first — fast-fail for garbage input
	// without paying for a DB read. New() does the trim + length +
	// non-empty checks and is the same gate the Bundle assembly uses.
	bundle, err := New(in)
	if err != nil {
		return nil, err
	}

	// Idempotency: any active TenantMembership for this GCID blocks a
	// fresh bootstrap. Phase 1 is strictly one-tenant-per-GCID self-
	// onboard; multi-tenant membership lands later via invite flows.
	already, err := s.repo.HasAnyMembership(ctx, bundle.Tenant.OwnerGCID)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: check membership: %w", err)
	}
	if already {
		return nil, ErrAlreadyMember
	}

	// Atomic three-row insert — adapter owns BEGIN/COMMIT.
	if err := s.repo.Persist(ctx, bundle); err != nil {
		return nil, fmt.Errorf("bootstrap: persist: %w", err)
	}

	// CHO-1751 Phase 1 — auto-subscribe the always-on baseline plan into
	// the v2 SubscriptionRegistry so the H+ management surface shows the
	// `Chora Base` tile + Included-by-default badge immediately after
	// onboarding. Without this, a freshly bootstrapped tenant renders an
	// empty /h/addons until they run the Setup Wizard step 2 by hand.
	//
	// Subscribe is idempotent — replaying this orchestration is safe.
	// On error we surface up: the pg transaction is already committed,
	// so the caller has to retry the same bootstrap (which Subscribe
	// will then no-op against the existing tenant row).
	if err := s.sub.Subscribe(bundle.Tenant.ID, BaseAddOnCode, bundle.Tenant.OwnerGCID); err != nil {
		return nil, fmt.Errorf("bootstrap: subscribe base: %w", err)
	}

	out := bundle.Output()
	return &out, nil
}

// CreateSubTenant provisions a persisted SUB-tenant (ADR-217 Phase 2.1). This
// is the OPERATOR path, distinct from Bootstrap (user self-onboard):
//
//   - the owner is the request's OwnerGCID (an operator provisions a managed
//     tenant for a designated owner — the owner need not be the caller);
//   - the one-tenant-per-GCID idempotency (HasAnyMembership) is NOT applied —
//     an operator may create many tenants, and the owner may already belong to
//     other tenants;
//   - parent_tenant_id is REQUIRED. The chora-tenancy parent-invariant trigger
//     (migration 0026) validates it resolves to the chora-master tree at
//     persist time — a foreign-root/cyclic parent fails Persist loudly.
//
// Everything else matches Bootstrap: the atomic tenant + owner-member + core
// entitlement + outbox event, then the always-on base add-on subscription.
// hosting_mode defaults to FRANCHISE when unspecified.
func (s *Service) CreateSubTenant(ctx context.Context, in Input) (*Output, error) {
	if strings.TrimSpace(in.ParentTenantID) == "" {
		return nil, fmt.Errorf("%w: parent_tenant_id required for a sub-tenant", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.HostingMode) == "" {
		in.HostingMode = HostingModeFranchise
	}

	// New validates name / owner_gcid / hosting_mode and assembles the atomic
	// trio (tenant + owner member + core entitlement) with parent + hosting set.
	bundle, err := New(in)
	if err != nil {
		return nil, err
	}

	// NOTE: HasAnyMembership is intentionally NOT consulted — see the doc above.
	if err := s.repo.Persist(ctx, bundle); err != nil {
		return nil, fmt.Errorf("bootstrap: persist sub-tenant: %w", err)
	}

	if err := s.sub.Subscribe(bundle.Tenant.ID, BaseAddOnCode, bundle.Tenant.OwnerGCID); err != nil {
		return nil, fmt.Errorf("bootstrap: subscribe base: %w", err)
	}

	out := bundle.Output()
	return &out, nil
}
