// Payments-subscriber PoolApplier — wires the events.PaymentsSubscriber's
// PoolApplier port to the existing tenant_mana_pool.PoolStore so chora-
// payments events (ADR-164) can credit / debit the canonical
// `tenant_mana_pools.balance_units` aggregate.
//
// Cross-DB-forbidden compliance: this adapter ONLY touches chora_tenancy.
// chora-payments owns the Stripe-correlation row; chora-tenancy owns the
// canonical balance. The subscriber is the seam.
package manapool

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// PaymentsPoolApplier wraps a pool.PoolStore + emits Credit / Debit ops
// against the canonical TenantManaPool.Credit / TenantManaPool.Debit
// domain methods. Implements events.PaymentsSubscriber.PoolApplier.
type PaymentsPoolApplier struct {
	store pool.PoolStore
}

// NewPaymentsPoolApplier constructs the adapter. `store` is REQUIRED —
// fail-loud on nil per `feedback_no_stubs_real_wiring`.
func NewPaymentsPoolApplier(store pool.PoolStore) (*PaymentsPoolApplier, error) {
	if store == nil {
		return nil, errors.New("manapool: PaymentsPoolApplier requires non-nil PoolStore")
	}
	return &PaymentsPoolApplier{store: store}, nil
}

// Credit adds `units` to the named tenant's pool.
//
// The chora.payments.tenant_mana_topup.payment_captured.v1 path REQUIRES
// the pool to exist (chora-tenancy pre-provisions on tenant setup). The
// chora.payments.familiar_egg_purchase.refunded.v1 (CreditOnly=true)
// path may run against a tenant whose pool was lazily created; we still
// require it (ADR-149 §"Unhatched egg expiry" refund-credit flow only
// applies when the buyer is a tenant member with a provisioned pool).
//
// `reason` is opaque to the pool model — recorded by upstream emitters
// for audit. Pool.Credit returns ErrInvalidArgument when units <= 0; we
// surface that loudly.
func (a *PaymentsPoolApplier) Credit(ctx context.Context, tenantID string, units int64, reason string) error {
	if a == nil || a.store == nil {
		return errors.New("manapool: PaymentsPoolApplier nil")
	}
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("manapool: Credit requires tenant_id")
	}
	p, err := a.store.GetByTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("manapool: pool lookup for tenant=%s: %w", tenantID, err)
	}
	if err := p.Credit(units); err != nil {
		return fmt.Errorf("manapool: pool credit (tenant=%s reason=%s units=%d): %w",
			tenantID, reason, units, err)
	}
	if err := a.store.Save(ctx, p); err != nil {
		return fmt.Errorf("manapool: pool save after credit (tenant=%s): %w", tenantID, err)
	}
	return nil
}

// Debit subtracts `units` from the named tenant's pool.
//
// `reason` is opaque to the pool model. Pool.Debit returns
// ErrInsufficientBalance if the pool would overdraw — refund cascades
// must NOT silently floor at zero (the operator needs to know if a refund
// arrived for a topup that has since been fully spent). The caller MAY
// catch ErrInsufficientBalance + fall back to a compensation policy.
func (a *PaymentsPoolApplier) Debit(ctx context.Context, tenantID string, units int64, reason string) error {
	if a == nil || a.store == nil {
		return errors.New("manapool: PaymentsPoolApplier nil")
	}
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("manapool: Debit requires tenant_id")
	}
	p, err := a.store.GetByTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("manapool: pool lookup for tenant=%s: %w", tenantID, err)
	}
	if err := p.Debit(units); err != nil {
		return fmt.Errorf("manapool: pool debit (tenant=%s reason=%s units=%d): %w",
			tenantID, reason, units, err)
	}
	if err := a.store.Save(ctx, p); err != nil {
		return fmt.Errorf("manapool: pool save after debit (tenant=%s): %w", tenantID, err)
	}
	return nil
}
