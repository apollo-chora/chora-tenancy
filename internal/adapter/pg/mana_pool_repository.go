// mana_pool_repository.go: pgx-backed TenantManaPool store (CHO-2414).
//
// Replaces manapool.InmemPoolRepo on the production wiring, which held the
// tenant subsidy balance in a process map: every chora-tenancy pod restart
// erased it, and the service's own boot-time durability guard reported
// `port=mana_pools verdict=IN_MEMORY status=VIOLATION`.
//
// Schema source: migrations/0003_tenant_mana_pool.sql. The table already
// existed in the live database (verified 2026-08-24: 17 columns, 0 rows), so
// this change ships no DDL for it.
//
// Hexagonal port mapped: internal/domain/tenant_mana_pool.PoolStore
// (GetByTenant + Save). Two consumers are wired in cmd/server/main.go and
// must share ONE instance or a checkout-paid top-up never reaches the H+
// read: the v2 mana-pool HTTP handlers (v2Deps.PoolRepo) and the ADR-164
// payments PoolApplier. A third consumer exists in the package but is NOT
// constructed by the composition root as of 2026-08-24: manapool's
// EnrollmentSubscriber and monthly EqualSplit MonthlyTicker take a
// SubscriberDeps that nothing in main.go builds, so the auto-allocation
// engine does not run in production at all. Their Pools field was widened
// to this port so they bind the durable store if they are ever wired.
//
// RLS contract: `tenant_mana_pools` has RLS ENABLED with the
// `tenant_isolation` policy `tenant_id = current_setting('chora.tenant_id',
// true)::uuid`, and chora_tenancy_app_rw is NOBYPASSRLS (migration 0013).
// Every statement here therefore runs inside RunInTenantTx, which issues
// SET LOCAL chora.tenant_id first. A bare pool query would read zero rows and
// fail the INSERT's WITH CHECK; because the table had never taken a
// production row, nothing would have surfaced that.
//
// Concurrency: the UPDATE is guarded on `version`. This adapter SHIPPED as
// last-write-wins, with that limitation documented right here as a known
// caveat, and it lost 20,000 mana units the first time it was handed two
// concurrent writes: both events read version 3 and balance 0, both wrote
// version 4, and nothing errored. Documenting a hole is not closing it.
//
// The write now refuses any row already at or beyond the incoming version
// (`AND version < $n RETURNING version`), and a zero-row result surfaces as
// ErrVersionConflict. It runs through QueryRow rather than Exec because the
// Tx port's Exec returns only an error, so the losing writer's zero rows
// were indistinguishable from success. That is precisely what made the loss
// silent.
//
// Stated narrowly, because the previous version of this comment overstated
// its own safety: the guard orders writers who derive the SAME next version
// from the same read, which is every caller today (all four Save sites make
// exactly one mutation per save). It does not order two writers who load the
// same version and derive DIFFERENT next versions.
//
// The refusal makes the lane self-healing rather than merely loud:
// idempotent.PostgresStore.Process does not Mark a key whose fn returned an
// error, so the loser's Nack is redelivered and the retry applies on top of
// the advanced row.
//
// Creation races are bounded instead by the UNIQUE constraint on tenant_id:
// the INSERT path has no prior version to guard against, and the loser gets a
// loud unique-violation error rather than a silent overwrite.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// ErrPolicyUnsupported is returned when an AllocationPolicy cannot be
// represented by the 0003 schema, or when a stored policy enum value has no
// domain counterpart. Fail loud: a silently dropped policy would turn
// auto-allocation off without anyone noticing.
var ErrPolicyUnsupported = errors.New("pg.ManaPoolRepository: unsupported allocation policy")

// ErrVersionConflict is returned when the stored row has already been advanced
// to this version or beyond, meaning another writer wrote between our read and
// our write. It is RETRYABLE and must be surfaced, never swallowed: the
// event handlers translate it into a Nack, and because
// idempotent.PostgresStore.Process does not Mark a key whose fn returned an
// error, redelivery re-reads the advanced row and applies on top of it.
//
// This exists because the first production use of this repository silently
// lost an update. Two payment_captured events were handled 3ms apart, both
// read version 3 / balance 0, both computed 20000, both wrote version 4, and
// 20000 of 40000 mana units disappeared with no error anywhere.
var ErrVersionConflict = errors.New("pg.ManaPoolRepository: version conflict, another writer advanced the pool")

// ManaPoolRepository is the pgx-backed implementation of pool.PoolStore.
type ManaPoolRepository struct {
	tq TxQuerier
}

// NewManaPoolRepository constructs the production-wired repository.
func NewManaPoolRepository(tq TxQuerier) *ManaPoolRepository {
	return &ManaPoolRepository{tq: tq}
}

// Compile-time check that the adapter satisfies the domain port.
var _ pool.PoolStore = (*ManaPoolRepository)(nil)

const selectPoolByTenant = `
        SELECT
            pool_id::text,
            tenant_id::text,
            balance_units,
            lifetime_topped_up_units,
            lifetime_allocated_units,
            monthly_topup_units,
            auto_allocation_policy::text,
            COALESCE(units_on_enrollment, 0),
            COALESCE(tier_amounts_json::text, ''),
            version,
            created_by_gcid::text,
            created_at,
            updated_at,
            last_topped_up_at
        FROM tenant_mana_pools
        WHERE tenant_id = $1::uuid
          AND deleted_at IS NULL
    `

// probeExistingPoolID deliberately does NOT filter deleted_at: the UNIQUE
// constraint on tenant_id covers soft-deleted rows too, so a probe that
// ignored them would report "no pool" and then hit a unique violation.
const probeExistingPoolID = `
        SELECT pool_id::text
        FROM tenant_mana_pools
        WHERE tenant_id = $1::uuid
    `

const insertPool = `
        INSERT INTO tenant_mana_pools (
            pool_id,
            tenant_id,
            balance_units,
            lifetime_topped_up_units,
            lifetime_allocated_units,
            monthly_topup_units,
            auto_allocation_policy,
            units_on_enrollment,
            tier_amounts_json,
            version,
            created_by_gcid,
            created_at,
            updated_at,
            last_topped_up_at,
            deleted_at
        ) VALUES (
            $1::uuid,
            $2::uuid,
            $3, $4, $5, $6,
            $7::tenant_mana_allocation_policy,
            $8,
            NULLIF($9, '')::jsonb,
            $10,
            $11::uuid,
            $12, $13, $14, $15
        )
    `

const updatePool = `
        UPDATE tenant_mana_pools SET
            balance_units             = $3,
            lifetime_topped_up_units  = $4,
            lifetime_allocated_units  = $5,
            monthly_topup_units       = $6,
            auto_allocation_policy    = $7::tenant_mana_allocation_policy,
            units_on_enrollment       = $8,
            tier_amounts_json         = NULLIF($9, '')::jsonb,
            version                   = $10,
            updated_at                = $11,
            last_topped_up_at         = $12,
            deleted_at                = $13
        WHERE tenant_id = $1::uuid
          AND pool_id   = $2::uuid
          AND version   < $10
        RETURNING version
    `

// GetByTenant returns the tenant's live pool, or pool.ErrPoolNotFound when
// the tenant has none or the row is soft-deleted.
func (r *ManaPoolRepository) GetByTenant(ctx context.Context, tenantID string) (*pool.TenantManaPool, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, errors.New("pg.ManaPoolRepository.GetByTenant: tenantID required")
	}

	var (
		p          pool.TenantManaPool
		policyKind string
		units      int64
		tiersJSON  string
		lastTopUp  *time.Time
	)
	err := r.tq.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		scanErr := tx.QueryRow(ctx, selectPoolByTenant, tenantID).Scan(
			&p.PoolID,
			&p.TenantID,
			&p.BalanceUnits,
			&p.LifetimeToppedUpUnits,
			&p.LifetimeAllocatedUnits,
			&p.MonthlyTopupUnits,
			&policyKind,
			&units,
			&tiersJSON,
			&p.Version,
			&p.CreatedByGCID,
			&p.CreatedAt,
			&p.UpdatedAt,
			&lastTopUp,
		)
		if errors.Is(scanErr, ErrNoRows) {
			return pool.ErrPoolNotFound
		}
		return scanErr
	})
	if err != nil {
		return nil, err
	}

	policy, err := decodePolicy(policyKind, units, tiersJSON)
	if err != nil {
		return nil, err
	}
	p.AutoAllocationPolicy = policy
	p.LastToppedUpAt = lastTopUp
	return &p, nil
}

// Save inserts the pool when the tenant has none and updates it when the
// stored row carries the same pool_id. A DIFFERENT pool_id for the tenant
// returns pool.ErrPoolAlreadyExists and writes nothing, matching the
// InmemPoolRepo semantic the HTTP layer maps to 409.
func (r *ManaPoolRepository) Save(ctx context.Context, p *pool.TenantManaPool) error {
	if p == nil {
		return errors.New("pg.ManaPoolRepository.Save: nil pool")
	}
	tenantID := strings.TrimSpace(p.TenantID)
	if tenantID == "" {
		return errors.New("pg.ManaPoolRepository.Save: pool carries no tenant_id")
	}

	policyKind, units, tiersJSON, err := encodePolicy(p.AutoAllocationPolicy)
	if err != nil {
		return err
	}

	return r.tq.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var existingPoolID string
		probeErr := tx.QueryRow(ctx, probeExistingPoolID, tenantID).Scan(&existingPoolID)
		switch {
		case probeErr == nil && existingPoolID != p.PoolID:
			return pool.ErrPoolAlreadyExists
		case probeErr != nil && !errors.Is(probeErr, ErrNoRows):
			return fmt.Errorf("pg.ManaPoolRepository.Save: probe existing pool: %w", probeErr)
		}

		if probeErr == nil {
			// Guarded UPDATE via QueryRow, not Exec: the Tx port's Exec
			// returns only an error, so a zero-row result (the losing writer
			// in a race) would be indistinguishable from success. RETURNING
			// makes the miss observable.
			var stored int64
			updErr := tx.QueryRow(ctx, updatePool,
				tenantID,
				p.PoolID,
				p.BalanceUnits,
				p.LifetimeToppedUpUnits,
				p.LifetimeAllocatedUnits,
				p.MonthlyTopupUnits,
				policyKind,
				units,
				tiersJSON,
				p.Version,
				p.UpdatedAt,
				p.LastToppedUpAt,
				p.DeletedAt,
			).Scan(&stored)
			if errors.Is(updErr, ErrNoRows) {
				return fmt.Errorf("%w (tenant=%s pool=%s attempted_version=%d)",
					ErrVersionConflict, tenantID, p.PoolID, p.Version)
			}
			return updErr
		}

		return tx.Exec(ctx, insertPool,
			p.PoolID,
			tenantID,
			p.BalanceUnits,
			p.LifetimeToppedUpUnits,
			p.LifetimeAllocatedUnits,
			p.MonthlyTopupUnits,
			policyKind,
			units,
			tiersJSON,
			p.Version,
			p.CreatedByGCID,
			p.CreatedAt,
			p.UpdatedAt,
			p.LastToppedUpAt,
			p.DeletedAt,
		)
	})
}

// encodePolicy maps the typed AllocationPolicy onto the three columns the
// 0003 schema uses (auto_allocation_policy, units_on_enrollment,
// tier_amounts_json).
//
// It refuses the two shapes the 0003 CHECK constraints reject, rather than
// letting them surface as a raw constraint violation from an arbitrary call
// site: chk_pool_on_enrollment_units requires units > 0 for on_enrollment,
// and chk_pool_tier_based_amounts requires a JSON object for tier_based.
// The domain's PolicyFromConfig accepts units_on_enrollment == 0, so that
// gap is real and reachable.
func encodePolicy(policy pool.AllocationPolicy) (kind string, units any, tiersJSON string, err error) {
	if policy == nil {
		return "", nil, "", fmt.Errorf("%w: policy is nil", ErrPolicyUnsupported)
	}
	switch p := policy.(type) {
	case pool.ManualPolicy:
		return string(pool.PolicyKindManual), nil, "", nil

	case pool.OnEnrollmentPolicy:
		if p.DefaultUnits <= 0 {
			return "", nil, "", fmt.Errorf(
				"%w: on_enrollment requires units_on_enrollment > 0 (chk_pool_on_enrollment_units), got %d",
				ErrPolicyUnsupported, p.DefaultUnits)
		}
		return string(pool.PolicyKindOnEnrollment), p.DefaultUnits, "", nil

	case pool.EqualSplitPolicy:
		return string(pool.PolicyKindEqualSplit), nil, "", nil

	case pool.TierBasedPolicy:
		tiers := p.TierToUnits
		if tiers == nil {
			// A nil map marshals to JSON null, which fails
			// chk_pool_tier_based_amounts. An empty object satisfies it and
			// means the same thing: no tier grants anything.
			tiers = map[string]int64{}
		}
		raw, mErr := json.Marshal(tiers)
		if mErr != nil {
			return "", nil, "", fmt.Errorf("%w: marshal tier_amounts: %v", ErrPolicyUnsupported, mErr)
		}
		return string(pool.PolicyKindTierBased), nil, string(raw), nil

	default:
		return "", nil, "", fmt.Errorf("%w: %T", ErrPolicyUnsupported, policy)
	}
}

// decodePolicy rebuilds the typed AllocationPolicy from the stored columns.
// An unrecognised enum value is an error, never a silent downgrade to
// ManualPolicy: that would switch auto-allocation off invisibly.
func decodePolicy(kind string, units int64, tiersJSON string) (pool.AllocationPolicy, error) {
	switch pool.PolicyKind(kind) {
	case pool.PolicyKindManual:
		return pool.ManualPolicy{}, nil
	case pool.PolicyKindOnEnrollment:
		return pool.OnEnrollmentPolicy{DefaultUnits: units}, nil
	case pool.PolicyKindEqualSplit:
		return pool.EqualSplitPolicy{}, nil
	case pool.PolicyKindTierBased:
		tiers := map[string]int64{}
		if strings.TrimSpace(tiersJSON) != "" {
			if err := json.Unmarshal([]byte(tiersJSON), &tiers); err != nil {
				return nil, fmt.Errorf("%w: tier_amounts_json is not a JSON object: %v", ErrPolicyUnsupported, err)
			}
		}
		return pool.TierBasedPolicy{TierToUnits: tiers}, nil
	default:
		return nil, fmt.Errorf("%w: stored auto_allocation_policy %q", ErrPolicyUnsupported, kind)
	}
}
