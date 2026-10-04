// allocation_repository.go — pgx-backed AllocationRepo for the per-user
// economy + egg-refund credit paths.
//
// Schema source: migrations/0003_tenant_mana_pool.sql (base) +
//
//	migrations/0009_tenant_mana_allocation_refund.sql
//	(egg_hard_expiry_refund reason + nullable source_pool_id
//	+ partial UNIQUE INDEX on idempotency_key).
//
// Hexagonal port mapped: chora-tenancy/internal/adapter/manapool.AllocationGetter +
//
//	AllocationSaver. The familiar_egg_sweeper's
//	CreditIssuer (manapool.NewCreditIssuer) consumes
//	these methods to issue durable refund credits.
//
// Idempotency contract: Save uses INSERT ... ON CONFLICT (idempotency_key)
// DO NOTHING. The CreditIssuer pre-checks via GetByIdempotencyKey;
// the DB-side ON CONFLICT is a belt-and-braces guard against concurrent
// sweepers / DLQ replays.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
)

// AllocationRepository is the pgx-backed implementation of the
// TenantManaAllocation port. Tenant-scoped reads + writes assume the
// caller has SET LOCAL chora.tenant_id within an open transaction (RLS
// enforcement per chora-tenancy/internal/rls package).
type AllocationRepository struct {
	q Querier
}

// NewAllocationRepository constructs the production-wired repository.
func NewAllocationRepository(q Querier) *AllocationRepository {
	return &AllocationRepository{q: q}
}

// Get returns an allocation by allocation_id (UUID). Returns
// manapool.ErrAllocationNotFound when no row matches.
func (r *AllocationRepository) Get(ctx context.Context, allocationID string) (*allocation.TenantManaAllocation, error) {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return nil, errors.New("pg.AllocationRepository.Get: allocationID required")
	}
	const q = `
        SELECT
            allocation_id::text,
            tenant_id::text,
            COALESCE(source_pool_id::text, '') AS source_pool_id,
            gcid::text,
            units,
            status::text,
            allocation_reason::text,
            COALESCE(allocated_by_gcid::text, '') AS allocated_by_gcid,
            allocated_at,
            expires_at,
            claimed_at,
            revoked_at,
            COALESCE(revoked_reason, '') AS revoked_reason,
            COALESCE(revoked_by_gcid::text, '') AS revoked_by_gcid,
            COALESCE(first_debit_entry_id, '') AS first_debit_entry_id,
            COALESCE(remaining_units, 0) AS remaining_units,
            updated_at,
            COALESCE(idempotency_key, '') AS idempotency_key
        FROM tenant_mana_allocations
        WHERE allocation_id = $1::uuid
    `
	row := r.q.QueryRow(ctx, q, allocationID)
	return scanAllocation(row)
}

// GetByIdempotencyKey returns an allocation by its idempotency_key
// (partial UNIQUE INDEX uidx_tenant_mana_alloc_idempotency from
// migration 0009). Returns manapool.ErrAllocationNotFound when no row
// carries the key. Empty key returns not-found rather than scanning the
// table — matches the inmem repo's safety check.
func (r *AllocationRepository) GetByIdempotencyKey(ctx context.Context, key string) (*allocation.TenantManaAllocation, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, ErrNoRows
	}
	const q = `
        SELECT
            allocation_id::text,
            tenant_id::text,
            COALESCE(source_pool_id::text, '') AS source_pool_id,
            gcid::text,
            units,
            status::text,
            allocation_reason::text,
            COALESCE(allocated_by_gcid::text, '') AS allocated_by_gcid,
            allocated_at,
            expires_at,
            claimed_at,
            revoked_at,
            COALESCE(revoked_reason, '') AS revoked_reason,
            COALESCE(revoked_by_gcid::text, '') AS revoked_by_gcid,
            COALESCE(first_debit_entry_id, '') AS first_debit_entry_id,
            COALESCE(remaining_units, 0) AS remaining_units,
            updated_at,
            COALESCE(idempotency_key, '') AS idempotency_key
        FROM tenant_mana_allocations
        WHERE idempotency_key = $1
        LIMIT 1
    `
	row := r.q.QueryRow(ctx, q, key)
	return scanAllocation(row)
}

// Save inserts or upserts a TenantManaAllocation row. When
// IdempotencyKey is non-empty, uses INSERT ... ON CONFLICT
// (idempotency_key) DO NOTHING — the 0009 partial UNIQUE INDEX
// guarantees only one row per non-null key. The caller's pre-check via
// GetByIdempotencyKey + this DB-side guard together cover the
// sweeper-retry + DLQ-replay races.
//
// SourcePoolID semantics:
//   - Empty string → INSERT NULL into source_pool_id; valid ONLY for
//     allocation_reason='egg_hard_expiry_refund' per 0009 CHECK
//     constraint chk_allocation_refund_source_pool.
//   - Non-empty → INSERT the value; the existing FK to
//     tenant_mana_pools(pool_id) applies.
func (r *AllocationRepository) Save(ctx context.Context, a *allocation.TenantManaAllocation) error {
	if a == nil {
		return errors.New("pg.AllocationRepository.Save: nil allocation")
	}
	const q = `
        INSERT INTO tenant_mana_allocations (
            allocation_id,
            tenant_id,
            source_pool_id,
            gcid,
            units,
            status,
            allocation_reason,
            allocated_by_gcid,
            allocated_at,
            expires_at,
            claimed_at,
            revoked_at,
            revoked_reason,
            revoked_by_gcid,
            first_debit_entry_id,
            remaining_units,
            updated_at,
            idempotency_key
        ) VALUES (
            $1::uuid,
            $2::uuid,
            NULLIF($3, '')::uuid,
            $4::uuid,
            $5,
            $6::tenant_mana_allocation_status,
            $7::tenant_mana_allocation_reason,
            NULLIF($8, '')::uuid,
            $9,
            $10,
            $11,
            $12,
            NULLIF($13, ''),
            NULLIF($14, '')::uuid,
            NULLIF($15, ''),
            $16,
            $17,
            NULLIF($18, '')
        )
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
        DO NOTHING
    `
	var remainingUnits any
	if a.RemainingUnits > 0 {
		remainingUnits = a.RemainingUnits
	}
	return r.q.Exec(ctx, q,
		a.AllocationID,
		a.TenantID,
		a.SourcePoolID,
		a.GCID,
		a.Units,
		string(a.Status),
		string(a.Reason),
		a.AllocatedByGCID,
		a.AllocatedAt,
		a.ExpiresAt,
		a.ClaimedAt,
		a.RevokedAt,
		a.RevokedReason,
		a.RevokedByGCID,
		a.FirstDebitEntryID,
		remainingUnits,
		a.UpdatedAt,
		a.IdempotencyKey,
	)
}

// scanAllocation maps a single row into the domain aggregate. Caller
// translates ErrNoRows → manapool.ErrAllocationNotFound at the boundary.
func scanAllocation(row Row) (*allocation.TenantManaAllocation, error) {
	var a allocation.TenantManaAllocation
	var statusStr, reasonStr string
	if err := row.Scan(
		&a.AllocationID,
		&a.TenantID,
		&a.SourcePoolID,
		&a.GCID,
		&a.Units,
		&statusStr,
		&reasonStr,
		&a.AllocatedByGCID,
		&a.AllocatedAt,
		&a.ExpiresAt,
		&a.ClaimedAt,
		&a.RevokedAt,
		&a.RevokedReason,
		&a.RevokedByGCID,
		&a.FirstDebitEntryID,
		&a.RemainingUnits,
		&a.UpdatedAt,
		&a.IdempotencyKey,
	); err != nil {
		return nil, fmt.Errorf("pg.scanAllocation: %w", err)
	}
	a.Status = allocation.Status(statusStr)
	a.Reason = allocation.Reason(reasonStr)
	return &a, nil
}
