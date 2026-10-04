// external_egress_repository.go — pgx-backed adapter for
// external_egress.Repository (CHO-2148).
//
// SQL contract:
//
//	Get:
//	  Runs inside RunInTenantTx so `SET LOCAL chora.tenant_id` is in scope —
//	  tenant_external_egress_policies is RLS-protected and app_rw is
//	  NOBYPASSRLS, so without the GUC the SELECT returns ZERO ROWS SILENTLY
//	  (it does not error). A missing row is indistinguishable from a
//	  policy-denied row, so a forgotten GUC would read as "egress OFF" —
//	  fail-closed, but for the wrong reason. The tx is mandatory.
//	  No row => the fail-closed DEFAULT (never a fabricated write).
//
//	Save:
//	  ONE transaction:
//	    1. UPSERT tenant_external_egress_policies ... RETURNING version
//	         guarded by `WHERE existing.version < EXCLUDED.version` (OCC).
//	         No row returned => a concurrent writer already advanced past us
//	         => ErrConcurrentUpdate, fail loud. Never a silent lost update.
//	    2. INSERT INTO outbox_events (the policy-changed event)
//	  Both, or neither. If these could drift apart, a crash between them
//	  leaves chora_tenancy and the chora-observability projection the
//	  model-gateway reads disagreeing about who may reach the open web.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

// TenantTxRunner is the narrow slice of *PgxPoolQuerier this repository needs.
// Narrowing it to an interface lets the unit suite assert the load-bearing
// property — policy row and outbox row inside ONE transaction — without a live
// database. *PgxPoolQuerier satisfies it.
type TenantTxRunner interface {
	RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, Tx) error) error
}

// ExternalEgressRepository is the production pgx-backed adapter for
// external_egress.Repository.
type ExternalEgressRepository struct {
	q TenantTxRunner
}

// NewExternalEgressRepository constructs the repository. Panics on a nil runner
// so a wiring bug fails loud at boot rather than at request time.
func NewExternalEgressRepository(q TenantTxRunner) *ExternalEgressRepository {
	if q == nil {
		panic("pg.NewExternalEgressRepository: nil TenantTxRunner")
	}
	return &ExternalEgressRepository{q: q}
}

const selectExternalEgressPolicySQL = `
    SELECT tenant_id::text, egress_enabled, daily_call_ceiling, version,
           updated_by_gcid::text, created_at, updated_at
      FROM tenant_external_egress_policies
     WHERE tenant_id = $1::uuid
`

// Get returns the persisted policy, or the fail-closed default when the tenant
// has never opted in. Absence IS the denial (ADR-220 D4) — a read must never
// create the row whose absence is what keeps a franchise tenant safe.
func (r *ExternalEgressRepository) Get(ctx context.Context, tenantID string) (external_egress.Policy, error) {
	if tenantID == "" {
		return external_egress.Policy{}, external_egress.ErrEmptyTenantID
	}

	policy := external_egress.DefaultPolicy(tenantID)

	err := r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var (
			id        string
			enabled   bool
			ceiling   int32
			version   int64
			actor     string
			createdAt time.Time
			updatedAt time.Time
		)
		scanErr := tx.QueryRow(ctx, selectExternalEgressPolicySQL, tenantID).
			Scan(&id, &enabled, &ceiling, &version, &actor, &createdAt, &updatedAt)
		if scanErr != nil {
			if errors.Is(scanErr, ErrNoRows) {
				// Never opted in. The fail-closed default already stands.
				return nil
			}
			return scanErr
		}

		policy = external_egress.Policy{
			TenantID:         id,
			EgressEnabled:    enabled,
			DailyCallCeiling: ceiling,
			Version:          version,
			UpdatedByGCID:    actor,
			CreatedAt:        createdAt,
			UpdatedAt:        updatedAt,
		}
		return nil
	})
	if err != nil {
		return external_egress.Policy{}, fmt.Errorf("pg.ExternalEgressRepository.Get: %w", err)
	}

	return policy, nil
}

// upsertExternalEgressPolicySQL — OCC on `version`.
//
// The `WHERE ... version < EXCLUDED.version` on the DO UPDATE means a stale
// writer matches no row and RETURNING yields nothing, which we surface as
// ErrConcurrentUpdate. Without this guard, two admins racing could let the
// LOSING write win — silently re-enabling egress a tenant had just turned off.
const upsertExternalEgressPolicySQL = `
    INSERT INTO tenant_external_egress_policies (
        tenant_id, egress_enabled, daily_call_ceiling, version,
        updated_by_gcid, created_at, updated_at
    )
    VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6, $6)
    ON CONFLICT (tenant_id) DO UPDATE SET
        egress_enabled     = EXCLUDED.egress_enabled,
        daily_call_ceiling = EXCLUDED.daily_call_ceiling,
        version            = EXCLUDED.version,
        updated_by_gcid    = EXCLUDED.updated_by_gcid
      WHERE tenant_external_egress_policies.version < EXCLUDED.version
    RETURNING version
`

const insertExternalEgressOutboxSQL = `
    INSERT INTO outbox_events (
        id, tenant_id, gcid, aggregate_type, aggregate_id,
        event_type, topic, payload, envelope, idempotency_key,
        occurred_at, status
    )
    VALUES ($1, $2, NULLIF($3, '')::uuid, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending')
`

// Save persists the changed policy and enqueues the policy-changed event in the
// SAME transaction. See the file-level SQL contract.
func (r *ExternalEgressRepository) Save(ctx context.Context, next external_egress.Policy, prev external_egress.Policy) error {
	if next.TenantID == "" {
		return external_egress.ErrEmptyTenantID
	}
	if next.UpdatedByGCID == "" {
		return external_egress.ErrEmptyActor
	}

	return r.q.RunInTenantTx(ctx, next.TenantID, func(ctx context.Context, tx Tx) error {
		// 1. Policy upsert, OCC-guarded.
		var persistedVersion int64
		err := tx.QueryRow(ctx, upsertExternalEgressPolicySQL,
			next.TenantID,
			next.EgressEnabled,
			next.DailyCallCeiling,
			next.Version,
			next.UpdatedByGCID,
			next.UpdatedAt,
		).Scan(&persistedVersion)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return external_egress.ErrConcurrentUpdate
			}
			return fmt.Errorf("pg.ExternalEgressRepository.Save: upsert policy: %w", err)
		}

		// 2. The policy-changed event — atomically with the row above.
		traceparent := traceparentFromContext(ctx)
		row, err := buildExternalEgressOutboxRow(next, prev, time.Now().UTC(), traceparent)
		if err != nil {
			return fmt.Errorf("pg.ExternalEgressRepository.Save: build outbox row: %w", err)
		}
		envBytes, err := json.Marshal(row.Envelope)
		if err != nil {
			return fmt.Errorf("pg.ExternalEgressRepository.Save: marshal outbox envelope: %w", err)
		}
		if err := tx.Exec(ctx, insertExternalEgressOutboxSQL,
			row.ID,
			row.TenantID,
			row.GCID,
			row.AggregateType,
			row.AggregateID,
			row.EventType,
			row.Topic,
			row.Payload,
			string(envBytes),
			row.IdempotencyKey,
			row.OccurredAt,
		); err != nil {
			return fmt.Errorf("pg.ExternalEgressRepository.Save: insert outbox: %w", err)
		}

		return nil
	})
}

// Compile-time check: *ExternalEgressRepository satisfies the domain port.
var _ external_egress.Repository = (*ExternalEgressRepository)(nil)
