// bootstrap_repository.go — pgx-backed implementation of bootstrap.Repository
// for the H+ Setup-Tenant Phase 1 endpoint (CHO-1628).
//
// SQL contract:
//
//	HasAnyMembership:
//	  Calls the `list_memberships_by_gcid(uuid)` SECURITY DEFINER
//	  function (migration 0011) which runs with `row_security = off`
//	  so the cross-tenant membership directory read is NOT filtered
//	  by the `tenant_isolation` RLS policy on members. Returns true
//	  iff the row count is > 0. The function already filters out
//	  soft-deleted + suspended members, so a `> 0` answer is
//	  authoritative for the idempotency rule.
//
//	Persist:
//	  Single transaction via `RunInTenantTx(b.Tenant.ID, fn)` so
//	  `SET LOCAL chora.tenant_id = '<new>'` is in scope before the
//	  RLS-protected INSERTs on `members` and `add_on_subscriptions`.
//	  The four statements MUST land atomically:
//
//	    1. INSERT INTO tenants            (no RLS)
//	    2. INSERT INTO members            (RLS-protected — relies on SET LOCAL)
//	    3. SELECT add_on_id FROM add_ons WHERE name = 'core'
//	    4. INSERT INTO add_on_subscriptions (RLS-protected — relies on SET LOCAL)
//
//	  Step 3 fails with ErrCoreAddOnNotFound if the `core` add-on row
//	  is missing from the catalogue — that is a deployment-seed issue,
//	  not a caller issue. The transaction rolls back so no orphan
//	  tenant or member rows survive.
//
// Schema references:
//
//   - services/chora-tenancy/migrations/0001_initial.sql
//     tenants, members, add_ons, add_on_subscriptions
//   - services/chora-tenancy/migrations/0011_list_memberships_by_gcid_security_definer.sql
//     list_memberships_by_gcid(uuid) SECURITY DEFINER fn
//   - services/chora-tenancy/migrations/0013_app_rw_nobypassrls.sql
//     app_rw is NOBYPASSRLS — SET LOCAL is mandatory
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

// ErrCoreAddOnNotFound is returned when the `core` add-on row is missing
// from the `add_ons` catalogue. Surfaces deployment/seed gaps loudly.
var ErrCoreAddOnNotFound = errors.New("pg.BootstrapRepository: core add-on not found in catalogue")

// BootstrapRepository is the production pgx-backed adapter for
// bootstrap.Repository.
type BootstrapRepository struct {
	q *PgxPoolQuerier
}

// NewBootstrapRepository constructs the production-wired repository.
// Panics on nil querier so wiring bugs fail loud at boot, not at request
// time.
func NewBootstrapRepository(q *PgxPoolQuerier) *BootstrapRepository {
	if q == nil {
		panic("pg.NewBootstrapRepository: nil PgxPoolQuerier")
	}
	return &BootstrapRepository{q: q}
}

// HasAnyMembership returns true iff the GCID is already tied to ANY
// tenant via a non-deleted, non-suspended members row. Uses the
// SECURITY DEFINER `list_memberships_by_gcid` function for the RLS
// bypass — see migration 0011 for the why.
func (r *BootstrapRepository) HasAnyMembership(ctx context.Context, gcid string) (bool, error) {
	var n int
	err := r.q.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM list_memberships_by_gcid($1::uuid)`,
		gcid,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("pg.BootstrapRepository.HasAnyMembership: %w", err)
	}
	return n > 0, nil
}

// resolveAddOnID maps an add-on CODE to its catalogue id.
//
// The column matters. `add_ons` carries both `name` and `code`, and since
// migration 0035 the canonical codes live in `code` while `name` holds a human
// display string ("Training Management Suite" for `tms`). This lookup used to
// read `WHERE name = $1` and passed a code; it worked only because `core` is
// the single row whose name equals its code, and the only code it was ever
// asked for. The moment a second code arrives, a name-keyed lookup finds
// nothing and rolls the whole tenant creation back.
//
// The `name` arm is a NARROW legacy fallback, not a general alternative:
// `core` predates the `code` column and an older database may still carry it
// with `code` NULL. It is restricted to `code IS NULL` on purpose. A plain
// `code = $1 OR name = $1` would be worse than the bug it fixes, because a
// code that happened to equal some OTHER row's display name would resolve to
// an arbitrary one of the two under LIMIT 1, granting an add-on nobody asked
// for and saying nothing. With the restriction, a row that HAS a code can only
// ever be matched by that code, so there is no ambiguity left to log.
//
// Returns ErrNoRows when the code is unknown, which the caller turns into a
// named failure that rolls the whole creation back.
func resolveAddOnID(ctx context.Context, tx Tx, code string) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
        SELECT add_on_id::text
        FROM   add_ons
        WHERE  code = $1
           OR  (code IS NULL AND name = $1)
        ORDER  BY (code = $1) DESC NULLS LAST
        LIMIT  1`,
		code,
	).Scan(&id)
	return id, err
}

// Persist commits the three-row Bundle inside a single transaction. See
// the file-level SQL contract for the per-statement sequence.
func (r *BootstrapRepository) Persist(ctx context.Context, b *bootstrap.Bundle) error {
	if b == nil {
		return errors.New("pg.BootstrapRepository.Persist: nil bundle")
	}

	return r.q.RunInTenantTx(ctx, b.Tenant.ID, func(ctx context.Context, tx Tx) error {
		// 1. tenants — no RLS on this table; slug/country/currency
		// default to their schema defaults (slug NULL, country 'SG',
		// currency 'SGD'). Created/Updated stamped from the Bundle.
		//
		// ADR-217: parent_tenant_id (NULL for a root, the parent UUID for a
		// sub-tenant) + hosting_mode (migration 0026). Empty hosting_mode
		// COALESCEs to PLATFORM_HOSTED (root default). The parent-invariant
		// trigger (0026) validates a non-NULL parent resolves to the
		// chora-master tree, so a foreign-root/cyclic parent fails this INSERT
		// and rolls back the whole bundle.
		insertTenant := `
            INSERT INTO tenants (
                tenant_id, name, parent_tenant_id, hosting_mode, branding,
                self_hosted, status, created_at, updated_at
            )
            VALUES ($1, $2, NULLIF($3, '')::uuid,
                    COALESCE(NULLIF($4, ''), 'PLATFORM_HOSTED')::tenant_hosting_mode,
                    $5::jsonb, $6, $7::tenant_status, $8, $8)
        `
		if err := tx.Exec(ctx, insertTenant,
			b.Tenant.ID,
			b.Tenant.Name,
			b.Tenant.ParentTenantID,
			b.Tenant.HostingMode,
			b.Tenant.Branding,
			b.Tenant.SelfHosted,
			b.Tenant.Status,
			b.CreatedAt,
		); err != nil {
			return fmt.Errorf("pg.BootstrapRepository.Persist: insert tenant: %w", err)
		}

		// 2. members — RLS-protected; SET LOCAL chora.tenant_id is
		// already in scope (matches b.Tenant.ID). Role lands in the
		// tenant_member_role ENUM via explicit cast.
		insertMember := `
            INSERT INTO members (
                member_id, tenant_id, gcid, role, invited_at, joined_at,
                created_at, updated_at
            )
            VALUES ($1, $2, $3::uuid, $4::tenant_member_role, $5, $5, $5, $5)
        `
		if err := tx.Exec(ctx, insertMember,
			b.OwnerMember.ID,
			b.OwnerMember.TenantID,
			b.OwnerMember.GCID,
			b.OwnerMember.Role,
			b.OwnerMember.JoinedAt,
		); err != nil {
			return fmt.Errorf("pg.BootstrapRepository.Persist: insert member: %w", err)
		}

		// 3. Resolve the `core` add-on id. add_ons has no RLS — readable
		// from inside the tenant-scoped tx without issue.
		addOnID, err := resolveAddOnID(ctx, tx, b.Entitlement.AddOnCode)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return fmt.Errorf("%w (code=%q)", ErrCoreAddOnNotFound, b.Entitlement.AddOnCode)
			}
			return fmt.Errorf("pg.BootstrapRepository.Persist: lookup core add-on: %w", err)
		}

		// 4. add_on_subscriptions (= TenantEntitlement) — RLS-protected.
		// status defaults to 'active'; price snap stays 0 for the free
		// core add-on. started_at + created_at stamped from Bundle.
		insertSub := `
            INSERT INTO add_on_subscriptions (
                subscription_id, tenant_id, add_on_id, status,
                started_at, created_at, updated_at
            )
            VALUES ($1, $2, $3::uuid, 'active'::entitlement_status, $4, $4, $4)
        `
		if err := tx.Exec(ctx, insertSub,
			b.Entitlement.ID,
			b.Entitlement.TenantID,
			addOnID,
			b.CreatedAt,
		); err != nil {
			return fmt.Errorf("pg.BootstrapRepository.Persist: insert subscription: %w", err)
		}

		// 4b. The add-ons chosen at creation (E1 / UX refactor wave W2), in the
		// SAME transaction, so a created organisation holds its entitlements
		// across a restart rather than only in the in-memory registry. An
		// unrecognised code fails the WHOLE creation: granting the subset we
		// happened to recognise would leave the operator unable to see which
		// half they got.
		for _, extra := range b.ExtraEntitlements {
			extraID, err := resolveAddOnID(ctx, tx, extra.AddOnCode)
			if err != nil {
				if errors.Is(err, ErrNoRows) {
					return fmt.Errorf("%w %q", bootstrap.ErrUnknownAddOnCode, extra.AddOnCode)
				}
				return fmt.Errorf("pg.BootstrapRepository.Persist: lookup add-on %q: %w", extra.AddOnCode, err)
			}
			if err := tx.Exec(ctx, insertSub,
				extra.ID,
				extra.TenantID,
				extraID,
				b.CreatedAt,
			); err != nil {
				return fmt.Errorf("pg.BootstrapRepository.Persist: insert subscription %q: %w", extra.AddOnCode, err)
			}
		}

		// 5. outbox_events — CHO-1630 Phase 2. Emit
		// chora.tenancy.tenant.bootstrapped.v1 atomically with the
		// three INSERTs above so the dispatcher (separate goroutine)
		// can durably drain it to Cloud Pub/Sub. chora-identity
		// subscribes and writes the mirror tenant_memberships row.
		//
		// outbox_events is NOT tenant-scoped — RLS is enforced via
		// the row's tenant_id column at write time (D6.3 isolation).
		traceparent := traceparentFromContext(ctx)
		row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), traceparent)
		if err != nil {
			return fmt.Errorf("pg.BootstrapRepository.Persist: build outbox row: %w", err)
		}
		envBytes, err := json.Marshal(row.Envelope)
		if err != nil {
			return fmt.Errorf("pg.BootstrapRepository.Persist: marshal outbox envelope: %w", err)
		}
		insertOutbox := `
            INSERT INTO outbox_events (
                id, tenant_id, gcid, aggregate_type, aggregate_id,
                event_type, topic, payload, envelope, idempotency_key,
                occurred_at, status
            )
            VALUES ($1, $2, NULLIF($3, '')::uuid, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending')
        `
		if err := tx.Exec(ctx, insertOutbox,
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
			return fmt.Errorf("pg.BootstrapRepository.Persist: insert outbox: %w", err)
		}

		return nil
	})
}

// traceparentFromContext extracts the W3C traceparent header from
// context if the HTTP layer stored it there. Returns empty when the
// context has no trace context (out-of-band bootstrap callers).
func traceparentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v := ctx.Value(traceparentCtxKey{})
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// traceparentCtxKey is the context-key type for the W3C traceparent
// header. Exported indirectly via WithTraceparent / traceparentFromContext.
type traceparentCtxKey struct{}

// WithTraceparent stamps a W3C traceparent string on the context so the
// outbox row can echo it into the event envelope. Called by the HTTP
// handler before invoking Service.Bootstrap.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return context.WithValue(ctx, traceparentCtxKey{}, traceparent)
}

// Compile-time check: *BootstrapRepository satisfies bootstrap.Repository.
var _ bootstrap.Repository = (*BootstrapRepository)(nil)
