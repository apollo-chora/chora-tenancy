// subscription_hydrator.go — CHO-1752 boot-time hydrator for the v2
// SubscriptionRegistry.
//
// The registry is in-memory (writes don't currently round-trip to
// Postgres), so every tenancy restart wipes per-tenant state including
// the always-on `base` plan that bootstrap (CHO-1751 Phase 1) writes via
// the registry's Subscribe path. This hydrator closes the read gap on
// boot: it pulls every active row from `add_on_subscriptions` (joined to
// `add_ons` for the code and `members` for the owner GCID) and surfaces
// them so `cmd/server/main.go` can replay them via `registry.Subscribe`.
//
// Add-on identity (CHO-2414). The registry is keyed on add_ons.code, so
// that is what this projection reads. It previously read add_ons.name, the
// human display name, which resolved only for the rows whose display name
// happened to also be a valid code: 2 of 9 on the live estate.
//
// Reading the code column is necessary but was not sufficient on its own.
// Migration 0021 backfilled add_ons.code as a slug of the display name
// (`lower(regexp_replace(name,'[^a-zA-Z0-9]+','_','g'))`), so the stored
// value disagreed with the canonical CHO-1745 catalogue code in 4 of 7
// rows: `training_management_suite` where the seed catalogue and the FE
// setup wizard both say `tms`. Migration 0033 repairs that data.
//
// The one surviving translation is `core` to `base`: the bootstrap saga
// still seeds the baseline add-on with name and code `core`, and
// pg.BootstrapRepository resolves it by `name = 'core'`, so the row is
// left alone in the database and mapped at read time instead.
//
// Query strategy: enumerate `tenants WHERE deleted_at IS NULL` against
// the non-RLS table, then per-tenant `RunInTenantTx` to SELECT the row
// set with `SET LOCAL chora.tenant_id` already applied. This is N+1 by
// design — N runs once at boot — and stays inside the existing
// `Querier` / `TxQuerier` ports without adding a SECURITY DEFINER
// function. If tenant counts ever climb into 5-digit territory, fold
// into a single SECURITY DEFINER query.
package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ActiveSubscriptionRow is the boot-hydration projection of one
// `add_on_subscriptions` row. Carries the minimum the v2
// SubscriptionRegistry needs to call `Subscribe(tenantID, addOnCode,
// ownerGCID)` plus the CHO-1776 dunning flag for pod-restart fidelity.
// AddOnCode is post-translation (`core` → `base`).
type ActiveSubscriptionRow struct {
	TenantID string
	// AddOnCode is add_ons.code, the machine code the SubscriptionRegistry
	// is keyed on, post `core` -> `base` translation. It was previously
	// read from add_ons.name (the human display name), which is why only
	// the rows whose display name coincided with a code ever replayed
	// (CHO-2414). Empty when add_ons.code is NULL (a row that predates the
	// migration-0021 backfill); the caller skips those by name.
	AddOnCode string
	// AddOnName is add_ons.name, carried for diagnostics only. A skip line
	// that prints only a code cannot tell the operator WHICH product failed
	// to replay.
	AddOnName string
	OwnerGCID string
	// PastDue mirrors add_on_subscriptions.past_due (CHO-1776). Hydrated
	// at boot so the FE dunning banner survives chora-tenancy pod
	// restarts; the in-memory registry's MarkPastDue is called by
	// main.go after Subscribe to re-apply the flag.
	PastDue bool
}

// SubscriptionHydrator loads every active subscription row from
// Postgres so chora-tenancy boot can replay them into the in-memory
// v2 SubscriptionRegistry.
type SubscriptionHydrator struct {
	q  QueryRunner
	tq TxQuerier
}

// NewSubscriptionHydrator binds the hydrator to a pgxpool querier.
// Panics on nil so wiring bugs fail loud at boot.
func NewSubscriptionHydrator(q *PgxPoolQuerier) *SubscriptionHydrator {
	if q == nil {
		panic("pg.NewSubscriptionHydrator: nil querier")
	}
	return NewSubscriptionHydratorFor(q, q)
}

// NewSubscriptionHydratorFor is the port-level constructor: `q` serves the
// non-tenant-scoped tenant enumeration and `tq` opens the per-tenant
// SET LOCAL chora.tenant_id transactions. Production passes the same
// *PgxPoolQuerier for both; tests pass stubs.
func NewSubscriptionHydratorFor(q QueryRunner, tq TxQuerier) *SubscriptionHydrator {
	if q == nil || tq == nil {
		panic("pg.NewSubscriptionHydratorFor: nil querier")
	}
	return &SubscriptionHydrator{q: q, tq: tq}
}

// LoadActive returns every active subscription row across every
// non-deleted tenant. Row errors during the per-tenant pass are
// returned as `error` — the caller (main.go) is expected to log the
// hydration count + continue boot when partial results land. A
// repository-level enumeration error fails the whole call.
//
// The owner_gcid lookup picks the canonical `owner` member when
// available; tenants without an owner row return an all-zeros GCID so
// the registry still gets a Subscribe call. The registry's Subscribe is
// idempotent so re-hydration on subsequent restarts is safe.
func (h *SubscriptionHydrator) LoadActive(ctx context.Context) ([]ActiveSubscriptionRow, error) {
	tenantRows, err := h.q.Query(ctx, `
        SELECT tenant_id::text
        FROM tenants
        WHERE deleted_at IS NULL
    `)
	if err != nil {
		return nil, fmt.Errorf("pg.SubscriptionHydrator: enumerate tenants: %w", err)
	}
	var tenantIDs []string
	for tenantRows.Next() {
		var id string
		if scanErr := tenantRows.Scan(&id); scanErr != nil {
			tenantRows.Close()
			return nil, fmt.Errorf("pg.SubscriptionHydrator: scan tenant_id: %w", scanErr)
		}
		tenantIDs = append(tenantIDs, id)
	}
	tenantRows.Close()
	if err := tenantRows.Err(); err != nil {
		return nil, fmt.Errorf("pg.SubscriptionHydrator: tenant enumeration: %w", err)
	}

	out := make([]ActiveSubscriptionRow, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		rows, err := h.loadOneTenant(ctx, tenantID)
		if err != nil {
			return out, fmt.Errorf("pg.SubscriptionHydrator: tenant %s: %w", tenantID, err)
		}
		out = append(out, rows...)
	}
	return out, nil
}

// loadOneTenant runs the per-tenant SELECT inside a `SET LOCAL
// chora.tenant_id` transaction so the RLS policy on
// `add_on_subscriptions` permits the read.
func (h *SubscriptionHydrator) loadOneTenant(ctx context.Context, tenantID string) ([]ActiveSubscriptionRow, error) {
	var rows []ActiveSubscriptionRow
	err := h.tq.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		queryRows, err := tx.Query(ctx, `
            SELECT
                s.tenant_id::text,
                COALESCE(a.code, '') AS add_on_code,
                a.name              AS add_on_name,
                COALESCE(
                    (SELECT m.gcid::text
                       FROM members m
                      WHERE m.tenant_id = s.tenant_id
                        AND m.role = 'owner'::tenant_member_role
                        AND m.deleted_at IS NULL
                      ORDER BY m.joined_at ASC
                      LIMIT 1),
                    '00000000-0000-0000-0000-000000000000'
                ) AS owner_gcid,
                s.past_due
            FROM add_on_subscriptions s
            JOIN add_ons a ON a.add_on_id = s.add_on_id
            WHERE s.tenant_id = $1
              AND s.status = 'active'::entitlement_status
              AND s.ended_at IS NULL
        `, tenantID)
		if err != nil {
			return fmt.Errorf("query subscriptions: %w", err)
		}
		defer queryRows.Close()
		for queryRows.Next() {
			var r ActiveSubscriptionRow
			var rawCode string
			if scanErr := queryRows.Scan(&r.TenantID, &rawCode, &r.AddOnName, &r.OwnerGCID, &r.PastDue); scanErr != nil {
				return fmt.Errorf("scan subscription row: %w", scanErr)
			}
			r.AddOnCode = TranslateAddOnCode(rawCode)
			rows = append(rows, r)
		}
		if err := queryRows.Err(); err != nil {
			return fmt.Errorf("subscription rows iter: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// TranslateAddOnCode maps the legacy `core` code (still written by the
// bootstrap saga's pg adapter) to the canonical CHO-1745 `base` code
// the v2 catalogue + H+ marketplace surface know about. All other codes
// pass through unchanged. Pure helper — exported for testing.
func TranslateAddOnCode(code string) string {
	if code == "core" {
		return "base"
	}
	return code
}

// Compile-time sanity check that pgx.Tx exposes the surface the
// queryRows ranging above expects (Next / Scan / Err / Close). The
// local `Tx` port already declares Query → Rows; this var keeps the
// import non-empty without adding runtime overhead.
var _ = pgx.ErrNoRows
