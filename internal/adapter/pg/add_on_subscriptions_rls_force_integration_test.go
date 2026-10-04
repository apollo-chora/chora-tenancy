//go:build integration

// add_on_subscriptions_rls_force_integration_test.go: the owner hole and the
// unguarded cast on `add_on_subscriptions` (UX Track U, row E4, migration 0040).
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_AddOnSubscriptionsRLS
//
// # WHY, AND WHY BEFORE THE WRITE-THROUGH
//
// Row E4 makes PostgreSQL the source of truth for entitlements. A write-through
// must not be built on a table whose owner can silently bypass the isolation
// that is supposed to protect it, so this lands first.
//
// Two defects, both measured live before the fix, both against the convention
// the rest of the estate already follows (FORCE ROW LEVEL SECURITY is on 243
// tables across 117 migrations, and policies cast NULLIF-guarded):
//
//  1. RLS was ENABLED but NOT FORCED (`relrowsecurity = t`,
//     `relforcerowsecurity = f`). PostgreSQL exempts a table's OWNER from its
//     own policies unless FORCE is set, and `chora_tenancy_migrate` owns this
//     table. So an owner connection read every tenant's rows with no tenant
//     context at all. That is the "an owner read is a FALSE FULL at
//     force=false" trap: it also means any test proving a tenant-scoped read
//     from an owner connection proved nothing.
//
//  2. The policy cast `current_setting('chora.tenant_id', true)::uuid` with no
//     NULLIF guard. An unset GUC yields NULL, which casts fine, but a GUC set
//     to the EMPTY STRING raises 22P02 and the query dies instead of returning
//     zero rows. The ADR-192 pattern is NULLIF-safe for exactly this reason:
//     an isolation failure should read as "no rows", never as a 500.
//
// NOT A LIVE BYPASS, verified rather than assumed: chora-tenancy connects as
// `chora_tenancy_app_rw` (deployment env `CHORA_DB_DSN_SECRET_ID` resolves
// `chora-dev-cloudsql-chora_tenancy-app_rw-dsn`, whose DSN username reads
// `chora_tenancy_app_rw`), and app_rw is NOBYPASSRLS per migration 0013 and was
// already filtered. FORCE closes the hole for the owner role, which is the
// migrations runner and this test suite. So this is hardening, not an incident.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// countSubscriptionsWithGUC opens a transaction, applies `SET LOCAL
// chora.tenant_id` when guc is non-nil, and counts visible rows for the seeded
// tenant. Returns the count and any error, so a caller can assert on either.
//
// A nil guc means "set nothing", which is the owner-with-no-tenant-context
// case; a pointer to "" means "set it to the empty string", which is the
// unguarded-cast case. The two are different states and the test needs both.
func countSubscriptionsWithGUC(
	t *testing.T, pool *pgxpool.Pool, guc *string, tenantID string,
) (int, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// ⚠ MEASURE AS THE ROLE PRODUCTION USES, NOT AS THE CONNECTION ROLE.
	//
	// `CHORA_TEST_DSN` connects as `chora_tenancy_migrate`, and on the local
	// mirror that role carries `rolsuper = true`. A SUPERUSER bypasses RLS
	// unconditionally, and FORCE ROW LEVEL SECURITY does NOT override that:
	// FORCE only removes the table-OWNER exemption. So without this SET LOCAL
	// ROLE every isolation arm below reads the row even after 0040, and the
	// test would report a false red against a correct migration.
	//
	// `chora_tenancy_app_rw` is what chora-tenancy actually connects as in
	// production (verified from the deployment's DSN secret), is NOBYPASSRLS
	// per migration 0013, and is not the table owner. It is therefore both the
	// honest subject and the only one that can measure this.
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE chora_tenancy_app_rw`); err != nil {
		t.Fatalf("set local role: %v", err)
	}

	if guc != nil {
		// set_config with is_local=true is SET LOCAL, and unlike SET LOCAL it
		// takes a bind parameter, so the empty string can be passed as data.
		if _, err := tx.Exec(ctx, `SELECT set_config('chora.tenant_id', $1, true)`, *guc); err != nil {
			return 0, err
		}
	}
	var n int
	err = tx.QueryRow(ctx,
		`SELECT count(*) FROM add_on_subscriptions WHERE tenant_id = $1`, tenantID).Scan(&n)
	return n, err
}

// seedOneSubscription writes a single row for a fresh tenant, INSIDE a tenant
// transaction so it works both before and after FORCE.
func seedOneSubscription(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	tenantID := uuid.Must(uuid.NewV7()).String()

	if _, err := pool.Exec(ctx, `
        INSERT INTO tenants (tenant_id, name, status)
        VALUES ($1, $2, 'active')`, tenantID, "E4 RLS "+tenantID[:8]); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	var addOnID string
	if err := pool.QueryRow(ctx, `SELECT add_on_id FROM add_ons LIMIT 1`).Scan(&addOnID); err != nil {
		t.Fatalf("read an add_on: %v", err)
	}

	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('chora.tenant_id', $1, true)`, tenantID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
            INSERT INTO add_on_subscriptions
                (subscription_id, tenant_id, add_on_id, status,
                 monthly_price_cents_snap, started_at, created_at, updated_at, past_due)
            VALUES ($1, $2, $3, 'active'::entitlement_status, 0, now(), now(), now(), false)`,
			uuid.Must(uuid.NewV7()).String(), tenantID, addOnID)
		return err
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	t.Cleanup(func() {
		c := context.Background()
		_ = pgx.BeginFunc(c, pool, func(tx pgx.Tx) error {
			_, _ = tx.Exec(c, `SELECT set_config('chora.tenant_id', $1, true)`, tenantID)
			_, _ = tx.Exec(c, `DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, tenantID)
			return nil
		})
		_, _ = pool.Exec(c, `DELETE FROM tenants WHERE tenant_id = $1`, tenantID)
	})
	return tenantID
}

// TestIntegration_AddOnSubscriptionsRLS_ForcedAndNullifGuarded is the live
// posture read. Asserted against pg_class and pg_policy rather than against the
// migration text, because the migration set is not the schema source.
func TestIntegration_AddOnSubscriptionsRLS_ForcedAndNullifGuarded(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()

	var enabled, forced bool
	if err := pool.QueryRow(ctx, `
        SELECT relrowsecurity, relforcerowsecurity
          FROM pg_class WHERE relname = 'add_on_subscriptions'`).Scan(&enabled, &forced); err != nil {
		t.Fatalf("read pg_class: %v", err)
	}
	if !enabled {
		t.Fatal("RLS is not even enabled on add_on_subscriptions")
	}
	if !forced {
		t.Fatal("RLS is ENABLED but NOT FORCED: the table-owning migrate role bypasses every policy, so an owner connection reads every tenant's entitlements with no tenant context")
	}

	var qual string
	if err := pool.QueryRow(ctx, `
        SELECT pg_get_expr(polqual, polrelid)
          FROM pg_policy WHERE polrelid = 'add_on_subscriptions'::regclass
           AND polname = 'tenant_isolation'`).Scan(&qual); err != nil {
		t.Fatalf("read pg_policy: %v", err)
	}
	if !strings.Contains(qual, "NULLIF") {
		t.Fatalf("tenant_isolation casts without a NULLIF guard (%s); an empty-string GUC raises 22P02 instead of returning zero rows", qual)
	}
}

// TestIntegration_AddOnSubscriptionsRLS_OwnerWithNoTenantSeesNothing is arm one:
// the false full. Before 0040 an owner connection with NO tenant context read
// the row; after, it reads none.
func TestIntegration_AddOnSubscriptionsRLS_OwnerWithNoTenantSeesNothing(t *testing.T) {
	pool := liveDB(t)
	tenantID := seedOneSubscription(t, pool)

	n, err := countSubscriptionsWithGUC(t, pool, nil, tenantID)
	if err != nil {
		t.Fatalf("owner read with no tenant context errored: %v", err)
	}
	if n != 0 {
		t.Fatalf("owner connection with NO tenant context saw %d row(s); FORCE ROW LEVEL SECURITY is off, so the owner bypasses tenant_isolation and every cross-tenant read from this role is a false full", n)
	}
}

// TestIntegration_AddOnSubscriptionsRLS_EmptyGUCIsZeroRowsNotAnError is arm two:
// the unguarded cast. An empty GUC must read as "no tenant, no rows", never as
// 22P02, or an isolation miss surfaces as a 500.
//
// ⚠ Measured, and worth stating because it corrects the obvious guess: BEFORE
// 0040 this arm did NOT fail with 22P02. It failed by returning the row, like
// the other two arms, because with FORCE off the owner bypasses RLS and the
// policy is never evaluated at all, so the unguarded cast never runs. The
// missing NULLIF is INVISIBLE from an owner connection and only becomes
// observable once FORCE is on. That is the same lesson as the false full: with
// force=false an owner connection cannot measure this table's isolation in any
// direction.
func TestIntegration_AddOnSubscriptionsRLS_EmptyGUCIsZeroRowsNotAnError(t *testing.T) {
	pool := liveDB(t)
	tenantID := seedOneSubscription(t, pool)

	empty := ""
	n, err := countSubscriptionsWithGUC(t, pool, &empty, tenantID)
	if err != nil {
		t.Fatalf("an EMPTY chora.tenant_id raised instead of returning zero rows: %v. The policy casts current_setting(...)::uuid with no NULLIF guard, so '' reaches ::uuid and raises 22P02 rather than isolating", err)
	}
	if n != 0 {
		t.Fatalf("an empty tenant GUC saw %d row(s), want 0", n)
	}
}

// TestIntegration_AddOnSubscriptionsRLS_WrongTenantSeesNothing is the arm that
// must hold in BOTH postures. Without it the two arms above could be satisfied
// by a policy that hides everything from everyone.
func TestIntegration_AddOnSubscriptionsRLS_WrongTenantSeesNothing(t *testing.T) {
	pool := liveDB(t)
	tenantID := seedOneSubscription(t, pool)

	other := uuid.Must(uuid.NewV7()).String()
	n, err := countSubscriptionsWithGUC(t, pool, &other, tenantID)
	if err != nil {
		t.Fatalf("wrong-tenant read errored: %v", err)
	}
	if n != 0 {
		t.Fatalf("tenant %s saw %d row(s) belonging to %s", other, n, tenantID)
	}
}

// TestIntegration_AddOnSubscriptionsRLS_RightTenantStillSeesItsOwn is the
// POSITIVE CONTROL. FORCE plus a NULLIF guard could both be satisfied by a
// table nobody can read at all; this proves the fix narrowed access rather than
// removing it, and that the seeded row genuinely exists.
func TestIntegration_AddOnSubscriptionsRLS_RightTenantStillSeesItsOwn(t *testing.T) {
	pool := liveDB(t)
	tenantID := seedOneSubscription(t, pool)

	n, err := countSubscriptionsWithGUC(t, pool, &tenantID, tenantID)
	if err != nil {
		t.Fatalf("own-tenant read errored: %v", err)
	}
	if n != 1 {
		t.Fatalf("tenant %s saw %d of its OWN rows, want 1; the fix removed access rather than narrowing it", tenantID, n)
	}
}
