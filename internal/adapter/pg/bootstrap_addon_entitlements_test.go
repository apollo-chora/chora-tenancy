//go:build integration

// bootstrap_addon_entitlements_test.go: E1 (UX refactor wave W2), the durable
// half of "a created organisation holds its entitlements after a restart".
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_BootstrapRepository_AddOn
//
// Two things these tests pin that a stubbed repository could not.
//
//  1. The add-on is resolved by CODE. `add_ons` carries both `name` and `code`,
//     and since migration 0035 the canonical codes live in `code` while `name`
//     holds a display string ("Training Management Suite" for `tms`). The
//     pre-E1 statement was `WHERE name = $1` and worked only because `core` is
//     the one row whose name equals its code, and the only code it was ever
//     asked for. Against a stub that distinction is invisible; against the real
//     table it is the difference between a granted add-on and a rolled-back
//     tenant.
//
//  2. Atomicity on an unknown code. The whole creation must fail with NO tenant
//     row left behind. Half a tenant is worse than none: the operator cannot
//     see which half they got.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

func TestIntegration_BootstrapRepository_AddOnEntitlements_persistedDurably(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	b, err := bootstrap.New(bootstrap.Input{
		Name:       "E1 Durable AddOns " + uuid.NewString()[:8],
		OwnerGCID:  uuid.NewString(),
		AddOnCodes: []string{"tms", "cms"},
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	if err := repo.Persist(context.Background(), b); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Read back through a FRESH query, joining to add_ons so we assert on
	// codes. Nothing in memory is consulted, which is the whole point.
	rows, err := pool.Query(context.Background(), `
        SELECT COALESCE(a.code, a.name)
        FROM   add_on_subscriptions s
        JOIN   add_ons a ON a.add_on_id = s.add_on_id
        WHERE  s.tenant_id = $1
        ORDER  BY COALESCE(a.code, a.name)`, b.Tenant.ID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, code)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := "cms,core,tms"
	if strings.Join(got, ",") != want {
		t.Fatalf("durable entitlement codes = %v, want [%s]. Before E1 only `core` was written and "+
			"the chosen add-ons lived in an in-memory registry that dies with the pod", got, want)
	}
}

func TestIntegration_BootstrapRepository_AddOnEntitlements_unknownCodeRollsBackEverything(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	b, err := bootstrap.New(bootstrap.Input{
		Name:       "E1 Unknown AddOn " + uuid.NewString()[:8],
		OwnerGCID:  uuid.NewString(),
		AddOnCodes: []string{"tms", "not-a-real-add-on"},
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	if err := repo.Persist(context.Background(), b); err == nil {
		t.Fatal("Persist accepted an unknown add-on code; the creation must fail loud instead of " +
			"granting the subset it recognised")
	}

	// Atomicity: no tenant, no member, no subscription. A partially created
	// organisation is the failure mode this test exists to exclude.
	for _, probe := range []struct {
		label string
		sql   string
	}{
		{"tenants", `SELECT count(*) FROM tenants WHERE tenant_id = $1`},
		{"members", `SELECT count(*) FROM members WHERE tenant_id = $1`},
		{"add_on_subscriptions", `SELECT count(*) FROM add_on_subscriptions WHERE tenant_id = $1`},
	} {
		var n int
		if err := pool.QueryRow(context.Background(), probe.sql, b.Tenant.ID).Scan(&n); err != nil {
			t.Fatalf("%s probe: %v", probe.label, err)
		}
		if n != 0 {
			t.Errorf("%s left %d row(s) behind after a failed create; the transaction must roll back whole", probe.label, n)
		}
	}
}

func TestIntegration_BootstrapRepository_AddOnEntitlements_resolvesByCodeNotName(t *testing.T) {
	// The sharp one. `tms` has name='Training Management Suite', so a lookup
	// keyed on `name` finds nothing and rolls the creation back. This test
	// fails against the pre-E1 statement and passes only when the resolution
	// reads `code`.
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	var wantID string
	if err := pool.QueryRow(context.Background(),
		`SELECT add_on_id::text FROM add_ons WHERE code = 'tms'`).Scan(&wantID); err != nil {
		t.Skipf("local catalogue has no `tms` row (apply migration 0035): %v", err)
	}

	b, err := bootstrap.New(bootstrap.Input{
		Name:       "E1 Code Not Name " + uuid.NewString()[:8],
		OwnerGCID:  uuid.NewString(),
		AddOnCodes: []string{"tms"},
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	if err := repo.Persist(context.Background(), b); err != nil {
		t.Fatalf("Persist: %v (a lookup keyed on add_ons.name cannot find `tms`, whose name is "+
			"'Training Management Suite')", err)
	}

	var gotID string
	if err := pool.QueryRow(context.Background(), `
        SELECT s.add_on_id::text
        FROM   add_on_subscriptions s
        JOIN   add_ons a ON a.add_on_id = s.add_on_id
        WHERE  s.tenant_id = $1 AND a.code = 'tms'`, b.Tenant.ID).Scan(&gotID); err != nil {
		t.Fatalf("no subscription resolved to the tms add_on: %v", err)
	}
	if gotID != wantID {
		t.Errorf("subscription points at %q, want the tms add_on %q", gotID, wantID)
	}
}

func TestIntegration_BootstrapRepository_AddOnEntitlements_nameArmCannotOutrankACodeMatch(t *testing.T) {
	// UX-subagent2's review point: a `name` fallback that can win against a
	// different row's `code` is worse than the bug it fixes, because it grants
	// an add-on nobody asked for and says nothing.
	//
	// The collision that is actually reachable, given `add_ons.name` is UNIQUE:
	//   row A  code = X          name = "<something else>"
	//   row B  code = NULL       name = X
	// Both satisfy the WHERE. Resolution must pick A, the code match. Row B is
	// the shape the legacy arm exists for (a pre-`code` row), which is exactly
	// why the arm cannot be allowed to outrank a real code.
	pool := liveDB(t)
	ctx := context.Background()
	code := "e1-decoy-" + uuid.NewString()[:8]

	// Sweep any decoy a PREVIOUS run left behind, before seeding this one.
	// The t.Cleanup below deletes by id and is enough when the test finishes,
	// but it does not run if the process is killed, and this mirror is shared:
	// two orphaned decoys from an interrupted run were already in it, and they
	// fail TestCatalogueSeed_FullMigrationSetLeavesNoDrift for everybody,
	// because the boot guard refuses to serve on catalogue drift. A test that
	// can only tidy up after a clean exit is not tidy on a shared database.
	if _, err := pool.Exec(ctx, `
        DELETE FROM add_on_subscriptions WHERE add_on_id IN (
            SELECT add_on_id FROM add_ons WHERE name LIKE 'e1-decoy-%' OR code LIKE 'e1-decoy-%')`); err != nil {
		t.Fatalf("sweep stale decoy subscriptions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
        DELETE FROM add_ons WHERE name LIKE 'e1-decoy-%' OR code LIKE 'e1-decoy-%'`); err != nil {
		t.Fatalf("sweep stale decoy add-ons: %v", err)
	}

	// Insert the LEGACY row first, on purpose. A naive `code = $1 OR name = $1
	// LIMIT 1` has no ORDER BY, so it returns whichever row the scan reaches
	// first; with the decoy physically ahead of the coded row, the naive query
	// picks the wrong one and this test fails against it. Seeded the other way
	// round the test passes under both queries and proves nothing, which is
	// how a guard ends up manufacturing confidence it has not earned.
	var legacyID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO add_ons (add_on_id, name, code) VALUES (gen_random_uuid(), $1, NULL)
         RETURNING add_on_id::text`, code).Scan(&legacyID); err != nil {
		t.Fatalf("seed legacy name-only row: %v", err)
	}
	var codedID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO add_ons (add_on_id, name, code) VALUES (gen_random_uuid(), $1, $2)
         RETURNING add_on_id::text`, code+" display", code).Scan(&codedID); err != nil {
		t.Fatalf("seed coded row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM add_on_subscriptions WHERE add_on_id IN ($1::uuid, $2::uuid)`, codedID, legacyID)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM add_ons WHERE add_on_id IN ($1::uuid, $2::uuid)`, codedID, legacyID)
	})

	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
	b, err := bootstrap.New(bootstrap.Input{
		Name:       "E1 Decoy " + uuid.NewString()[:8],
		OwnerGCID:  uuid.NewString(),
		AddOnCodes: []string{code},
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	if err := repo.Persist(ctx, b); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	var gotID string
	if err := pool.QueryRow(ctx,
		`SELECT s.add_on_id::text FROM add_on_subscriptions s
          JOIN add_ons a ON a.add_on_id = s.add_on_id
         WHERE s.tenant_id = $1 AND COALESCE(a.code, a.name) <> 'core'`,
		b.Tenant.ID).Scan(&gotID); err != nil {
		t.Fatalf("no non-core subscription written: %v", err)
	}
	if gotID == legacyID {
		t.Fatalf("resolved to the name-only legacy row; the name arm must never outrank a code match")
	}
	if gotID != codedID {
		t.Errorf("resolved to %q, want the CODED row %q", gotID, codedID)
	}
}
