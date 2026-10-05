//go:build integration

// tenant_hierarchy_migration_integration_test.go — live verification of the
// ADR-217 (Hierarchical tenancy, Model β) Phase-1 migration 0026. CHO-2003.
//
// Reuses the liveDB(t) harness in integration_test.go (same pg_test package).
// Point it at the local Postgres 18:
//
//	# local Postgres
//	export CHORA_TEST_DSN='postgres://chora_tenancy_app_rw:dev@localhost:5432/chora_tenancy?sslmode=disable'
//	go test -tags integration ./internal/adapter/pg/ -run ADR217 -v
//
// Every write runs inside a transaction that is ROLLED BACK — this suite never
// persists a row (tenants + members inserts use savepoints / rollback). It
// asserts the state migration 0026 must produce, so it FAILS before 0026 is
// applied (RED) and PASSES after (GREEN).
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// choraMasterID is the immutable public + operator + franchisor-root tenant
// (ADR-182 D1 / ADR-217 D1), seeded by migration 0021.
const choraMasterID = "00000000-0000-7000-8000-000000000001"

// the four active customer tenants re-parented under chora-master by 0026.
var adr217ReparentedIDs = []string{
	"11111111-1111-7111-8111-111111111111", // Mighty Mind Tuition Agency
	"22222222-2222-7222-8222-222222222222", // Chen's Coaching
	"019e899d-99d7-7064-a732-6c9ca6955cda", // ACE
	"019eb1b4-bfdc-7591-ab0d-3aa33488d1f3", // MTM Phyllis Academy
}

// TestIntegration_ADR217_Schema_And_MasterRename verifies (a) the hosting_mode
// enum column and (c) the chora-master display-name rename.
func TestIntegration_ADR217_Schema_And_MasterRename(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (a) column presence + type + nullability + default.
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.columns
		   WHERE table_name='tenants' AND column_name='hosting_mode')`).Scan(&exists); err != nil {
		t.Fatalf("column probe: %v", err)
	}
	if !exists {
		t.Fatalf("tenants.hosting_mode column missing — migration 0026 not applied (RED)")
	}
	var isNullable, colDefault, udtName string
	if err := pool.QueryRow(ctx,
		`SELECT is_nullable, COALESCE(column_default,''), udt_name
		   FROM information_schema.columns
		  WHERE table_name='tenants' AND column_name='hosting_mode'`).
		Scan(&isNullable, &colDefault, &udtName); err != nil {
		t.Fatalf("column meta: %v", err)
	}
	if isNullable != "NO" {
		t.Errorf("hosting_mode is_nullable = %q, want NO", isNullable)
	}
	if udtName != "tenant_hosting_mode" {
		t.Errorf("hosting_mode udt = %q, want tenant_hosting_mode", udtName)
	}
	if !strings.Contains(colDefault, "PLATFORM_HOSTED") {
		t.Errorf("hosting_mode default = %q, want it to contain PLATFORM_HOSTED", colDefault)
	}

	// (a) enum labels — exactly the proto/OpenAPI wire set, in order.
	var labels []string
	if err := pool.QueryRow(ctx,
		`SELECT array_agg(enumlabel::text ORDER BY enumsortorder)
		   FROM pg_enum
		  WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname='tenant_hosting_mode')`).
		Scan(&labels); err != nil {
		t.Fatalf("enum labels: %v", err)
	}
	want := []string{"PLATFORM_HOSTED", "WHITE_LABEL", "FRANCHISE", "SELF_HOST"}
	if strings.Join(labels, ",") != strings.Join(want, ",") {
		t.Errorf("tenant_hosting_mode labels = %v, want %v", labels, want)
	}

	// (c) chora-master rename — name flipped, slug + id + root-status unchanged.
	var name, slug, parent string
	if err := pool.QueryRow(ctx,
		`SELECT name, COALESCE(slug,''), COALESCE(parent_tenant_id::text,'')
		   FROM tenants WHERE tenant_id = $1`, choraMasterID).
		Scan(&name, &slug, &parent); err != nil {
		t.Fatalf("chora-master read: %v", err)
	}
	if name != "Chora Platform" {
		t.Errorf("chora-master name = %q, want 'Chora Platform' (RED if still 'Chora')", name)
	}
	if slug != "chora-master" {
		t.Errorf("chora-master slug = %q, want 'chora-master' (must be immutable)", slug)
	}
	if parent != "" {
		t.Errorf("chora-master parent = %q, want root (NULL)", parent)
	}
}

// TestIntegration_ADR217_ReparentedTenants_Franchise verifies (b): each of the
// four target tenants (when present) is a direct child of chora-master and is
// classified FRANCHISE. Absent rows are skipped (a bare mirror has none unless
// seeded), so the same test runs against local + dev.
func TestIntegration_ADR217_ReparentedTenants_Franchise(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	checked := 0
	for _, id := range adr217ReparentedIDs {
		var parent, hostingMode string
		var live bool
		err := pool.QueryRow(ctx,
			`SELECT COALESCE(parent_tenant_id::text,''), hosting_mode::text, (deleted_at IS NULL)
			   FROM tenants WHERE tenant_id = $1`, id).Scan(&parent, &hostingMode, &live)
		if err != nil {
			// pgx.ErrNoRows surfaces as "no rows in result set".
			if strings.Contains(err.Error(), "no rows") {
				t.Logf("tenant %s absent — skipping (seed it to assert locally)", id)
				continue
			}
			t.Fatalf("read tenant %s: %v", id, err)
		}
		if !live {
			t.Logf("tenant %s soft-deleted — skipping", id)
			continue
		}
		checked++
		if parent != choraMasterID {
			t.Errorf("tenant %s parent = %q, want chora-master %s (RED if unparented)", id, parent, choraMasterID)
		}
		if hostingMode != "FRANCHISE" {
			t.Errorf("tenant %s hosting_mode = %q, want FRANCHISE", id, hostingMode)
		}
	}
	t.Logf("ADR-217 re-parent assertions checked on %d/%d target tenants", checked, len(adr217ReparentedIDs))
}

// TestIntegration_ADR217_ParentInvariant_Trigger verifies (d): the parent
// invariant accepts the chora-master tree, exempts roots, and rejects
// foreign-root parents + cycles. Non-destructive (single rolled-back tx +
// savepoints around the expected-rejection writes).
func TestIntegration_ADR217_ParentInvariant_Trigger(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // never commits

	insertTenant := func(id, parent string) error {
		if parent == "" {
			_, e := tx.Exec(ctx,
				`INSERT INTO tenants (tenant_id, name, status) VALUES ($1, 'adr217-inv', 'active')`, id)
			return e
		}
		_, e := tx.Exec(ctx,
			`INSERT INTO tenants (tenant_id, name, status, parent_tenant_id) VALUES ($1, 'adr217-inv', 'active', $2)`,
			id, parent)
		return e
	}
	mustOK := func(desc, id, parent string) {
		if e := insertTenant(id, parent); e != nil {
			t.Fatalf("%s: expected success, got %v", desc, e)
		}
	}
	// SAVEPOINT-wrapped expected rejection: recovers the aborted tx so the
	// suite continues whether the write was (correctly) rejected or (pre-0026)
	// wrongly allowed.
	rejectN := 0
	expectRejected := func(sp, desc, sql string, args ...any) {
		if _, e := tx.Exec(ctx, "SAVEPOINT "+sp); e != nil {
			t.Fatalf("savepoint %s: %v", sp, e)
		}
		_, e := tx.Exec(ctx, sql, args...)
		if e == nil {
			t.Errorf("%s: expected rejection, got success (RED — trigger absent?)", desc)
		} else {
			rejectN++
			t.Logf("%s correctly rejected: %v", desc, e)
		}
		if _, e := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp); e != nil {
			t.Fatalf("rollback to %s: %v", sp, e)
		}
	}

	// Root exempt: a NULL-parent tenant is always allowed.
	foreignRoot := uuid.NewString()
	mustOK("root (parent NULL) exempt", foreignRoot, "")

	// Master tree accepted: direct child + grandchild resolve to chora-master.
	child := uuid.NewString()
	mustOK("direct child of chora-master", child, choraMasterID)
	grandchild := uuid.NewString()
	mustOK("grandchild resolves to chora-master", grandchild, child)

	// Foreign-root parent rejected.
	expectRejected("s_foreign", "child of a foreign (non-master) root",
		`INSERT INTO tenants (tenant_id, name, status, parent_tenant_id) VALUES ($1,'adr217-inv','active',$2)`,
		uuid.NewString(), foreignRoot)

	// Cycle rejected: A→master, B→A, then A→B closes the loop.
	a := uuid.NewString()
	mustOK("A under chora-master", a, choraMasterID)
	b := uuid.NewString()
	mustOK("B under A", b, a)
	expectRejected("s_cycle", "cycle A↔B",
		`UPDATE tenants SET parent_tenant_id = $1 WHERE tenant_id = $2`, b, a)

	if rejectN != 2 {
		t.Errorf("expected 2 invariant rejections, got %d", rejectN)
	}
}

// TestIntegration_ADR217_RLS_Isolation_Preserved verifies re-parented tenants
// that now SHARE the chora-master parent stay RLS-isolated on `members`
// (re-parenting changes only parent_tenant_id, never tenant_id). Non-destructive
// (single rolled-back tx; RLS re-evaluates per query against chora.tenant_id).
func TestIntegration_ADR217_RLS_Isolation_Preserved(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := uuid.NewString()
	tenantB := uuid.NewString()
	gcid := uuid.NewString()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // never commits

	// Two tenants, both children of chora-master (tenants is RLS-free).
	for _, id := range []string{tenantA, tenantB} {
		if _, err := tx.Exec(ctx,
			`INSERT INTO tenants (tenant_id, name, status, parent_tenant_id)
			 VALUES ($1, 'adr217-rls', 'active', $2)`, id, choraMasterID); err != nil {
			t.Fatalf("insert child tenant %s: %v", id, err)
		}
	}

	setTenant := func(tid string) {
		if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = '"+tid+"'"); err != nil {
			t.Fatalf("SET LOCAL chora.tenant_id=%s: %v", tid, err)
		}
	}
	countByGCID := func() int {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM members WHERE gcid = $1`, gcid).Scan(&n); err != nil {
			t.Fatalf("count members: %v", err)
		}
		return n
	}

	// Insert a member under tenant A (RLS WITH CHECK requires GUC == row tenant).
	setTenant(tenantA)
	if _, err := tx.Exec(ctx,
		`INSERT INTO members (tenant_id, gcid, role) VALUES ($1, $2, 'admin')`, tenantA, gcid); err != nil {
		t.Fatalf("insert member under A: %v", err)
	}

	setTenant(tenantA)
	if got := countByGCID(); got != 1 {
		t.Fatalf("tenant A: expected 1 member, got %d", got)
	}
	setTenant(tenantB)
	if got := countByGCID(); got != 0 {
		t.Errorf("RLS LEAK: tenant B (shares parent chora-master with A) saw %d rows, want 0", got)
	}
	setTenant(tenantA)
	if got := countByGCID(); got != 1 {
		t.Errorf("tenant A re-read: expected 1, got %d", got)
	}
}
