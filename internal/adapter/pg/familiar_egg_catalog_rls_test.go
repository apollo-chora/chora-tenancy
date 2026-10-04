// familiar_egg_catalog_rls_test.go: tenant-scoped egg SKU reachability
// under the LIVE familiar_egg_catalog RLS policies.
//
// chora_tenancy.familiar_egg_catalog is ENABLE + FORCE ROW LEVEL SECURITY
// (migration 0007) and chora_tenancy_app_rw is NOBYPASSRLS (migration 0013),
// so the policy below is the whole truth about what a query can see:
//
//	tenant_or_platform_visibility  FOR SELECT USING
//	  (tenant_id IS NULL)
//	  OR (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid)
//
// A statement that never runs `SET LOCAL chora.tenant_id` leaves the GUC
// empty, NULLIF maps it to NULL, and the policy collapses to "platform rows
// only": every tenant-scoped SKU becomes structurally unreachable. That is
// the defect these tests pin.
//
// rlsCatalogDB is the wire-realistic double: it evaluates that exact policy
// expression against a per-transaction GUC that ONLY RunInTenantTx sets, so
// a repository that queries the bare pool fails here for the same reason it
// fails against Cloud SQL. The live-DB counterpart is
// familiar_egg_catalog_rls_integration_test.go (build tag `integration`).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

const (
	rlsTenantA = "019fdaf7-0000-7000-8000-00000000000a"
	rlsTenantB = "019fdaf7-0000-7000-8000-00000000000b"
)

// rlsCatalogRow is one familiar_egg_catalog row. tenantID "" == SQL NULL
// == platform-global.
type rlsCatalogRow struct {
	sku      string
	tenantID string
}

// rlsCatalogDB satisfies pg.QueryRunner, pg.TxQuerier and pg.Tx, filtering
// every READ through the live visibility policy. Writes are recorded, not
// policed. tenant_admin_mutation (which Postgres applies as WITH CHECK on
// INSERT, since the policy declares no explicit WITH CHECK) is proven
// against the real database in the integration test, so the write tests here
// assert the RLS SCOPE the statement runs under rather than re-modelling
// Postgres.
type rlsCatalogDB struct {
	rows []rlsCatalogRow

	// guc is chora.tenant_id. "" == unset or empty string, which the
	// NULLIF in the policy maps to NULL. Only RunInTenantTx sets it.
	guc string

	// setLocalTenants records every RunInTenantTx scope, in order.
	setLocalTenants []string
	// lastSQL records the most recent statement.
	lastSQL string
	// execSQL records every Exec statement, in order.
	execSQL []string
}

// visible is the tenant_or_platform_visibility USING expression, verbatim.
func (db *rlsCatalogDB) visible(r rlsCatalogRow) bool {
	if r.tenantID == "" {
		return true // tenant_id IS NULL
	}
	if db.guc == "" {
		return false // NULLIF(current_setting('chora.tenant_id', true), '') IS NULL
	}
	return r.tenantID == db.guc
}

// RunInTenantTx satisfies pg.TxQuerier, mirroring PgxPoolQuerier: an empty
// tenant is rejected before any statement runs (SET LOCAL is interpolated,
// so the real runner validates the identifier), and the GUC is restored on
// exit because SET LOCAL is transaction-scoped.
func (db *rlsCatalogDB) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	if tenantID == "" {
		return errors.New("rlsCatalogDB.RunInTenantTx: empty tenant_id")
	}
	db.setLocalTenants = append(db.setLocalTenants, tenantID)
	prev := db.guc
	db.guc = tenantID
	defer func() { db.guc = prev }()
	return fn(ctx, db)
}

// selectRows applies RLS plus any SQL-level tenant predicate the repository
// carries as its own second line of defence.
func (db *rlsCatalogDB) selectRows(args []any) []rlsCatalogRow {
	var sku, tenantPredicate string
	if len(args) > 0 {
		sku, _ = args[0].(string)
	}
	if len(args) > 1 {
		tenantPredicate, _ = args[1].(string)
	}
	out := make([]rlsCatalogRow, 0, len(db.rows))
	for _, r := range db.rows {
		if sku != "" && r.sku != sku {
			continue
		}
		if !db.visible(r) {
			continue
		}
		if tenantPredicate != "" && r.tenantID != "" && r.tenantID != tenantPredicate {
			continue
		}
		out = append(out, r)
	}
	return out
}

func (db *rlsCatalogDB) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	db.lastSQL = sql
	rows := db.selectRows(args)
	if len(rows) == 0 {
		return &stubRow{scanFn: func(...any) error { return pg.ErrNoRows }}
	}
	return &stubRow{scanFn: catalogScanFn(rows[0].sku, rows[0].tenantID)}
}

func (db *rlsCatalogDB) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	db.lastSQL = sql
	// The list query passes (tenantID, at), not a sku, so match on tenant
	// visibility alone.
	var tenantPredicate string
	if len(args) > 0 {
		tenantPredicate, _ = args[0].(string)
	}
	out := &stubRows{}
	for _, r := range db.rows {
		if !db.visible(r) {
			continue
		}
		if tenantPredicate != "" && r.tenantID != "" && r.tenantID != tenantPredicate {
			continue
		}
		out.rows = append(out.rows, catalogScanFn(r.sku, r.tenantID))
	}
	return out, nil
}

func (db *rlsCatalogDB) Exec(_ context.Context, sql string, _ ...any) error {
	db.lastSQL = sql
	db.execSQL = append(db.execSQL, sql)
	return nil
}

// seededCatalog is one platform SKU plus one SKU owned by tenant A.
func seededCatalog() *rlsCatalogDB {
	return &rlsCatalogDB{rows: []rlsCatalogRow{
		{sku: "egg.trial.v1", tenantID: ""},
		{sku: "egg.tenant-a.v1", tenantID: rlsTenantA},
	}}
}

// -----------------------------------------------------------------------------
// GetBySKU
// -----------------------------------------------------------------------------

// The owning tenant MUST be able to resolve its own SKU. Before the fix the
// repository queried the bare pool, the GUC stayed empty, and this SKU was
// invisible: the reveal path 500'd on a lookup that could never succeed.
func TestCatalogRepository_GetBySKU_TenantScopedSKU_ResolvableByOwningTenant(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	e, ok, err := r.GetBySKU(context.Background(), rlsTenantA, "egg.tenant-a.v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok || e == nil {
		t.Fatalf("tenant-scoped SKU not resolvable by its owning tenant: ok=%v entry=%v", ok, e)
	}
	if e.TenantID != rlsTenantA {
		t.Errorf("tenant_id: got %q, want %q", e.TenantID, rlsTenantA)
	}
	if len(db.setLocalTenants) == 0 {
		t.Fatal("no SET LOCAL chora.tenant_id ran: the lookup queried the bare pool")
	}
	if db.setLocalTenants[0] != rlsTenantA {
		t.Errorf("SET LOCAL chora.tenant_id = %q, want %q", db.setLocalTenants[0], rlsTenantA)
	}
}

// A different tenant MUST NOT resolve it. RLS gives not-found, never a row.
func TestCatalogRepository_GetBySKU_TenantScopedSKU_NotResolvableByOtherTenant(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	e, ok, err := r.GetBySKU(context.Background(), rlsTenantB, "egg.tenant-a.v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok || e != nil {
		t.Fatalf("tenant B resolved tenant A's SKU: ok=%v entry=%+v", ok, e)
	}
}

// A platform SKU (tenant_id IS NULL) stays visible to every tenant.
func TestCatalogRepository_GetBySKU_PlatformSKU_ResolvableByEveryTenant(t *testing.T) {
	t.Parallel()
	for _, tenant := range []string{rlsTenantA, rlsTenantB} {
		db := seededCatalog()
		r := pg.NewCatalogRepository(db)

		e, ok, err := r.GetBySKU(context.Background(), tenant, "egg.trial.v1")
		if err != nil {
			t.Fatalf("tenant %s: err: %v", tenant, err)
		}
		if !ok || e == nil {
			t.Fatalf("tenant %s: platform SKU not resolvable: ok=%v", tenant, ok)
		}
		if e.TenantID != "" {
			t.Errorf("tenant %s: platform SKU tenant_id: got %q, want empty", tenant, e.TenantID)
		}
	}
}

// The SQL carries its own tenant predicate as a second line of defence, so
// the statement is correct when read in isolation and stays correct if the
// policy is ever relaxed.
func TestCatalogRepository_GetBySKU_CarriesTenantPredicate(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	if _, _, err := r.GetBySKU(context.Background(), rlsTenantA, "egg.tenant-a.v1"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(db.lastSQL, "tenant_id IS NULL OR tenant_id = $2") {
		t.Errorf("expected a platform-OR-tenant predicate in the SQL; got %q", db.lastSQL)
	}
}

// An empty tenant is a caller bug, not a platform-wide read: it must fail
// loud rather than silently degrade to platform-only results. The guard is
// the shared runner's (PgxPoolQuerier.RunInTenantTx validates the value it
// interpolates into SET LOCAL), which is exactly the path the repository now
// takes. The point of the test is that the lookup goes through it at all.
func TestCatalogRepository_GetBySKU_EmptyTenantFailsLoud(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	if _, _, err := r.GetBySKU(context.Background(), "", "egg.trial.v1"); err == nil {
		t.Fatal("expected an error for an empty tenant_id, got nil")
	}
}

// -----------------------------------------------------------------------------
// ListForTenant
// -----------------------------------------------------------------------------

// The catalog list must include the tenant's own SKUs, not just platform
// rows. Same unset-GUC defect, quieter symptom: rows silently disappear.
func TestCatalogRepository_ListForTenant_IncludesTenantScopedSKUs(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	list, err := r.ListForTenant(context.Background(), rlsTenantA, time.Now().UTC())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("got %d entries, want 2 (platform + own tenant): %+v", len(list), list)
	}
	if len(db.setLocalTenants) == 0 || db.setLocalTenants[0] != rlsTenantA {
		t.Errorf("SET LOCAL chora.tenant_id = %v, want %q", db.setLocalTenants, rlsTenantA)
	}
}

// A different tenant sees platform rows only.
func TestCatalogRepository_ListForTenant_ExcludesOtherTenantsSKUs(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	list, err := r.ListForTenant(context.Background(), rlsTenantB, time.Now().UTC())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("got %d entries, want 1 (platform only): %+v", len(list), list)
	}
	if list[0].SKU != "egg.trial.v1" {
		t.Errorf("sku: got %q, want the platform SKU", list[0].SKU)
	}
}

// -----------------------------------------------------------------------------
// Upsert / SoftDelete
// -----------------------------------------------------------------------------

// tenant_admin_mutation applies as WITH CHECK on INSERT, so an admin upsert
// on the bare pool is rejected outright: the H+ marketplace could not create
// a tenant SKU at all. The write must run under the entry's own tenant.
func TestCatalogRepository_Upsert_RunsUnderEntryTenantSession(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	entry := &familiareag.CatalogEntry{
		SKU:               "egg.tenant-a.new.v1",
		TenantID:          rlsTenantA,
		DisplayName:       "New",
		PriceCents:        1500,
		Currency:          "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 50, "fox": 50},
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	}
	if err := r.Upsert(context.Background(), entry); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(db.setLocalTenants) == 0 || db.setLocalTenants[0] != rlsTenantA {
		t.Errorf("SET LOCAL chora.tenant_id = %v, want %q", db.setLocalTenants, rlsTenantA)
	}
}

// The app role is NOBYPASSRLS and the mutation policy is tenant_id = GUC, so
// a platform-global row (tenant_id IS NULL) can never be written by the app.
// Refuse it here rather than emitting a statement Postgres will reject.
func TestCatalogRepository_Upsert_EmptyTenantFailsLoud(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	entry := &familiareag.CatalogEntry{SKU: "egg.platform.v1", TenantID: ""}
	if err := r.Upsert(context.Background(), entry); err == nil {
		t.Fatal("expected an error for a platform-global upsert, got nil")
	}
	if len(db.execSQL) != 0 {
		t.Errorf("statement reached the DB despite the guard: %v", db.execSQL)
	}
}

// SoftDelete on the bare pool matched zero rows under the mutation policy
// and still reported success. It must run under the owning tenant.
func TestCatalogRepository_SoftDelete_RunsUnderTenantSession(t *testing.T) {
	t.Parallel()
	db := seededCatalog()
	r := pg.NewCatalogRepository(db)

	if err := r.SoftDelete(context.Background(), rlsTenantA, "egg.tenant-a.v1"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(db.setLocalTenants) == 0 || db.setLocalTenants[0] != rlsTenantA {
		t.Errorf("SET LOCAL chora.tenant_id = %v, want %q", db.setLocalTenants, rlsTenantA)
	}
	if !strings.Contains(db.lastSQL, "tenant_id = $2") {
		t.Errorf("expected a tenant predicate on the soft-delete; got %q", db.lastSQL)
	}
}
