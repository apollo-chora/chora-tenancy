//go:build integration

// Live Cloud SQL integration tests for chora-tenancy. Run with:
//
//	export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/sa-keys/dale-cli-chora-489812.json
//	export CHORA_TEST_DSN_SECRET_ID=chora-dev-cloudsql-chora_tenancy-app_rw-dsn
//	export CHORA_TEST_DB_PROJECT=chora-489812
//	go test -tags integration ./internal/adapter/pg/...
//
// Verifies:
//
//  1. Tenant round-trip (Save → Get).
//  2. RLS isolation on members table — insert under tenant A, query
//     under tenant B → 0 rows.
//  3. D0.1 (CHO-1538) — the `list_memberships_by_gcid` SECURITY DEFINER
//     function returns the FULL cross-tenant set for a GCID (the mint
//     directory-query case) while tenant-scoped reads of `members` stay
//     RLS-isolated. Run against the app_rw role DSN — that role does NOT
//     carry BYPASSRLS, so it proves the SECURITY DEFINER bypass works for
//     the directory query AND that RLS is still live for direct reads.
package pg_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/apollo-chora/chora-common/db"
	cgcsecrets "github.com/apollo-chora/chora-common/secrets"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

func liveDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CHORA_TEST_DSN")
	secretID := os.Getenv("CHORA_TEST_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		t.Skip("set CHORA_TEST_DSN or CHORA_TEST_DSN_SECRET_ID to run integration tests")
	}
	project := os.Getenv("CHORA_TEST_DB_PROJECT")
	if project == "" {
		project = "chora-489812"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var fetcher cgcdb.SecretFetcher
	var sclient *cgcsecrets.Client
	if secretID != "" && dsn == "" {
		c, err := cgcsecrets.NewClient(ctx, project)
		if err != nil {
			t.Fatalf("secret manager: %v", err)
		}
		sclient = c
		fetcher = c
	}
	pool, err := cgcdb.Bootstrap(ctx, cgcdb.BootstrapOptions{
		DSN:             dsn,
		SecretID:        secretID,
		SecretFetcher:   fetcher,
		RewriteFromPort: 6432,
		RewriteToPort:   5432,
		AppName:         "chora-tenancy-pg-integration-test",
	})
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	})
	return pool
}

func TestIntegration_TenantRepository_RoundTrip(t *testing.T) {
	pool := liveDB(t)
	r := pg.NewTenantRepository(pg.NewPgxPoolQuerier(pool))

	ctx := context.Background()
	slug := fmt.Sprintf("phyllis-%s", uuid.NewString()[:8])
	tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "Phyllis IT - " + slug,
		OwnerGCID:   uuid.NewString(),
		Slug:        slug,
	})
	if err != nil {
		t.Fatalf("NewLightTenant: %v", err)
	}
	if err := r.Save(ctx, tt); err != nil {
		t.Fatalf("save: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, tt.ID)
	})

	got, ok, err := r.Get(ctx, tt.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatalf("expected tenant present")
	}
	if got.Slug != slug {
		t.Errorf("Slug: %q", got.Slug)
	}
	if got.Country != "SG" {
		t.Errorf("Country: %q", got.Country)
	}

	bySlug, ok, err := r.FindBySlug(ctx, slug)
	if err != nil {
		t.Fatalf("FindBySlug: %v", err)
	}
	if !ok {
		t.Fatalf("expected slug to resolve")
	}
	if bySlug.ID != tt.ID {
		t.Errorf("FindBySlug returned wrong tenant: %q vs %q", bySlug.ID, tt.ID)
	}
}

// TestIntegration_RLS_MembersIsolation verifies tenant A's members are
// invisible under tenant B's session.
func TestIntegration_RLS_MembersIsolation(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	r := pg.NewTenantRepository(pg.NewPgxPoolQuerier(pool))

	// Two distinct tenants.
	makeTenant := func() *tenant.Tenant {
		slug := fmt.Sprintf("rls-%s", uuid.NewString()[:8])
		tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
			DisplayName: "RLS Test - " + slug,
			OwnerGCID:   uuid.NewString(),
			Slug:        slug,
		})
		if err != nil {
			t.Fatalf("NewLightTenant: %v", err)
		}
		if err := r.Save(ctx, tt); err != nil {
			t.Fatalf("save: %v", err)
		}
		return tt
	}
	a := makeTenant()
	b := makeTenant()

	t.Cleanup(func() {
		for _, tt := range []*tenant.Tenant{a, b} {
			_, _ = pool.Exec(context.Background(),
				`DELETE FROM members WHERE tenant_id = $1`, tt.ID)
			_, _ = pool.Exec(context.Background(),
				`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, tt.ID)
		}
	})

	// Insert a member under tenant A.
	gcid := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", a.ID)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("set local A: %v", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO members (member_id, tenant_id, gcid, role, invited_at, joined_at, created_at, updated_at)
        VALUES (gen_random_uuid(), $1, $2, 'admin', now(), now(), now(), now())`,
		a.ID, gcid); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert under A: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// Read under tenant B — must see 0 rows.
	tx2, _ := pool.Begin(ctx)
	defer tx2.Rollback(ctx)
	if _, err := tx2.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", b.ID)); err != nil {
		t.Fatalf("set local B: %v", err)
	}
	var count int
	if err := tx2.QueryRow(ctx, `SELECT count(*) FROM members WHERE gcid = $1`, gcid).Scan(&count); err != nil {
		t.Fatalf("count under B: %v", err)
	}
	if count != 0 {
		t.Errorf("RLS LEAK: tenant B saw %d members for tenant A's gcid", count)
	}

	// Read under tenant A — must see 1 row.
	tx3, _ := pool.Begin(ctx)
	defer tx3.Rollback(ctx)
	if _, err := tx3.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", a.ID)); err != nil {
		t.Fatalf("set local A2: %v", err)
	}
	var countA int
	if err := tx3.QueryRow(ctx, `SELECT count(*) FROM members WHERE gcid = $1`, gcid).Scan(&countA); err != nil {
		t.Fatalf("count under A: %v", err)
	}
	if countA != 1 {
		t.Errorf("expected 1 row under tenant A, got %d", countA)
	}

	t.Logf("RLS isolation OK: tenant A=%d, tenant B=%d", countA, count)
}

// TestIntegration_ListMembershipsByGCID_CrossTenantViaSecurityDefiner is the
// D0.1 (CHO-1538) chaos test. It seeds 3 tenants × 2 users each, then:
//
//  1. Asserts MembershipRepository.ListByGCID (which calls the
//     `list_memberships_by_gcid` SECURITY DEFINER function) returns the
//     FULL cross-tenant membership set for a user who belongs to all 3
//     tenants — under the app_rw role, with NO `chora.tenant_id` set.
//     This is the mint flow's directory-query case: pre-fix it returned
//     zero rows once RLS was re-enabled on `members`.
//  2. Asserts a tenant-scoped direct `SELECT` on `members` (the existing
//     ListByTenant-style path) still respects RLS: with
//     `chora.tenant_id = tenant_A`, zero `tenant_B` / `tenant_C` rows are
//     visible. This proves the SECURITY DEFINER bypass did NOT punch a
//     hole in tenant isolation for ordinary reads.
func TestIntegration_ListMembershipsByGCID_CrossTenantViaSecurityDefiner(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tr := pg.NewTenantRepository(pg.NewPgxPoolQuerier(pool))

	// --- seed: 3 tenants ---
	makeTenant := func(label string) *tenant.Tenant {
		slug := fmt.Sprintf("secdef-%s-%s", label, uuid.NewString()[:8])
		tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
			DisplayName: "SecDef Test " + label + " - " + slug,
			OwnerGCID:   uuid.NewString(),
			Slug:        slug,
		})
		if err != nil {
			t.Fatalf("NewLightTenant(%s): %v", label, err)
		}
		if err := tr.Save(ctx, tt); err != nil {
			t.Fatalf("save tenant %s: %v", label, err)
		}
		return tt
	}
	tenantA := makeTenant("A")
	tenantB := makeTenant("B")
	tenantC := makeTenant("C")
	allTenants := []*tenant.Tenant{tenantA, tenantB, tenantC}

	t.Cleanup(func() {
		for _, tt := range allTenants {
			_, _ = pool.Exec(context.Background(),
				`DELETE FROM members WHERE tenant_id = $1`, tt.ID)
			_, _ = pool.Exec(context.Background(),
				`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, tt.ID)
		}
	})

	// --- seed: 2 users per tenant. userAll belongs to ALL 3 tenants; the
	// other 5 users each belong to exactly one tenant. Insert each member
	// row inside a tx with SET LOCAL chora.tenant_id so the write itself
	// satisfies the RLS WITH CHECK (RLS is live on members post-fix). ---
	userAll := uuid.NewString() // member of A, B, C
	soloUsers := map[string]string{
		tenantA.ID: uuid.NewString(),
		tenantB.ID: uuid.NewString(),
		tenantC.ID: uuid.NewString(),
	}
	insertMember := func(tenantID, gcid, role string) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin insert member: %v", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tenantID)); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("set local %s: %v", tenantID, err)
		}
		if _, err := tx.Exec(ctx, `
            INSERT INTO members (member_id, tenant_id, gcid, role, invited_at, joined_at, created_at, updated_at)
            VALUES (gen_random_uuid(), $1, $2, $3, now(), now(), now(), now())`,
			tenantID, gcid, role); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert member (tenant=%s gcid=%s role=%s): %v", tenantID, gcid, role, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit member: %v", err)
		}
	}
	// userAll: 2 roles in tenantA (multi-role row), 1 each in B + C.
	insertMember(tenantA.ID, userAll, "learner")
	insertMember(tenantA.ID, userAll, "author")
	insertMember(tenantB.ID, userAll, "instructor")
	insertMember(tenantC.ID, userAll, "auditor")
	// solo users — 1 per tenant.
	insertMember(tenantA.ID, soloUsers[tenantA.ID], "learner")
	insertMember(tenantB.ID, soloUsers[tenantB.ID], "learner")
	insertMember(tenantC.ID, soloUsers[tenantC.ID], "learner")

	// --- assertion 1: cross-tenant directory query via SECURITY DEFINER ---
	// app_rw role, NO chora.tenant_id set — the mint flow's exact posture.
	repo := pg.NewMembershipRepository(pg.NewPgxPoolQuerier(pool))
	rows, err := repo.ListByGCID(ctx, userAll, false)
	if err != nil {
		t.Fatalf("ListByGCID(userAll): %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("ListByGCID(userAll): expected 3 cross-tenant memberships, got %d (RLS bypass not working?)", len(rows))
	}
	seenTenants := map[string]bool{}
	for _, r := range rows {
		seenTenants[r.TenantID] = true
		if r.TenantID == tenantA.ID {
			// tenantA carries 2 roles for userAll — must aggregate.
			if len(r.Roles) != 2 {
				t.Errorf("tenantA membership: expected 2 aggregated roles, got %v", r.Roles)
			}
		}
		if r.TenantSlug == "" {
			t.Errorf("membership for tenant %s has empty slug (JOIN tenants broken?)", r.TenantID)
		}
	}
	for _, tt := range allTenants {
		if !seenTenants[tt.ID] {
			t.Errorf("ListByGCID(userAll) missing tenant %s", tt.ID)
		}
	}

	// A solo user resolves to exactly 1 tenant.
	soloRows, err := repo.ListByGCID(ctx, soloUsers[tenantB.ID], false)
	if err != nil {
		t.Fatalf("ListByGCID(soloUser B): %v", err)
	}
	if len(soloRows) != 1 || soloRows[0].TenantID != tenantB.ID {
		t.Errorf("ListByGCID(soloUser B): expected 1 row for tenantB, got %d rows %+v", len(soloRows), soloRows)
	}

	// --- assertion 2: tenant-scoped direct read still RLS-isolated ---
	// With chora.tenant_id = tenantA, a direct SELECT on members must see
	// ONLY tenantA rows — zero tenantB / tenantC rows for userAll.
	txScoped, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin scoped read: %v", err)
	}
	defer txScoped.Rollback(ctx)
	if _, err := txScoped.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tenantA.ID)); err != nil {
		t.Fatalf("set local tenantA scoped: %v", err)
	}
	var leakedRows int
	if err := txScoped.QueryRow(ctx,
		`SELECT count(*) FROM members WHERE gcid = $1 AND tenant_id <> $2`,
		userAll, tenantA.ID).Scan(&leakedRows); err != nil {
		t.Fatalf("scoped leak count: %v", err)
	}
	if leakedRows != 0 {
		t.Errorf("RLS LEAK: tenant-scoped read under tenantA saw %d rows for other tenants", leakedRows)
	}
	var tenantARows int
	if err := txScoped.QueryRow(ctx,
		`SELECT count(*) FROM members WHERE gcid = $1`,
		userAll).Scan(&tenantARows); err != nil {
		t.Fatalf("scoped tenantA count: %v", err)
	}
	if tenantARows != 2 {
		// userAll has 2 rows in tenantA (learner + author).
		t.Errorf("expected 2 tenantA member rows for userAll under RLS, got %d", tenantARows)
	}

	t.Logf("D0.1 OK: cross-tenant directory query returned %d tenants; tenant-scoped read leaked %d rows",
		len(rows), leakedRows)
}
