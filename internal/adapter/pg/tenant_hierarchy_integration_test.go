//go:build integration

// tenant_hierarchy_integration_test.go — ADR-217 Phase 2.3b (CHO-2010).
// TenantHierarchyRepo.GetHierarchy returns the DIRECT children of a tenant with
// a per-child DISTINCT-gcid live-member count. Seeds an ISOLATED subtree
// (P under chora-master → C1, C2 under P) so the assertions are exact, and
// proves: (1) is_parent + name-sorted children, (2) user_count reads under each
// child's OWN RLS scope and de-dups multi-role rows, (3) a leaf returns
// is_parent=false + empty children, (4) atom_count is 0.
package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

const choraMasterForHierarchy = "00000000-0000-7000-8000-000000000001"

func TestIntegration_ADR217_GetTenantHierarchy_ChildrenAndCounts(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	uniq := uuid.NewString()[:8]
	prefix := "adr217hier-" + uniq + "-"
	p := uuid.NewString()  // parent under chora-master
	c1 := uuid.NewString() // child of P (name sorts first)
	c2 := uuid.NewString() // child of P (name sorts second, no members)

	// Seed the subtree (tenants is RLS-free). P under chora-master satisfies the
	// migration-0026 parent-invariant trigger; C1/C2 under P resolve up to
	// chora-master too.
	seedTenant := func(id, parent, name string) {
		_, err := pool.Exec(ctx,
			`INSERT INTO tenants (tenant_id, name, status, parent_tenant_id) VALUES ($1,$2,'active',$3)`,
			id, name, parent)
		if err != nil {
			t.Fatalf("seed tenant %s: %v", name, err)
		}
	}
	seedTenant(p, choraMasterForHierarchy, prefix+"parent")
	seedTenant(c1, p, prefix+"aaa-child1")
	seedTenant(c2, p, prefix+"bbb-child2")

	// Seed members into C1 under C1's RLS scope: gcid1 with TWO roles
	// (multi-role row — must de-dup to one user) + gcid2. C2 gets none.
	gcid1, gcid2 := uuid.NewString(), uuid.NewString()
	insertMember := func(tenantID, gcid, role string) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin member: %v", err)
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
	insertMember(c1, gcid1, "learner")
	insertMember(c1, gcid1, "author") // 2nd role, same gcid → DISTINCT counts once
	insertMember(c1, gcid2, "learner")

	t.Cleanup(func() {
		bg := context.Background()
		// members first (FK ON DELETE RESTRICT; RLS → delete under child scope).
		if tx, err := pool.Begin(bg); err == nil {
			_, _ = tx.Exec(bg, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", c1))
			_, _ = tx.Exec(bg, `DELETE FROM members WHERE tenant_id = $1`, c1)
			_ = tx.Commit(bg)
		}
		// children before parent (parent_tenant_id FK).
		for _, id := range []string{c1, c2, p} {
			_, _ = pool.Exec(bg, `DELETE FROM tenants WHERE tenant_id = $1`, id)
		}
	})

	repo := pg.NewTenantHierarchyRepo(pg.NewPgxPoolQuerier(pool))

	// --- P: parent with two children, name-sorted, counts under child RLS. ---
	h, err := repo.GetHierarchy(ctx, p)
	if err != nil {
		t.Fatalf("GetHierarchy(P): %v", err)
	}
	if !h.IsParent {
		t.Error("is_parent = false, want true")
	}
	if len(h.Children) != 2 {
		t.Fatalf("children len = %d, want 2 (%+v)", len(h.Children), h.Children)
	}
	// ORDER BY name → aaa-child1 first, bbb-child2 second.
	if h.Children[0].TenantID != c1 || h.Children[1].TenantID != c2 {
		t.Errorf("children order wrong: got [%s, %s], want [%s, %s]",
			h.Children[0].TenantID, h.Children[1].TenantID, c1, c2)
	}
	if h.Children[0].TenantName != prefix+"aaa-child1" {
		t.Errorf("child[0] name = %q", h.Children[0].TenantName)
	}
	if h.Children[0].UserCount != 2 {
		t.Errorf("child C1 user_count = %d, want 2 (DISTINCT gcid; multi-role must de-dup)", h.Children[0].UserCount)
	}
	if h.Children[1].UserCount != 0 {
		t.Errorf("child C2 user_count = %d, want 0", h.Children[1].UserCount)
	}
	for _, c := range h.Children {
		if c.AtomCount != 0 {
			t.Errorf("atom_count = %d, want 0 (cross-DB deferred)", c.AtomCount)
		}
	}

	// --- C2: a leaf tenant → is_parent=false, empty children. ---
	leaf, err := repo.GetHierarchy(ctx, c2)
	if err != nil {
		t.Fatalf("GetHierarchy(C2 leaf): %v", err)
	}
	if leaf.IsParent {
		t.Error("leaf is_parent = true, want false")
	}
	if len(leaf.Children) != 0 {
		t.Errorf("leaf children len = %d, want 0", len(leaf.Children))
	}
}
