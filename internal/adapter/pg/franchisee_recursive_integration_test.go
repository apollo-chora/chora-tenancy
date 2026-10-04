//go:build integration

// franchisee_recursive_integration_test.go — ADR-217 Phase 2.3a (CHO-2007).
// ListFranchisees now returns the WHOLE descendant subtree of chora-master
// (WITH RECURSIVE), not just direct children. Verifies a multi-level tree
// (master -> A -> B -> C) is fully returned, while a foreign root (matching the
// name filter but NOT under chora-master) is excluded.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

const choraMasterForFranchisee = "00000000-0000-7000-8000-000000000001"

func TestIntegration_ADR217_ListFranchisees_RecursiveDescendants(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	uniq := uuid.NewString()[:8]
	prefix := "adr217rec-" + uniq + "-"
	a := uuid.NewString() // child of master
	b := uuid.NewString() // grandchild (child of A)
	c := uuid.NewString() // great-grandchild (child of B)
	foreign := uuid.NewString()

	t.Cleanup(func() {
		bg := context.Background()
		for _, id := range []string{c, b, a, foreign} {
			_, _ = pool.Exec(bg, `DELETE FROM tenants WHERE tenant_id = $1`, id)
		}
	})

	// Seed the tree (tenants is RLS-free). Each insert under the master tree
	// fires the migration-0026 parent-invariant trigger (all resolve → OK).
	seed := func(id, parent, name string) {
		var err error
		if parent == "" {
			_, err = pool.Exec(ctx, `INSERT INTO tenants (tenant_id, name, status) VALUES ($1,$2,'active')`, id, name)
		} else {
			_, err = pool.Exec(ctx, `INSERT INTO tenants (tenant_id, name, status, parent_tenant_id) VALUES ($1,$2,'active',$3)`, id, name, parent)
		}
		if err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	seed(a, choraMasterForFranchisee, prefix+"A")
	seed(b, a, prefix+"B")
	seed(c, b, prefix+"C")
	seed(foreign, "", prefix+"FOREIGN") // a root NOT under chora-master

	repo := pg.NewTransactionLedgerReadRepo(pg.NewPgxPoolQuerier(pool))
	rows, err := repo.ListFranchisees(ctx, prefix, 100, 0)
	if err != nil {
		t.Fatalf("ListFranchisees: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.TenantID] = true
	}
	// The whole subtree (direct + grand + great-grand) must be present.
	for _, id := range []string{a, b, c} {
		if !got[id] {
			t.Errorf("recursive descendant %s missing from ListFranchisees (direct-only regression?)", id)
		}
	}
	// The foreign root matches the name filter but is NOT in chora-master's
	// subtree — it must be excluded.
	if got[foreign] {
		t.Errorf("foreign root %s leaked into ListFranchisees (subtree bound broken)", foreign)
	}
}
