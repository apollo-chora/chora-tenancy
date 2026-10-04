//go:build integration

// atom_count_repository_integration_test.go — live RLS + upsert verification for
// the ADR-217 Debt 3 (CHO-2011) tenant_atom_counts projection (migration 0027).
//
// Reuses the liveDB(t) harness (integration_test.go, same pg_test package):
//
//	export CHORA_TEST_DSN='postgres://chora_tenancy_app_rw:dev@localhost:5432/chora_tenancy?sslmode=disable'
//	go test -tags integration ./internal/adapter/pg/ -run AtomCount
//
// The app_rw DSN role is NOBYPASSRLS, so this proves the policy itself. Writes
// go through the real AtomCountWriteRepo (which owns its tx + SET LOCAL); reads
// run in rollback transactions. Random tenant UUIDs + a committing t.Cleanup
// keep the live DB clean.
package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
)

func TestAtomCountWriteRepo_ApplyDelta_RLS_And_Fold(t *testing.T) {
	pool := liveDB(t) // t.Skip()s when no DSN configured
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := pg.NewAtomCountWriteRepo(pg.NewPgxPoolQuerier(pool))

	tenantA := uuid.NewString()
	tenantB := uuid.NewString()
	t.Cleanup(func() {
		cctx := context.Background()
		for _, tid := range []string{tenantA, tenantB} {
			tx, err := pool.Begin(cctx)
			if err != nil {
				continue
			}
			_, _ = tx.Exec(cctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tid))
			_, _ = tx.Exec(cctx, "DELETE FROM tenant_atom_counts WHERE tenant_id = $1::uuid", tid)
			_ = tx.Commit(cctx)
		}
	})

	// readClamped runs the EXACT SQL the tenant-hierarchy repo uses per child.
	readClamped := func(tid string) int64 {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tid)); err != nil {
			t.Fatalf("set scope: %v", err)
		}
		var n int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE((SELECT GREATEST(atom_count, 0) FROM tenant_atom_counts), 0)`,
		).Scan(&n); err != nil {
			t.Fatalf("read clamped: %v", err)
		}
		return n
	}

	// readRaw returns the stored (unclamped) count + whether any row is visible
	// under this tenant's RLS scope.
	readRaw := func(tid string) (int64, bool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tid)); err != nil {
			t.Fatalf("set scope: %v", err)
		}
		var n int64
		if err := tx.QueryRow(ctx, `SELECT atom_count FROM tenant_atom_counts`).Scan(&n); err != nil {
			return 0, false // no row visible under this scope
		}
		return n, true
	}

	mustApply := func(tid string, d int64) {
		if err := repo.ApplyDelta(ctx, tid, d); err != nil {
			t.Fatalf("ApplyDelta(%s,%d): %v", tid, d, err)
		}
	}

	// created +1, created +1 (fold onto the existing row), archived -1 → net 1.
	mustApply(tenantA, 1)
	mustApply(tenantA, 1)
	if got := readClamped(tenantA); got != 2 {
		t.Fatalf("after two creates want 2, got %d", got)
	}
	mustApply(tenantA, -1)
	if got := readClamped(tenantA); got != 1 {
		t.Fatalf("after archive want 1, got %d", got)
	}

	// RLS isolation: tenant B cannot see tenant A's counter row.
	if _, visible := readRaw(tenantB); visible {
		t.Fatal("RLS breach: tenant B sees a counter row before any write")
	}
	if got := readClamped(tenantB); got != 0 {
		t.Fatalf("tenant B with no row must clamp to 0, got %d", got)
	}

	// Isolated archive (the atom's create predates the projection): the raw
	// fold goes negative, but the hierarchy read clamps it to 0.
	mustApply(tenantB, -1)
	if raw, ok := readRaw(tenantB); !ok || raw != -1 {
		t.Fatalf("isolated archive want raw -1 visible, got raw=%d ok=%v", raw, ok)
	}
	if got := readClamped(tenantB); got != 0 {
		t.Fatalf("negative fold must clamp to 0 on read, got %d", got)
	}
}
