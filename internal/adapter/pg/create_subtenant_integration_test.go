//go:build integration

// create_subtenant_integration_test.go — ADR-217 Phase 2.1 (CHO-2006). Live
// verification of the persisted sub-tenant create-path: BootstrapRepository.
// Persist now writes parent_tenant_id + hosting_mode, and the migration-0026
// parent-invariant trigger validates the parent resolves to the chora-master
// tree (a foreign-root parent fails the whole atomic bundle).
//
// Run: see integration_test.go (CHORA_TEST_DSN or CHORA_TEST_DSN_SECRET_ID).
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

const choraMasterForSubtenant = "00000000-0000-7000-8000-000000000001"

func TestIntegration_ADR217_Persist_SubTenant_UnderMaster(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
	owner := uuid.NewString()
	bundle, err := bootstrap.New(bootstrap.Input{
		Name:           "ADR217 Persist Subtenant " + uuid.NewString()[:8],
		OwnerGCID:      owner,
		ParentTenantID: choraMasterForSubtenant,
		HostingMode:    bootstrap.HostingModeFranchise,
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	childID := bundle.Tenant.ID

	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pool.Exec(bg, `DELETE FROM outbox_events WHERE tenant_id = $1`, childID)
		_, _ = pool.Exec(bg, `DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, childID)
		_, _ = pool.Exec(bg, `DELETE FROM members WHERE tenant_id = $1`, childID)
		// tenant_entitlements holds the core-entitlement row (FK to tenants
		// ON DELETE RESTRICT) — must be removed BEFORE the tenants delete or
		// the FK blocks it and the sub-tenant leaks (see live 019f1ed2 leak).
		_, _ = pool.Exec(bg, `DELETE FROM tenant_entitlements WHERE tenant_id = $1`, childID)
		_, _ = pool.Exec(bg, `DELETE FROM tenants WHERE tenant_id = $1`, childID)
	})

	if err := repo.Persist(ctx, bundle); err != nil {
		t.Fatalf("Persist sub-tenant under chora-master: %v", err)
	}

	// tenants is RLS-free — read the persisted child directly.
	var parent, hostingMode, statusStr string
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(parent_tenant_id::text,''), hosting_mode::text, status::text
		   FROM tenants WHERE tenant_id = $1`, childID).
		Scan(&parent, &hostingMode, &statusStr); err != nil {
		t.Fatalf("read child tenant: %v", err)
	}
	if parent != choraMasterForSubtenant {
		t.Errorf("parent_tenant_id = %q, want chora-master %q", parent, choraMasterForSubtenant)
	}
	if hostingMode != "FRANCHISE" {
		t.Errorf("hosting_mode = %q, want FRANCHISE", hostingMode)
	}
	if statusStr != "active" {
		t.Errorf("status = %q, want active", statusStr)
	}

	// Owner membership landed atomically (read under the child's RLS scope).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = '"+childID+"'"); err != nil {
		t.Fatalf("set local: %v", err)
	}
	var ownerCount int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM members WHERE tenant_id = $1 AND gcid = $2 AND role = 'owner'`,
		childID, owner).Scan(&ownerCount); err != nil {
		t.Fatalf("count owner member: %v", err)
	}
	if ownerCount != 1 {
		t.Errorf("owner membership rows = %d, want 1", ownerCount)
	}
}

func TestIntegration_ADR217_Persist_SubTenant_ForeignRoot_Rejected(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A committed foreign root (parent NULL, NOT in the chora-master tree).
	foreignRoot := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (tenant_id, name, status) VALUES ($1, 'adr217-foreign-root', 'active')`,
		foreignRoot); err != nil {
		t.Fatalf("seed foreign root: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE tenant_id = $1`, foreignRoot)
	})

	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
	bundle, err := bootstrap.New(bootstrap.Input{
		Name:           "ADR217 Foreign Parent " + uuid.NewString()[:8],
		OwnerGCID:      uuid.NewString(),
		ParentTenantID: foreignRoot,
		HostingMode:    bootstrap.HostingModeFranchise,
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	childID := bundle.Tenant.ID
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pool.Exec(bg, `DELETE FROM outbox_events WHERE tenant_id = $1`, childID)
		_, _ = pool.Exec(bg, `DELETE FROM members WHERE tenant_id = $1`, childID)
		// Defensive parity with the success-path cleanup: on the rare partial
		// persist, drop the FK-RESTRICT entitlement row before the tenant.
		_, _ = pool.Exec(bg, `DELETE FROM tenant_entitlements WHERE tenant_id = $1`, childID)
		_, _ = pool.Exec(bg, `DELETE FROM tenants WHERE tenant_id = $1`, childID)
	})

	// The parent-invariant trigger must reject this — the whole bundle rolls back.
	err = repo.Persist(ctx, bundle)
	if err == nil {
		t.Fatalf("expected Persist to be rejected (foreign-root parent), got nil")
	}
	if !strings.Contains(err.Error(), "chora-master") && !strings.Contains(err.Error(), "23514") {
		t.Logf("rejected with (any error acceptable): %v", err)
	}

	// No child tenant row should exist (atomic rollback).
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tenants WHERE tenant_id = $1`, childID).Scan(&n); err != nil {
		t.Fatalf("count child: %v", err)
	}
	if n != 0 {
		t.Errorf("child tenant persisted despite rejection: %d rows", n)
	}
}
