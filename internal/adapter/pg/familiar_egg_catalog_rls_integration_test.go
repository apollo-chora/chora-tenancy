//go:build integration

// familiar_egg_catalog_rls_integration_test.go: live proof that a
// tenant-scoped egg SKU is reachable by its owning tenant and by nobody else.
//
// Reuses the liveDB(t) harness in integration_test.go (same pg_test package):
//
//	export CHORA_TEST_DSN=postgres://chora_tenancy_migrate:chora@localhost:5432/chora_tenancy?sslmode=disable
//	go test -tags integration ./internal/adapter/pg/...
//
// The app_rw DSN role is NOBYPASSRLS and familiar_egg_catalog is FORCE RLS,
// so this exercises the policy itself rather than a model of it. The whole
// test runs inside ONE transaction that is ROLLED BACK: it never persists a
// catalog row.
//
// The first sub-check is the regression: with NO chora.tenant_id GUC (the
// pre-fix bare-pool state) the tenant-scoped SKU is invisible, which is why
// the reveal path returned 500 on a lookup that could never succeed.
package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFamiliarEggCatalog_RLS_TenantScopedSKUReachability(t *testing.T) {
	pool := liveDB(t) // t.Skip()s when no DSN configured
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := uuid.NewString()
	tenantB := uuid.NewString()
	tenantSKU := "egg.rls-probe." + tenantA
	const platformSKU = "egg.trial.v1" // seeded platform-global by migration 0007

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // non-destructive: never commits

	setTenant := func(tid string) {
		t.Helper()
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tid)); err != nil {
			t.Fatalf("SET LOCAL chora.tenant_id=%s: %v", tid, err)
		}
	}
	clearTenant := func() {
		t.Helper()
		if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = ''"); err != nil {
			t.Fatalf("SET LOCAL chora.tenant_id='': %v", err)
		}
	}
	visible := func(sku string) int {
		t.Helper()
		var n int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM familiar_egg_catalog WHERE sku = $1 AND deleted_at IS NULL`, sku,
		).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", sku, err)
		}
		return n
	}

	// Insert the tenant-scoped SKU under tenant A. The tenant_admin_mutation
	// policy applies as WITH CHECK on INSERT, so this REQUIRES the GUC,
	// which is itself the second half of the defect (an admin upsert on the
	// bare pool is rejected outright).
	setTenant(tenantA)
	if _, err := tx.Exec(ctx,
		`INSERT INTO familiar_egg_catalog
		   (sku, tenant_id, display_name, price_cents, currency,
		    breed_distribution, purchasable, soft_expiry_days, hard_expiry_days)
		 VALUES ($1, $2, 'RLS probe', 100, 'SGD',
		         '{"owl":50,"fox":50}'::jsonb, TRUE, 30, 60)`,
		tenantSKU, tenantA,
	); err != nil {
		t.Fatalf("insert tenant-scoped SKU under tenant A: %v", err)
	}

	// Positive control FIRST: the platform SKU is visible with no GUC at all.
	// A zero here would mean the harness is broken, not that RLS is working.
	clearTenant()
	if got := visible(platformSKU); got != 1 {
		t.Fatalf("positive control: platform SKU %s visible=%d with no GUC; want 1", platformSKU, got)
	}

	// THE DEFECT: no GUC (the bare-pool state) hides the tenant-scoped SKU.
	if got := visible(tenantSKU); got != 0 {
		t.Errorf("no-GUC visibility of the tenant SKU = %d; want 0 (policy collapses to platform-only)", got)
	}

	// The owning tenant resolves it.
	setTenant(tenantA)
	if got := visible(tenantSKU); got != 1 {
		t.Errorf("tenant A visibility of its own SKU = %d; want 1", got)
	}

	// A different tenant does not.
	setTenant(tenantB)
	if got := visible(tenantSKU); got != 0 {
		t.Errorf("tenant B visibility of tenant A's SKU = %d; want 0", got)
	}

	// The platform SKU stays visible to both.
	for _, tid := range []string{tenantA, tenantB} {
		setTenant(tid)
		if got := visible(platformSKU); got != 1 {
			t.Errorf("tenant %s visibility of platform SKU = %d; want 1", tid, got)
		}
	}
}
