//go:build integration

// Live Postgres integration tests for the legacy /api/* pgx adapters (A7).
//
// Run with:
//
//	export CHORA_TEST_DSN=postgres://chora_tenancy_migrate:chora@localhost:5432/chora_tenancy?sslmode=disable
//	go test -tags integration ./internal/adapter/pg/...
//
// Verifies (against the app_rw role — NOBYPASSRLS, migration 0013):
//
//  1. LegacyTenantStore.Get reads the Phyllis demo tenant rows from the
//     real `tenants` table — the exact A7 bug (in-mem stub returned 404
//     for a tenant that EXISTS in Postgres).
//  2. LegacyEntitlementStore round-trips through the SET LOCAL
//     chora.tenant_id transaction: Subscribe → ListActiveByTenant →
//     Cancel. add_on_subscriptions is RLS-protected, so without the
//     tenant GUC the read would see zero rows even after a committed
//     write — this proves the TxQuerier seam resolves RLS.
//  3. Cross-tenant RLS isolation: an entitlement written under tenant A
//     is invisible to a ListActiveByTenant under tenant B.
//
// liveDB + the env contract are shared with integration_test.go.
package pg_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	tenancy "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

// Phyllis demo cast (deterministic UUIDs from the seed data).
const (
	itMTMTenant  = "11111111-1111-7111-8111-111111111111"
	itChenTenant = "22222222-2222-7222-8222-222222222222"
)

// TestIntegration_LegacyTenantStore_GetSeededTenants proves the exact A7
// regression is closed: the demo tenant rows EXIST in the `tenants` table
// and the pgx adapter returns them (the in-memory stub the legacy handler
// previously used returned 404).
func TestIntegration_LegacyTenantStore_GetSeededTenants(t *testing.T) {
	pool := liveDB(t)
	store := pg.NewLegacyTenantStore(pg.NewPgxPoolQuerier(pool))

	mtm, ok := store.Get(itMTMTenant)
	if !ok {
		t.Fatalf("MTM tenant %s NOT found — A7 regression: tenant row missing or adapter broken", itMTMTenant)
	}
	if mtm.DisplayName == "" {
		t.Errorf("MTM tenant DisplayName empty")
	}
	if mtm.Status != tenancy.TenantStatusActive {
		t.Errorf("MTM tenant Status = %q, want active", mtm.Status)
	}

	chen, ok := store.Get(itChenTenant)
	if !ok {
		t.Fatalf("Chen tenant %s NOT found", itChenTenant)
	}
	if chen.DisplayName == "" {
		t.Errorf("Chen tenant DisplayName empty")
	}

	// A non-existent tenant must return ok=false (domain 404, NOT an error).
	if _, ok := store.Get("00000000-0000-7000-8000-0000000000bad"); ok {
		t.Errorf("expected ok=false for a non-existent tenant id")
	}
}

// TestIntegration_LegacyEntitlementStore_RLSRoundTrip proves the
// RLS-aware tenant-tx wiring: a write under SET LOCAL chora.tenant_id is
// readable back ONLY when the same GUC is set. Without the TxQuerier seam
// the ListActiveByTenant read would return zero rows even after a
// committed Subscribe — the original A7 failure class.
func TestIntegration_LegacyEntitlementStore_RLSRoundTrip(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	store := pg.NewLegacyEntitlementStore(pg.NewPgxPoolQuerier(pool))

	// Seed a throwaway add_on (add_ons is NOT RLS-protected) so the
	// add_on_subscriptions FK is satisfied. add_on_id is a fresh UUID so
	// the test is re-runnable.
	addOnID := uuid.NewString()
	addOnName := "A7-IT-" + addOnID[:8]
	if _, err := pool.Exec(ctx, `
        INSERT INTO add_ons (add_on_id, name, description, monthly_price_cents, currency, capabilities, created_at)
        VALUES ($1, $2, 'A7 integration test add-on', 4900, 'SGD', '{}', now())
    `, addOnID, addOnName); err != nil {
		t.Fatalf("seed add_on: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM add_on_subscriptions WHERE add_on_id = $1`, addOnID)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM add_ons WHERE add_on_id = $1`, addOnID)
	})

	// Subscribe under the MTM tenant — runs inside SET LOCAL chora.tenant_id.
	ent, err := store.Subscribe(itMTMTenant, addOnID, 4900)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if ent.Status != tenancy.EntitlementStatusActive {
		t.Errorf("Subscribe returned Status = %q, want active", ent.Status)
	}
	if ent.TenantID != itMTMTenant {
		t.Errorf("Subscribe returned TenantID = %q", ent.TenantID)
	}

	// ListActiveByTenant under MTM MUST see it — this is the read that
	// returned empty before the fix.
	mtmEnts := store.ListActiveByTenant(itMTMTenant)
	found := false
	for _, e := range mtmEnts {
		if e.AddOnID == addOnID {
			found = true
			if e.MonthlyPriceCentsSnapshot != 4900 {
				t.Errorf("entitlement MonthlyPriceCentsSnapshot = %d, want 4900", e.MonthlyPriceCentsSnapshot)
			}
		}
	}
	if !found {
		t.Fatalf("A7 regression: Subscribe committed but ListActiveByTenant(%s) did not return it — RLS tenant-tx broken", itMTMTenant)
	}

	// Cross-tenant RLS isolation — Chen's tenant must NOT see MTM's entitlement.
	chenEnts := store.ListActiveByTenant(itChenTenant)
	for _, e := range chenEnts {
		if e.AddOnID == addOnID {
			t.Errorf("RLS LEAK: Chen tenant saw MTM's entitlement for add_on %s", addOnID)
		}
	}

	// Subscribe again is idempotent (ON CONFLICT) — still exactly one row.
	if _, err := store.Subscribe(itMTMTenant, addOnID, 4900); err != nil {
		t.Fatalf("idempotent Subscribe: %v", err)
	}
	count := 0
	for _, e := range store.ListActiveByTenant(itMTMTenant) {
		if e.AddOnID == addOnID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 active entitlement after re-Subscribe, got %d", count)
	}

	// Cancel removes it from the active list.
	cancelled, err := store.Cancel(itMTMTenant, addOnID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.Status != tenancy.EntitlementStatusCancelled {
		t.Errorf("Cancel returned Status = %q, want cancelled", cancelled.Status)
	}
	if cancelled.CancelledAt == nil {
		t.Errorf("Cancel did not stamp CancelledAt")
	}
	for _, e := range store.ListActiveByTenant(itMTMTenant) {
		if e.AddOnID == addOnID {
			t.Errorf("Cancel did not remove the entitlement from the active list")
		}
	}

	// Cancel of an unknown (tenant, add_on) pair → ErrEntitlementNotFound.
	if _, err := store.Cancel(itMTMTenant, uuid.NewString()); err != tenancy.ErrEntitlementNotFound {
		t.Errorf("Cancel(unknown) = %v, want ErrEntitlementNotFound", err)
	}
}
