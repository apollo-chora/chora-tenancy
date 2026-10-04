//go:build integration

// Integration tests for SubscriptionHydrator.LoadActive (CHO-1752).
//
// Skip semantics mirror the other live-DB integration tests in this
// package: CHORA_TEST_DSN (or CHORA_TEST_DSN_SECRET_ID) drives liveDB(t)
// and the test skips when neither is set, so `go test ./...` stays
// fast.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

// TestIntegration_SubscriptionHydrator_LoadActive_picksUpBootstrapRow —
// drive the full saga end-to-end against live pg: bootstrap a tenant via
// the repository (writes 4 rows: tenants + members + add_on_subscriptions
// + outbox_events), then call LoadActive and assert the row surfaces
// with the canonical `base` code (post-`core` translation) + the owner
// GCID.
func TestIntegration_SubscriptionHydrator_LoadActive_picksUpBootstrapRow(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
	hydrator := pg.NewSubscriptionHydrator(pg.NewPgxPoolQuerier(pool))

	// Seed: bootstrap a fresh tenant + its `core` subscription via the
	// repository. The orchestrator layer is not under test here — we want
	// the pg row state isolated.
	in := bootstrap.Input{
		Name:      "Hydrator Probe " + uuid.NewString()[:8],
		OwnerGCID: uuid.NewString(),
	}
	b, err := bootstrap.New(in)
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	ctx := context.Background()
	if err := repo.Persist(ctx, b); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM members WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM outbox_events WHERE tenant_id = $1`, b.Tenant.ID)
	})

	rows, err := hydrator.LoadActive(ctx)
	if err != nil {
		t.Fatalf("LoadActive: %v", err)
	}

	// The dev DB may carry other historical tenants; pick out the seeded one.
	var got *pg.ActiveSubscriptionRow
	for i := range rows {
		if rows[i].TenantID == b.Tenant.ID {
			got = &rows[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("LoadActive did not return a row for the seeded tenant %s; got %d total rows", b.Tenant.ID, len(rows))
	}
	// CHO-1745 canonical code is `base`; the hydrator translates from
	// the legacy `core` written by the bootstrap saga.
	if got.AddOnCode != "base" {
		t.Errorf("AddOnCode: got %q, want %q (core→base translation)", got.AddOnCode, "base")
	}
	if !strings.EqualFold(got.OwnerGCID, in.OwnerGCID) {
		t.Errorf("OwnerGCID: got %q, want %q", got.OwnerGCID, in.OwnerGCID)
	}
}
