//go:build integration

// subscription_repository_integration_test.go: the durable write side row E4
// needs, under every ordering option (UX Track U, row E4 slice 2).
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_SubscriptionRepository
//
// # WHAT THIS IS
//
// Slice 1 proved the TABLE can hold an `add_on.Subscription`. This proves a
// REPOSITORY can put one there and read it back unchanged. It is deliberately
// independent of the ordering question (whether the registry is written before
// or after PG, and what happens to memory when a persist fails): every option
// on that fork needs exactly this durable Upsert underneath it.
//
// Two invariants it pins, both of which have bitten this estate before:
//
//  1. Every write runs inside `RunInTenantTx`, never as a bare query.
//     `add_on_subscriptions` is RLS-protected and, since migration 0040,
//     FORCED, so a bare write fails the WITH CHECK and a bare read returns
//     zero rows. The seam is the only correct access path.
//
//  2. Upsert is IDEMPOTENT on (tenant_id, add_on_id), which the table already
//     enforces with a UNIQUE constraint. A write-through that inserted blindly
//     would violate it on the second mutation of the same subscription, which
//     is the common case (subscribe then deactivate).
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// subRepoFixture seeds a tenant and returns the ids the tests write against.
func subRepoFixture(t *testing.T, pool *pgxpool.Pool) (tenantID, addOnID, addOnCode string) {
	t.Helper()
	ctx := context.Background()
	tenantID = uuid.Must(uuid.NewV7()).String()

	if _, err := pool.Exec(ctx, `
        INSERT INTO tenants (tenant_id, name, status)
        VALUES ($1, $2, 'active')`, tenantID, "E4 Repo "+tenantID[:8]); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT add_on_id, code FROM add_ons LIMIT 1`).Scan(&addOnID, &addOnCode); err != nil {
		t.Fatalf("read an add_on: %v", err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_ = pgx.BeginFunc(c, pool, func(tx pgx.Tx) error {
			_, _ = tx.Exec(c, `SELECT set_config('chora.tenant_id', $1, true)`, tenantID)
			_, _ = tx.Exec(c, `DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, tenantID)
			return nil
		})
		_, _ = pool.Exec(c, `DELETE FROM tenants WHERE tenant_id = $1`, tenantID)
	})
	return tenantID, addOnID, addOnCode
}

// TestIntegration_SubscriptionRepository_UpsertRoundTripsEveryField is the core
// arm: a fully-populated Subscription survives write-then-read with no field
// quietly dropped. `grace_ends_at` is asserted explicitly because losing it
// makes a grace period read back as active forever.
func TestIntegration_SubscriptionRepository_UpsertRoundTripsEveryField(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID, addOnCode := subRepoFixture(t, pool)

	repo := pg.NewSubscriptionRepository(pg.NewPgxPoolQuerier(pool))

	graceEnds := time.Now().UTC().Add(72 * time.Hour).Truncate(time.Microsecond)
	schedAt := time.Now().UTC().Add(14 * 24 * time.Hour).Truncate(time.Microsecond)
	renewAt := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Microsecond)
	owner := uuid.Must(uuid.NewV7()).String()

	want := &addon.Subscription{
		ID:                        uuid.Must(uuid.NewV7()).String(),
		TenantID:                  tenantID,
		AddOnCode:                 addOnCode,
		AddOnID:                   addOnID,
		PlanFamily:                "growth",
		CurrentTier:               "standard",
		MonthlyPriceCentsSnapshot: 4200,
		OwnerGCID:                 owner,
		Status:                    addon.StatusGracePeriod,
		Version:                   11,
		ActivatedAt:               time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Microsecond),
		GraceEndsAt:               &graceEnds,
		ScheduledTier:             "pro",
		ScheduledEffectiveAt:      &schedAt,
		NextRenewalAt:             &renewAt,
		UpdatedAt:                 time.Now().UTC().Truncate(time.Microsecond),
		PastDue:                   true,
	}

	if err := repo.Upsert(ctx, want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := repo.Get(ctx, tenantID, addOnCode)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Status != addon.StatusGracePeriod {
		t.Errorf("Status = %q, want %q", got.Status, addon.StatusGracePeriod)
	}
	if got.GraceEndsAt == nil || !got.GraceEndsAt.UTC().Equal(graceEnds) {
		t.Errorf("GraceEndsAt = %v, want %v. Losing it makes IsActive treat this grace period as active forever", got.GraceEndsAt, graceEnds)
	}
	if got.Version != 11 {
		t.Errorf("Version = %d, want 11", got.Version)
	}
	if got.OwnerGCID != owner {
		t.Errorf("OwnerGCID = %q, want %q", got.OwnerGCID, owner)
	}
	if got.CurrentTier != "standard" || got.PlanFamily != "growth" {
		t.Errorf("tier/family = %q/%q, want standard/growth", got.CurrentTier, got.PlanFamily)
	}
	if got.ScheduledTier != "pro" || got.ScheduledEffectiveAt == nil || !got.ScheduledEffectiveAt.UTC().Equal(schedAt) {
		t.Errorf("scheduled = %q @ %v, want pro @ %v", got.ScheduledTier, got.ScheduledEffectiveAt, schedAt)
	}
	if got.NextRenewalAt == nil || !got.NextRenewalAt.UTC().Equal(renewAt) {
		t.Errorf("NextRenewalAt = %v, want %v", got.NextRenewalAt, renewAt)
	}
	if !got.PastDue {
		t.Error("PastDue = false, want true")
	}
	if got.MonthlyPriceCentsSnapshot != 4200 {
		t.Errorf("price snapshot = %d, want 4200", got.MonthlyPriceCentsSnapshot)
	}
}

// TestIntegration_SubscriptionRepository_UpsertIsIdempotent pins the second
// write. (tenant_id, add_on_id) is UNIQUE, so a repository that INSERTed
// blindly would fail on the second mutation of the same subscription, which is
// the ordinary case: subscribe, then deactivate.
func TestIntegration_SubscriptionRepository_UpsertIsIdempotent(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID, addOnCode := subRepoFixture(t, pool)
	repo := pg.NewSubscriptionRepository(pg.NewPgxPoolQuerier(pool))

	sub := &addon.Subscription{
		ID:          uuid.Must(uuid.NewV7()).String(),
		TenantID:    tenantID,
		AddOnCode:   addOnCode,
		AddOnID:     addOnID,
		CurrentTier: "standard",
		Status:      addon.StatusActive,
		Version:     1,
		ActivatedAt: time.Now().UTC().Truncate(time.Microsecond),
		UpdatedAt:   time.Now().UTC().Truncate(time.Microsecond),
	}
	if err := repo.Upsert(ctx, sub); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}

	// The same subscription, deactivated. This is what a write-through does on
	// the second mutation.
	deactivated := time.Now().UTC().Truncate(time.Microsecond)
	sub.Status = addon.StatusDeactivated
	sub.Version = 2
	sub.DeactivatedAt = &deactivated
	if err := repo.Upsert(ctx, sub); err != nil {
		t.Fatalf("second Upsert (the deactivate) failed; a blind INSERT would violate the (tenant_id, add_on_id) unique constraint here: %v", err)
	}

	got, err := repo.Get(ctx, tenantID, addOnCode)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != addon.StatusDeactivated {
		t.Errorf("Status = %q after the deactivate upsert, want %q", got.Status, addon.StatusDeactivated)
	}
	if got.Version != 2 {
		t.Errorf("Version = %d, want 2", got.Version)
	}

	var n int
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('chora.tenant_id', $1, true)`, tenantID); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`SELECT count(*) FROM add_on_subscriptions WHERE tenant_id = $1 AND add_on_id = $2`,
			tenantID, addOnID).Scan(&n)
	}); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("row count = %d after two upserts, want 1; the second write duplicated instead of updating", n)
	}
}

// TestIntegration_SubscriptionRepository_GetIsTenantScoped is the isolation
// arm. The repository must not become a way around the policy 0040 just
// hardened, so another tenant's id must not reach this row.
func TestIntegration_SubscriptionRepository_GetIsTenantScoped(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID, addOnCode := subRepoFixture(t, pool)
	repo := pg.NewSubscriptionRepository(pg.NewPgxPoolQuerier(pool))

	if err := repo.Upsert(ctx, &addon.Subscription{
		ID:          uuid.Must(uuid.NewV7()).String(),
		TenantID:    tenantID,
		AddOnCode:   addOnCode,
		AddOnID:     addOnID,
		Status:      addon.StatusActive,
		ActivatedAt: time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	other := uuid.Must(uuid.NewV7()).String()
	got, err := repo.Get(ctx, other, addOnCode)
	if err == nil && got != nil {
		t.Fatalf("tenant %s read tenant %s's subscription through the repository", other, tenantID)
	}
}
