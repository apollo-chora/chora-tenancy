//go:build integration

// add_on_subscription_roundtrip_integration_test.go: can the DURABLE table hold
// the state the in-memory registry holds? (UX Track U, row E4 slice 1.)
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_AddOnSubscriptionRoundTrip
//
// # WHY THIS EXISTS, BEFORE ANY WRITE-THROUGH IS WRITTEN
//
// Row E4 asks every runtime entitlement mutation to write through to
// `add_on_subscriptions`, because today they mutate the per-pod in-memory
// SubscriptionRegistry only and a pod restart re-hydrates from PG, resurrecting
// an add-on the admin deactivated (CHO-1811, "breakage 4/5").
//
// A write-through is worthless if the destination cannot hold the state. This
// test is the precondition: it asserts the TABLE can round-trip what
// `add_on.Subscription` carries, so a later slice cannot quietly persist a
// subset and call the divergence closed.
//
// ⚠ THE ONE THAT BITES, AND IT BITES THE OTHER WAY. `Subscription.IsActive`
// reads:
//
//	case StatusGracePeriod:
//	    if s.GraceEndsAt == nil { return true }
//
// So a grace period persisted WITHOUT its end date reads back as active
// FOREVER. Writing through against a table with no `grace_ends_at` would not
// merely fail to fix CHO-1811; it would convert a time-bounded grace period
// into a permanent free entitlement. CHO-1811 wrongly RESTORES an entitlement
// an admin removed. This would wrongly GRANT one nobody paid for, which is
// worse, and it is why the schema is proven before the write-through exists.
//
// This test asserts the SCHEMA, not the repository. There is deliberately no
// write-through code yet; slice 2 adds it, and this is what tells slice 2 it
// has somewhere honest to write.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// allDomainStatuses is the complete set the registry can put a subscription in.
// Sourced from the domain constants rather than retyped, so a seventh status
// added later fails this test instead of silently having no column to live in.
var allDomainStatuses = []addon.Status{
	addon.StatusPendingActivation,
	addon.StatusActive,
	addon.StatusGracePeriod,
	addon.StatusSuspended,
	addon.StatusPendingDeactivation,
	addon.StatusDeactivated,
}

func seedSubscriptionTenant(t *testing.T, pool *pgxpool.Pool) (tenantID, addOnID string) {
	t.Helper()
	ctx := context.Background()
	tenantID = uuid.Must(uuid.NewV7()).String()

	if _, err := pool.Exec(ctx, `
        INSERT INTO tenants (tenant_id, name, status)
        VALUES ($1, $2, 'active')`, tenantID, "E4 RoundTrip "+tenantID[:8]); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	// Set the tenant GUC on this connection for the rest of the test.
	//
	// Migration 0040 FORCEs RLS on add_on_subscriptions, so the table owner is
	// no longer exempt. These inserts pass on the local mirror without a GUC
	// only because `chora_tenancy_migrate` happens to carry `rolsuper` there,
	// and a superuser bypasses RLS whatever FORCE says. Depending on that would
	// make this test pass for a reason that does not exist in production, so it
	// sets the context explicitly. Session-scoped (is_local=false) because the
	// assertions below span several separate statements on a pooled connection.
	if _, err := pool.Exec(ctx, `SELECT set_config('chora.tenant_id', $1, false)`, tenantID); err != nil {
		t.Fatalf("set tenant context: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `SELECT set_config('chora.tenant_id', '', false)`)
	})
	// Any catalogue row satisfies the FK; the test is about the subscription
	// columns, not the catalogue.
	if err := pool.QueryRow(ctx, `SELECT add_on_id FROM add_ons LIMIT 1`).Scan(&addOnID); err != nil {
		t.Fatalf("read an add_on to hang the FK off: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE tenant_id = $1`, tenantID)
	})
	return tenantID, addOnID
}

// TestIntegration_AddOnSubscriptionRoundTrip_EveryDomainStatusIsStorable pins
// that the `entitlement_status` enum can express every state the registry can
// reach. Before migration 0039 the enum carried only `active` and `cancelled`,
// so four of the six raised 22P02 and any write-through would have had to
// collapse them, destroying the distinction between "suspended", "in grace"
// and "gone".
func TestIntegration_AddOnSubscriptionRoundTrip_EveryDomainStatusIsStorable(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID := seedSubscriptionTenant(t, pool)

	for _, status := range allDomainStatuses {
		status := status
		t.Run(string(status), func(t *testing.T) {
			subID := uuid.Must(uuid.NewV7()).String()
			if _, err := pool.Exec(ctx, `
                INSERT INTO add_on_subscriptions
                    (subscription_id, tenant_id, add_on_id, status,
                     monthly_price_cents_snap, started_at, created_at, updated_at, past_due)
                VALUES ($1, $2, $3, $4::entitlement_status, 0, now(), now(), now(), false)`,
				subID, tenantID, addOnID, string(status)); err != nil {
				t.Fatalf("status %q is not storable: %v", status, err)
			}

			var got string
			if err := pool.QueryRow(ctx,
				`SELECT status::text FROM add_on_subscriptions WHERE subscription_id = $1`,
				subID).Scan(&got); err != nil {
				t.Fatalf("read back status %q: %v", status, err)
			}
			if got != string(status) {
				t.Fatalf("status round-tripped as %q, want %q; a write-through would silently change state", got, status)
			}
			// Unique (tenant_id, add_on_id) means one row at a time.
			if _, err := pool.Exec(ctx, `DELETE FROM add_on_subscriptions WHERE subscription_id = $1`, subID); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
		})
	}
}

// TestIntegration_AddOnSubscriptionRoundTrip_EveryDomainFieldSurvives is the
// column half. Each field below exists on `add_on.Subscription` and had NO
// column before 0039, so a write-through would have dropped it on the floor and
// a restart would have read back a subscription that never existed.
func TestIntegration_AddOnSubscriptionRoundTrip_EveryDomainFieldSurvives(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID := seedSubscriptionTenant(t, pool)

	subID := uuid.Must(uuid.NewV7()).String()
	owner := uuid.Must(uuid.NewV7()).String()
	graceEnds := time.Now().UTC().Add(72 * time.Hour).Truncate(time.Microsecond)
	scheduledAt := time.Now().UTC().Add(14 * 24 * time.Hour).Truncate(time.Microsecond)
	renewalAt := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Microsecond)
	deactivatedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	if _, err := pool.Exec(ctx, `
        INSERT INTO add_on_subscriptions
            (subscription_id, tenant_id, add_on_id, status,
             monthly_price_cents_snap, started_at, created_at, updated_at, past_due,
             owner_gcid, version, grace_ends_at, scheduled_tier,
             scheduled_effective_at, next_renewal_at, deactivated_at)
        VALUES ($1, $2, $3, 'grace_period'::entitlement_status,
                0, now(), now(), now(), false,
                $4, $5, $6, $7, $8, $9, $10)`,
		subID, tenantID, addOnID,
		owner, int64(7), graceEnds, "pro", scheduledAt, renewalAt, deactivatedAt,
	); err != nil {
		t.Fatalf("the durable table cannot hold the domain state: %v", err)
	}

	var (
		gotOwner       string
		gotVersion     int64
		gotGrace       time.Time
		gotSchedTier   string
		gotSchedAt     time.Time
		gotRenewal     time.Time
		gotDeactivated time.Time
	)
	if err := pool.QueryRow(ctx, `
        SELECT owner_gcid, version, grace_ends_at, scheduled_tier,
               scheduled_effective_at, next_renewal_at, deactivated_at
          FROM add_on_subscriptions WHERE subscription_id = $1`, subID).
		Scan(&gotOwner, &gotVersion, &gotGrace, &gotSchedTier,
			&gotSchedAt, &gotRenewal, &gotDeactivated); err != nil {
		t.Fatalf("read back: %v", err)
	}

	if gotOwner != owner {
		t.Errorf("owner_gcid = %q, want %q", gotOwner, owner)
	}
	if gotVersion != 7 {
		t.Errorf("version = %d, want 7; ChangeTier's optimistic concurrency resets without it", gotVersion)
	}
	if !gotGrace.UTC().Equal(graceEnds) {
		t.Errorf("grace_ends_at = %v, want %v. THIS IS THE ONE THAT BITES: IsActive treats a grace period with a nil end as active FOREVER, so losing this field grants a permanent entitlement nobody paid for", gotGrace.UTC(), graceEnds)
	}
	if gotSchedTier != "pro" {
		t.Errorf("scheduled_tier = %q, want %q", gotSchedTier, "pro")
	}
	if !gotSchedAt.UTC().Equal(scheduledAt) {
		t.Errorf("scheduled_effective_at = %v, want %v", gotSchedAt.UTC(), scheduledAt)
	}
	if !gotRenewal.UTC().Equal(renewalAt) {
		t.Errorf("next_renewal_at = %v, want %v", gotRenewal.UTC(), renewalAt)
	}
	if !gotDeactivated.UTC().Equal(deactivatedAt) {
		t.Errorf("deactivated_at = %v, want %v", gotDeactivated.UTC(), deactivatedAt)
	}
}

// TestIntegration_AddOnSubscriptionRoundTrip_NewColumnsAreNullable is the
// compatibility arm. 0039 is additive and every existing row predates it, so the
// seven columns must be nullable: a NOT NULL over a column the running binary
// never writes breaks production with zero code shipped.
func TestIntegration_AddOnSubscriptionRoundTrip_NewColumnsAreNullable(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, addOnID := seedSubscriptionTenant(t, pool)

	subID := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `
        INSERT INTO add_on_subscriptions
            (subscription_id, tenant_id, add_on_id, status,
             monthly_price_cents_snap, started_at, created_at, updated_at, past_due)
        VALUES ($1, $2, $3, 'active'::entitlement_status, 0, now(), now(), now(), false)`,
		subID, tenantID, addOnID); err != nil {
		t.Fatalf("a row written the pre-0039 way must still insert: %v", err)
	}

	var version *int64
	var grace *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT version, grace_ends_at FROM add_on_subscriptions WHERE subscription_id = $1`,
		subID).Scan(&version, &grace); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if version != nil {
		t.Errorf("version defaulted to %v; an unwritten column must read NULL so a caller can tell 'never set' from 'set to zero'", *version)
	}
	if grace != nil {
		t.Errorf("grace_ends_at defaulted to %v, want NULL", *grace)
	}
}
