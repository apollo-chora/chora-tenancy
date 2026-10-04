//go:build integration

// membership_suspended_integration_test.go: the suspended-owner gap that the
// closure pre-flight depends on (UX Track U, E3 slice 8).
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_MembershipSuspended
//
// # WHY THIS EXISTS
//
// `list_memberships_by_gcid` filters `m.suspended_at IS NULL` (migration 0011).
// The one-live-owner index (0036) and the membership write guards deliberately
// do NOT consider suspension, so the two disagree about what "holds the tenant"
// means: a suspended owner still holds the only live owner row, yet the
// directory read cannot see it. The closure ownership pre-flight is built on
// that read, so a suspended owner could close their account and orphan the
// organisation the pre-flight exists to protect.
//
// NOT A LIVE BUG TODAY, and this says so on purpose: nothing in the repo writes
// `members.suspended_at` outside a test (the v2 suspend handler writes an
// in-memory store, not the table), so the gap is unreachable until a suspend
// path is built. This pins the behaviour BEFORE that path exists, which is the
// only cheap moment to do it.
//
// The default stays false. Mint is the other caller of this read and must keep
// excluding suspended members: a suspended member should not authenticate back
// into the tenant that suspended them. Only the closure pre-flight opts in.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	tenancygrpc "github.com/apollo-chora/chora-tenancy/internal/adapter/grpc"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

// seedOwner creates a tenant with a single owner row, suspended or not, and
// registers cleanup. Returns (tenantID, ownerGCID).
func seedOwner(t *testing.T, pool *pgxpool.Pool, suspended bool) (string, string) {
	t.Helper()
	ctx := context.Background()
	tenantID := uuid.Must(uuid.NewV7()).String()
	owner := uuid.Must(uuid.NewV7()).String()

	if _, err := pool.Exec(ctx, `
        INSERT INTO tenants (tenant_id, name, status)
        VALUES ($1, $2, 'active')`, tenantID, "E3 Slice8 "+tenantID[:8]); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	// The row is live in every sense the write guards care about: not
	// soft-deleted, role owner, and the only owner row on the tenant. The one
	// variable is suspension, passed as NULL or now() rather than branching
	// into two INSERTs, so both arms exercise the same statement.
	var suspendedAt *time.Time
	if suspended {
		now := time.Now().UTC()
		suspendedAt = &now
	}
	if _, err := pool.Exec(ctx, `
        INSERT INTO members (tenant_id, gcid, role, joined_at, suspended_at)
        VALUES ($1, $2, 'owner'::tenant_member_role, now(), $3)`,
		tenantID, owner, suspendedAt); err != nil {
		t.Fatalf("seed owner (suspended=%v): %v", suspended, err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM members WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE tenant_id = $1`, tenantID)
	})
	return tenantID, owner
}

func TestIntegration_MembershipSuspended_HiddenByDefaultVisibleWhenAsked(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, owner := seedOwner(t, pool, true)
	repo := pg.NewMembershipRepository(pg.NewPgxPoolQuerier(pool))

	t.Run("excluded by default, so mint still refuses a suspended member", func(t *testing.T) {
		rows, err := repo.ListByGCID(ctx, owner, false)
		if err != nil {
			t.Fatalf("ListByGCID(includeSuspended=false): %v", err)
		}
		if tenantIn(rows, tenantID) {
			t.Fatalf("suspended owner's tenant %s is visible at includeSuspended=false; mint would let a suspended member back into the tenant that suspended them", tenantID)
		}
	})

	t.Run("visible when asked, so the closure pre-flight can see the owner", func(t *testing.T) {
		rows, err := repo.ListByGCID(ctx, owner, true)
		if err != nil {
			t.Fatalf("ListByGCID(includeSuspended=true): %v", err)
		}
		if !tenantIn(rows, tenantID) {
			t.Fatalf("suspended owner's tenant %s is INVISIBLE at includeSuspended=true; the closure pre-flight would clear this GCID and orphan the organisation it still owns", tenantID)
		}
		for _, row := range rows {
			if row.TenantID == tenantID && !contains(row.Roles, "owner") {
				t.Fatalf("roles for %s = %v, want the owner role carried through; the pre-flight keys on it", tenantID, row.Roles)
			}
		}
	})
}

// TestIntegration_MembershipSuspended_UnsuspendedIsUnaffected is the positive
// control. Without it a function that returned nothing at all would satisfy the
// default-false arm above and read as correct.
func TestIntegration_MembershipSuspended_UnsuspendedIsUnaffected(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()
	tenantID, owner := seedOwner(t, pool, false)
	repo := pg.NewMembershipRepository(pg.NewPgxPoolQuerier(pool))

	for _, includeSuspended := range []bool{false, true} {
		rows, err := repo.ListByGCID(ctx, owner, includeSuspended)
		if err != nil {
			t.Fatalf("ListByGCID(includeSuspended=%v): %v", includeSuspended, err)
		}
		if !tenantIn(rows, tenantID) {
			t.Fatalf("an UNSUSPENDED owner vanished at includeSuspended=%v; the flag must widen the read, never narrow it", includeSuspended)
		}
	}
}

func tenantIn(rows []tenancygrpc.MembershipRow, tenantID string) bool {
	for _, r := range rows {
		if r.TenantID == tenantID {
			return true
		}
	}
	return false
}
