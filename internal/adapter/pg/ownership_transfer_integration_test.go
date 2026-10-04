//go:build integration

// ownership_transfer_integration_test.go: the accept transaction (UX Track U,
// E3 slice 2, S7-B4).
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_OwnershipTransfer
//
// This is the half that cannot be tested against a stub. The transaction has to
// satisfy a constraint the schema enforces and Go cannot see: 0036's
// uq_members_one_live_owner_per_tenant is a PARTIAL UNIQUE INDEX, and a partial
// unique index cannot be deferred, so the outgoing owner row must be
// soft-deleted BEFORE the incoming one is inserted. Against a fake repository
// either order passes.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/ownership"
)

type transferFixture struct {
	tenantID string
	owner    string
	nominee  string
	repo     *pg.OwnershipRepository
	pool     *pgxpool.Pool
}

// seedTenant creates a tenant with an owner and a second live member, which is
// the state every handover starts from.
func seedTransfer(t *testing.T, pool *pgxpool.Pool) transferFixture {
	t.Helper()
	ctx := context.Background()
	tenantID := uuid.Must(uuid.NewV7()).String()
	owner := uuid.Must(uuid.NewV7()).String()
	nominee := uuid.Must(uuid.NewV7()).String()

	if _, err := pool.Exec(ctx, `
        INSERT INTO tenants (tenant_id, name, status)
        VALUES ($1, $2, 'active')`, tenantID, "E3 Transfer "+tenantID[:8]); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	for _, m := range []struct{ gcid, role string }{
		{owner, "owner"},
		{nominee, "admin"},
	} {
		if _, err := pool.Exec(ctx, `
            INSERT INTO members (tenant_id, gcid, role, joined_at)
            VALUES ($1, $2, $3::tenant_member_role, now())`,
			tenantID, m.gcid, m.role); err != nil {
			t.Fatalf("seed member %s: %v", m.role, err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM ownership_offers WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM members WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE tenant_id = $1`, tenantID)
	})
	return transferFixture{
		tenantID: tenantID,
		owner:    owner,
		nominee:  nominee,
		repo:     pg.NewOwnershipRepository(pg.NewPgxPoolQuerier(pool)),
		pool:     pool,
	}
}

func (f transferFixture) newOffer(t *testing.T) *ownership.Offer {
	t.Helper()
	o, err := ownership.NewOffer(ownership.OfferInput{
		TenantID:    f.tenantID,
		FromGCID:    f.owner,
		ToGCID:      f.nominee,
		InitiatedBy: f.owner,
		Initiator:   ownership.InitiatorOwner,
		Now:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewOffer: %v", err)
	}
	if err := f.repo.CreateOffer(context.Background(), o); err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	return o
}

// liveRoles reads the roles a GCID actually holds, which is the only claim that
// matters after the swap.
func (f transferFixture) liveRoles(t *testing.T, gcid string) []string {
	t.Helper()
	rows, err := f.pool.Query(context.Background(), `
        SELECT role::text FROM members
        WHERE tenant_id = $1 AND gcid = $2 AND deleted_at IS NULL
        ORDER BY role::text`, f.tenantID, gcid)
	if err != nil {
		t.Fatalf("liveRoles: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

func TestIntegration_OwnershipTransfer_SwapsTheOwnerRowWithoutEmptyingTheTenant(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}

	// The tenant has exactly one live owner and it is the nominee. If the
	// transaction had inserted before deleting, 0036's partial unique index
	// would have refused it mid-transaction and this would be an error above.
	var owners int
	if err := f.pool.QueryRow(context.Background(), `
        SELECT count(*) FROM members
        WHERE tenant_id = $1 AND role = 'owner' AND deleted_at IS NULL`,
		f.tenantID).Scan(&owners); err != nil {
		t.Fatalf("count owners: %v", err)
	}
	if owners != 1 {
		t.Fatalf("live owner rows = %d, want exactly 1", owners)
	}
	if !contains(f.liveRoles(t, f.nominee), "owner") {
		t.Fatalf("the nominee does not own the tenant: roles = %v", f.liveRoles(t, f.nominee))
	}

	// The leaving owner keeps a membership and loses only ownership. Stripping
	// every row is closer to a delete, and it is defect D1: RemoveMembership
	// soft-deletes every live role with no role filter.
	leaving := f.liveRoles(t, f.owner)
	if contains(leaving, "owner") {
		t.Fatalf("the outgoing owner still owns the tenant: %v", leaving)
	}
	if !contains(leaving, "admin") {
		t.Fatalf("the outgoing owner was left with %v; they must keep a membership, not be revoked", leaving)
	}
}

// The audit row and the membership writes are one unit. A rolled-back transfer
// must not leave a claim that it happened, and a completed one must not go
// unrecorded.
func TestIntegration_OwnershipTransfer_WritesTheAuditRowAtomically(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}

	var events int
	if err := f.pool.QueryRow(context.Background(), `
        SELECT count(*) FROM outbox_events
        WHERE tenant_id = $1 AND event_type = 'chora.governance.audit.recorded.v1'`,
		f.tenantID).Scan(&events); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if events != 1 {
		t.Fatalf("audit outbox rows = %d, want 1", events)
	}
}

func TestIntegration_OwnershipTransfer_RefusesANomineeWhoIsNotALiveMember(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	// Soft-delete the nominee's only membership after the offer is made: the
	// roster can change between nomination and acceptance, and the check that
	// matters is the one at accept time.
	o := f.newOffer(t)
	if _, err := f.pool.Exec(context.Background(), `
        UPDATE members SET deleted_at = now()
        WHERE tenant_id = $1 AND gcid = $2`, f.tenantID, f.nominee); err != nil {
		t.Fatalf("soft-delete nominee: %v", err)
	}

	err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC())
	if err != pg.ErrNomineeNotAMember {
		t.Fatalf("err = %v, want ErrNomineeNotAMember. Handing ownership to a non-member leaves the organisation owned by somebody who cannot act in it", err)
	}
	if contains(f.liveRoles(t, f.nominee), "owner") {
		t.Fatal("a refused transfer still wrote the owner row")
	}
	if !contains(f.liveRoles(t, f.owner), "owner") {
		t.Fatal("a refused transfer removed the outgoing owner anyway, leaving the tenant unowned")
	}
}

func TestIntegration_OwnershipTransfer_RefusesASuspendedNominee(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	if _, err := f.pool.Exec(context.Background(), `
        UPDATE members SET suspended_at = now()
        WHERE tenant_id = $1 AND gcid = $2`, f.tenantID, f.nominee); err != nil {
		t.Fatalf("suspend nominee: %v", err)
	}

	// ReplaceRoles and RemoveMembership already refuse on a suspended member;
	// the handover refuses the same way rather than inventing a new answer.
	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != pg.ErrNomineeSuspended {
		t.Fatalf("err = %v, want ErrNomineeSuspended", err)
	}
}

func TestIntegration_OwnershipTransfer_RefusesASecondAcceptance(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	at := time.Now().UTC()
	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, at); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, at); err != pg.ErrOfferNotPending {
		t.Fatalf("err = %v, want ErrOfferNotPending", err)
	}
}

func TestIntegration_OwnershipTransfer_RefusesAnybodyButTheNominee(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.owner, time.Now().UTC()); err != pg.ErrNotTheNominee {
		t.Fatalf("err = %v, want ErrNotTheNominee", err)
	}
}

// Two pending offers for one tenant would each try to write an owner row, and
// 0036 would fail the second at commit with an index violation the caller
// cannot interpret. Refusing the second OFFER gives them a reason instead.
func TestIntegration_OwnershipTransfer_RefusesASecondPendingOffer(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	f.newOffer(t)

	second, err := ownership.NewOffer(ownership.OfferInput{
		TenantID:    f.tenantID,
		FromGCID:    f.owner,
		ToGCID:      uuid.Must(uuid.NewV7()).String(),
		InitiatedBy: f.owner,
		Initiator:   ownership.InitiatorOwner,
		Now:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewOffer: %v", err)
	}
	if err := f.repo.CreateOffer(context.Background(), second); err != pg.ErrOfferAlreadyPending {
		t.Fatalf("err = %v, want ErrOfferAlreadyPending", err)
	}
}
