//go:build integration

// ownership_lifecycle_integration_test.go: decline, revoke and the reads that
// S7a and S7b are built on, plus the operator override's rule about the leaving
// owner (UX Track U, E3 slice 4, S7-B4).
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/ -run Integration_OwnershipLifecycle
//
// These need the live schema, not a stub: the terminal states are written by
// UPDATE against an enum column, the one-pending-offer rule is a partial unique
// index, and the read has to prove that settling an offer frees the tenant to
// start another one.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/ownership"
)

// --- decline ----------------------------------------------------------------

func TestIntegration_OwnershipLifecycle_DeclineSettlesAndFreesTheTenant(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	ctx := context.Background()

	if err := f.repo.DeclineOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("DeclineOffer: %v", err)
	}

	got, err := f.repo.PendingOffer(ctx, f.tenantID)
	if !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("PendingOffer after decline = (%v, %v), want ErrOfferNotFound", got, err)
	}

	// Ownership did not move: declining is a refusal, not a transfer.
	if roles := f.liveRoles(t, f.owner); !contains(roles, "owner") {
		t.Errorf("owner roles = %v, want owner still held", roles)
	}
	if roles := f.liveRoles(t, f.nominee); contains(roles, "owner") {
		t.Errorf("nominee roles = %v, want no owner row", roles)
	}

	// The tenant can be offered again. The partial unique index only counts
	// PENDING rows, so a settled offer must not block the next one.
	if _, err := f.repo.PendingOffer(ctx, f.tenantID); !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("PendingOffer = %v, want ErrOfferNotFound", err)
	}
	f.newOffer(t)
}

func TestIntegration_OwnershipLifecycle_DeclineRefusesAnybodyButTheNominee(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	err := f.repo.DeclineOffer(context.Background(), f.tenantID, o.OfferID, f.owner, time.Now().UTC())
	if !errors.Is(err, pg.ErrNotTheNominee) {
		t.Fatalf("DeclineOffer by the owner = %v, want ErrNotTheNominee", err)
	}
}

// An expired offer can still be declined. Saying no to something that lapsed is
// harmless, and the record is more honest than a silent timeout.
func TestIntegration_OwnershipLifecycle_DeclineAcceptsAnExpiredOffer(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	f.expire(t, o.OfferID)

	if err := f.repo.DeclineOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("DeclineOffer on an expired offer: %v", err)
	}
	if got := f.offerStatus(t, o.OfferID); got != string(ownership.StatusDeclined) {
		t.Errorf("status = %q, want declined", got)
	}
}

func TestIntegration_OwnershipLifecycle_DeclineRefusesASettledOffer(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	ctx := context.Background()

	if err := f.repo.DeclineOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("first DeclineOffer: %v", err)
	}
	err := f.repo.DeclineOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrOfferNotPending) {
		t.Fatalf("second DeclineOffer = %v, want ErrOfferNotPending", err)
	}
}

// A declined offer must not be acceptable afterwards, or a nominee's refusal
// could be overturned by replaying the accept.
func TestIntegration_OwnershipLifecycle_AcceptRefusesADeclinedOffer(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	ctx := context.Background()

	if err := f.repo.DeclineOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("DeclineOffer: %v", err)
	}
	err := f.repo.AcceptOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrOfferNotPending) {
		t.Fatalf("AcceptOffer after decline = %v, want ErrOfferNotPending", err)
	}
	if roles := f.liveRoles(t, f.nominee); contains(roles, "owner") {
		t.Errorf("nominee roles = %v, want no owner row", roles)
	}
}

// --- revoke -----------------------------------------------------------------

func TestIntegration_OwnershipLifecycle_RevokeSettlesAndFreesTheTenant(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	ctx := context.Background()

	if err := f.repo.RevokeOffer(ctx, f.tenantID, o.OfferID, f.owner, time.Now().UTC()); err != nil {
		t.Fatalf("RevokeOffer: %v", err)
	}
	if got := f.offerStatus(t, o.OfferID); got != string(ownership.StatusRevoked) {
		t.Errorf("status = %q, want revoked", got)
	}
	if _, err := f.repo.PendingOffer(ctx, f.tenantID); !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("PendingOffer after revoke = %v, want ErrOfferNotFound", err)
	}
	f.newOffer(t)
}

// Only the initiator withdraws. The nominee's way out is to decline, which is
// recorded as a refusal rather than as the initiator changing their mind.
func TestIntegration_OwnershipLifecycle_RevokeRefusesAnybodyButTheInitiator(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	err := f.repo.RevokeOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrNotTheInitiator) {
		t.Fatalf("RevokeOffer by the nominee = %v, want ErrNotTheInitiator", err)
	}
}

// --- the read S7a and S7b share ---------------------------------------------

func TestIntegration_OwnershipLifecycle_PendingOfferCarriesBothParties(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	got, err := f.repo.PendingOffer(context.Background(), f.tenantID)
	if err != nil {
		t.Fatalf("PendingOffer: %v", err)
	}
	if got.OfferID != o.OfferID || got.FromGCID != f.owner || got.ToGCID != f.nominee {
		t.Fatalf("offer = %+v, want %s from %s to %s", got, o.OfferID, f.owner, f.nominee)
	}
	if got.Initiator != ownership.InitiatorOwner {
		t.Errorf("initiator = %q, want owner", got.Initiator)
	}
	if got.Status != ownership.StatusPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
	if got.ExpiresAt.IsZero() {
		t.Error("expires_at is zero, so the screen cannot say when the offer lapses")
	}
}

// A pending row past its TTL is still RETURNED, and reported as not live. The
// screen has to be able to say "this expired" rather than "there is nothing
// here": no reaper has run, so absence would be a lie about the row.
func TestIntegration_OwnershipLifecycle_PendingOfferReturnsALapsedRowAsNotLive(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	f.expire(t, o.OfferID)

	got, err := f.repo.PendingOffer(context.Background(), f.tenantID)
	if err != nil {
		t.Fatalf("PendingOffer: %v", err)
	}
	if got.IsLive(time.Now().UTC()) {
		t.Error("a row past its TTL reported live")
	}
	if got.Status != ownership.StatusPending {
		t.Errorf("status = %q, want the stored pending value untouched", got.Status)
	}
}

func TestIntegration_OwnershipLifecycle_PendingOfferIsTenantScoped(t *testing.T) {
	pool := liveDB(t)
	a := seedTransfer(t, pool)
	b := seedTransfer(t, pool)
	a.newOffer(t)

	if _, err := b.repo.PendingOffer(context.Background(), b.tenantID); !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("tenant B saw tenant A's offer: %v", err)
	}
}

// --- the operator override (spec 13.4) --------------------------------------

// The override creates ownership where the owner row is already gone. That path
// already worked; what this pins is the row the tenant ends up with.
func TestIntegration_OwnershipLifecycle_OperatorOverrideCreatesOwnershipWhenTheOwnerIsGone(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	ctx := context.Background()
	f.stripOwnerRow(t)

	o := f.newOperatorOffer(t, "the owner left the company")
	if err := f.repo.AcceptOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("AcceptOffer on the override: %v", err)
	}
	if roles := f.liveRoles(t, f.nominee); !contains(roles, "owner") {
		t.Fatalf("nominee roles = %v, want owner", roles)
	}
}

// Spec 13.4: the leaving owner's roles are UNTOUCHED on the override. The
// owner-initiated path demotes them to admin because they are present and
// keeping their seat; the override exists for an owner who is gone, closing or
// closed, and writing them a fresh admin row would resurrect a membership the
// closure saga is winding down.
func TestIntegration_OwnershipLifecycle_OperatorOverrideDoesNotResurrectTheLeavingOwner(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	ctx := context.Background()
	f.stripOwnerRow(t)

	o := f.newOperatorOffer(t, "the owner closed their account")
	if err := f.repo.AcceptOffer(ctx, f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("AcceptOffer on the override: %v", err)
	}

	if roles := f.liveRoles(t, f.owner); len(roles) != 0 {
		t.Fatalf("the departed owner gained rows %v, want none written on the override path", roles)
	}
}

// The owner-initiated path is unchanged: the outgoing owner keeps a seat.
func TestIntegration_OwnershipLifecycle_OwnerInitiatedStillDemotesToAdmin(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)

	if err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC()); err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	roles := f.liveRoles(t, f.owner)
	if !contains(roles, "admin") {
		t.Fatalf("outgoing owner roles = %v, want admin", roles)
	}
	if contains(roles, "owner") {
		t.Fatalf("outgoing owner roles = %v, want the owner row released", roles)
	}
}

// --- the not-found translation (defect found in slice 2) --------------------

// An unknown offer id must be a NAMED refusal the handler can turn into a 404,
// not a wrapped scan error that reads as a 500. The pg package has its own
// ErrNoRows sentinel (runtime.go) and the Tx layer converts pgx's before the
// repository sees it, so a pgx.ErrNoRows comparison here is always false.
func TestIntegration_OwnershipLifecycle_AcceptUnknownOfferIsNotFound(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	unknown := uuid.Must(uuid.NewV7()).String()

	err := f.repo.AcceptOffer(context.Background(), f.tenantID, unknown, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("AcceptOffer on an unknown offer = %v, want ErrOfferNotFound", err)
	}
}

func TestIntegration_OwnershipLifecycle_DeclineUnknownOfferIsNotFound(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	unknown := uuid.Must(uuid.NewV7()).String()

	err := f.repo.DeclineOffer(context.Background(), f.tenantID, unknown, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrOfferNotFound) {
		t.Fatalf("DeclineOffer on an unknown offer = %v, want ErrOfferNotFound", err)
	}
}

// The owner-initiated path must refuse to create ownership out of nothing when
// the outgoing owner row has already been stripped. Only the operator override
// may do that, and only with a stated reason.
func TestIntegration_OwnershipLifecycle_OwnerInitiatedRefusesWhenTheOwnerRowIsGone(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	o := f.newOffer(t)
	f.stripOwnerRow(t)

	err := f.repo.AcceptOffer(context.Background(), f.tenantID, o.OfferID, f.nominee, time.Now().UTC())
	if !errors.Is(err, pg.ErrOutgoingOwnerGone) {
		t.Fatalf("AcceptOffer with no owner row = %v, want ErrOutgoingOwnerGone", err)
	}
	if roles := f.liveRoles(t, f.nominee); contains(roles, "owner") {
		t.Fatalf("nominee roles = %v, want the transaction rolled back", roles)
	}
}

// --- fixture helpers used only here -----------------------------------------

// expire back-dates the offer, which is the only way to reach the lapsed state
// without waiting 14 days. created_at moves with it: 0037 carries
// CHECK (expires_at > created_at), so moving only expires_at is refused.
func (f transferFixture) expire(t *testing.T, offerID string) {
	t.Helper()
	if _, err := f.pool.Exec(context.Background(), `
        UPDATE ownership_offers
        SET    created_at = now() - interval '15 days',
               expires_at = now() - interval '1 hour'
        WHERE  offer_id = $1`, offerID); err != nil {
		t.Fatalf("expire offer: %v", err)
	}
}

func (f transferFixture) offerStatus(t *testing.T, offerID string) string {
	t.Helper()
	var s string
	if err := f.pool.QueryRow(context.Background(), `
        SELECT status::text FROM ownership_offers WHERE offer_id = $1`, offerID).Scan(&s); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return s
}

// stripOwnerRow reproduces the D1/D2 damage the override exists to repair: the
// tenant has members but no live owner row.
func (f transferFixture) stripOwnerRow(t *testing.T) {
	t.Helper()
	if _, err := f.pool.Exec(context.Background(), `
        UPDATE members SET deleted_at = now()
        WHERE tenant_id = $1 AND gcid = $2 AND role = 'owner'`, f.tenantID, f.owner); err != nil {
		t.Fatalf("strip owner row: %v", err)
	}
}

func (f transferFixture) newOperatorOffer(t *testing.T, reason string) *ownership.Offer {
	t.Helper()
	operator := uuid.Must(uuid.NewV7()).String()
	o, err := ownership.NewOffer(ownership.OfferInput{
		TenantID:    f.tenantID,
		FromGCID:    f.owner,
		ToGCID:      f.nominee,
		InitiatedBy: operator,
		Initiator:   ownership.InitiatorOperator,
		Reason:      reason,
		Now:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewOffer (operator): %v", err)
	}
	if err := f.repo.CreateOffer(context.Background(), o); err != nil {
		t.Fatalf("CreateOffer (operator): %v", err)
	}
	return o
}

// --- who is being replaced (slice 5, the handler's input) -------------------

// The FE never sends from_gcid: a GCID alone never proves ownership, so the
// outgoing owner is resolved server-side.
func TestIntegration_OwnershipLifecycle_OutgoingOwnerIsTheLiveOwner(t *testing.T) {
	f := seedTransfer(t, liveDB(t))

	gcid, live, err := f.repo.OutgoingOwnerGCID(context.Background(), f.tenantID)
	if err != nil {
		t.Fatalf("OutgoingOwnerGCID: %v", err)
	}
	if gcid != f.owner {
		t.Errorf("gcid = %q, want the live owner %q", gcid, f.owner)
	}
	if !live {
		t.Error("live = false for a tenant whose owner row is present")
	}
}

// When the owner row has been stripped, the override still needs to name who
// is being replaced, so the most recent soft-deleted owner is returned and
// flagged as not live. Inventing a placeholder GCID would put a fiction on the
// audit row.
func TestIntegration_OwnershipLifecycle_OutgoingOwnerFallsBackToTheStrippedOwner(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	f.stripOwnerRow(t)

	gcid, live, err := f.repo.OutgoingOwnerGCID(context.Background(), f.tenantID)
	if err != nil {
		t.Fatalf("OutgoingOwnerGCID: %v", err)
	}
	if gcid != f.owner {
		t.Errorf("gcid = %q, want the stripped owner %q", gcid, f.owner)
	}
	if live {
		t.Error("live = true for a tenant whose owner row is soft-deleted")
	}
}

// A tenant that never had an owner is a bootstrap defect, not a handover case.
// It gets its own named refusal rather than a handover against nobody.
func TestIntegration_OwnershipLifecycle_OutgoingOwnerRefusesATenantThatNeverHadOne(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	if _, err := f.pool.Exec(context.Background(), `
        DELETE FROM members WHERE tenant_id = $1 AND role = 'owner'`, f.tenantID); err != nil {
		t.Fatalf("remove owner rows: %v", err)
	}

	_, _, err := f.repo.OutgoingOwnerGCID(context.Background(), f.tenantID)
	if !errors.Is(err, pg.ErrNoOwnerEver) {
		t.Fatalf("OutgoingOwnerGCID = %v, want ErrNoOwnerEver", err)
	}
}

// The most RECENT stripped owner wins. A tenant that changed hands more than
// once carries several soft-deleted owner rows, and the one being replaced is
// the last of them.
func TestIntegration_OwnershipLifecycle_OutgoingOwnerPicksTheMostRecentStrippedOwner(t *testing.T) {
	f := seedTransfer(t, liveDB(t))
	ctx := context.Background()
	older := uuid.Must(uuid.NewV7()).String()

	if _, err := f.pool.Exec(ctx, `
        INSERT INTO members (tenant_id, gcid, role, joined_at, created_at, updated_at, deleted_at)
        VALUES ($1, $2, 'owner', now() - interval '30 days', now() - interval '30 days',
                now() - interval '30 days', now() - interval '20 days')`,
		f.tenantID, older); err != nil {
		t.Fatalf("seed older owner: %v", err)
	}
	f.stripOwnerRow(t)

	gcid, live, err := f.repo.OutgoingOwnerGCID(ctx, f.tenantID)
	if err != nil {
		t.Fatalf("OutgoingOwnerGCID: %v", err)
	}
	if live {
		t.Error("live = true with no live owner row")
	}
	if gcid != f.owner {
		t.Errorf("gcid = %q, want the most recently stripped owner %q, not %q", gcid, f.owner, older)
	}
}
