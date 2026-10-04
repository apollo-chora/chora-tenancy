// offer_test.go: the two-party ownership handover offer (UX Track U, E3, S7-B2).
//
// Ownership is the one role no admin API can grant, and it carries the
// tenant-deletion authority, so it moves only when the nominee agrees. This
// aggregate is the agreement: a handover writes nothing to members until an
// offer reaches accepted.
//
// The shape deliberately mirrors chora_identity's pending_invites (live since
// migration 0026) rather than inventing one: a pending row, a TTL, terminal
// states, and at most one live offer per tenant.
package ownership_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/ownership"
)

const (
	tenant   = "01931f2a-0000-7000-8000-00000000t001"
	owner    = "01931f2a-0000-7000-8000-00000000g001"
	nominee  = "01931f2a-0000-7000-8000-00000000g002"
	operator = "01931f2a-0000-7000-8000-00000000g003"
)

var now = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

func validInput() ownership.OfferInput {
	return ownership.OfferInput{
		TenantID:    tenant,
		FromGCID:    owner,
		ToGCID:      nominee,
		InitiatedBy: owner,
		Initiator:   ownership.InitiatorOwner,
		Now:         now,
	}
}

func TestNewOffer_defaultsToPendingWithATTL(t *testing.T) {
	o, err := ownership.NewOffer(validInput())
	if err != nil {
		t.Fatalf("NewOffer: %v", err)
	}
	if o.Status != ownership.StatusPending {
		t.Fatalf("status = %q, want pending", o.Status)
	}
	if o.OfferID == "" {
		t.Fatal("offer needs an id")
	}
	if !o.ExpiresAt.After(now) {
		t.Fatalf("expires_at %v must be after creation %v", o.ExpiresAt, now)
	}
	if got := o.ExpiresAt.Sub(now); got != ownership.DefaultTTL {
		t.Fatalf("TTL = %v, want %v (mirrors pending_invites)", got, ownership.DefaultTTL)
	}
}

func TestNewOffer_refusesSelfNomination(t *testing.T) {
	in := validInput()
	in.ToGCID = in.FromGCID
	// A no-op that looks like an action is worse than a refusal: the owner
	// would see a pending handover that can never change anything.
	if _, err := ownership.NewOffer(in); err != ownership.ErrSelfNomination {
		t.Fatalf("err = %v, want ErrSelfNomination", err)
	}
}

func TestNewOffer_refusesMissingParties(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ownership.OfferInput)
	}{
		{"no tenant", func(i *ownership.OfferInput) { i.TenantID = "" }},
		{"no outgoing owner", func(i *ownership.OfferInput) { i.FromGCID = "" }},
		{"no nominee", func(i *ownership.OfferInput) { i.ToGCID = "" }},
		{"no initiator", func(i *ownership.OfferInput) { i.InitiatedBy = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mutate(&in)
			if _, err := ownership.NewOffer(in); err == nil {
				t.Fatalf("accepted an offer with %s", tc.name)
			}
		})
	}
}

// The operator override exists so a handover is never wedged by an unreachable
// owner, and the reason is what makes it auditable rather than arbitrary.
func TestNewOffer_operatorOverrideRequiresAReason(t *testing.T) {
	in := validInput()
	in.Initiator = ownership.InitiatorOperator
	in.InitiatedBy = operator
	if _, err := ownership.NewOffer(in); err != ownership.ErrReasonRequired {
		t.Fatalf("err = %v, want ErrReasonRequired", err)
	}
	in.Reason = "  "
	if _, err := ownership.NewOffer(in); err != ownership.ErrReasonRequired {
		t.Fatalf("blank reason accepted; err = %v", err)
	}
	in.Reason = "owner unreachable for 60 days, ticket CHO-9999"
	if _, err := ownership.NewOffer(in); err != nil {
		t.Fatalf("operator override with a reason: %v", err)
	}
}

// An owner-initiated handover may carry a note but never needs one.
func TestNewOffer_ownerInitiatedNeedsNoReason(t *testing.T) {
	if _, err := ownership.NewOffer(validInput()); err != nil {
		t.Fatalf("owner-initiated offer refused: %v", err)
	}
}

func TestNewOffer_refusesAnUnknownInitiatorKind(t *testing.T) {
	in := validInput()
	in.Initiator = "tenant_admin"
	// An admin cannot promote themselves; ownership is the one role that is
	// not admin-grantable and that property has to survive this feature.
	if _, err := ownership.NewOffer(in); err != ownership.ErrInitiatorNotAllowed {
		t.Fatalf("err = %v, want ErrInitiatorNotAllowed", err)
	}
}

func TestOffer_acceptMovesToAccepted(t *testing.T) {
	o, _ := ownership.NewOffer(validInput())
	at := now.Add(time.Hour)
	if err := o.Accept(nominee, at); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if o.Status != ownership.StatusAccepted {
		t.Fatalf("status = %q, want accepted", o.Status)
	}
	if o.RespondedAt == nil || !o.RespondedAt.Equal(at) {
		t.Fatalf("responded_at = %v, want %v", o.RespondedAt, at)
	}
}

// Only the nominee. An offer accepted by anyone else is not consent.
func TestOffer_acceptRefusesAnybodyButTheNominee(t *testing.T) {
	o, _ := ownership.NewOffer(validInput())
	if err := o.Accept(owner, now.Add(time.Hour)); err != ownership.ErrNotTheNominee {
		t.Fatalf("err = %v, want ErrNotTheNominee", err)
	}
	if o.Status != ownership.StatusPending {
		t.Fatalf("a refused accept moved the offer to %q", o.Status)
	}
}

// The load-bearing one. Expiry is a fact about time, not a status somebody
// remembered to write: a pending row past its TTL must not be acceptable, or
// an offer left un-reaped becomes a permanent standing claim on ownership.
func TestOffer_acceptRefusesAPendingRowPastItsTTL(t *testing.T) {
	o, _ := ownership.NewOffer(validInput())
	late := o.ExpiresAt.Add(time.Second)
	if err := o.Accept(nominee, late); err != ownership.ErrOfferExpired {
		t.Fatalf("err = %v, want ErrOfferExpired", err)
	}
	if o.Status != ownership.StatusPending {
		t.Fatalf("a refused accept mutated the status to %q", o.Status)
	}
	// And exactly at the boundary it is still live: an inclusive expiry would
	// silently shorten every offer by whatever the clock skew happens to be.
	fresh, _ := ownership.NewOffer(validInput())
	if err := fresh.Accept(nominee, fresh.ExpiresAt); err != nil {
		t.Fatalf("accept at the expiry instant: %v", err)
	}
}

func TestOffer_terminalStatesAreTerminal(t *testing.T) {
	later := now.Add(time.Hour)
	for _, tc := range []struct {
		name  string
		reach func(*ownership.Offer) error
		then  func(*ownership.Offer) error
	}{
		{"accept twice", func(o *ownership.Offer) error { return o.Accept(nominee, later) },
			func(o *ownership.Offer) error { return o.Accept(nominee, later) }},
		{"decline after accept", func(o *ownership.Offer) error { return o.Accept(nominee, later) },
			func(o *ownership.Offer) error { return o.Decline(nominee, later) }},
		{"revoke after accept", func(o *ownership.Offer) error { return o.Accept(nominee, later) },
			func(o *ownership.Offer) error { return o.Revoke(owner, later) }},
		{"accept after decline", func(o *ownership.Offer) error { return o.Decline(nominee, later) },
			func(o *ownership.Offer) error { return o.Accept(nominee, later) }},
		{"accept after revoke", func(o *ownership.Offer) error { return o.Revoke(owner, later) },
			func(o *ownership.Offer) error { return o.Accept(nominee, later) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, _ := ownership.NewOffer(validInput())
			if err := tc.reach(o); err != nil {
				t.Fatalf("reaching the terminal state: %v", err)
			}
			before := o.Status
			if err := tc.then(o); err != ownership.ErrOfferNotPending {
				t.Fatalf("err = %v, want ErrOfferNotPending", err)
			}
			if o.Status != before {
				t.Fatalf("a refused transition changed %q to %q", before, o.Status)
			}
		})
	}
}

func TestOffer_declineIsTheNomineesAloneAndRevokeIsTheInitiators(t *testing.T) {
	later := now.Add(time.Hour)

	o, _ := ownership.NewOffer(validInput())
	if err := o.Decline(owner, later); err != ownership.ErrNotTheNominee {
		t.Fatalf("decline by the initiator: err = %v, want ErrNotTheNominee", err)
	}

	o2, _ := ownership.NewOffer(validInput())
	if err := o2.Revoke(nominee, later); err != ownership.ErrNotTheInitiator {
		t.Fatalf("revoke by the nominee: err = %v, want ErrNotTheInitiator", err)
	}
}

// Expire is what a reaper calls. It is idempotent on an already-expired offer
// and refuses to touch one that reached a terminal state on purpose, so a
// sweep can never rewrite somebody's decline as a timeout.
func TestOffer_expire(t *testing.T) {
	o, _ := ownership.NewOffer(validInput())
	past := o.ExpiresAt.Add(time.Second)
	if err := o.Expire(past); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if o.Status != ownership.StatusExpired {
		t.Fatalf("status = %q, want expired", o.Status)
	}

	declined, _ := ownership.NewOffer(validInput())
	if err := declined.Decline(nominee, now.Add(time.Hour)); err != nil {
		t.Fatalf("Decline: %v", err)
	}
	if err := declined.Expire(past); err != ownership.ErrOfferNotPending {
		t.Fatalf("a sweep rewrote a decline as a timeout: err = %v", err)
	}

	early, _ := ownership.NewOffer(validInput())
	if err := early.Expire(now.Add(time.Minute)); err != ownership.ErrOfferNotExpired {
		t.Fatalf("expired an offer that is still live: err = %v", err)
	}
}
