// Package ownership holds the two-party tenant ownership handover (UX Track U,
// E3, S7-B2 to B7).
//
// # WHY AN OFFER AND NOT A ROLE CHANGE
//
// Ownership is one row in chora_tenancy.members with role='owner'. It is the
// only role no admin API can grant, and it carries the tenant-deletion
// authority. Two consequences shape this package.
//
// First, a handover cannot be "edit their role in the roster": that call is
// ReplaceRoles, which soft-deletes every row outside the new set, and 'owner'
// can never be in that set. Changing the owner's role there destroys ownership
// rather than moving it. The B4 guards now refuse exactly that, so the handover
// has to be its own mechanism.
//
// Second, a handover writes nothing until the nominee accepts. That is not
// politeness: assigning the deletion authority for an organisation to someone
// who never agreed is a liability transfer without consent.
//
// The shape mirrors chora_identity's pending_invites, live since migration
// 0026: a pending row, a TTL, terminal states, one live offer at a time. This
// package is pure. The transaction that swaps the members rows, the audit event
// that precedes it, and the closure interaction all live in the adapters.
package ownership

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DefaultTTL matches pending_invites, which is the precedent this offer copies
// rather than a fresh guess at how long a person needs to answer.
const DefaultTTL = 14 * 24 * time.Hour

// Status is the offer lifecycle. Only pending is non-terminal.
type Status string

const (
	StatusPending  Status = "pending"
	StatusAccepted Status = "accepted"
	StatusDeclined Status = "declined"
	StatusRevoked  Status = "revoked"
	StatusExpired  Status = "expired"
)

// Initiator says who started the handover, which decides what the offer must
// carry and which audit shape it produces.
type Initiator string

const (
	// InitiatorOwner is the normal case: the current owner hands over.
	InitiatorOwner Initiator = "owner"
	// InitiatorOperator is the override for an owner who is gone, unreachable
	// or closing. It still requires the nominee to accept.
	InitiatorOperator Initiator = "operator"
)

var (
	ErrTenantRequired      = errors.New("ownership: tenant_id required")
	ErrFromRequired        = errors.New("ownership: outgoing owner gcid required")
	ErrToRequired          = errors.New("ownership: nominee gcid required")
	ErrInitiatorRequired   = errors.New("ownership: initiating gcid required")
	ErrSelfNomination      = errors.New("ownership: the owner cannot nominate themselves")
	ErrReasonRequired      = errors.New("ownership: the operator override requires a reason")
	ErrInitiatorNotAllowed = errors.New("ownership: only the current owner or a platform operator may start a handover")
	ErrOfferNotPending     = errors.New("ownership: offer is no longer pending")
	ErrOfferExpired        = errors.New("ownership: offer has expired")
	ErrOfferNotExpired     = errors.New("ownership: offer has not expired")
	ErrNotTheNominee       = errors.New("ownership: only the nominee may answer this offer")
	ErrNotTheInitiator     = errors.New("ownership: only the initiator may revoke this offer")
)

// OfferInput is what a caller supplies. Everything else is derived.
type OfferInput struct {
	TenantID    string
	FromGCID    string
	ToGCID      string
	InitiatedBy string
	Initiator   Initiator
	// Reason is free text, mandatory on the operator override and stored on
	// the audit row. A handover nobody can explain later is not auditable.
	Reason string
	// Note is optional in both cases and is shown to the nominee.
	Note string
	Now  time.Time
	// TTL overrides DefaultTTL. Zero means DefaultTTL.
	TTL time.Duration
}

// Offer is the aggregate. Its invariants are about CONSENT and TIME; the
// membership invariants (one live owner, nominee is a member, neither party
// suspended) belong to the repository, which can see the rows.
type Offer struct {
	OfferID     string
	TenantID    string
	FromGCID    string
	ToGCID      string
	InitiatedBy string
	Initiator   Initiator
	Reason      string
	Note        string
	Status      Status
	CreatedAt   time.Time
	ExpiresAt   time.Time
	RespondedAt *time.Time
	RespondedBy string
}

// NewOffer validates and builds a pending offer.
func NewOffer(in OfferInput) (*Offer, error) {
	tenantID := strings.TrimSpace(in.TenantID)
	from := strings.TrimSpace(in.FromGCID)
	to := strings.TrimSpace(in.ToGCID)
	by := strings.TrimSpace(in.InitiatedBy)
	reason := strings.TrimSpace(in.Reason)

	switch {
	case tenantID == "":
		return nil, ErrTenantRequired
	case from == "":
		return nil, ErrFromRequired
	case to == "":
		return nil, ErrToRequired
	case by == "":
		return nil, ErrInitiatorRequired
	}
	switch in.Initiator {
	case InitiatorOwner, InitiatorOperator:
	default:
		// A tenant admin cannot promote themselves. Ownership is the one role
		// that is not admin-grantable and that property survives the handover.
		return nil, ErrInitiatorNotAllowed
	}
	if from == to {
		return nil, ErrSelfNomination
	}
	if in.Initiator == InitiatorOperator && reason == "" {
		return nil, ErrReasonRequired
	}

	ttl := in.TTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	created := in.Now.UTC()
	return &Offer{
		OfferID:     uuid.Must(uuid.NewV7()).String(),
		TenantID:    tenantID,
		FromGCID:    from,
		ToGCID:      to,
		InitiatedBy: by,
		Initiator:   in.Initiator,
		Reason:      reason,
		Note:        strings.TrimSpace(in.Note),
		Status:      StatusPending,
		CreatedAt:   created,
		ExpiresAt:   created.Add(ttl),
	}, nil
}

// IsLive reports whether the offer can still be answered at the given instant.
// Expiry is a fact about TIME, not a status somebody remembered to write: a
// pending row past its TTL is not live even though no reaper has touched it.
func (o *Offer) IsLive(at time.Time) bool {
	return o.Status == StatusPending && !at.After(o.ExpiresAt)
}

// Accept moves the offer to accepted. Only the nominee may call it, which is
// what makes the handover consent rather than assignment.
func (o *Offer) Accept(byGCID string, at time.Time) error {
	if o.Status != StatusPending {
		return ErrOfferNotPending
	}
	if strings.TrimSpace(byGCID) != o.ToGCID {
		return ErrNotTheNominee
	}
	// Checked after the identity so a stranger cannot learn an offer's expiry.
	if at.After(o.ExpiresAt) {
		return ErrOfferExpired
	}
	o.settle(StatusAccepted, byGCID, at)
	return nil
}

// Decline is the nominee's refusal. An expired offer still declines: saying no
// to something that lapsed is harmless and the record is more honest than a
// silent timeout.
func (o *Offer) Decline(byGCID string, at time.Time) error {
	if o.Status != StatusPending {
		return ErrOfferNotPending
	}
	if strings.TrimSpace(byGCID) != o.ToGCID {
		return ErrNotTheNominee
	}
	o.settle(StatusDeclined, byGCID, at)
	return nil
}

// Revoke is the initiator withdrawing before an answer.
func (o *Offer) Revoke(byGCID string, at time.Time) error {
	if o.Status != StatusPending {
		return ErrOfferNotPending
	}
	if strings.TrimSpace(byGCID) != o.InitiatedBy {
		return ErrNotTheInitiator
	}
	o.settle(StatusRevoked, byGCID, at)
	return nil
}

// Expire is what a sweep calls. It refuses an offer that reached a terminal
// state deliberately, so a reaper can never rewrite somebody's decline as a
// timeout, and it refuses one that is still live, so a sweep with a wrong clock
// cannot cancel offers early.
func (o *Offer) Expire(at time.Time) error {
	if o.Status != StatusPending {
		return ErrOfferNotPending
	}
	if !at.After(o.ExpiresAt) {
		return ErrOfferNotExpired
	}
	o.settle(StatusExpired, "", at)
	return nil
}

func (o *Offer) settle(s Status, by string, at time.Time) {
	settled := at.UTC()
	o.Status = s
	o.RespondedAt = &settled
	o.RespondedBy = strings.TrimSpace(by)
}
