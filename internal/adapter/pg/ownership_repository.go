// ownership_repository.go: persistence for the two-party ownership handover
// (UX Track U, E3 slice 2, S7-B4).
//
// The domain aggregate owns the invariants about CONSENT and TIME. This file
// owns the ones that need to see rows: the nominee is a live, unsuspended
// member, the tenant never ends up unowned, and the audit is atomic with the
// write.
//
// ⚠ THE NO-ROWS SENTINEL. This package converts pgx.ErrNoRows to its own
// pg.ErrNoRows in the Row wrapper (runtime.go), so a repository comparing
// against pgx.ErrNoRows is always false. Slice 2 did exactly that, which made
// two named refusals unreachable: an unknown offer id surfaced as a wrapped
// scan error rather than ErrOfferNotFound, and the operator override could
// never take its "the owner row is already gone" branch. Caught in slice 4 by
// tests written for those two cases; both were RED before the fix.
//
// ⚠ THE ORDERING CONSTRAINT. Migration 0036 created
// uq_members_one_live_owner_per_tenant, a PARTIAL UNIQUE INDEX. A partial
// unique index cannot be deferred: Postgres defers CONSTRAINTs, and a unique
// constraint cannot carry a WHERE clause. So the accept transaction soft-deletes
// the outgoing owner row BEFORE inserting the incoming one. Insert-then-delete
// violates the index mid-transaction and fails. The first-launch spec 13.3.3
// lists the insert first; that is an unordered list of effects, not a statement
// order. Proven on the live local mirror both ways before this was written.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/ownership"
)

// Refusals the handler maps to 4xx. Each names a situation the caller can fix,
// which is why none of them is a 500.
var (
	ErrOfferAlreadyPending = errors.New("tenancy: this tenant already has a pending ownership offer")
	ErrOfferNotFound       = errors.New("tenancy: ownership offer not found")
	ErrOfferNotPending     = errors.New("tenancy: ownership offer is no longer pending")
	ErrOfferExpired        = errors.New("tenancy: ownership offer has expired")
	ErrNotTheNominee       = errors.New("tenancy: only the nominee may accept this offer")
	ErrNomineeNotAMember   = errors.New("tenancy: the nominee is not a live member of this tenant")
	ErrNomineeSuspended    = errors.New("tenancy: the nominee's membership is suspended")
	ErrOutgoingOwnerGone   = errors.New("tenancy: the outgoing owner no longer holds the owner role")
	ErrNotTheInitiator     = errors.New("tenancy: only the initiator may withdraw this offer")
	ErrNoOwnerEver         = errors.New("tenancy: this tenant has never had an owner row")
)

const auditEventType = "chora.governance.audit.recorded.v1"

// OwnershipRepository persists offers and performs the swap.
type OwnershipRepository struct {
	q TxQuerier
}

func NewOwnershipRepository(q TxQuerier) *OwnershipRepository {
	return &OwnershipRepository{q: q}
}

// CreateOffer inserts a pending offer.
//
// The one-pending-offer rule is a partial unique index rather than a read
// followed by an insert, so two racing initiators cannot both pass a check and
// then both write. The unique violation is translated here: a caller who sees
// ErrOfferAlreadyPending can act on it, where a raw 23505 tells them nothing.
func (r *OwnershipRepository) CreateOffer(ctx context.Context, o *ownership.Offer) error {
	return r.q.RunInTenantTx(ctx, o.TenantID, func(ctx context.Context, tx Tx) error {
		err := tx.Exec(ctx, `
            INSERT INTO ownership_offers (
                offer_id, tenant_id, from_gcid, to_gcid, initiated_by,
                initiator, reason, note, status, created_at, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6::ownership_offer_initiator,
                      NULLIF($7, ''), NULLIF($8, ''), 'pending', $9, $10)`,
			o.OfferID, o.TenantID, o.FromGCID, o.ToGCID, o.InitiatedBy,
			string(o.Initiator), o.Reason, o.Note, o.CreatedAt, o.ExpiresAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" &&
				strings.Contains(pgErr.ConstraintName, "one_pending_per_tenant") {
				return ErrOfferAlreadyPending
			}
			return fmt.Errorf("insert ownership offer: %w", err)
		}
		return nil
	})
}

// AcceptOffer is the handover. Everything happens in ONE transaction inside
// RunInTenantTx, so RLS is enforcing throughout and a refusal leaves nothing
// behind.
//
// Order of statements, and each position is load-bearing:
//
//  1. Lock and re-read the offer. The domain already checked consent and time
//     when the offer was made; between then and now the row may have been
//     accepted, declined, revoked or expired, so it is checked again against
//     the row rather than against the caller's copy.
//  2. Check the nominee's membership. The roster can change between nomination
//     and acceptance, so this is the check that matters, not the one at
//     nomination.
//  3. Write the audit row FIRST, per the ADR-165 audit-before-SQL rule. Inside
//     one transaction that also buys the stronger property: no audit without
//     the transfer, and no transfer without the audit.
//  4. Soft-delete the outgoing owner row. BEFORE the insert, see the file
//     header.
//  5. Insert or resurrect the incoming owner row.
//  6. Ensure the outgoing owner keeps a membership, as admin. Losing only
//     ownership is the whole point: stripping every row is closer to a delete
//     and is exactly the defect RemoveMembership has.
func (r *OwnershipRepository) AcceptOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error {
	return r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var (
			fromGCID, toGCID, status, initiator string
			reason                              *string
			expiresAt                           time.Time
		)
		err := tx.QueryRow(ctx, `
            SELECT from_gcid, to_gcid, status::text, initiator::text, reason, expires_at
            FROM   ownership_offers
            WHERE  offer_id = $1 AND tenant_id = $2
            FOR UPDATE`, offerID, tenantID).
			Scan(&fromGCID, &toGCID, &status, &initiator, &reason, &expiresAt)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return ErrOfferNotFound
			}
			return fmt.Errorf("lock offer: %w", err)
		}
		if status != string(ownership.StatusPending) {
			return ErrOfferNotPending
		}
		if strings.TrimSpace(byGCID) != toGCID {
			return ErrNotTheNominee
		}
		// Expiry is a fact about time, not a status a reaper remembered to
		// write: a pending row past its TTL is not acceptable.
		if at.After(expiresAt) {
			return ErrOfferExpired
		}

		if err := assertLiveUnsuspendedMember(ctx, tx, tenantID, toGCID); err != nil {
			return err
		}

		if err := insertOwnershipAudit(ctx, tx, tenantID, offerID, fromGCID, toGCID, byGCID, initiator, reason, at); err != nil {
			return err
		}

		// 4. Outgoing owner row FIRST.
		var released int
		err = tx.QueryRow(ctx, `
            UPDATE members
            SET    deleted_at = $3, updated_at = $3
            WHERE  tenant_id = $1 AND gcid = $2
              AND  role = 'owner' AND deleted_at IS NULL
            RETURNING 1`, tenantID, fromGCID, at).Scan(&released)
		if err != nil {
			if !errors.Is(err, ErrNoRows) {
				return fmt.Errorf("release outgoing owner: %w", err)
			}
			// The operator override handles a tenant whose owner row is already
			// gone (spec 13.4); the owner-initiated path must not silently
			// create ownership out of nothing.
			if initiator != string(ownership.InitiatorOperator) {
				return ErrOutgoingOwnerGone
			}
		}

		// 5. Incoming owner: insert, or resurrect a soft-deleted row. The
		//    conflict target is (tenant_id, gcid, role) per 0021.
		if err := tx.Exec(ctx, `
            INSERT INTO members (tenant_id, gcid, role, joined_at, created_at, updated_at)
            VALUES ($1, $2, 'owner', $3, $3, $3)
            ON CONFLICT (tenant_id, gcid, role) DO UPDATE
            SET deleted_at = NULL, suspended_at = NULL, updated_at = $3`,
			tenantID, toGCID, at); err != nil {
			return fmt.Errorf("install incoming owner: %w", err)
		}

		// 6. The outgoing owner keeps a membership, on the OWNER-INITIATED
		//    path only. Spec 13.4: on the operator override the leaving
		//    owner's roles are untouched, because that path exists for an
		//    owner who is gone, closing or closed, and an INSERT here would
		//    either hand a seat to somebody who never asked for it or
		//    resurrect a membership the closure saga is winding down. Only
		//    their owner row is released, in step 4.
		//
		//    chora-tenancy cannot read closure state (it lives in
		//    chora_ai_kernel and cross-DB reads are forbidden), so the
		//    initiator is the discriminator rather than the saga: it is
		//    known locally, it is on the row, and it is right in every
		//    override case, not only the closing one.
		if initiator != string(ownership.InitiatorOperator) {
			if err := tx.Exec(ctx, `
                INSERT INTO members (tenant_id, gcid, role, joined_at, created_at, updated_at)
                VALUES ($1, $2, 'admin', $3, $3, $3)
                ON CONFLICT (tenant_id, gcid, role) DO UPDATE
                SET deleted_at = NULL, updated_at = $3`,
				tenantID, fromGCID, at); err != nil {
				return fmt.Errorf("demote outgoing owner to admin: %w", err)
			}
		}

		if err := tx.Exec(ctx, `
            UPDATE ownership_offers
            SET    status = 'accepted', responded_at = $2, responded_by = $3
            WHERE  offer_id = $1`, offerID, at, byGCID); err != nil {
			return fmt.Errorf("settle offer: %w", err)
		}
		return nil
	})
}

// DeclineOffer is the nominee refusing. It writes no membership row and no
// audit event: nothing about the tenant's ownership changed, and the offer row
// itself carries the durable record (status, responded_at, responded_by).
//
// An EXPIRED offer still declines. Saying no to something that lapsed is
// harmless, and a recorded refusal is more honest than a silent timeout, which
// is the same rule the aggregate applies.
func (r *OwnershipRepository) DeclineOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error {
	return r.settleOffer(ctx, tenantID, offerID, byGCID, ownership.StatusDeclined, at)
}

// RevokeOffer is the initiator withdrawing before an answer. Only the initiator
// may call it: the nominee's way out is to decline, which is recorded as a
// refusal rather than as the initiator changing their mind. On the operator
// override the initiator is the operator who created the offer, so a different
// operator cannot quietly withdraw somebody else's override.
func (r *OwnershipRepository) RevokeOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error {
	return r.settleOffer(ctx, tenantID, offerID, byGCID, ownership.StatusRevoked, at)
}

// settleOffer moves a pending offer to a terminal state, checking the actor
// against the role that state requires.
//
// The row is locked FOR UPDATE and re-read rather than trusting the caller's
// copy, for the same reason AcceptOffer does: between reading an offer and
// answering it, somebody else may have answered it already.
func (r *OwnershipRepository) settleOffer(ctx context.Context, tenantID, offerID, byGCID string, to ownership.Status, at time.Time) error {
	return r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var toGCID, initiatedBy, status string
		err := tx.QueryRow(ctx, `
            SELECT to_gcid, initiated_by, status::text
            FROM   ownership_offers
            WHERE  offer_id = $1 AND tenant_id = $2
            FOR UPDATE`, offerID, tenantID).
			Scan(&toGCID, &initiatedBy, &status)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return ErrOfferNotFound
			}
			return fmt.Errorf("lock offer: %w", err)
		}
		if status != string(ownership.StatusPending) {
			return ErrOfferNotPending
		}

		actor := strings.TrimSpace(byGCID)
		switch to {
		case ownership.StatusDeclined:
			if actor != toGCID {
				return ErrNotTheNominee
			}
		case ownership.StatusRevoked:
			if actor != initiatedBy {
				return ErrNotTheInitiator
			}
		default:
			return fmt.Errorf("settleOffer: %q is not a settleable state here", to)
		}

		if err := tx.Exec(ctx, `
            UPDATE ownership_offers
            SET    status = $2::ownership_offer_status, responded_at = $3, responded_by = $4
            WHERE  offer_id = $1`, offerID, string(to), at, actor); err != nil {
			return fmt.Errorf("settle offer: %w", err)
		}
		return nil
	})
}

// PendingOffer returns the tenant's single pending offer, or ErrOfferNotFound.
// One row at most exists by construction: 0037 carries a partial unique index
// on (tenant_id) WHERE status = 'pending'.
//
// A row past its TTL is RETURNED, not hidden. No reaper writes the expired
// status today, so filtering on expires_at here would report "there is nothing
// here" about a row that exists, and S7a has to be able to say the offer
// lapsed. Callers ask the aggregate with Offer.IsLive(now).
//
// Both S7a and S7b read this one row: the caller is the initiator or the
// nominee, and the handler decides which side of it to render. That keeps the
// read tenant-scoped, so it runs inside RunInTenantTx with RLS enforcing and
// needs no cross-tenant path.
func (r *OwnershipRepository) PendingOffer(ctx context.Context, tenantID string) (*ownership.Offer, error) {
	var out *ownership.Offer
	err := r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var (
			offerID, fromGCID, toGCID, initiatedBy, initiator, status string
			reason, note                                              *string
			createdAt, expiresAt                                      time.Time
		)
		err := tx.QueryRow(ctx, `
            SELECT offer_id, from_gcid, to_gcid, initiated_by,
                   initiator::text, status::text, reason, note,
                   created_at, expires_at
            FROM   ownership_offers
            WHERE  tenant_id = $1 AND status = 'pending'`, tenantID).
			Scan(&offerID, &fromGCID, &toGCID, &initiatedBy,
				&initiator, &status, &reason, &note, &createdAt, &expiresAt)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return ErrOfferNotFound
			}
			return fmt.Errorf("read pending offer: %w", err)
		}
		out = &ownership.Offer{
			OfferID:     offerID,
			TenantID:    tenantID,
			FromGCID:    fromGCID,
			ToGCID:      toGCID,
			InitiatedBy: initiatedBy,
			Initiator:   ownership.Initiator(initiator),
			Reason:      derefOr(reason),
			Note:        derefOr(note),
			Status:      ownership.Status(status),
			CreatedAt:   createdAt.UTC(),
			ExpiresAt:   expiresAt.UTC(),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// derefOr reads a nullable text column back as the empty string. The columns
// are NULLIF(..., ”) on write, so NULL and "" mean the same absence.
func derefOr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// OutgoingOwnerGCID answers "who is being replaced", and whether that person
// still holds the tenant. The FE never sends from_gcid: a GCID alone never
// proves ownership (spec 13.6), so the outgoing side is resolved here.
//
// live=true is the ordinary handover. live=false means the owner row has
// already been stripped, which is the case the operator override exists for;
// the most RECENTLY soft-deleted owner is returned, because a tenant that has
// changed hands carries several and the one being replaced is the last of them.
// Returning a placeholder instead would put a fiction on the audit row.
//
// ErrNoOwnerEver is a tenant with no owner row at all, live or deleted. Both
// creation paths write one inside the create transaction, so this is a
// bootstrap defect rather than a handover case, and it is named rather than
// smoothed over.
func (r *OwnershipRepository) OutgoingOwnerGCID(ctx context.Context, tenantID string) (string, bool, error) {
	var (
		gcid string
		live bool
	)
	err := r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		// One query, ordered so a live row always outranks a deleted one and
		// the newest deleted row outranks older ones. A separate "is there a
		// live owner" probe would race the strip it exists to detect.
		err := tx.QueryRow(ctx, `
            SELECT gcid, deleted_at IS NULL
            FROM   members
            WHERE  tenant_id = $1 AND role = 'owner'
            ORDER  BY (deleted_at IS NULL) DESC, deleted_at DESC NULLS FIRST
            LIMIT  1`, tenantID).Scan(&gcid, &live)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				return ErrNoOwnerEver
			}
			return fmt.Errorf("read outgoing owner: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return gcid, live, nil
}

// assertLiveUnsuspendedMember refuses a nominee who cannot act in the tenant.
// Ownership by a non-member leaves an organisation owned by somebody with no
// way in, and a suspended nominee is refused the same way ReplaceRoles and
// RemoveMembership already refuse a suspended member.
func assertLiveUnsuspendedMember(ctx context.Context, tx Tx, tenantID, gcid string) error {
	rows, err := tx.Query(ctx, `
        SELECT suspended_at IS NOT NULL
        FROM   members
        WHERE  tenant_id = $1 AND gcid = $2 AND deleted_at IS NULL`,
		tenantID, gcid)
	if err != nil {
		return fmt.Errorf("read nominee membership: %w", err)
	}
	defer rows.Close()
	anyLive, anyUsable := false, false
	for rows.Next() {
		var suspended bool
		if err := rows.Scan(&suspended); err != nil {
			return fmt.Errorf("scan nominee membership: %w", err)
		}
		anyLive = true
		if !suspended {
			anyUsable = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("nominee membership rows: %w", err)
	}
	switch {
	case !anyLive:
		return ErrNomineeNotAMember
	case !anyUsable:
		// Every live row is suspended. One unsuspended row is enough, because
		// a member with any usable role can act in the tenant.
		return ErrNomineeSuspended
	}
	return nil
}

// insertOwnershipAudit writes the governance audit event to the outbox in the
// same transaction as the transfer, before the membership rows move.
//
// The generic audit event is reused rather than minting a bespoke one, so the
// handover lands in the same O+ stream as every other membership-authority
// change (ADR-194 D1 uses it for the operator cross-tenant grant).
func insertOwnershipAudit(ctx context.Context, tx Tx, tenantID, offerID, fromGCID, toGCID, actorGCID, initiator string, reason *string, at time.Time) error {
	eventID := uuid.Must(uuid.NewV7()).String()
	payload := map[string]any{
		"action":               "tenant_ownership_transferred",
		"tenant_id":            tenantID,
		"actor_gcid":           actorGCID,
		"subject_from_gcid":    fromGCID,
		"subject_to_gcid":      toGCID,
		"initiator":            initiator,
		"offer_id":             offerID,
		"accepted_at":          at.UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension": "accountability",
	}
	if reason != nil && strings.TrimSpace(*reason) != "" {
		payload["reason"] = *reason
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal audit payload: %w", err)
	}
	// The idempotency key is the offer, not the moment: a retried accept must
	// not produce a second audit row claiming a second handover.
	envelope := map[string]any{
		"event_id":        eventID,
		"idempotency_key": "ownership-transfer:" + offerID,
		"tenant_id":       tenantID,
		"gcid":            actorGCID,
		"occurred_at":     at.UTC().Format(time.RFC3339Nano),
		"source_service":  "chora-tenancy",
		"schema_version":  "1",
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal audit envelope: %w", err)
	}
	if err := tx.Exec(ctx, `
        INSERT INTO outbox_events (
            id, tenant_id, gcid, aggregate_type, aggregate_id,
            event_type, topic, payload, envelope, idempotency_key,
            occurred_at, status
        ) VALUES ($1, $2, $3, 'ownership_offer', $4, $5, $6, $7, $8, $9, $10, 'pending')`,
		eventID, tenantID, actorGCID, offerID, auditEventType, auditEventType,
		payloadJSON, envelopeJSON, "ownership-transfer:"+offerID, at); err != nil {
		return fmt.Errorf("write ownership audit: %w", err)
	}
	return nil
}
