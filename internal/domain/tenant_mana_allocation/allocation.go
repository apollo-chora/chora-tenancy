// Package allocation implements the TenantManaAllocation aggregate per
// BE-USR-3 (Wave 3 of the per-user economy / Q2=v1 tenant subsidy track).
//
// TenantManaAllocation is the append-only audit row written by chora-tenancy
// when a tenant's pool grants mana to a specific learner. chora-identity
// consumes the `granted` event and credits the learner's UserMana wallet
// (reason=tenant_subsidy); on first debit hitting the allocation,
// chora-identity emits its own UserManaDebited which loops back to
// chora-tenancy as a `claimed` transition.
//
// Source-of-truth references:
//   - chora-contracts/proto/events/tenancy/tenant_mana_allocation.proto
//   - chora-contracts/openapi/tenancy-admin.yaml v1.2 (mana-allocations)
//   - docs/architecture/adrs/adr-142-per-user-economy-mana.md
//
// Aggregate invariants:
//   - Append-only: a row is created once and only its status (+ tracking
//     fields) mutate via the explicit transitions Claim / Revoke / Expire.
//   - Status transitions: pending → claimed | revoked | expired (terminal).
//     Re-claim of a claimed row is a NO-OP (idempotent first-debit
//     semantics). Re-revoke / re-expire return ErrInvalidTransition.
//   - Manual reasons require AllocatedByGCID; auto-policy reasons may omit
//     it (system-driven).
//   - Revoke returns units to pool ONLY when prior status was pending;
//     claimed allocations stay debited (already with the learner).
package allocation

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

var (
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrInvalidTransition = errors.New("invalid status transition")
)

// MaxRevokedReasonLength is the upper bound for the free-text revocation
// reason. Mirrors the OpenAPI `RevokeTenantManaAllocationRequest.reason`
// schema (maxLength: 512).
const MaxRevokedReasonLength = 512

// -----------------------------------------------------------------------------
// UUIDv7
// -----------------------------------------------------------------------------

// NewUUIDv7 returns an RFC 9562 §5.7 UUIDv7 string.
func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// Status + Reason enums
// -----------------------------------------------------------------------------

// Status is the lifecycle state of an allocation.
type Status string

const (
	StatusPending Status = "pending"
	StatusClaimed Status = "claimed"
	StatusExpired Status = "expired"
	StatusRevoked Status = "revoked"
)

// Reason is why an allocation was issued.
type Reason string

const (
	ReasonManual       Reason = "manual"
	ReasonOnEnrollment Reason = "on_enrollment"
	ReasonEqualSplit   Reason = "equal_split"
	ReasonTierBased    Reason = "tier_based"
	ReasonPromo        Reason = "promo"
	// ReasonEggHardExpiryRefund is issued by the familiar-egg sweeper when
	// an unhatched egg purchase passes its hard_expiry_at. The credit is
	// granted to the original purchaser; auditable as a non-cash refund
	// (mana credits) per ADR-149 §"Unhatched egg expiry".
	ReasonEggHardExpiryRefund Reason = "egg_hard_expiry_refund"
)

// IsValid reports whether r is a recognised reason value.
func (r Reason) IsValid() bool {
	switch r {
	case ReasonManual, ReasonOnEnrollment, ReasonEqualSplit, ReasonTierBased, ReasonPromo, ReasonEggHardExpiryRefund:
		return true
	default:
		return false
	}
}

// -----------------------------------------------------------------------------
// TenantManaAllocation
// -----------------------------------------------------------------------------

// TenantManaAllocation is the append-only audit row.
type TenantManaAllocation struct {
	AllocationID      string
	TenantID          string
	GCID              string // recipient learner
	Units             int64
	SourcePoolID      string // empty for platform-side reasons (egg_hard_expiry_refund); UUID otherwise
	Status            Status
	Reason            Reason
	AllocatedByGCID   string // empty for auto-policy reasons
	AllocatedAt       time.Time
	ExpiresAt         *time.Time
	ClaimedAt         *time.Time
	RevokedAt         *time.Time
	RevokedReason     string
	RevokedByGCID     string
	FirstDebitEntryID string // populated on Claim
	RemainingUnits    int64  // optional; populated on Claim when known
	UpdatedAt         time.Time
	// IdempotencyKey is the de-dup hint used by reason-specific issuance
	// paths (e.g., familiar-egg sweeper retries). When non-empty, the
	// repository enforces uniqueness via the 0009 partial UNIQUE INDEX on
	// chora_tenancy.tenant_mana_allocations(idempotency_key).
	IdempotencyKey string
}

// NewParams is the input bundle for New(). Keeps the constructor signature
// stable as we add optional fields (note, expires_at, etc.).
type NewParams struct {
	TenantID        string
	GCID            string
	SourcePoolID    string
	Units           int64
	Reason          Reason
	AllocatedByGCID string
	ExpiresAt       *time.Time
}

// New constructs a fresh TenantManaAllocation in `pending` status.
func New(p NewParams) (*TenantManaAllocation, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(p.GCID) == "" {
		return nil, fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(p.SourcePoolID) == "" {
		return nil, fmt.Errorf("%w: source_pool_id required", ErrInvalidArgument)
	}
	if p.Units <= 0 {
		return nil, fmt.Errorf("%w: units must be > 0", ErrInvalidArgument)
	}
	if !p.Reason.IsValid() {
		return nil, fmt.Errorf("%w: invalid reason %q", ErrInvalidArgument, p.Reason)
	}
	if p.Reason == ReasonManual && strings.TrimSpace(p.AllocatedByGCID) == "" {
		return nil, fmt.Errorf("%w: manual reason requires allocated_by_gcid", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &TenantManaAllocation{
		AllocationID:    NewUUIDv7(),
		TenantID:        strings.TrimSpace(p.TenantID),
		GCID:            strings.TrimSpace(p.GCID),
		SourcePoolID:    strings.TrimSpace(p.SourcePoolID),
		Units:           p.Units,
		Status:          StatusPending,
		Reason:          p.Reason,
		AllocatedByGCID: strings.TrimSpace(p.AllocatedByGCID),
		AllocatedAt:     now,
		ExpiresAt:       p.ExpiresAt,
		UpdatedAt:       now,
	}, nil
}

// Claim transitions a pending allocation to claimed on the first learner
// debit. Idempotent on a claimed allocation (NO-OP) — the recorded
// FirstDebitEntryID + ClaimedAt are NOT rewritten by subsequent calls.
// Returns ErrInvalidTransition if status is revoked or expired.
func (a *TenantManaAllocation) Claim(firstDebitEntryID string, remainingUnits int64) error {
	switch a.Status {
	case StatusClaimed:
		return nil
	case StatusRevoked, StatusExpired:
		return fmt.Errorf("%w: cannot claim allocation in status %q", ErrInvalidTransition, a.Status)
	}
	if strings.TrimSpace(firstDebitEntryID) == "" {
		return fmt.Errorf("%w: first_debit_entry_id required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	a.Status = StatusClaimed
	a.FirstDebitEntryID = firstDebitEntryID
	a.RemainingUnits = remainingUnits
	a.ClaimedAt = &now
	a.UpdatedAt = now
	return nil
}

// RevokeResult describes the side-effect of a revocation. The caller (HTTP
// adapter) uses this to decide whether to credit the pool.
type RevokeResult struct {
	UnitsReturnedToPool bool
	ReturnedUnits       int64
}

// Revoke transitions a pending or claimed allocation to revoked. Pending
// allocations return their units to the pool; claimed allocations do not
// (units are already with the learner).
func (a *TenantManaAllocation) Revoke(reason, byGCID string) (RevokeResult, error) {
	if strings.TrimSpace(reason) == "" {
		return RevokeResult{}, fmt.Errorf("%w: reason required", ErrInvalidArgument)
	}
	if len(reason) > MaxRevokedReasonLength {
		return RevokeResult{}, fmt.Errorf("%w: reason exceeds %d chars", ErrInvalidArgument, MaxRevokedReasonLength)
	}
	if strings.TrimSpace(byGCID) == "" {
		return RevokeResult{}, fmt.Errorf("%w: revoked_by_gcid required", ErrInvalidArgument)
	}
	switch a.Status {
	case StatusRevoked, StatusExpired:
		return RevokeResult{}, fmt.Errorf("%w: cannot revoke allocation in status %q", ErrInvalidTransition, a.Status)
	}
	res := RevokeResult{}
	if a.Status == StatusPending {
		res.UnitsReturnedToPool = true
		res.ReturnedUnits = a.Units
	}
	now := time.Now().UTC()
	a.Status = StatusRevoked
	a.RevokedAt = &now
	a.RevokedReason = reason
	a.RevokedByGCID = strings.TrimSpace(byGCID)
	a.UpdatedAt = now
	return res, nil
}

// ExpireResult mirrors RevokeResult for the housekeeping job.
type ExpireResult = RevokeResult

// Expire transitions a pending or claimed allocation to expired.
func (a *TenantManaAllocation) Expire() (ExpireResult, error) {
	switch a.Status {
	case StatusRevoked, StatusExpired:
		return ExpireResult{}, fmt.Errorf("%w: cannot expire allocation in status %q", ErrInvalidTransition, a.Status)
	}
	res := ExpireResult{}
	if a.Status == StatusPending {
		res.UnitsReturnedToPool = true
		res.ReturnedUnits = a.Units
	}
	now := time.Now().UTC()
	a.Status = StatusExpired
	a.UpdatedAt = now
	return res, nil
}

// IsExpired returns true when ExpiresAt is set and now >= ExpiresAt. Used
// by the housekeeping job to find candidates without a status-change.
func (a *TenantManaAllocation) IsExpired(now time.Time) bool {
	if a.ExpiresAt == nil {
		return false
	}
	return !now.Before(*a.ExpiresAt)
}
