// Package pool — PolicyEngine drives auto-allocation events.
//
// The engine is invoked from two sources:
//
//   - OnLearnerEnrolled — invoked from the chora.identity.user_membership.created.v1
//     subscriber inside chora-tenancy. Consults the
//     tenant's pool policy and emits an allocation
//     intent if applicable.
//   - OnMonthlyTick     — invoked from a Cloud Scheduler / monthly cron.
//     For EqualSplit policies, computes per-learner
//     units and emits one intent per active learner.
//
// The engine is stateless aside from its dependencies. It returns a slice
// of AllocationIntent values; the caller (HTTP/event adapter) is
// responsible for atomically creating the TenantManaAllocation row,
// debiting the pool, and publishing the
// chora.tenancy.tenant_mana_allocation.granted.v1 event.
package pool

import (
	"context"
	"errors"
	"strings"
)

// AllocationReason is a copy of the allocation_reason enum value used by the
// allocation aggregate. Duplicated here as plain strings so this package
// stays free of cross-aggregate import cycles. Values match the OpenAPI
// `AllocationReason` enum.
type AllocationReason string

const (
	ReasonManual       AllocationReason = "manual"
	ReasonOnEnrollment AllocationReason = "on_enrollment"
	ReasonEqualSplit   AllocationReason = "equal_split"
	ReasonTierBased    AllocationReason = "tier_based"
	ReasonPromo        AllocationReason = "promo"
)

// AllocationIntent describes a single allocation the engine wants to issue.
// The caller materialises it into a TenantManaAllocation aggregate row +
// pool debit + Pub/Sub event in a single atomic step.
type AllocationIntent struct {
	TenantID     string
	GCID         string
	SourcePoolID string
	Units        int64
	Reason       AllocationReason
}

// PoolStore is the repository port used by the engine. The HTTP layer
// supplies an in-memory or Postgres-backed implementation; the engine
// itself does not care.
type PoolStore interface {
	GetByTenant(ctx context.Context, tenantID string) (*TenantManaPool, error)
	Save(ctx context.Context, p *TenantManaPool) error
}

// LearnerCounter is the port used to look up active learner state for the
// EqualSplit monthly tick. Implementations cannot cross databases (cross-DB
// queries forbidden) — the chora-tenancy service maintains its own
// `members` table populated from chora.identity.user_membership.* events.
type LearnerCounter interface {
	CountActiveLearners(ctx context.Context, tenantID string) (int, error)
	ListActiveLearners(ctx context.Context, tenantID string) ([]LearnerContext, error)
}

// PolicyEngine wires the dependencies together.
type PolicyEngine struct {
	store   PoolStore
	counter LearnerCounter
}

// NewPolicyEngine constructs a PolicyEngine. `counter` may be nil for
// engines that only ever serve OnLearnerEnrolled (e.g., a unit-test path);
// OnMonthlyTick will then return an error if invoked.
func NewPolicyEngine(store PoolStore, counter LearnerCounter) *PolicyEngine {
	return &PolicyEngine{store: store, counter: counter}
}

// OnLearnerEnrolled consults the tenant's pool policy and returns 0 or 1
// intent. Pool absence + insufficient balance + manual policy all reduce
// to "no intent" (no error). Returns an error only on infrastructure
// failure inside the store.
func (e *PolicyEngine) OnLearnerEnrolled(ctx context.Context, tenantID, gcid string, lctx LearnerContext) ([]AllocationIntent, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(gcid) == "" {
		return nil, nil
	}
	p, err := e.store.GetByTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, ErrPoolNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if p.IsDeleted() {
		return nil, nil
	}
	if lctx.GCID == "" {
		lctx.GCID = gcid
	}
	units, ok := p.AutoAllocationPolicy.UnitsForLearner(lctx)
	if !ok || units <= 0 {
		return nil, nil
	}
	if p.BalanceUnits < units {
		// Insufficient pool — silent no-op (the chora.tenancy.tenant_mana_pool.depleted.v1
		// event already drives admin notifications elsewhere).
		return nil, nil
	}
	reason := allocationReasonFor(p.AutoAllocationPolicy.Kind())
	return []AllocationIntent{{
		TenantID:     tenantID,
		GCID:         gcid,
		SourcePoolID: p.PoolID,
		Units:        units,
		Reason:       reason,
	}}, nil
}

// OnMonthlyTick fires the EqualSplit distribution. For other policy kinds
// returns no intents. Caller is responsible for ensuring the tick fires
// at-most-once per tenant per calendar month (idempotency lives at the
// scheduler / event-bus level).
func (e *PolicyEngine) OnMonthlyTick(ctx context.Context, tenantID string) ([]AllocationIntent, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, nil
	}
	p, err := e.store.GetByTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, ErrPoolNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if p.IsDeleted() {
		return nil, nil
	}
	split, ok := p.AutoAllocationPolicy.(EqualSplitPolicy)
	if !ok {
		return nil, nil
	}
	if p.MonthlyTopupUnits <= 0 {
		return nil, nil
	}
	if e.counter == nil {
		return nil, errors.New("monthly tick requires a LearnerCounter")
	}
	learners, err := e.counter.ListActiveLearners(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if len(learners) == 0 {
		return nil, nil
	}
	per := split.ComputeMonthlyUnits(p.MonthlyTopupUnits, len(learners))
	if per <= 0 {
		return nil, nil
	}
	out := make([]AllocationIntent, 0, len(learners))
	remaining := p.BalanceUnits
	for _, l := range learners {
		if remaining < per {
			break // pool exhausted mid-distribution; stop.
		}
		out = append(out, AllocationIntent{
			TenantID:     tenantID,
			GCID:         l.GCID,
			SourcePoolID: p.PoolID,
			Units:        per,
			Reason:       ReasonEqualSplit,
		})
		remaining -= per
	}
	return out, nil
}

// RecordClaim is invoked by the chora.identity.user_mana.debited.v1 subscriber
// inside chora-tenancy when the FIRST debit hits an allocation. The caller
// loads the allocation, calls Claim on it, persists, and republishes
// chora.tenancy.tenant_mana_allocation.claimed.v1. The engine itself
// performs no work beyond input validation — this method exists so the
// adapter has a typed entry point for symmetry with the other engine
// methods and so future cross-aggregate logic (e.g., spend-order replay)
// has a natural home.
func (e *PolicyEngine) RecordClaim(_ context.Context, allocationID string, unitsClaimed int64) error {
	if strings.TrimSpace(allocationID) == "" {
		return errors.New("allocation_id required")
	}
	if unitsClaimed <= 0 {
		return errors.New("units_claimed must be > 0")
	}
	return nil
}

func allocationReasonFor(k PolicyKind) AllocationReason {
	switch k {
	case PolicyKindOnEnrollment:
		return ReasonOnEnrollment
	case PolicyKindEqualSplit:
		return ReasonEqualSplit
	case PolicyKindTierBased:
		return ReasonTierBased
	default:
		return ReasonManual
	}
}
