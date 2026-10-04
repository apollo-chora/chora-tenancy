// Package pool — AllocationPolicy value object + variants.
//
// Per ADR-142 and the OpenAPI/proto contracts there are 4 policy variants
// driving the auto-allocation engine:
//
//   - ManualPolicy{}            — admin allocates one-at-a-time per learner.
//     No auto-allocation events fire.
//   - OnEnrollmentPolicy{N}     — N units granted when a learner enrols in a
//     tenant course.
//   - EqualSplitPolicy{}        — pool's monthly drip distributed equally
//     across active learners (computed at the
//     month boundary by OnMonthlyTick).
//   - TierBasedPolicy{tier→N}   — different allocation amounts per learner
//     role (lookup by GCID's role at enrollment).
//
// PolicyConfig is the API config block (matches the OpenAPI
// AllocationPolicyConfig schema). PolicyFromConfig materialises a typed
// AllocationPolicy from the wire config.
package pool

import (
	"fmt"
	"strings"
)

// PolicyKind discriminates among the 4 variants.
type PolicyKind string

const (
	PolicyKindManual       PolicyKind = "manual"
	PolicyKindOnEnrollment PolicyKind = "on_enrollment"
	PolicyKindEqualSplit   PolicyKind = "equal_split"
	PolicyKindTierBased    PolicyKind = "tier_based"
)

// LearnerContext is the input passed to UnitsForLearner. Captures the data
// the policy consults to decide whether/how much to allocate.
type LearnerContext struct {
	GCID string
	Role string // e.g., "power_user", "standard", "auditor"
}

// AllocationPolicy is the strategy interface. Implementations are immutable
// value objects.
type AllocationPolicy interface {
	Kind() PolicyKind

	// UnitsForLearner returns (units, ok) for an enrollment-time decision.
	// `ok=false` signals "no auto-allocation under this policy/context".
	UnitsForLearner(LearnerContext) (int64, bool)
}

// -----------------------------------------------------------------------------
// ManualPolicy
// -----------------------------------------------------------------------------

// ManualPolicy fires no auto-allocations.
type ManualPolicy struct{}

func (ManualPolicy) Kind() PolicyKind                             { return PolicyKindManual }
func (ManualPolicy) UnitsForLearner(LearnerContext) (int64, bool) { return 0, false }

// -----------------------------------------------------------------------------
// OnEnrollmentPolicy
// -----------------------------------------------------------------------------

// OnEnrollmentPolicy grants `DefaultUnits` to every learner on enrolment.
type OnEnrollmentPolicy struct {
	DefaultUnits int64
}

func (OnEnrollmentPolicy) Kind() PolicyKind { return PolicyKindOnEnrollment }

func (p OnEnrollmentPolicy) UnitsForLearner(LearnerContext) (int64, bool) {
	if p.DefaultUnits <= 0 {
		return 0, false
	}
	return p.DefaultUnits, true
}

// -----------------------------------------------------------------------------
// EqualSplitPolicy
// -----------------------------------------------------------------------------

// EqualSplitPolicy distributes the pool's monthly_topup_units equally across
// the active learner population at the monthly tick. Does NOT fire on
// enrollment.
type EqualSplitPolicy struct{}

func (EqualSplitPolicy) Kind() PolicyKind                             { return PolicyKindEqualSplit }
func (EqualSplitPolicy) UnitsForLearner(LearnerContext) (int64, bool) { return 0, false }

// ComputeMonthlyUnits returns the per-learner allotment when the monthly
// tick fires. Floor division — leftover pennies stay in the pool.
func (EqualSplitPolicy) ComputeMonthlyUnits(monthlyPool int64, activeLearners int) int64 {
	if monthlyPool <= 0 || activeLearners <= 0 {
		return 0
	}
	return monthlyPool / int64(activeLearners)
}

// -----------------------------------------------------------------------------
// TierBasedPolicy
// -----------------------------------------------------------------------------

// TierBasedPolicy maps learner role → units; learners outside the map get no
// auto-allocation.
type TierBasedPolicy struct {
	TierToUnits map[string]int64
}

func (TierBasedPolicy) Kind() PolicyKind { return PolicyKindTierBased }

func (p TierBasedPolicy) UnitsForLearner(ctx LearnerContext) (int64, bool) {
	if p.TierToUnits == nil {
		return 0, false
	}
	units, ok := p.TierToUnits[ctx.Role]
	if !ok || units <= 0 {
		return 0, false
	}
	return units, true
}

// -----------------------------------------------------------------------------
// PolicyConfig — wire-format config (OpenAPI AllocationPolicyConfig)
// -----------------------------------------------------------------------------

// PolicyConfig matches the OpenAPI `AllocationPolicyConfig` schema. Used by
// the HTTP layer to ship a config block from `PATCH .../mana-pool/policy`
// into the domain via PolicyFromConfig.
type PolicyConfig struct {
	Policy            string
	UnitsOnEnrollment int64
	TierAmounts       map[string]int64
	AllocationTTLDays int
}

// PolicyFromConfig materialises a typed AllocationPolicy from the wire
// config. Returns ErrInvalidArgument for unknown policy kinds or invalid
// per-policy parameters.
func PolicyFromConfig(c PolicyConfig) (AllocationPolicy, error) {
	switch strings.TrimSpace(c.Policy) {
	case string(PolicyKindManual):
		return ManualPolicy{}, nil
	case string(PolicyKindOnEnrollment):
		if c.UnitsOnEnrollment < 0 {
			return nil, fmt.Errorf("%w: units_on_enrollment must be >= 0", ErrInvalidArgument)
		}
		return OnEnrollmentPolicy{DefaultUnits: c.UnitsOnEnrollment}, nil
	case string(PolicyKindEqualSplit):
		return EqualSplitPolicy{}, nil
	case string(PolicyKindTierBased):
		for role, units := range c.TierAmounts {
			if units < 0 {
				return nil, fmt.Errorf("%w: tier_amounts[%q] must be >= 0", ErrInvalidArgument, role)
			}
		}
		return TierBasedPolicy{TierToUnits: c.TierAmounts}, nil
	default:
		return nil, fmt.Errorf("%w: unknown policy %q", ErrInvalidArgument, c.Policy)
	}
}
