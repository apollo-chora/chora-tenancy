// Package external_egress holds the TenantExternalEgressPolicy aggregate —
// the SOURCE OF TRUTH for whether a tenant's learners may reach the open web
// through the model-gateway's grounded-search egress (ADR-220 D4, ADR-231 D6;
// governance model resolved in PLAN.md §4.2.4).
//
// chora-tenancy owns this entitlement because it is deterministic tenant
// administration, not AI-runtime state. chora-observability holds only a
// read-projection of it (`external_egress_policy`), fed by the
// `chora.tenancy.external_egress_policy.updated.v1` event — cross-DB writes are
// forbidden, so that projection is the ONLY bridge. The model-gateway consults
// the projection fail-closed on every grounded call.
//
// The safety posture is default-deny: a tenant with no row is OFF. Franchise
// tenants (schools, minors) therefore start OFF with no backfill required —
// absence IS the denial.
package external_egress

import (
	"errors"
	"time"
)

const (
	// DefaultDailyCallCeiling is the per-tenant/day grounded-call budget a
	// tenant gets when it opts in without naming one. Mirrors the default on
	// the chora_observability read-copy so an opt-in with no explicit ceiling
	// projects to the same number the gateway already assumes.
	DefaultDailyCallCeiling int32 = 50

	// MaxDailyCallCeiling caps what a tenant admin may self-serve. Every
	// grounded call is a priced external-egress action (ADR-231 D6), so an
	// unbounded ceiling is a cost hole a tenant could open on itself. Raising
	// a tenant above this is a platform-operator decision, not self-service.
	MaxDailyCallCeiling int32 = 1000
)

var (
	// ErrEmptyTenantID — the policy has no tenant identity.
	ErrEmptyTenantID = errors.New("external_egress: tenant_id is required")

	// ErrEmptyActor — a change arrived without an actor GCID. External web
	// egress is governance-sensitive: an unattributed change is refused so the
	// audit trail can always answer "who turned this on" (IMDA D1).
	ErrEmptyActor = errors.New("external_egress: updated_by_gcid is required — an egress change must name its actor")

	// ErrCeilingOutOfRange — the daily ceiling is negative or above the
	// platform hard max.
	ErrCeilingOutOfRange = errors.New("external_egress: daily_call_ceiling out of range")
)

// Policy is the per-tenant external-egress entitlement. One row per tenant;
// tenant_id is the identity (no surrogate key).
type Policy struct {
	TenantID         string
	EgressEnabled    bool
	DailyCallCeiling int32

	// Version is monotonic and increments ONLY on a real change. It is the
	// stale-event guard the chora-observability projection uses to reject an
	// out-of-order Pub/Sub delivery, so it must never move backwards and never
	// move without a corresponding change. Version 0 means "never persisted".
	Version int64

	// UpdatedByGCID is the admin who made the most recent change.
	UpdatedByGCID string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// DefaultPolicy returns the fail-closed default for a tenant that has never
// opted in. It is NOT a persisted row and must never be written back as one on
// a read — absence is the denial (ADR-220 D4). Version 0 marks it unpersisted.
func DefaultPolicy(tenantID string) Policy {
	return Policy{
		TenantID:         tenantID,
		EgressEnabled:    false,
		DailyCallCeiling: DefaultDailyCallCeiling,
		Version:          0,
	}
}

// Persisted reports whether this policy has ever been written. A default
// (never-opted-in) policy is not persisted; the admin read path uses this to
// render "not opted in" rather than fabricating a row.
func (p *Policy) Persisted() bool { return p.Version > 0 }

// Update applies an admin change and reports whether anything ACTUALLY changed.
//
// An identical write is a no-op: no version bump, no event, no audit row — so
// the audit trail records real decisions only, and a client retry cannot inflate
// the version the projection guards on.
func (p *Policy) Update(enabled bool, ceiling int32, actorGCID string, now time.Time) (changed bool, err error) {
	if p.TenantID == "" {
		return false, ErrEmptyTenantID
	}
	if actorGCID == "" {
		return false, ErrEmptyActor
	}
	if ceiling < 0 || ceiling > MaxDailyCallCeiling {
		return false, ErrCeilingOutOfRange
	}

	if p.Persisted() && p.EgressEnabled == enabled && p.DailyCallCeiling == ceiling {
		return false, nil
	}

	if !p.Persisted() {
		p.CreatedAt = now
	}
	p.EgressEnabled = enabled
	p.DailyCallCeiling = ceiling
	p.UpdatedByGCID = actorGCID
	p.UpdatedAt = now
	p.Version++

	return true, nil
}
