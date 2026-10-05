// Package pool implements the TenantManaPool aggregate for the
// chora-tenancy service. Per ADR-142 (Q2 = v1, tenant subsidy track) the
// pool is a tenant-funded prepaid mana balance used to subsidise per-learner
// Familiar action costs (corporate L&D / cohort sponsorship use case).
//
// Hexagonal: this package is the pure domain core for the mana-pool +
// allocation feature set. It has NO dependencies on HTTP, persistence,
// Stripe, the event bus, or any other infrastructure. All such concerns live in
// adapter packages (`internal/adapter/...`) that import this domain.
//
// Aggregate invariants (per BE-USR-3 brief):
//
//   - One TenantManaPool per tenant_id (idempotent creation).
//   - balance_units >= 0; debit overdraw is rejected with ErrInsufficientBalance.
//   - Optimistic concurrency via Version (caller bumps + checks on persist).
//   - Lifetime counters are monotonic on net activity (top-up adds; revoke
//     credits-back reduces lifetime_allocated).
//   - Soft delete via DeletedAt — never hard-delete (per ddd-enforcement).
//   - UUIDv7 IDs (sortable + creation-order equivalent).
//
// Source-of-truth references:
//   - chora-contracts/proto/events/tenancy/tenant_mana_pool.proto
//   - chora-contracts/openapi/tenancy-admin.yaml v1.2 (mana-pool endpoints)
//   - docs/architecture/adrs/adr-142-per-user-economy-mana.md
package pool

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
	// ErrInvalidArgument signals a guard-clause failure in a constructor or
	// mutator (empty required field, non-positive amount, etc.).
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrInsufficientBalance is returned when Debit would drive the pool
	// balance below zero.
	ErrInsufficientBalance = errors.New("insufficient pool balance")

	// ErrPoolNotFound is returned by repository ports when a tenant has no
	// pool. The PolicyEngine treats this as a no-op (auto-allocation cannot
	// fire without a pool).
	ErrPoolNotFound = errors.New("tenant mana pool not found")

	// ErrPoolAlreadyExists is returned by repository ports on duplicate
	// pool creation for a tenant. The HTTP layer maps this to 409 (and
	// returns the existing pool).
	ErrPoolAlreadyExists = errors.New("tenant mana pool already exists")
)

// LowBalanceThresholdPct is the default percentage of monthly_topup_units at
// which IsLowBalance flips true. 10% per the proto field comment.
const LowBalanceThresholdPct = 10

// -----------------------------------------------------------------------------
// UUIDv7 — minimal local generator (deps-free)
// -----------------------------------------------------------------------------

// NewUUIDv7 returns a freshly generated RFC 9562 §5.7 UUIDv7 string.
//
// Format: unix_ts_ms (48) || ver (4) || rand_a (12) || var (2) || rand_b (62).
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
		// Fallback: pad with time bits to keep the service up under entropy
		// starvation. The result is still version-7 shaped.
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// TenantManaPool aggregate root
// -----------------------------------------------------------------------------

// TenantManaPool is the aggregate root for a tenant's prepaid mana balance.
//
// Invariants (enforced by methods):
//   - BalanceUnits >= 0 (Debit rejects overdraw)
//   - LifetimeToppedUpUnits, LifetimeAllocatedUnits track net activity
//   - Version increments on EVERY mutation for OCC at the persistence layer
//   - UpdatedAt advances strictly forward
type TenantManaPool struct {
	PoolID                 string
	TenantID               string
	BalanceUnits           int64
	MonthlyTopupUnits      int64
	AutoAllocationPolicy   AllocationPolicy
	LifetimeToppedUpUnits  int64
	LifetimeAllocatedUnits int64
	Version                int64
	CreatedByGCID          string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	LastToppedUpAt         *time.Time
	DeletedAt              *time.Time
}

// NewPool constructs a fresh TenantManaPool for `tenantID`, owned by
// `createdByGCID`. The `policy` argument may not be nil — pass
// `ManualPolicy{}` for the no-auto-allocation default.
func NewPool(tenantID, createdByGCID string, policy AllocationPolicy) (*TenantManaPool, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(createdByGCID) == "" {
		return nil, fmt.Errorf("%w: created_by_gcid required", ErrInvalidArgument)
	}
	if policy == nil {
		return nil, fmt.Errorf("%w: policy required (use ManualPolicy{} for default)", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &TenantManaPool{
		PoolID:               NewUUIDv7(),
		TenantID:             strings.TrimSpace(tenantID),
		BalanceUnits:         0,
		MonthlyTopupUnits:    0,
		AutoAllocationPolicy: policy,
		Version:              1,
		CreatedByGCID:        strings.TrimSpace(createdByGCID),
		CreatedAt:            now,
		UpdatedAt:            now,
	}, nil
}

// TopUp credits `units` to the pool balance, recording the Stripe charge for
// reconciliation. `autoRenew=true` indicates this is the monthly auto-renew
// job; in that case `toppedUpByGCID` is empty (system-driven).
func (p *TenantManaPool) TopUp(units, chargedCents int64, currency, stripePaymentIntentID string, autoRenew bool, toppedUpByGCID string) error {
	if units <= 0 {
		return fmt.Errorf("%w: top-up units must be > 0", ErrInvalidArgument)
	}
	if chargedCents <= 0 {
		return fmt.Errorf("%w: charged_cents must be > 0", ErrInvalidArgument)
	}
	if !autoRenew && strings.TrimSpace(toppedUpByGCID) == "" {
		return fmt.Errorf("%w: manual top-ups require topped_up_by_gcid", ErrInvalidArgument)
	}
	if strings.TrimSpace(currency) == "" {
		return fmt.Errorf("%w: currency required", ErrInvalidArgument)
	}
	if strings.TrimSpace(stripePaymentIntentID) == "" {
		return fmt.Errorf("%w: stripe_payment_intent_id required", ErrInvalidArgument)
	}
	now := nowAfter(p.UpdatedAt)
	p.BalanceUnits += units
	p.LifetimeToppedUpUnits += units
	p.Version++
	p.LastToppedUpAt = &now
	p.UpdatedAt = now
	return nil
}

// Debit subtracts `units` from the pool balance. Rejects overdraw with
// ErrInsufficientBalance.
func (p *TenantManaPool) Debit(units int64) error {
	if units <= 0 {
		return fmt.Errorf("%w: debit units must be > 0", ErrInvalidArgument)
	}
	if p.BalanceUnits < units {
		return ErrInsufficientBalance
	}
	now := nowAfter(p.UpdatedAt)
	p.BalanceUnits -= units
	p.LifetimeAllocatedUnits += units
	p.Version++
	p.UpdatedAt = now
	return nil
}

// Credit returns `units` to the pool balance (used on revoke / expire of
// pending allocations). Reduces LifetimeAllocatedUnits to keep the dashboard
// metric reflective of NET allocation.
func (p *TenantManaPool) Credit(units int64) error {
	if units <= 0 {
		return fmt.Errorf("%w: credit units must be > 0", ErrInvalidArgument)
	}
	now := nowAfter(p.UpdatedAt)
	p.BalanceUnits += units
	if p.LifetimeAllocatedUnits >= units {
		p.LifetimeAllocatedUnits -= units
	} else {
		p.LifetimeAllocatedUnits = 0
	}
	p.Version++
	p.UpdatedAt = now
	return nil
}

// SetMonthlyTopupUnits configures the auto-renew amount. Set to 0 to disable
// auto-renew entirely.
func (p *TenantManaPool) SetMonthlyTopupUnits(units int64) error {
	if units < 0 {
		return fmt.Errorf("%w: monthly_topup_units must be >= 0", ErrInvalidArgument)
	}
	now := nowAfter(p.UpdatedAt)
	p.MonthlyTopupUnits = units
	p.Version++
	p.UpdatedAt = now
	return nil
}

// SetPolicy replaces the auto-allocation policy.
func (p *TenantManaPool) SetPolicy(policy AllocationPolicy) error {
	if policy == nil {
		return fmt.Errorf("%w: policy required", ErrInvalidArgument)
	}
	now := nowAfter(p.UpdatedAt)
	p.AutoAllocationPolicy = policy
	p.Version++
	p.UpdatedAt = now
	return nil
}

// LowBalanceThreshold is the configured low-balance trigger value. Default
// is 10% of MonthlyTopupUnits; if monthly_topup is 0, threshold is 0 (no
// low-balance signal — only the fully-depleted signal applies).
func (p *TenantManaPool) LowBalanceThreshold() int64 {
	return (p.MonthlyTopupUnits * LowBalanceThresholdPct) / 100
}

// IsLowBalance returns true when the pool is at or below the low-balance
// threshold AND the threshold is meaningful (>0). At zero balance use
// IsFullyDepleted instead.
func (p *TenantManaPool) IsLowBalance() bool {
	t := p.LowBalanceThreshold()
	return t > 0 && p.BalanceUnits <= t
}

// IsFullyDepleted returns true when the pool balance has reached zero.
func (p *TenantManaPool) IsFullyDepleted() bool {
	return p.BalanceUnits == 0
}

// SoftDelete stamps the DeletedAt timestamp. Idempotent. Never hard-delete
// the row (preserves audit trail per ddd-enforcement).
func (p *TenantManaPool) SoftDelete() {
	if p.DeletedAt != nil {
		return
	}
	now := time.Now().UTC()
	p.DeletedAt = &now
	p.UpdatedAt = now
}

// IsDeleted reports the soft-deleted status.
func (p *TenantManaPool) IsDeleted() bool {
	return p.DeletedAt != nil
}

// nowAfter returns now() guaranteed to be > prev. Required because the test
// suite asserts UpdatedAt strictly advances on consecutive mutations and
// time.Now() can return identical millis under tight loops.
func nowAfter(prev time.Time) time.Time {
	now := time.Now().UTC()
	if !now.After(prev) {
		return prev.Add(time.Microsecond)
	}
	return now
}
