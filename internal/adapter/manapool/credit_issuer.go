// credit_issuer.go — ADR-149 §"Unhatched egg expiry" refund issuer.
//
// CreditIssuer satisfies familiar_egg_sweeper.ManaCreditIssuer. When the
// sweeper detects a 60-day unhatched egg, it transitions the purchase row
// to `expired` and calls IssueRefundCredit to grant the purchaser a mana
// credit equivalent to the egg's amountCents.
//
// Idempotency: the AllocationID is derived deterministically from the
// purchase id (`expiry-<purchase_id>`). A Get-before-Save guard prevents
// double-publish on re-runs (sweeper retry, dlq replay, manual rerun).
//
// Publishes `chora.tenancy.tenant_mana_allocation.granted.v1` via the
// canonical manapool.Publisher. chora-identity (the wallet owner)
// subscribes and credits the user's UserMana wallet.
//
// TODO(iter_g.7): The amount-cents → mana-units conversion is currently
// a 1:1 placeholder. Iter G.7 will plumb the canonical ADR-142 ratio
// (e.g., $9.99 standard tier = N units/month) via a CreditConverter port.
package manapool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
)

// AllocationGetter is the read port the issuer uses to check whether a
// refund has already been issued for a given purchase (idempotency).
//
// Two lookup paths are supported. Production (pg-backed) prefers
// GetByIdempotencyKey because the 0009 schema enforces uniqueness on
// the idempotency_key column. Get(allocationID) is retained for
// in-memory test/dev paths where allocations are keyed by deterministic
// id rather than a separate idempotency_key column.
type AllocationGetter interface {
	Get(ctx context.Context, allocationID string) (*allocation.TenantManaAllocation, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*allocation.TenantManaAllocation, error)
}

// AllocationSaver is the write port the issuer uses to persist a new
// refund-credit allocation row.
type AllocationSaver interface {
	Save(ctx context.Context, a *allocation.TenantManaAllocation) error
}

// AllocationGrantedPublisher is the publish port the issuer uses to emit
// chora.tenancy.tenant_mana_allocation.granted.v1.
type AllocationGrantedPublisher interface {
	PublishAllocationGranted(EnvelopeHeader, allocation.AllocationGranted)
}

// CreditIssuer satisfies familiar_egg_sweeper.ManaCreditIssuer with real
// pg-backed (or in-memory) persistence + Pub/Sub publish.
type CreditIssuer struct {
	repoR     AllocationGetter
	repoW     AllocationSaver
	publisher AllocationGrantedPublisher
	now       func() time.Time

	// platformGCID identifies the platform actor that "allocated" the
	// refund. Used as AllocatedByGCID on the audit row.
	platformGCID string
}

// CreditIssuerConfig is the constructor input.
type CreditIssuerConfig struct {
	// Repo provides both read (Get / GetByIdempotencyKey) + write (Save).
	// REQUIRED. Production wires PgAllocationRepo (chora_tenancy DB; 0009
	// schema); tests wire InmemAllocationRepo. A single concrete value
	// satisfies both small interfaces.
	Repo interface {
		AllocationGetter
		AllocationSaver
	}
	// Publisher emits the granted event. REQUIRED.
	Publisher AllocationGrantedPublisher
	// Now is a clock fn for tests. Defaults to time.Now().UTC.
	Now func() time.Time
	// PlatformGCID stamped as AllocatedByGCID. Defaults to "platform".
	PlatformGCID string
}

// NewCreditIssuer constructs an issuer. Returns error on missing deps.
func NewCreditIssuer(cfg CreditIssuerConfig) (*CreditIssuer, error) {
	if cfg.Repo == nil {
		return nil, errors.New("credit_issuer: Repo required")
	}
	if cfg.Publisher == nil {
		return nil, errors.New("credit_issuer: Publisher required")
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	plat := strings.TrimSpace(cfg.PlatformGCID)
	if plat == "" {
		plat = "platform"
	}
	return &CreditIssuer{
		repoR:        cfg.Repo,
		repoW:        cfg.Repo,
		publisher:    cfg.Publisher,
		now:          now,
		platformGCID: plat,
	}, nil
}

// IssueRefundCredit satisfies familiar_egg_sweeper.ManaCreditIssuer.
//
// Idempotent: re-running with the same purchaseID is a no-op — the
// Get-before-Save guard short-circuits before the second publish.
//
// Currency: amountCents is mapped 1:1 to mana Units (placeholder per
// Iter G.7 TODO). The granted event payload carries the unit count;
// downstream wallet credit uses the same value verbatim.
func (i *CreditIssuer) IssueRefundCredit(
	ctx context.Context,
	tenantID, gcid string,
	amountCents int64,
	purchaseID string,
) error {
	tenantID = strings.TrimSpace(tenantID)
	gcid = strings.TrimSpace(gcid)
	purchaseID = strings.TrimSpace(purchaseID)

	if tenantID == "" {
		return errors.New("credit_issuer: tenantID required")
	}
	if gcid == "" {
		return errors.New("credit_issuer: gcid required")
	}
	if purchaseID == "" {
		return errors.New("credit_issuer: purchaseID required (used as idempotency key)")
	}
	if amountCents <= 0 {
		return fmt.Errorf("credit_issuer: amountCents must be > 0; got %d", amountCents)
	}

	idemKey := DeriveEggRefundIdempotencyKey(purchaseID)

	// Idempotency check — if an allocation with this key already exists,
	// this is a re-run and we MUST NOT double-publish. Pg-backed repos
	// also enforce the same invariant at the DB layer via the partial
	// UNIQUE INDEX from migration 0009 (`uidx_tenant_mana_alloc_idempotency`).
	if existing, err := i.repoR.GetByIdempotencyKey(ctx, idemKey); err == nil && existing != nil {
		// Already issued; no-op. Return nil so the sweeper continues.
		return nil
	}

	now := i.now()
	row := &allocation.TenantManaAllocation{
		AllocationID:    allocation.NewUUIDv7(),
		TenantID:        tenantID,
		GCID:            gcid,
		Units:           amountCents, // Iter G.8: replace with proper ADR-142 conversion
		SourcePoolID:    "",          // platform-side refund — no tenant pool sourced; 0009 schema allows NULL for this reason
		Status:          allocation.StatusPending,
		Reason:          allocation.ReasonEggHardExpiryRefund,
		AllocatedByGCID: i.platformGCID,
		AllocatedAt:     now,
		UpdatedAt:       now,
		IdempotencyKey:  idemKey,
	}
	if err := i.repoW.Save(ctx, row); err != nil {
		return fmt.Errorf("credit_issuer: save allocation %s (idem=%s): %w", row.AllocationID, idemKey, err)
	}

	i.publisher.PublishAllocationGranted(
		EnvelopeHeader{TenantID: tenantID, GCID: gcid},
		allocation.AllocationGranted{
			AllocationID:    row.AllocationID,
			TenantID:        row.TenantID,
			GCID:            row.GCID,
			SourcePoolID:    row.SourcePoolID,
			Units:           row.Units,
			Reason:          row.Reason,
			AllocatedByGCID: row.AllocatedByGCID,
			AllocatedAt:     row.AllocatedAt,
		},
	)
	return nil
}

// DeriveEggRefundIdempotencyKey returns the deterministic idempotency
// key used for egg-expiry refunds. Exposed so tests and the sweeper can
// assert key construction without re-implementing the rule.
//
// The "expiry-" prefix stands out in queries + logs and distinguishes
// these from other reason families that may use the same column in
// future. The key is stored in `tenant_mana_allocations.idempotency_key`
// (per migration 0009) and used as the partial-UNIQUE-INDEX de-dup hint.
func DeriveEggRefundIdempotencyKey(purchaseID string) string {
	return "expiry-" + strings.TrimSpace(purchaseID)
}

// DeriveEggRefundAllocationID is retained for back-compat with the
// pre-0009 in-memory deterministic-ID path. Now delegates to the
// idempotency-key form. Deprecated: prefer DeriveEggRefundIdempotencyKey.
//
// Deprecated: use DeriveEggRefundIdempotencyKey.
func DeriveEggRefundAllocationID(purchaseID string) string {
	return DeriveEggRefundIdempotencyKey(purchaseID)
}
