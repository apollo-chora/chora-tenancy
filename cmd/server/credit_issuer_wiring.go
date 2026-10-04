// credit_issuer_wiring.go — wires the real familiar-egg refund credit
// issuer (replacing the Iter G.3 NoopManaCreditIssuer).
//
// Adapts the canonical outbox publisher to the manapool.Publisher surface
// the CreditIssuer expects. Egg refund events flow through the same
// outbox → dispatcher → Pub/Sub pipeline as every other tenancy event,
// preserving end-to-end ordering + DLQ + retry behaviour.
//
// Scope note: this Iter G.5 wiring uses an in-memory allocation repo
// because no pg AllocationRepo exists yet for chora-tenancy. Iter G.7
// will promote the repo to pgx-backed for durability.
package main

import (
	"context"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareggsweeper "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiar_egg_sweeper"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
	allocation "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_allocation"
)

// allocationGrantedOutboxAdapter satisfies manapool.AllocationGrantedPublisher
// by delegating to the canonical outbox publisher (which writes to
// outbox_events for the dispatcher to drain to Pub/Sub).
type allocationGrantedOutboxAdapter struct {
	inner *tenancyoutbox.Publisher
}

func (a allocationGrantedOutboxAdapter) PublishAllocationGranted(hdr manapool.EnvelopeHeader, ev allocation.AllocationGranted) {
	payload := map[string]interface{}{
		"allocation_id":     ev.AllocationID,
		"tenant_id":         ev.TenantID,
		"gcid":              ev.GCID,
		"source_pool_id":    ev.SourcePoolID,
		"units":             ev.Units,
		"reason":            string(ev.Reason),
		"allocated_by_gcid": ev.AllocatedByGCID,
		"allocated_at":      ev.AllocatedAt.UTC().Format(time.RFC3339),
	}
	if ev.ExpiresAt != nil {
		payload["expires_at"] = ev.ExpiresAt.UTC().Format(time.RFC3339)
	}
	a.inner.Publish(
		"chora.tenancy.tenant_mana_allocation.granted.v1",
		events.Header{TenantID: hdr.TenantID, GCID: hdr.GCID, Traceparent: hdr.Traceparent},
		payload,
	)
}

// newFamiliarEggCreditIssuer builds the real CreditIssuer. Returns the
// familiar-egg-sweeper port type so callers can pass it directly to
// familiareggsweeper.Config.ManaCredits.
//
// allocRepo can be the manapool in-memory repo (Iter G.5 default) or a
// future pg-backed AllocationRepo (Iter G.7+). The CreditIssuer treats
// it as an opaque Get+Save port.
func newFamiliarEggCreditIssuer(
	allocRepo interface {
		manapool.AllocationGetter
		manapool.AllocationSaver
	},
	outboxPublisher *tenancyoutbox.Publisher,
) (familiareggsweeper.ManaCreditIssuer, error) {
	if outboxPublisher == nil {
		return nil, errCreditIssuerWiringNoPublisher
	}
	pub := allocationGrantedOutboxAdapter{inner: outboxPublisher}
	issuer, err := manapool.NewCreditIssuer(manapool.CreditIssuerConfig{
		Repo:      allocRepo,
		Publisher: pub,
	})
	if err != nil {
		return nil, err
	}
	return creditIssuerAdapter{inner: issuer}, nil
}

// creditIssuerAdapter satisfies familiareggsweeper.ManaCreditIssuer by
// delegating to manapool.CreditIssuer. Signature is identical; the only
// reason for the adapter is package separation.
type creditIssuerAdapter struct {
	inner *manapool.CreditIssuer
}

func (a creditIssuerAdapter) IssueRefundCredit(ctx context.Context, tenantID, gcid string, amountCents int64, purchaseID string) error {
	return a.inner.IssueRefundCredit(ctx, tenantID, gcid, amountCents, purchaseID)
}

// errCreditIssuerWiringNoPublisher is returned when the outbox publisher
// is nil. Kept as a typed sentinel so the wiring caller can fall back to
// the no-op issuer in dev without hard-failing the binary.
var errCreditIssuerWiringNoPublisher = staticErr("credit issuer wiring: outbox publisher required")

type staticErr string

func (e staticErr) Error() string { return string(e) }
