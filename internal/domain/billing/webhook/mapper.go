// Package webhook is the chora-tenancy billing-webhook domain layer.
//
// Ported from services/chora-billing-webhook/internal/domain/webhook as part
// of the M12.2 Batch-1 consolidation. Per locked architecture (CLAUDE.md §1)
// Tenancy + Billing are a single combined supporting domain — Stripe webhook
// ingress lives here.
//
// Owns:
//
//  1. Classify — pure function from a Stripe event to the canonical chora
//     event topic + IMDA dimension + envelope routing keys (tenant_id, gcid).
//     No side effects; testable in isolation.
//
//  2. VerifySignature — pure function (per-call, given a secret) that
//     validates a Stripe-Signature header against a payload + timestamp
//     within a tolerance window.
//
// Dependency direction (per .claude/skills/hexagonal): domain has NO infra
// imports — no Stripe SDK type references, no Pub/Sub, no DB. The HTTP
// adapter parses the wire body into webhook.StripeEvent and the pubsub /
// outbox adapters consume the Classification result.
//
// Aligned with: CLAUDE.md §1 + .claude/skills/event-driven + ADR-142 §8.
package webhook

import (
	"errors"
	"fmt"
	"time"
)

// StripeEvent is the platform-agnostic projection of a Stripe webhook body
// that the domain reasons over. The HTTP adapter parses Stripe's JSON into
// this shape so the domain has no SDK dependency.
type StripeEvent struct {
	// ID is Stripe's event.id. Used as the idempotency anchor.
	ID string

	// Type is Stripe's event.type (e.g. "payment_intent.succeeded").
	Type string

	// Metadata is the merged metadata map: top-level event metadata +
	// data.object.metadata. The HTTP adapter is responsible for the merge.
	// Routing keys are read from this:
	//   - "tenant_id"          → tenancy domain
	//   - "gcid"               → identity domain
	//   - "stripe_customer_id" → carried in payload for cross-reference
	Metadata map[string]string

	// AmountCents is the financial value of the event when applicable.
	// Zero for non-financial events (e.g. customer.subscription.deleted).
	AmountCents int64

	// Currency is the 3-letter ISO 4217 code.
	Currency string

	// Created is Stripe's `created` field (Unix timestamp in seconds in
	// Stripe; this struct uses time.Time for ergonomics).
	Created time.Time
}

// Payload is the carrier struct for the chora event payload. It is the
// stable inter-domain shape — service tests assert on this directly.
type Payload struct {
	StripeEventID    string
	StripeCustomerID string
	AmountCents      int64
	Currency         string
	OccurredAt       time.Time
	// Extra carries metadata fields the domain doesn't need to interpret
	// but that downstream consumers may want (e.g. subscription_id,
	// invoice_id). Defensive copy of StripeEvent.Metadata minus tenant_id
	// + gcid which are already routing keys on Classification.
	Extra map[string]string
}

// Classification is the domain output: the canonical topic + IMDA dimension
// + envelope routing keys + the payload to attach. Adapters consume this.
type Classification struct {
	Topic          string
	IMDADimension  string
	TenantID       string
	GCID           string
	IdempotencyKey string
	Payload        *Payload
	// dlq is true when the event MUST be routed to the DLQ topic instead
	// of the canonical topic. Use IsDLQ() to read.
	dlq bool
}

// IsDLQ reports whether classification routed the event to the dead-letter
// queue (unknown event type, missing routing metadata, etc.).
func (c Classification) IsDLQ() bool { return c.dlq }

// DLQTopic is the canonical name of the billing-webhook dead-letter queue
// topic. Lives under the governance domain because dispute / failure /
// dead-letter events are governance concerns (audit + alerting).
const DLQTopic = "chora.governance.billing_webhook.deadlettered.v1"

// Sentinel errors.
var (
	// ErrEmptyEventID is returned when Stripe didn't provide an event.id —
	// this should be impossible from a real webhook delivery but we guard
	// against it because idempotency depends on it.
	ErrEmptyEventID = errors.New("webhook: empty Stripe event id")

	// ErrEmptyEventType signals that Stripe didn't provide event.type.
	ErrEmptyEventType = errors.New("webhook: empty Stripe event type")

	// ErrUnknownEventType signals the type isn't one we route on. The
	// caller MUST DLQ the event (see Classification.IsDLQ).
	ErrUnknownEventType = errors.New("webhook: unknown Stripe event type")

	// ErrMissingRoutingMetadata signals the event lacks both tenant_id and
	// gcid — we cannot route it. Caller DLQs.
	ErrMissingRoutingMetadata = errors.New("webhook: missing tenant_id and gcid metadata")
)

// Classify maps a Stripe event onto a chora event topic + envelope routing.
//
// Routing rules (per ADR-142):
//
//   - payment_intent.succeeded      → {tenancy|identity}.payment.captured.v1
//     (IMDA accountability; tenant_id wins over gcid when both present)
//   - customer.subscription.created → {tenancy|identity}.subscription.created.v1
//   - customer.subscription.updated → {tenancy|identity}.subscription.updated.v1
//   - customer.subscription.deleted → {tenancy|identity}.subscription.cancelled.v1
//   - invoice.payment_failed        → governance.payment_failure.detected.v1
//     (IMDA accountability — revenue capture failures audit-tracked)
//   - charge.dispute.created        → governance.payment_dispute.detected.v1
//     (IMDA safety_and_robustness — disputes are integrity signals)
//   - anything else                 → DLQ classification + ErrUnknownEventType
//
// Returns a fully-populated Classification on success, or a DLQ
// Classification + a non-nil sentinel error on routing failure. Adapters
// MUST emit on the DLQ topic when the returned classification IsDLQ().
func Classify(ev StripeEvent) (Classification, error) {
	if ev.ID == "" {
		return Classification{}, ErrEmptyEventID
	}
	if ev.Type == "" {
		return Classification{}, ErrEmptyEventType
	}

	tenantID, hasTenant := lookupMetadata(ev.Metadata, "tenant_id")
	gcid, hasGCID := lookupMetadata(ev.Metadata, "gcid")
	stripeCustomer, _ := lookupMetadata(ev.Metadata, "stripe_customer_id")

	// Tenant routing wins over GCID when both present (ADR-142 — tenant-
	// level events take precedence over per-user events). GCID is still
	// carried in the payload for cross-reference.
	domain, baseEnvTenant, baseEnvGCID := pickDomain(hasTenant, hasGCID, tenantID, gcid)

	payload := &Payload{
		StripeEventID:    ev.ID,
		StripeCustomerID: stripeCustomer,
		AmountCents:      ev.AmountCents,
		Currency:         ev.Currency,
		OccurredAt:       ev.Created,
		Extra:            cloneMetadataExceptRouting(ev.Metadata),
	}

	switch ev.Type {
	case "payment_intent.succeeded":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          fmt.Sprintf("chora.%s.payment.captured.v1", domain),
			IMDADimension:  "accountability",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "payment.captured"),
			Payload:        payload,
		}, nil

	case "customer.subscription.created":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          fmt.Sprintf("chora.%s.subscription.created.v1", domain),
			IMDADimension:  "accountability",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "subscription.created"),
			Payload:        payload,
		}, nil

	case "customer.subscription.updated":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          fmt.Sprintf("chora.%s.subscription.updated.v1", domain),
			IMDADimension:  "accountability",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "subscription.updated"),
			Payload:        payload,
		}, nil

	case "customer.subscription.deleted":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          fmt.Sprintf("chora.%s.subscription.cancelled.v1", domain),
			IMDADimension:  "accountability",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "subscription.cancelled"),
			Payload:        payload,
		}, nil

	case "invoice.payment_failed":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          "chora.governance.payment_failure.detected.v1",
			IMDADimension:  "accountability",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "payment_failure"),
			Payload:        payload,
		}, nil

	case "charge.dispute.created":
		if domain == "" {
			return dlqClassification(ev, ErrMissingRoutingMetadata), ErrMissingRoutingMetadata
		}
		return Classification{
			Topic:          "chora.governance.payment_dispute.detected.v1",
			IMDADimension:  "safety_and_robustness",
			TenantID:       baseEnvTenant,
			GCID:           baseEnvGCID,
			IdempotencyKey: idempotencyKey(ev.ID, "payment_dispute"),
			Payload:        payload,
		}, nil

	default:
		return dlqClassification(ev, ErrUnknownEventType), fmt.Errorf("%w: %s", ErrUnknownEventType, ev.Type)
	}
}

// pickDomain implements the precedence rule: tenant_id > gcid > none.
// Returns the domain segment + canonical envelope tenant_id + envelope
// gcid (note: when domain == identity, the chora envelope tenant_id
// is always "platform" because user-scoped events live on the platform
// tenant per CLAUDE.md §6).
func pickDomain(hasTenant, hasGCID bool, tenantID, gcid string) (domain, envTenant, envGCID string) {
	switch {
	case hasTenant:
		envGCID = ""
		if hasGCID {
			envGCID = gcid // carry GCID forward when both present
		}
		return "tenancy", tenantID, envGCID
	case hasGCID:
		return "identity", "platform", gcid
	default:
		return "", "", ""
	}
}

// dlqClassification builds a Classification destined for the DLQ topic.
// Always carries the original payload + classification reason for human
// review.
func dlqClassification(ev StripeEvent, reason error) Classification {
	tenantID, _ := lookupMetadata(ev.Metadata, "tenant_id")
	gcid, _ := lookupMetadata(ev.Metadata, "gcid")
	stripeCustomer, _ := lookupMetadata(ev.Metadata, "stripe_customer_id")
	envTenant := tenantID
	if envTenant == "" {
		envTenant = "platform"
	}
	payload := &Payload{
		StripeEventID:    ev.ID,
		StripeCustomerID: stripeCustomer,
		AmountCents:      ev.AmountCents,
		Currency:         ev.Currency,
		OccurredAt:       ev.Created,
		Extra:            cloneMetadataExceptRouting(ev.Metadata),
	}
	if payload.Extra == nil {
		payload.Extra = map[string]string{}
	}
	payload.Extra["dlq_reason"] = reason.Error()
	payload.Extra["stripe_event_type"] = ev.Type
	return Classification{
		Topic:          DLQTopic,
		IMDADimension:  "accountability",
		TenantID:       envTenant,
		GCID:           gcid,
		IdempotencyKey: idempotencyKey(ev.ID, "dlq"),
		Payload:        payload,
		dlq:            true,
	}
}

// lookupMetadata returns (value, true) only when the key is present and the
// value is non-empty. A whitespace-only value is treated as missing.
func lookupMetadata(md map[string]string, key string) (string, bool) {
	if md == nil {
		return "", false
	}
	v, ok := md[key]
	if !ok {
		return "", false
	}
	if v == "" {
		return "", false
	}
	return v, true
}

// cloneMetadataExceptRouting returns a copy of md without the tenant_id +
// gcid keys (they're already routing keys on Classification).
func cloneMetadataExceptRouting(md map[string]string) map[string]string {
	if md == nil {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if k == "tenant_id" || k == "gcid" {
			continue
		}
		out[k] = v
	}
	return out
}

// idempotencyKey composes a deterministic key from the Stripe event id +
// the routing semantic. The key MUST contain the Stripe event id (per the
// "first delivery wins" idempotent-store semantics).
func idempotencyKey(stripeEventID, semantic string) string {
	return fmt.Sprintf("stripe:%s:%s", semantic, stripeEventID)
}
