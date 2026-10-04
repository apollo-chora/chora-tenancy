// Package pubsub is the Iter G.3 outbox-publisher facade for the 4 new
// chora.tenancy.familiar_egg.* event topics (ADR-149).
//
// Topics (per chora-contracts/proto/events/tenancy/familiar_egg.proto):
//
//   - chora.tenancy.familiar_egg.checkout_started.v1
//   - chora.tenancy.familiar_egg.payment_succeeded.v1
//   - chora.tenancy.familiar_egg.payment_failed.v1
//   - chora.tenancy.familiar_egg.refunded.v1
//
// The facade wraps the canonical OutboxPublisher (per
// `services/chora-tenancy/internal/adapter/outbox/publisher.go`) and emits
// payload shapes that match the proto messages. JSON serialisation of the
// payload map matches the Protobuf field names so subscribers see a
// schema-aligned event regardless of wire format.
package pubsub

import (
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

// Canonical topic names.
const (
	TopicCheckoutStarted   = "chora.tenancy.familiar_egg.checkout_started.v1"
	TopicPaymentSucceeded  = "chora.tenancy.familiar_egg.payment_succeeded.v1"
	TopicPaymentFailed     = "chora.tenancy.familiar_egg.payment_failed.v1"
	TopicRefunded          = "chora.tenancy.familiar_egg.refunded.v1"
)

// EggPublisher is the port the HTTP + webhook handlers use to emit
// FamiliarEgg events. Production wires the OutboxPublisher; tests wire
// the in-process Recorder.
type EggPublisher interface {
	Publish(topic string, h events.Header, payload map[string]interface{}) events.PublishedEvent
	PublishWithError(topic string, h events.Header, payload map[string]interface{}) (events.PublishedEvent, error)
}

// EmitCheckoutStarted publishes the checkout_started.v1 event.
func EmitCheckoutStarted(p EggPublisher, h events.Header, purchase *familiareag.Purchase) (events.PublishedEvent, error) {
	payload := map[string]interface{}{
		"purchase_id":             purchase.PurchaseID,
		"purchaser_gcid":          purchase.PurchaserGCID,
		"target_tenant_id":        purchase.TenantID,
		"egg_sku":                 purchase.EggSKU,
		"stripe_session_id":       purchase.StripeSessionID,
		"stripe_checkout_url":     purchase.StripeCheckoutURL,
		"amount_cents":            purchase.AmountCents,
		"currency":                purchase.Currency,
		"suggested_focal_atom_id": purchase.SuggestedFocalAtomID,
		"checkout_started_at":     purchase.CheckoutStartedAt.UTC().Format(time.RFC3339Nano),
		// IMDA D1 evidence — accountability of the purchase event.
		"chora_imda_dimension": "accountability",
	}
	return p.PublishWithError(TopicCheckoutStarted, h, payload)
}

// EmitPaymentSucceeded publishes the payment_succeeded.v1 event.
func EmitPaymentSucceeded(p EggPublisher, h events.Header, purchase *familiareag.Purchase) (events.PublishedEvent, error) {
	var paidAt string
	if purchase.PaidAt != nil {
		paidAt = purchase.PaidAt.UTC().Format(time.RFC3339Nano)
	}
	payload := map[string]interface{}{
		"purchase_id":              purchase.PurchaseID,
		"purchaser_gcid":           purchase.PurchaserGCID,
		"target_tenant_id":         purchase.TenantID,
		"egg_sku":                  purchase.EggSKU,
		"stripe_session_id":        purchase.StripeSessionID,
		"stripe_payment_intent_id": purchase.StripePaymentIntentID,
		"stripe_charge_id":         purchase.StripeChargeID,
		"amount_cents_paid":        purchase.AmountCentsPaid,
		"amount_cents_refunded":    purchase.AmountCentsRefunded,
		"currency":                 purchase.Currency,
		"suggested_focal_atom_id":  purchase.SuggestedFocalAtomID,
		"paid_at":                  paidAt,
		"chora_imda_dimension":     "accountability",
	}
	return p.PublishWithError(TopicPaymentSucceeded, h, payload)
}

// EmitPaymentFailed publishes the payment_failed.v1 event.
func EmitPaymentFailed(p EggPublisher, h events.Header, purchase *familiareag.Purchase) (events.PublishedEvent, error) {
	var failedAt string
	if purchase.FailedAt != nil {
		failedAt = purchase.FailedAt.UTC().Format(time.RFC3339Nano)
	}
	payload := map[string]interface{}{
		"purchase_id":            purchase.PurchaseID,
		"purchaser_gcid":         purchase.PurchaserGCID,
		"egg_sku":                purchase.EggSKU,
		"stripe_session_id":      purchase.StripeSessionID,
		"stripe_failure_code":    purchase.StripeFailureCode,
		"stripe_failure_message": purchase.StripeFailureMessage,
		"failed_at":              failedAt,
		"chora_imda_dimension":   "accountability",
	}
	return p.PublishWithError(TopicPaymentFailed, h, payload)
}

// EmitRefunded publishes the refunded.v1 event.
func EmitRefunded(p EggPublisher, h events.Header, purchase *familiareag.Purchase) (events.PublishedEvent, error) {
	var refundedAt string
	switch {
	case purchase.RefundedAt != nil:
		refundedAt = purchase.RefundedAt.UTC().Format(time.RFC3339Nano)
	case purchase.ExpiredAt != nil:
		refundedAt = purchase.ExpiredAt.UTC().Format(time.RFC3339Nano)
	}
	payload := map[string]interface{}{
		"purchase_id":           purchase.PurchaseID,
		"purchaser_gcid":        purchase.PurchaserGCID,
		"egg_sku":               purchase.EggSKU,
		"stripe_charge_id":      purchase.StripeChargeID,
		"stripe_refund_id":      purchase.StripeRefundID,
		"amount_cents_refunded": purchase.AmountCentsRefunded,
		"currency":              purchase.Currency,
		"reason":                purchase.RefundReason,
		"credit_only":           purchase.RefundCreditOnly,
		"refunded_at":           refundedAt,
		"chora_imda_dimension":  "accountability",
	}
	return p.PublishWithError(TopicRefunded, h, payload)
}
