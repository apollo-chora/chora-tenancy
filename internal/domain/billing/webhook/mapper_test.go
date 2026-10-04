// Package webhook tests — domain layer mapping Stripe events to Chora events.
// Ported from services/chora-billing-webhook/internal/domain/webhook as part
// of the M12.2 Batch-1 consolidation.
package webhook_test

import (
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/webhook"
)

func TestClassifyStripeEvent_RoutingByMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		stripeEventID  string
		stripeType     string
		metadata       map[string]string
		amountCents    int64
		wantTopic      string
		wantIMDADim    string
		wantTenantID   string
		wantGCID       string
		wantClassError bool
		wantDLQ        bool
	}{
		{
			name:          "payment_intent.succeeded with tenant_id → tenancy.payment.captured.v1",
			stripeEventID: "evt_pay_tenant_1",
			stripeType:    "payment_intent.succeeded",
			metadata:      map[string]string{"tenant_id": "tnt_acme"},
			amountCents:   199900,
			wantTopic:     "chora.tenancy.payment.captured.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "tnt_acme",
		},
		{
			name:          "payment_intent.succeeded with gcid → identity.payment.captured.v1",
			stripeEventID: "evt_pay_user_1",
			stripeType:    "payment_intent.succeeded",
			metadata:      map[string]string{"gcid": "0197a-test-gcid"},
			amountCents:   999,
			wantTopic:     "chora.identity.payment.captured.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "platform",
			wantGCID:      "0197a-test-gcid",
		},
		{
			name:          "subscription.created tenant routing",
			stripeEventID: "evt_sub_tenant_1",
			stripeType:    "customer.subscription.created",
			metadata:      map[string]string{"tenant_id": "tnt_acme"},
			amountCents:   0,
			wantTopic:     "chora.tenancy.subscription.created.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "tnt_acme",
		},
		{
			name:          "subscription.updated user routing",
			stripeEventID: "evt_sub_user_1",
			stripeType:    "customer.subscription.updated",
			metadata:      map[string]string{"gcid": "0197a-user-1"},
			wantTopic:     "chora.identity.subscription.updated.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "platform",
			wantGCID:      "0197a-user-1",
		},
		{
			name:          "subscription.deleted tenant routing → cancelled",
			stripeEventID: "evt_sub_del_1",
			stripeType:    "customer.subscription.deleted",
			metadata:      map[string]string{"tenant_id": "tnt_acme"},
			wantTopic:     "chora.tenancy.subscription.cancelled.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "tnt_acme",
		},
		{
			name:          "invoice.payment_failed → governance.payment_failure.detected.v1",
			stripeEventID: "evt_invoice_fail_1",
			stripeType:    "invoice.payment_failed",
			metadata:      map[string]string{"tenant_id": "tnt_acme"},
			amountCents:   199900,
			wantTopic:     "chora.governance.payment_failure.detected.v1",
			wantIMDADim:   "accountability",
			wantTenantID:  "tnt_acme",
		},
		{
			name:          "charge.dispute.created → governance.payment_dispute.detected.v1",
			stripeEventID: "evt_dispute_1",
			stripeType:    "charge.dispute.created",
			metadata:      map[string]string{"tenant_id": "tnt_acme"},
			amountCents:   199900,
			wantTopic:     "chora.governance.payment_dispute.detected.v1",
			wantIMDADim:   "safety_and_robustness",
			wantTenantID:  "tnt_acme",
		},
		{
			name:           "unknown event type → DLQ classification",
			stripeEventID:  "evt_unknown_1",
			stripeType:     "checkout.session.async_payment_failed",
			metadata:       map[string]string{"tenant_id": "tnt_acme"},
			wantClassError: true,
			wantDLQ:        true,
		},
		{
			name:           "missing both tenant_id and gcid metadata → DLQ classification",
			stripeEventID:  "evt_orphan_1",
			stripeType:     "payment_intent.succeeded",
			metadata:       map[string]string{},
			amountCents:    100,
			wantClassError: true,
			wantDLQ:        true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev := webhook.StripeEvent{
				ID:          tc.stripeEventID,
				Type:        tc.stripeType,
				Metadata:    tc.metadata,
				AmountCents: tc.amountCents,
				Currency:    "USD",
				Created:     time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
			}

			got, err := webhook.Classify(ev)
			if tc.wantClassError {
				if err == nil {
					t.Fatalf("Classify(%s/%s): expected error, got nil (classified=%+v)", tc.stripeType, tc.stripeEventID, got)
				}
				if !got.IsDLQ() {
					t.Fatalf("Classify error case must yield DLQ classification, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Classify(%s/%s): unexpected error: %v", tc.stripeType, tc.stripeEventID, err)
			}
			if got.Topic != tc.wantTopic {
				t.Errorf("Topic = %q; want %q", got.Topic, tc.wantTopic)
			}
			if got.IMDADimension != tc.wantIMDADim {
				t.Errorf("IMDADimension = %q; want %q", got.IMDADimension, tc.wantIMDADim)
			}
			if got.TenantID != tc.wantTenantID {
				t.Errorf("TenantID = %q; want %q", got.TenantID, tc.wantTenantID)
			}
			if got.GCID != tc.wantGCID {
				t.Errorf("GCID = %q; want %q", got.GCID, tc.wantGCID)
			}
			if got.IdempotencyKey == "" {
				t.Errorf("IdempotencyKey must be non-empty (derived from Stripe event ID)")
			}
			if !strings.Contains(got.IdempotencyKey, tc.stripeEventID) {
				t.Errorf("IdempotencyKey %q must include Stripe event ID %q", got.IdempotencyKey, tc.stripeEventID)
			}
		})
	}
}

func TestClassifyStripeEvent_PreferTenantOverGCID(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:   "evt_dual_meta",
		Type: "payment_intent.succeeded",
		Metadata: map[string]string{
			"tenant_id": "tnt_acme",
			"gcid":      "0197a-user-1",
		},
		AmountCents: 100,
		Currency:    "USD",
		Created:     time.Now().UTC(),
	}
	got, err := webhook.Classify(ev)
	if err != nil {
		t.Fatalf("Classify: unexpected error: %v", err)
	}
	if got.Topic != "chora.tenancy.payment.captured.v1" {
		t.Errorf("dual-metadata routing: Topic = %q; want chora.tenancy.payment.captured.v1", got.Topic)
	}
	if got.TenantID != "tnt_acme" {
		t.Errorf("dual-metadata routing: TenantID = %q; want tnt_acme", got.TenantID)
	}
	if got.GCID != "0197a-user-1" {
		t.Errorf("dual-metadata routing: GCID should still be carried in payload; got %q", got.GCID)
	}
}

func TestClassifyStripeEvent_RejectsEmptyEventID(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:       "",
		Type:     "payment_intent.succeeded",
		Metadata: map[string]string{"tenant_id": "tnt_acme"},
	}
	if _, err := webhook.Classify(ev); err == nil {
		t.Fatalf("Classify on empty Stripe event ID must return error")
	}
}

func TestClassifyStripeEvent_RejectsEmptyType(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:       "evt_empty_type",
		Type:     "",
		Metadata: map[string]string{"tenant_id": "tnt_acme"},
	}
	if _, err := webhook.Classify(ev); err == nil {
		t.Fatalf("Classify on empty Stripe event type must return error")
	}
}

func TestClassifyStripeEvent_RoutesEachUserSubscriptionVerb(t *testing.T) {
	t.Parallel()
	cases := []struct {
		stripeType string
		wantTopic  string
	}{
		{"customer.subscription.created", "chora.identity.subscription.created.v1"},
		{"customer.subscription.updated", "chora.identity.subscription.updated.v1"},
		{"customer.subscription.deleted", "chora.identity.subscription.cancelled.v1"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.stripeType, func(t *testing.T) {
			t.Parallel()
			ev := webhook.StripeEvent{
				ID:       "evt_user_sub_" + tc.stripeType,
				Type:     tc.stripeType,
				Metadata: map[string]string{"gcid": "0197a-user"},
			}
			got, err := webhook.Classify(ev)
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Topic != tc.wantTopic {
				t.Errorf("Topic = %q; want %q", got.Topic, tc.wantTopic)
			}
			if got.TenantID != "platform" {
				t.Errorf("user-scoped subscription must use envelope tenant_id=platform; got %q", got.TenantID)
			}
		})
	}
}

func TestClassifyStripeEvent_RoutingMissingForKnownTypes(t *testing.T) {
	t.Parallel()
	knownTypes := []string{
		"payment_intent.succeeded",
		"customer.subscription.created",
		"customer.subscription.updated",
		"customer.subscription.deleted",
		"invoice.payment_failed",
		"charge.dispute.created",
	}
	for _, st := range knownTypes {
		st := st
		t.Run(st, func(t *testing.T) {
			t.Parallel()
			ev := webhook.StripeEvent{ID: "evt_orphan_" + st, Type: st, Metadata: map[string]string{}}
			got, err := webhook.Classify(ev)
			if err == nil {
				t.Fatalf("Classify on orphan metadata must error")
			}
			if !got.IsDLQ() {
				t.Fatalf("orphan classification must DLQ")
			}
			if got.Topic != webhook.DLQTopic {
				t.Errorf("DLQ topic = %q; want %q", got.Topic, webhook.DLQTopic)
			}
		})
	}
}

func TestClassifyStripeEvent_NilMetadata(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:       "evt_nil_md",
		Type:     "payment_intent.succeeded",
		Metadata: nil,
	}
	got, err := webhook.Classify(ev)
	if err == nil {
		t.Fatalf("Classify with nil metadata must error (no routing keys)")
	}
	if !got.IsDLQ() {
		t.Fatalf("nil metadata must DLQ")
	}
}

func TestClassifyStripeEvent_WhitespaceMetadata(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:       "evt_ws_md",
		Type:     "payment_intent.succeeded",
		Metadata: map[string]string{"tenant_id": "", "gcid": ""},
	}
	got, err := webhook.Classify(ev)
	if err == nil {
		t.Fatalf("empty-string routing values must DLQ")
	}
	if !got.IsDLQ() {
		t.Fatalf("classification must DLQ")
	}
}

func TestClassification_PayloadShape(t *testing.T) {
	t.Parallel()
	ev := webhook.StripeEvent{
		ID:          "evt_pay_shape",
		Type:        "payment_intent.succeeded",
		Metadata:    map[string]string{"tenant_id": "tnt_acme", "stripe_customer_id": "cus_acme"},
		AmountCents: 199900,
		Currency:    "USD",
		Created:     time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
	}
	got, err := webhook.Classify(ev)
	if err != nil {
		t.Fatalf("Classify: unexpected error: %v", err)
	}
	if got.Payload == nil {
		t.Fatalf("Payload must be non-nil")
	}
	if got.Payload.StripeEventID != "evt_pay_shape" {
		t.Errorf("Payload.StripeEventID = %q; want evt_pay_shape", got.Payload.StripeEventID)
	}
	if got.Payload.AmountCents != 199900 {
		t.Errorf("Payload.AmountCents = %d; want 199900", got.Payload.AmountCents)
	}
	if got.Payload.Currency != "USD" {
		t.Errorf("Payload.Currency = %q; want USD", got.Payload.Currency)
	}
	if got.Payload.OccurredAt.IsZero() {
		t.Errorf("Payload.OccurredAt must be set from Stripe event Created timestamp")
	}
	if got.Payload.StripeCustomerID != "cus_acme" {
		t.Errorf("Payload.StripeCustomerID = %q; want cus_acme", got.Payload.StripeCustomerID)
	}
}
