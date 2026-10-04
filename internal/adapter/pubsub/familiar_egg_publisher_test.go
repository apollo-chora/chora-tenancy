package pubsub_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareggpub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pubsub"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

func TestEmitCheckoutStarted_PublishesToCheckoutStartedTopic(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	p := samplePurchase()
	_, err := familiareggpub.EmitCheckoutStarted(r, events.Header{TenantID: p.TenantID}, p)
	if err != nil {
		t.Fatalf("EmitCheckoutStarted: %v", err)
	}
	got := r.RecordedByTopic(familiareggpub.TopicCheckoutStarted)
	if len(got) != 1 {
		t.Fatalf("expected 1 event; got %d", len(got))
	}
	if got[0].Payload["purchase_id"] != p.PurchaseID {
		t.Errorf("purchase_id payload missing")
	}
	if got[0].Payload["target_tenant_id"] != p.TenantID {
		t.Errorf("target_tenant_id missing")
	}
	if got[0].Payload["chora_imda_dimension"] != "accountability" {
		t.Errorf("imda dimension missing")
	}
}

func TestEmitPaymentSucceeded_PublishesPaidEvent(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	p := samplePurchase()
	_ = p.MarkPaid(time.Now().UTC(), "pi_1", "ch_1", 999)
	_, err := familiareggpub.EmitPaymentSucceeded(r, events.Header{TenantID: p.TenantID}, p)
	if err != nil {
		t.Fatalf("EmitPaymentSucceeded: %v", err)
	}
	got := r.RecordedByTopic(familiareggpub.TopicPaymentSucceeded)
	if len(got) != 1 {
		t.Fatalf("expected 1 event; got %d", len(got))
	}
	if got[0].Payload["amount_cents_paid"].(int64) != 999 {
		t.Errorf("amount_paid missing")
	}
}

func TestEmitPaymentFailed_PublishesFailureCode(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	p := samplePurchase()
	_ = p.MarkPaymentFailed(time.Now().UTC(), "card_declined", "Card was declined")
	_, err := familiareggpub.EmitPaymentFailed(r, events.Header{TenantID: p.TenantID}, p)
	if err != nil {
		t.Fatalf("EmitPaymentFailed: %v", err)
	}
	got := r.RecordedByTopic(familiareggpub.TopicPaymentFailed)
	if len(got) != 1 {
		t.Fatalf("expected 1 event; got %d", len(got))
	}
	if got[0].Payload["stripe_failure_code"] != "card_declined" {
		t.Errorf("failure_code missing")
	}
}

func TestEmitRefunded_PublishesReason(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	p := samplePurchase()
	_ = p.MarkPaid(time.Now().UTC(), "pi_1", "ch_1", 999)
	_ = p.MarkRefunded(time.Now().UTC(), "re_1", 999, "customer_request", false)
	_, err := familiareggpub.EmitRefunded(r, events.Header{TenantID: p.TenantID}, p)
	if err != nil {
		t.Fatalf("EmitRefunded: %v", err)
	}
	got := r.RecordedByTopic(familiareggpub.TopicRefunded)
	if len(got) != 1 {
		t.Fatalf("expected 1 event; got %d", len(got))
	}
	if got[0].Payload["reason"] != "customer_request" {
		t.Errorf("reason missing")
	}
	if got[0].Payload["credit_only"] != false {
		t.Errorf("credit_only missing")
	}
}

func TestEmitRefunded_ExpiredPathUsesExpiredAt(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	p := samplePurchase()
	_ = p.MarkPaid(time.Now().UTC(), "pi_1", "ch_1", 999)
	expiry := time.Now().UTC().Add(24 * time.Hour)
	_ = p.MarkExpired(expiry, 999)
	_, err := familiareggpub.EmitRefunded(r, events.Header{TenantID: p.TenantID}, p)
	if err != nil {
		t.Fatalf("EmitRefunded: %v", err)
	}
	got := r.RecordedByTopic(familiareggpub.TopicRefunded)
	if len(got) != 1 {
		t.Fatalf("expected 1 event")
	}
	if got[0].Payload["credit_only"] != true {
		t.Errorf("credit_only must be true for expired path")
	}
	if got[0].Payload["reason"] != "hard_expiry_unhatched" {
		t.Errorf("reason for expired path wrong")
	}
}

func samplePurchase() *familiareag.Purchase {
	now := time.Now().UTC()
	return familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "01970000-0000-7000-8000-aaaaaaaaaaa1",
		TenantID:          "01970000-0000-7000-8000-bbbbbbbbbbb1",
		PurchaserGCID:     "01970000-0000-7000-8000-ccccccccccc1",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_x",
		StripeCheckoutURL: "https://stripe.example/cs_test_x",
		Now:               now,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
}
