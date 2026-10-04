package billingwebhook_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/billingwebhook"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/webhook"
)

const testWebhookSecret = "whsec_test_handler"

// recordingPublisher captures every Publish call.
type recordingPublisher struct {
	mu         sync.Mutex
	captured   []webhook.Classification
	publishErr error
}

func (p *recordingPublisher) Publish(_ context.Context, c webhook.Classification) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publishErr != nil {
		return p.publishErr
	}
	p.captured = append(p.captured, c)
	return nil
}

func (p *recordingPublisher) Captured() []webhook.Classification {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]webhook.Classification, len(p.captured))
	copy(out, p.captured)
	return out
}

func makeRouter(t *testing.T, pub billingwebhook.Publisher) http.Handler {
	t.Helper()
	return billingwebhook.NewRouter(billingwebhook.Deps{
		WebhookSecret:    testWebhookSecret,
		SignatureSkew:    5 * time.Minute,
		IdempotencyStore: idempotent.NewMemoryStore(),
		IdempotencyTTL:   time.Hour,
		Publisher:        pub,
		Now:              func() time.Time { return time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC) },
	})
}

func signStripe(secret, body string, ts time.Time) string {
	signedPayload := fmt.Sprintf("%d.%s", ts.Unix(), body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signedPayload))
	return fmt.Sprintf("t=%d,v1=%s", ts.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	rt := makeRouter(t, &recordingPublisher{})
	req := httptest.NewRequest(http.MethodGet, "/webhooks/healthz", nil)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", rec.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("body=%v", body)
	}
}

func TestIndex(t *testing.T) {
	t.Parallel()
	rt := makeRouter(t, &recordingPublisher{})
	req := httptest.NewRequest(http.MethodGet, "/webhooks/", nil)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestStripeWebhook_RejectsGET(t *testing.T) {
	t.Parallel()
	rt := makeRouter(t, &recordingPublisher{})
	req := httptest.NewRequest(http.MethodGet, "/webhooks/stripe", nil)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=405", rec.Code)
	}
}

func TestStripeWebhook_RejectsBadSignature(t *testing.T) {
	t.Parallel()
	rt := makeRouter(t, &recordingPublisher{})
	body := `{"id":"evt_1","type":"payment_intent.succeeded"}`
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(body))
	req.Header.Set("Stripe-Signature", "t=123,v1=deadbeef")
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=401", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "STRIPE_SIGNATURE_INVALID") {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestStripeWebhook_HappyPathPaymentCaptured(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":      "evt_pay_1",
		"type":    "payment_intent.succeeded",
		"created": 1747353600,
		"data": map[string]any{
			"object": map[string]any{
				"id":       "pi_1",
				"amount":   float64(199_900),
				"currency": "usd",
				"customer": "cus_test",
				"metadata": map[string]any{"tenant_id": "tnt_acme"},
			},
		},
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	captured := pub.Captured()
	if len(captured) != 1 {
		t.Fatalf("len(captured) = %d; want 1", len(captured))
	}
	if captured[0].Topic != "chora.tenancy.payment.captured.v1" {
		t.Errorf("Topic = %q", captured[0].Topic)
	}
	if captured[0].TenantID != "tnt_acme" {
		t.Errorf("TenantID = %q", captured[0].TenantID)
	}
	if captured[0].Payload.StripeCustomerID != "cus_test" {
		t.Errorf("StripeCustomerID = %q", captured[0].Payload.StripeCustomerID)
	}
	if captured[0].Payload.AmountCents != 199_900 {
		t.Errorf("AmountCents = %d", captured[0].Payload.AmountCents)
	}
	if captured[0].Payload.Currency != "USD" {
		t.Errorf("Currency = %q", captured[0].Payload.Currency)
	}
}

func TestStripeWebhook_IdempotencyShortCircuit(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":      "evt_idem_1",
		"type":    "payment_intent.succeeded",
		"created": 1747353600,
		"data": map[string]any{
			"object": map[string]any{
				"amount":   float64(100),
				"currency": "usd",
				"metadata": map[string]any{"tenant_id": "tnt_acme"},
			},
		},
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	sig := signStripe(testWebhookSecret, string(body), ts)
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
		req.Header.Set("Stripe-Signature", sig)
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("iter %d: status=%d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	if c := pub.Captured(); len(c) != 1 {
		t.Errorf("expected exactly 1 publish across 3 deliveries; got %d", len(c))
	}
}

func TestStripeWebhook_DLQOnUnknownType(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":   "evt_unknown_1",
		"type": "checkout.session.async_payment_failed",
		"data": map[string]any{
			"object": map[string]any{
				"metadata": map[string]any{"tenant_id": "tnt_acme"},
			},
		},
		"created": 1747353600,
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d want=202", rec.Code)
	}
	if c := pub.Captured(); len(c) != 1 {
		t.Errorf("DLQ classification must still be published exactly once; got %d", len(c))
	}
	if cap := pub.Captured(); !cap[0].IsDLQ() {
		t.Errorf("classification not flagged as DLQ")
	}
	if pub.Captured()[0].Topic != webhook.DLQTopic {
		t.Errorf("DLQ topic = %q; want %q", pub.Captured()[0].Topic, webhook.DLQTopic)
	}
}

func TestStripeWebhook_DLQOnMissingMetadata(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":   "evt_orphan_1",
		"type": "payment_intent.succeeded",
		"data": map[string]any{
			"object": map[string]any{
				"amount":   float64(100),
				"currency": "usd",
			},
		},
		"created": 1747353600,
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("orphan-meta status=%d want=202 body=%s", rec.Code, rec.Body.String())
	}
}

func TestStripeWebhook_GovernanceRoutingForFailureAndDispute(t *testing.T) {
	t.Parallel()
	cases := []struct {
		stripeType string
		wantTopic  string
	}{
		{"invoice.payment_failed", "chora.governance.payment_failure.detected.v1"},
		{"charge.dispute.created", "chora.governance.payment_dispute.detected.v1"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.stripeType, func(t *testing.T) {
			t.Parallel()
			pub := &recordingPublisher{}
			rt := makeRouter(t, pub)
			body, _ := json.Marshal(map[string]any{
				"id":      "evt_" + tc.stripeType,
				"type":    tc.stripeType,
				"created": 1747353600,
				"data": map[string]any{
					"object": map[string]any{
						"amount":   float64(100),
						"currency": "usd",
						"metadata": map[string]any{"tenant_id": "tnt_acme"},
					},
				},
			})
			ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
			req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
			rec := httptest.NewRecorder()
			rt.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			cap := pub.Captured()
			if len(cap) != 1 {
				t.Fatalf("len=%d", len(cap))
			}
			if cap[0].Topic != tc.wantTopic {
				t.Errorf("Topic = %q; want %q", cap[0].Topic, tc.wantTopic)
			}
		})
	}
}

func TestStripeWebhook_PublishFailureReturns500(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{publishErr: fmt.Errorf("pubsub down")}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":      "evt_pub_fail",
		"type":    "payment_intent.succeeded",
		"created": 1747353600,
		"data": map[string]any{
			"object": map[string]any{
				"amount":   float64(100),
				"currency": "usd",
				"metadata": map[string]any{"tenant_id": "tnt_acme"},
			},
		},
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want=500", rec.Code)
	}
}

func TestStripeWebhook_RejectsUnparseableJSON(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body := []byte("{not json")
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=400", rec.Code)
	}
}

func TestStripeWebhook_NotFoundOnOtherPath(t *testing.T) {
	t.Parallel()
	rt := makeRouter(t, &recordingPublisher{})
	req := httptest.NewRequest(http.MethodGet, "/something-else", nil)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=404", rec.Code)
	}
}

// String-encoded amounts exercise the toInt64(string) branch of the
// amount picker (Stripe occasionally emits amounts as JSON strings).
func TestStripeWebhook_HappyPathStringAmount(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{
		"id":      "evt_pay_str",
		"type":    "payment_intent.succeeded",
		"created": 1747353600,
		"data": map[string]any{
			"object": map[string]any{
				"id":              "pi_str",
				"amount":          "4900",
				"amount_captured": "9800",
				"currency":        "sgd",
				"metadata":        map[string]any{"tenant_id": "tnt_acme"},
			},
		},
	})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	captured := pub.Captured()
	if len(captured) != 1 {
		t.Fatalf("len(captured) = %d; want 1", len(captured))
	}
	// amount_captured (string "9800") wins over amount (string "4900").
	if captured[0].Payload.AmountCents != 9800 {
		t.Fatalf("AmountCents = %d; want 9800", captured[0].Payload.AmountCents)
	}
}

func TestStripeWebhook_EventMissingID_Rejected(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{"type": "payment_intent.succeeded", "created": 1747353600})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatal("expected non-OK for event without id")
	}
}

func TestStripeWebhook_EventMissingType_Rejected(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	rt := makeRouter(t, pub)
	body, _ := json.Marshal(map[string]any{"id": "evt_x", "created": 1747353600})
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(testWebhookSecret, string(body), ts))
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatal("expected non-OK for event without type")
	}
}

func TestNewRouter_AppliesDefaults(t *testing.T) {
	t.Parallel()
	// Zero skew/TTL/Now → defaults applied; routes still served.
	rt := billingwebhook.NewRouter(billingwebhook.Deps{WebhookSecret: "whsec_x", Publisher: &recordingPublisher{}})
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhooks/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: %d", rec.Code)
	}
}
