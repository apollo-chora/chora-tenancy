package familiareggstripe_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiareggstripe"
)

func TestStubClient_CreateCheckoutSession_HappyPath(t *testing.T) {
	t.Parallel()
	c := familiareggstripe.NewStubClient()
	sess, err := c.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{
		TenantID:           "t-1",
		PurchaserGCID:      "g-1",
		EggSKU:             "egg.standard.v1",
		PurchaseID:         "p-1",
		AmountCents:        999,
		Currency:           "SGD",
		ProductDisplayName: "Standard Egg",
		SuccessURL:         "https://example.test/ok",
		CancelURL:          "https://example.test/cancel",
	})
	if err != nil {
		t.Fatalf("CreateCheckoutSession: %v", err)
	}
	if sess.SessionID == "" || sess.CheckoutURL == "" {
		t.Errorf("empty stub response: %+v", sess)
	}
	if len(c.Calls()) != 1 {
		t.Errorf("expected 1 call, got %d", len(c.Calls()))
	}
}

func TestStubClient_CreateCheckoutSession_ErrorPath(t *testing.T) {
	t.Parallel()
	c := familiareggstripe.NewStubClient()
	c.CreateErr = errors.New("boom")
	_, err := c.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestRESTClient_CreateCheckoutSession_PostsFormToStripe(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s", r.Method)
		}
		if !strings.Contains(string(body), "metadata%5Bchora_purchase_id%5D=p-1") {
			t.Errorf("expected purchase_id metadata; body=%s", string(body))
		}
		if r.Header.Get("Idempotency-Key") != "idem-key-1" {
			t.Errorf("missing idempotency key")
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "cs_xyz", "url": "https://stripe.test/cs_xyz"})
	}))
	defer srv.Close()

	c := familiareggstripe.NewRESTClient(familiareggstripe.Config{
		APIBase:   srv.URL,
		SecretKey: "sk_test_xx",
	})
	sess, err := c.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{
		TenantID:           "t-1",
		PurchaserGCID:      "g-1",
		EggSKU:             "egg.standard.v1",
		PurchaseID:         "p-1",
		AmountCents:        999,
		Currency:           "SGD",
		ProductDisplayName: "Standard Egg",
		SuccessURL:         "https://example.test/ok",
		CancelURL:          "https://example.test/cancel",
		IdempotencyKey:     "idem-key-1",
	})
	if err != nil {
		t.Fatalf("CreateCheckoutSession: %v", err)
	}
	if sess.SessionID != "cs_xyz" || sess.CheckoutURL != "https://stripe.test/cs_xyz" {
		t.Errorf("unexpected session: %+v", sess)
	}
}

func TestRESTClient_CreateCheckoutSession_RejectsEmptyConfig(t *testing.T) {
	t.Parallel()
	c := familiareggstripe.NewRESTClient(familiareggstripe.Config{})
	_, err := c.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{})
	if !errors.Is(err, familiareggstripe.ErrNoAPIBase) {
		t.Errorf("expected ErrNoAPIBase, got %v", err)
	}
	c2 := familiareggstripe.NewRESTClient(familiareggstripe.Config{APIBase: "http://stub"})
	_, err = c2.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{})
	if !errors.Is(err, familiareggstripe.ErrNoSecretKey) {
		t.Errorf("expected ErrNoSecretKey, got %v", err)
	}
}

func TestRESTClient_CreateCheckoutSession_SurfacesStripeError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"bad"}}`))
	}))
	defer srv.Close()
	c := familiareggstripe.NewRESTClient(familiareggstripe.Config{APIBase: srv.URL, SecretKey: "sk_test_xx"})
	_, err := c.CreateCheckoutSession(context.Background(), familiareggstripe.CheckoutSessionInput{
		AmountCents:        999,
		Currency:           "SGD",
		ProductDisplayName: "Egg",
		SuccessURL:         "https://x", CancelURL: "https://y",
	})
	if err == nil {
		t.Fatalf("expected error on non-200")
	}
}

// ----------------------------------------------------------------------------
// Event parsing
// ----------------------------------------------------------------------------

func TestParseEvent_CheckoutSessionCompleted(t *testing.T) {
	t.Parallel()
	body := []byte(`{
        "id": "evt_test_1",
        "type": "checkout.session.completed",
        "created": 1735689600,
        "data": {
            "object": {
                "id": "cs_test_1",
                "payment_intent": "pi_test_1",
                "amount_total": 999,
                "currency": "sgd",
                "metadata": {"chora_purchase_id": "p-1", "chora_tenant_id": "t-1"}
            }
        }
    }`)
	pe, err := familiareggstripe.ParseEvent(body)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if pe.Type != "checkout.session.completed" || pe.SessionID != "cs_test_1" || pe.PaymentIntentID != "pi_test_1" {
		t.Errorf("unexpected parse: %+v", pe)
	}
	if pe.AmountCents != 999 || pe.Currency != "SGD" {
		t.Errorf("amount/currency: got %d/%s", pe.AmountCents, pe.Currency)
	}
	if pe.Metadata["chora_purchase_id"] != "p-1" {
		t.Errorf("metadata missing")
	}
}

func TestParseEvent_ChargeFailed(t *testing.T) {
	t.Parallel()
	body := []byte(`{
        "id": "evt_test_2",
        "type": "charge.failed",
        "data": {
            "object": {
                "id": "ch_x",
                "failure_code": "card_declined",
                "failure_message": "Your card was declined."
            }
        }
    }`)
	pe, err := familiareggstripe.ParseEvent(body)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if pe.ChargeID != "ch_x" || pe.FailureCode != "card_declined" {
		t.Errorf("unexpected: %+v", pe)
	}
}

func TestParseEvent_ChargeRefunded_ExtractsRefundID(t *testing.T) {
	t.Parallel()
	body := []byte(`{
        "id": "evt_test_3",
        "type": "charge.refunded",
        "data": {
            "object": {
                "id": "ch_x",
                "amount_refunded": 999,
                "currency": "sgd",
                "refunds": {"data": [{"id": "re_x"}]}
            }
        }
    }`)
	pe, err := familiareggstripe.ParseEvent(body)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if pe.RefundID != "re_x" {
		t.Errorf("refund_id: got %q", pe.RefundID)
	}
	if pe.AmountCents != 999 {
		t.Errorf("amount: got %d", pe.AmountCents)
	}
}

func TestParseEvent_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := familiareggstripe.ParseEvent([]byte(`not json`))
	if err == nil {
		t.Errorf("expected error on bad JSON")
	}
}

func TestParseEvent_RejectsMissingID(t *testing.T) {
	t.Parallel()
	_, err := familiareggstripe.ParseEvent([]byte(`{"type":"x"}`))
	if err == nil {
		t.Errorf("expected error on missing id")
	}
}

func TestParseEvent_RejectsMissingType(t *testing.T) {
	t.Parallel()
	_, err := familiareggstripe.ParseEvent([]byte(`{"id":"evt_x"}`))
	if err == nil {
		t.Errorf("expected error on missing type")
	}
}
