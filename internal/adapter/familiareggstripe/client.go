// Package familiareggstripe is the Stripe adapter for Iter G.3 Familiar Egg
// Checkout sessions (ADR-149).
//
// Two surfaces:
//
//   - Client interface — abstracts Stripe Checkout Session creation so tests
//     can inject a deterministic fake. Production HTTPClient (RESTClient
//     below) calls POST /v1/checkout/sessions over net/http (the Stripe Go
//     SDK is not vendored; service uses the same thin REST pattern as
//     billingstripe).
//
//   - Event parser — parseEvent decodes the Stripe webhook body into a
//     ParsedEvent and routes by event.type. Signature verification is
//     delegated to internal/domain/billing/webhook.VerifySignature (the
//     reusable HMAC-SHA256 implementation, shared with billing webhooks).
//
// Per `feedback_no_inline_config`: API base URL + secret key + webhook
// secret + success/cancel URLs all come from env vars. Empty values
// surface as typed errors at call time.
package familiareggstripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Sentinel errors.
var (
	ErrNoAPIBase     = errors.New("familiareggstripe: STRIPE_API_BASE required (no inline config)")
	ErrNoSecretKey   = errors.New("familiareggstripe: STRIPE_API_KEY required (Secret Manager)")
	ErrInvalidEvent  = errors.New("familiareggstripe: invalid Stripe event body")
)

// CheckoutSessionInput captures the fields the chora-tenancy /checkout
// endpoint passes to Stripe Checkout. Mirrors the relevant POST
// /v1/checkout/sessions fields.
type CheckoutSessionInput struct {
	TenantID            string
	PurchaserGCID       string
	EggSKU              string
	PurchaseID          string
	AmountCents         int64
	Currency            string // ISO 4217
	ProductDisplayName  string
	SuccessURL          string
	CancelURL           string
	// IdempotencyKey is the Stripe `Idempotency-Key` header value. Production
	// callers should set this to chora.tenancy.familiar_egg.{purchase_id}
	// so retries are safe.
	IdempotencyKey string
}

// CheckoutSession is the relevant subset of the Stripe Checkout Session
// response.
type CheckoutSession struct {
	SessionID   string
	CheckoutURL string
}

// Client is the port the Familiar Egg HTTP handlers + webhook handler use
// to talk to Stripe. The production RESTClient calls Stripe over HTTP; the
// fake (StubClient) is used in tests.
type Client interface {
	CreateCheckoutSession(ctx context.Context, in CheckoutSessionInput) (*CheckoutSession, error)
}

// -----------------------------------------------------------------------------
// RESTClient — production Stripe HTTP client
// -----------------------------------------------------------------------------

// Config wires construction-time defaults for the RESTClient.
type Config struct {
	APIBase    string // e.g. https://api.stripe.com
	SecretKey  string
	HTTPClient *http.Client // optional; defaults to 30s timeout
}

// RESTClient implements Client by calling Stripe's REST API.
type RESTClient struct {
	cfg Config
}

// NewRESTClient constructs a RESTClient with sane defaults.
func NewRESTClient(cfg Config) *RESTClient {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &RESTClient{cfg: cfg}
}

// CreateCheckoutSession implements Client.
func (c *RESTClient) CreateCheckoutSession(ctx context.Context, in CheckoutSessionInput) (*CheckoutSession, error) {
	if strings.TrimSpace(c.cfg.APIBase) == "" {
		return nil, ErrNoAPIBase
	}
	if strings.TrimSpace(c.cfg.SecretKey) == "" {
		return nil, ErrNoSecretKey
	}
	form := url.Values{}
	form.Set("mode", "payment")
	form.Set("success_url", in.SuccessURL)
	form.Set("cancel_url", in.CancelURL)
	form.Set("line_items[0][price_data][currency]", strings.ToLower(in.Currency))
	form.Set("line_items[0][price_data][product_data][name]", in.ProductDisplayName)
	form.Set("line_items[0][price_data][unit_amount]", strconv.FormatInt(in.AmountCents, 10))
	form.Set("line_items[0][quantity]", "1")
	// Surface chora context to Stripe (lands on session.metadata).
	form.Set("metadata[chora_purchase_id]", in.PurchaseID)
	form.Set("metadata[chora_tenant_id]", in.TenantID)
	form.Set("metadata[chora_gcid]", in.PurchaserGCID)
	form.Set("metadata[chora_egg_sku]", in.EggSKU)

	endpoint := strings.TrimRight(c.cfg.APIBase, "/") + "/v1/checkout/sessions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("familiareggstripe: build request: %w", err)
	}
	req.SetBasicAuth(c.cfg.SecretKey, "")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Stripe-Version", "2024-11-20.acacia")
	if in.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", in.IdempotencyKey)
	}

	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("familiareggstripe: do POST: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("familiareggstripe: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("familiareggstripe: POST /v1/checkout/sessions: status %d body=%s", resp.StatusCode, string(body))
	}
	var out struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("familiareggstripe: decode response: %w", err)
	}
	return &CheckoutSession{SessionID: out.ID, CheckoutURL: out.URL}, nil
}

// -----------------------------------------------------------------------------
// StubClient — test fake
// -----------------------------------------------------------------------------

// StubClient is the in-memory test fake. Returns deterministic
// (cs_test_N, https://stub.invalid/cs_test_N) responses + records all
// CreateCheckoutSession calls for assertions.
type StubClient struct {
	calls []CheckoutSessionInput
	seq   int
	// CreateErr lets tests force a failure path.
	CreateErr error
}

// NewStubClient constructs a StubClient.
func NewStubClient() *StubClient { return &StubClient{} }

// CreateCheckoutSession implements Client.
func (s *StubClient) CreateCheckoutSession(_ context.Context, in CheckoutSessionInput) (*CheckoutSession, error) {
	if s.CreateErr != nil {
		return nil, s.CreateErr
	}
	s.calls = append(s.calls, in)
	s.seq++
	id := fmt.Sprintf("cs_test_%d", s.seq)
	return &CheckoutSession{
		SessionID:   id,
		CheckoutURL: "https://stub.invalid/" + id,
	}, nil
}

// Calls returns the recorded inputs.
func (s *StubClient) Calls() []CheckoutSessionInput { return append([]CheckoutSessionInput(nil), s.calls...) }

// -----------------------------------------------------------------------------
// Webhook event parsing
// -----------------------------------------------------------------------------

// Event types this Iter G.3 webhook handler cares about.
const (
	EventCheckoutSessionCompleted        = "checkout.session.completed"
	EventCheckoutSessionAsyncPaymentFail = "checkout.session.async_payment_failed"
	EventChargeFailed                    = "charge.failed"
	EventChargeRefunded                  = "charge.refunded"
)

// ParsedEvent is the decoded Stripe event body.
type ParsedEvent struct {
	ID          string
	Type        string
	CreatedUnix int64
	// SessionID is data.object.id when type starts with checkout.session.*
	// Empty for charge.* events.
	SessionID string
	// PaymentIntentID / ChargeID / RefundID / FailureCode / FailureMessage
	// pulled from data.object as available.
	PaymentIntentID string
	ChargeID        string
	RefundID        string
	FailureCode     string
	FailureMessage  string
	// AmountCents + Currency populated for paid sessions.
	AmountCents int64
	Currency    string
	// Metadata is the merged session-level metadata; carries chora_purchase_id.
	Metadata map[string]string
}

// ParseEvent decodes a Stripe webhook body into a ParsedEvent. Caller MUST
// have already verified the Stripe-Signature header.
func ParseEvent(body []byte) (*ParsedEvent, error) {
	var w struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Created int64  `json:"created"`
		Data    struct {
			Object map[string]any `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if w.ID == "" {
		return nil, fmt.Errorf("%w: missing event id", ErrInvalidEvent)
	}
	if w.Type == "" {
		return nil, fmt.Errorf("%w: missing event type", ErrInvalidEvent)
	}
	pe := &ParsedEvent{
		ID:          w.ID,
		Type:        w.Type,
		CreatedUnix: w.Created,
		Metadata:    map[string]string{},
	}
	obj := w.Data.Object
	if obj == nil {
		return pe, nil
	}
	if strings.HasPrefix(w.Type, "checkout.session.") {
		if id, _ := obj["id"].(string); id != "" {
			pe.SessionID = id
		}
		if pi, _ := obj["payment_intent"].(string); pi != "" {
			pe.PaymentIntentID = pi
		}
	}
	if strings.HasPrefix(w.Type, "charge.") {
		if id, _ := obj["id"].(string); id != "" {
			pe.ChargeID = id
		}
		if pi, _ := obj["payment_intent"].(string); pi != "" {
			pe.PaymentIntentID = pi
		}
		if w.Type == EventChargeFailed {
			if c, _ := obj["failure_code"].(string); c != "" {
				pe.FailureCode = c
			}
			if m, _ := obj["failure_message"].(string); m != "" {
				pe.FailureMessage = m
			}
		}
		if w.Type == EventChargeRefunded {
			// Look at the refunds.data[0].id if present.
			if r, ok := obj["refunds"].(map[string]any); ok {
				if data, ok := r["data"].([]any); ok && len(data) > 0 {
					if first, ok := data[0].(map[string]any); ok {
						if id, _ := first["id"].(string); id != "" {
							pe.RefundID = id
						}
					}
				}
			}
		}
	}
	// metadata
	if md, ok := obj["metadata"].(map[string]any); ok {
		for k, v := range md {
			if s, ok := v.(string); ok {
				pe.Metadata[k] = s
			}
		}
	}
	// amount + currency
	if v, ok := obj["amount_total"].(float64); ok && v > 0 {
		pe.AmountCents = int64(v)
	} else if v, ok := obj["amount_captured"].(float64); ok && v > 0 {
		pe.AmountCents = int64(v)
	} else if v, ok := obj["amount"].(float64); ok && v > 0 {
		pe.AmountCents = int64(v)
	} else if v, ok := obj["amount_refunded"].(float64); ok && v > 0 {
		pe.AmountCents = int64(v)
	}
	if c, _ := obj["currency"].(string); c != "" {
		pe.Currency = strings.ToUpper(c)
	}
	return pe, nil
}
