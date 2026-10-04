// Package billingwebhook is the chora-tenancy HTTP surface for Stripe
// webhook ingress.
//
// Ported from services/chora-billing-webhook/internal/adapter/http as part
// of the M12.2 Batch-1 consolidation. Per locked architecture (CLAUDE.md §1)
// Tenancy + Billing are a single combined supporting domain — Stripe webhook
// ingress lives here, not in a standalone chora-billing-webhook service.
//
// 2 endpoints:
//
//	GET  /webhooks/healthz       — liveness for the webhook seam
//	POST /webhooks/stripe        — Stripe webhook ingress (signature verified)
//
// Errors (RFC 7807-ish JSON shape):
//
//	{ "code": "...", "message": "..." }
//
// Tracing: per-route OTel span; envelope traceparent extracted/minted via
// chora-common/observability.HTTPMiddleware (mounted by the caller of
// NewRouter).
package billingwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-common/observability"

	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/webhook"
)

// Publisher is the port the HTTP adapter calls to fan-out a verified
// webhook event to chora topics.
type Publisher interface {
	Publish(ctx context.Context, c webhook.Classification) error
}

// Deps wires the handler.
type Deps struct {
	WebhookSecret    string
	SignatureSkew    time.Duration // typical 5*time.Minute
	IdempotencyStore idempotent.Store
	IdempotencyTTL   time.Duration
	Publisher        Publisher
	Now              func() time.Time
}

// NewRouter mounts the HTTP routes.
func NewRouter(d Deps) http.Handler {
	if d.SignatureSkew == 0 {
		d.SignatureSkew = 5 * time.Minute
	}
	if d.IdempotencyTTL == 0 {
		// Stripe retries up to 3 days; cover that window comfortably.
		d.IdempotencyTTL = 7 * 24 * time.Hour
	}
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	h := &handler{deps: d}
	mux := http.NewServeMux()
	mux.HandleFunc("/webhooks/healthz", healthz)
	mux.HandleFunc("/webhooks/stripe", h.stripeWebhook)
	mux.HandleFunc("/webhooks/", h.index)
	return mux
}

type handler struct {
	deps Deps
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *handler) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/webhooks/" && r.URL.Path != "/webhooks" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "chora-tenancy-billing-webhook",
		"domain":  "Tenancy + Billing (combined supporting domain, Stripe webhook seam)",
		"project": resolveProject(),
		"team":    "Team 3 (Platform)",
	})
}

// -----------------------------------------------------------------------------
// /webhooks/stripe
// -----------------------------------------------------------------------------

func (h *handler) stripeWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only POST is supported on /webhooks/stripe")
		return
	}

	ctx, span := observability.StartSpan(r.Context(), "webhook.stripe.handle")
	defer span.End()

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB cap
	_ = r.Body.Close()
	if err != nil {
		writeError(w, http.StatusBadRequest, "STRIPE_BODY_READ", err.Error())
		return
	}
	header := r.Header.Get("Stripe-Signature")
	if err := webhook.VerifySignature(h.deps.WebhookSecret, header, body, h.deps.Now(), h.deps.SignatureSkew); err != nil {
		writeError(w, http.StatusUnauthorized, "STRIPE_SIGNATURE_INVALID", err.Error())
		return
	}

	ev, err := parseStripeEvent(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "STRIPE_BODY_PARSE", err.Error())
		return
	}

	cls, classifyErr := webhook.Classify(ev)

	// Idempotency anchored on Stripe event ID. Even DLQ classifications
	// dedupe (a duplicate-DLQ would just create noise for human review).
	idemKey := "stripe-webhook:" + ev.ID
	processErr := h.deps.IdempotencyStore.Process(ctx, idemKey, h.deps.IdempotencyTTL, func() error {
		return h.deps.Publisher.Publish(ctx, cls)
	})
	if processErr != nil {
		// Publish failed — let Stripe retry. Caller's idempotent.Process
		// did NOT mark the key, so re-delivery will run again.
		writeError(w, http.StatusInternalServerError, "PUBLISH_FAILED", processErr.Error())
		return
	}

	if cls.IsDLQ() {
		// 202 Accepted — we received it but human review is required.
		writeJSON(w, http.StatusAccepted, map[string]any{
			"status": "dlq",
			"reason": classifyErrorMessage(classifyErr),
		})
		return
	}

	// Successful classification + publish.
	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "accepted",
		"topic":           cls.Topic,
		"event_id":        ev.ID,
		"idempotency_key": cls.IdempotencyKey,
	})
}

func classifyErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// -----------------------------------------------------------------------------
// Stripe body parsing
// -----------------------------------------------------------------------------

// stripeEventWire mirrors the bits of the Stripe webhook JSON we care about.
type stripeEventWire struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Created  int64             `json:"created"`
	Metadata map[string]string `json:"metadata"`
	Data     struct {
		Object map[string]any `json:"object"`
	} `json:"data"`
}

// parseStripeEvent converts the wire bytes to the domain's StripeEvent.
//
//   - Event-level metadata + data.object.metadata are merged
//     (data.object.metadata wins on conflict because Stripe's `Customer.metadata`
//     is the more specific, intentional source).
//   - Amount + currency come from data.object's
//     `amount_captured` (Charge) / `amount` (PaymentIntent) /
//     `amount_due` (Invoice) — first-non-zero wins.
func parseStripeEvent(body []byte) (webhook.StripeEvent, error) {
	var w stripeEventWire
	if err := json.Unmarshal(body, &w); err != nil {
		return webhook.StripeEvent{}, fmt.Errorf("decode stripe event: %w", err)
	}
	if w.ID == "" {
		return webhook.StripeEvent{}, errors.New("stripe event missing id")
	}
	if w.Type == "" {
		return webhook.StripeEvent{}, errors.New("stripe event missing type")
	}

	merged := map[string]string{}
	for k, v := range w.Metadata {
		merged[k] = v
	}
	if obj := w.Data.Object; obj != nil {
		// data.object.metadata
		if md, ok := obj["metadata"].(map[string]any); ok {
			for k, v := range md {
				if s, ok := v.(string); ok {
					merged[k] = s
				}
			}
		}
		// data.object.customer (string customer id)
		if c, ok := obj["customer"].(string); ok && c != "" {
			merged["stripe_customer_id"] = c
		}
		// subscription_id (data.object.id when type starts with customer.subscription.*)
		if strings.HasPrefix(w.Type, "customer.subscription.") {
			if id, ok := obj["id"].(string); ok && id != "" {
				merged["subscription_id"] = id
			}
		}
		// invoice_id
		if strings.HasPrefix(w.Type, "invoice.") {
			if id, ok := obj["id"].(string); ok && id != "" {
				merged["invoice_id"] = id
			}
		}
	}

	amountCents, currency := pickAmountAndCurrency(w.Data.Object)

	return webhook.StripeEvent{
		ID:          w.ID,
		Type:        w.Type,
		Metadata:    merged,
		AmountCents: amountCents,
		Currency:    strings.ToUpper(currency),
		Created:     time.Unix(w.Created, 0).UTC(),
	}, nil
}

// pickAmountAndCurrency reads amount/currency from the supplied data.object
// in Stripe-priority order: amount_captured, amount, amount_due,
// amount_total. Returns 0/"" when none is set.
func pickAmountAndCurrency(obj map[string]any) (int64, string) {
	if obj == nil {
		return 0, ""
	}
	currency, _ := obj["currency"].(string)
	keys := []string{"amount_captured", "amount", "amount_due", "amount_total"}
	for _, k := range keys {
		if v, ok := obj[k]; ok {
			if i, ok := toInt64(v); ok && i > 0 {
				return i, currency
			}
		}
	}
	return 0, currency
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}

// -----------------------------------------------------------------------------
// JSON helpers
// -----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"code": code, "message": message})
}
