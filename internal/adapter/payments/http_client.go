// http_client.go — CHO-1767. HTTP client wrapping the chora-payments
// admin endpoints introduced in CHO-1764 + CHO-1765:
//
//	POST {addr}/api/v1/admin/tenant-addons:change-tier
//	POST {addr}/api/v1/admin/tenant-addons:preview-tier-change
//
// Both endpoints take JSON in/out. The client is a thin wrapper —
// chora-tenancy validates input, then dispatches here; payments owns
// Stripe Subscription.update + Invoice.upcoming.
//
// Wired via CHORA_PAYMENTS_HTTP_ADDR (e.g. http://chora-payments.payments.
// svc.cluster.local:8080 in cluster, http://localhost:8083 for local).
package payments

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPClient calls chora-payments' tenant-addon admin endpoints. The
// interface is defined here so v2_handlers.go can inject a fake.
type HTTPClient interface {
	ChangeTenantAddonTier(ctx context.Context, in ChangeTierIn) (ChangeTierOut, error)
	PreviewTenantAddonTier(ctx context.Context, in PreviewTierIn) (PreviewTierOut, error)
}

// ChangeTierIn mirrors chora-payments' HTTP ChangeTierRequest.
type ChangeTierIn struct {
	TenantID       string `json:"tenant_id"`
	AdminGCID      string `json:"admin_gcid"`
	AddonCode      string `json:"addon_code"`
	TargetTierCode string `json:"target_tier_code"`
	Currency       string `json:"currency"`
	ProrationMode  string `json:"proration_mode"`
	// EffectiveAt — CHO-1772. "" / "immediate" → existing CHO-1764 path;
	// "end_of_cycle" → Stripe SubscriptionSchedule path. The tenancy
	// adapter translates the FE's nullable ISO timestamp into this
	// 3-value string before forwarding.
	EffectiveAt string `json:"effective_at,omitempty"`
}

// ChangeTierOut mirrors chora-payments' HTTP ChangeTierResponse.
type ChangeTierOut struct {
	PurchaseID           string `json:"purchase_id"`
	StripeSubscriptionID string `json:"stripe_subscription_id"`
	FromTier             string `json:"from_tier"`
	ToTier               string `json:"to_tier"`
	BillingDeltaCents    int64  `json:"billing_delta_cents"`
	Currency             string `json:"currency"`
	EffectiveAt          string `json:"effective_at"`
	SubscriptionStatus   string `json:"subscription_status"`
	// CHO-1772 — populated when the end_of_cycle path drove the response.
	DeferredToCycleEnd   bool   `json:"deferred_to_cycle_end"`
	ScheduleID           string `json:"schedule_id,omitempty"`
	ScheduledTierCode    string `json:"scheduled_tier_code,omitempty"`
	ScheduledEffectiveAt string `json:"scheduled_effective_at,omitempty"`
}

// PreviewTierIn mirrors chora-payments' HTTP PreviewTierRequest.
type PreviewTierIn struct {
	TenantID       string `json:"tenant_id"`
	AddonCode      string `json:"addon_code"`
	TargetTierCode string `json:"target_tier_code"`
	Currency       string `json:"currency"`
	ProrationMode  string `json:"proration_mode"`
	// EffectiveAt — CHO-1772, mirrors ChangeTierIn.EffectiveAt.
	EffectiveAt string `json:"effective_at,omitempty"`
}

// PreviewTierOut mirrors chora-payments' HTTP PreviewTierResponse.
type PreviewTierOut struct {
	PurchaseID            string `json:"purchase_id"`
	FromTier              string `json:"from_tier"`
	ToTier                string `json:"to_tier"`
	BillingDeltaCents     int64  `json:"billing_delta_cents"`
	NextInvoiceTotalCents int64  `json:"next_invoice_total_cents"`
	Currency              string `json:"currency"`
	ProrationMode         string `json:"proration_mode"`
	// CHO-1772 — populated when the end_of_cycle path drove the preview.
	DeferredToCycleEnd bool   `json:"deferred_to_cycle_end"`
	EffectiveAt        string `json:"effective_at,omitempty"`
}

// HTTPError carries the structured `{"error", "message"}` shape
// chora-payments returns on 4xx/5xx so the tenancy handler can map
// `no_stripe_subscription` / `stripe_price_missing` / etc. into the
// right FE banner.
type HTTPError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("payments-http %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// Common error codes returned by chora-payments. Tenancy handlers
// errors.Is-check these to drive FE response shape.
var (
	ErrNoStripeSubscription = errors.New("no_stripe_subscription")
	ErrStripePriceMissing   = errors.New("stripe_price_missing")
	ErrTierUnchanged        = errors.New("tier_unchanged")
	ErrAddonNotFound        = errors.New("addon_not_found")
)

// HTTPClientConfig is the HTTPClientImpl constructor input.
type HTTPClientConfig struct {
	Addr    string // base URL (no trailing slash)
	Timeout time.Duration
	HTTP    *http.Client // injectable for tests; defaults to a 10s client
}

// HTTPClientImpl is the production HTTPClient implementation.
type HTTPClientImpl struct {
	addr string
	http *http.Client
}

// NewHTTPClient constructs a client. Empty addr returns an error so
// main.go can fail-loud at boot when the env var isn't set in a path
// that depends on it.
func NewHTTPClient(cfg HTTPClientConfig) (*HTTPClientImpl, error) {
	cfg.Addr = strings.TrimRight(cfg.Addr, "/")
	if cfg.Addr == "" {
		return nil, errors.New("payments: HTTP addr required (set CHORA_PAYMENTS_HTTP_ADDR)")
	}
	httpC := cfg.HTTP
	if httpC == nil {
		t := cfg.Timeout
		if t <= 0 {
			t = 10 * time.Second
		}
		httpC = &http.Client{Timeout: t}
	}
	return &HTTPClientImpl{addr: cfg.Addr, http: httpC}, nil
}

// ChangeTenantAddonTier POSTs to {addr}/api/v1/admin/tenant-addons:change-tier.
func (c *HTTPClientImpl) ChangeTenantAddonTier(ctx context.Context, in ChangeTierIn) (ChangeTierOut, error) {
	var out ChangeTierOut
	if err := c.doJSON(ctx, "/api/v1/admin/tenant-addons:change-tier", in, &out); err != nil {
		return ChangeTierOut{}, err
	}
	return out, nil
}

// PreviewTenantAddonTier POSTs to {addr}/api/v1/admin/tenant-addons:preview-tier-change.
func (c *HTTPClientImpl) PreviewTenantAddonTier(ctx context.Context, in PreviewTierIn) (PreviewTierOut, error) {
	var out PreviewTierOut
	if err := c.doJSON(ctx, "/api/v1/admin/tenant-addons:preview-tier-change", in, &out); err != nil {
		return PreviewTierOut{}, err
	}
	return out, nil
}

func (c *HTTPClientImpl) doJSON(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("payments-http: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("payments-http: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("payments-http: do: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var ed struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &ed)
		httpErr := &HTTPError{
			StatusCode: resp.StatusCode,
			Code:       ed.Error,
			Message:    ed.Message,
		}
		return wrapKnownCode(httpErr)
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("payments-http: decode: %w (body=%s)", err, string(respBody))
		}
	}
	return nil
}

// wrapKnownCode pairs the HTTPError with one of the sentinel errors so
// the caller can `errors.Is(err, payments.ErrNoStripeSubscription)`.
func wrapKnownCode(e *HTTPError) error {
	switch e.Code {
	case "no_stripe_subscription":
		return fmt.Errorf("%w: %s", ErrNoStripeSubscription, e.Message)
	case "stripe_price_missing":
		return fmt.Errorf("%w: %s", ErrStripePriceMissing, e.Message)
	case "tier_unchanged":
		return fmt.Errorf("%w: %s", ErrTierUnchanged, e.Message)
	case "addon_not_found":
		return fmt.Errorf("%w: %s", ErrAddonNotFound, e.Message)
	default:
		return e
	}
}
