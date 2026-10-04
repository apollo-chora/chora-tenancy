// Package billingstripe is the Stripe Charges API client used by the
// daily reconciliation Cloud Run Job under chora-tenancy.
//
// Ported from services/chora-billing-webhook/internal/adapter/stripe as part
// of the M12.2 Batch-1 consolidation. Package name `billingstripe` (NOT
// `stripe`) avoids colliding with the existing `stripestub` package which
// owns Stripe Subscription Schedule stubs.
//
// Design choices:
//
//   - Thin REST client (net/http) over the Stripe Charges + Balance APIs.
//     The Stripe Go SDK is heavyweight; this MVP only needs aggregated
//     sums for a date window. Production swap-in to the official SDK is
//     drop-in via the StripeFeeder port.
//
//   - No inline config (per .claude/skills/secrets-and-env): URL + key
//     come from env via Config{}. Empty key surfaces as a typed error
//     at call time so wiring failures show up early.
//
//   - Idempotent retries: HTTP 5xx + 429 trigger exponential backoff up to
//     MaxRetries. Stripe's responses are deterministic for read calls.
//
// Aligned with: CLAUDE.md (no inline config) + .claude/skills/secrets-and-env.
package billingstripe

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

	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
)

// Config wires construction-time defaults.
type Config struct {
	// APIBase is the Stripe API base URL. Production: https://api.stripe.com.
	// Tests pass an httptest.Server URL.
	APIBase string

	// SecretKey is the STRIPE_SECRET_KEY env value (Secret Manager-sourced).
	// Empty values are rejected at call time.
	SecretKey string

	// HTTPClient is injectable; defaults to http.DefaultClient with a 30s
	// timeout when nil.
	HTTPClient *http.Client

	// MaxRetries caps the per-call retry count; 0 → 3.
	MaxRetries int

	// RetryBaseDelay caps the exponential-backoff base; 0 → 100ms.
	RetryBaseDelay time.Duration
}

// Client is the Stripe API client. Implements reconciliation.StripeFeeder.
type Client struct {
	cfg Config
}

// New constructs a Client with sane defaults.
func New(cfg Config) *Client {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBaseDelay == 0 {
		cfg.RetryBaseDelay = 100 * time.Millisecond
	}
	return &Client{cfg: cfg}
}

// Sentinel errors.
var (
	// ErrNoAPIBase is returned when Config.APIBase is empty.
	ErrNoAPIBase = errors.New("billingstripe: APIBase required (no inline config)")
	// ErrNoSecretKey is returned when Config.SecretKey is empty.
	ErrNoSecretKey = errors.New("billingstripe: SecretKey required (Secret Manager)")
)

// FetchCapturedCentsForDay fetches the sum of `amount_captured` across
// successful charges for the supplied UTC day.
//
// Stripe REST: GET /v1/charges?created[gte]=<unix-start>&created[lt]=<unix-end>
// Pagination: cursor-based via `starting_after`. Up to 100 per page.
func (c *Client) FetchCapturedCentsForDay(ctx context.Context, day time.Time) (int64, error) {
	if strings.TrimSpace(c.cfg.APIBase) == "" {
		return 0, ErrNoAPIBase
	}
	if strings.TrimSpace(c.cfg.SecretKey) == "" {
		return 0, ErrNoSecretKey
	}
	startUTC := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	endUTC := startUTC.Add(24 * time.Hour)

	var total int64
	startingAfter := ""
	for {
		page, hasMore, lastID, err := c.fetchChargesPage(ctx, startUTC, endUTC, startingAfter)
		if err != nil {
			return 0, err
		}
		for _, ch := range page {
			if ch.Status == "succeeded" && ch.Captured {
				total += ch.AmountCaptured
			}
		}
		if !hasMore || lastID == "" {
			break
		}
		startingAfter = lastID
	}
	return total, nil
}

// chargesPageResponse mirrors the Stripe /v1/charges list-endpoint shape.
type chargesPageResponse struct {
	Object  string         `json:"object"`
	Data    []stripeCharge `json:"data"`
	HasMore bool           `json:"has_more"`
}

type stripeCharge struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	Captured       bool   `json:"captured"`
	AmountCaptured int64  `json:"amount_captured"`
	Currency       string `json:"currency"`
	Created        int64  `json:"created"`
}

func (c *Client) fetchChargesPage(ctx context.Context, startUTC, endUTC time.Time, startingAfter string) ([]stripeCharge, bool, string, error) {
	q := url.Values{}
	q.Set("created[gte]", strconv.FormatInt(startUTC.Unix(), 10))
	q.Set("created[lt]", strconv.FormatInt(endUTC.Unix(), 10))
	q.Set("limit", "100")
	if startingAfter != "" {
		q.Set("starting_after", startingAfter)
	}
	u := strings.TrimRight(c.cfg.APIBase, "/") + "/v1/charges?" + q.Encode()

	body, err := c.doRequestWithRetry(ctx, "GET", u, nil)
	if err != nil {
		return nil, false, "", err
	}
	var resp chargesPageResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, false, "", fmt.Errorf("billingstripe: decode charges page: %w", err)
	}
	lastID := ""
	if n := len(resp.Data); n > 0 {
		lastID = resp.Data[n-1].ID
	}
	return resp.Data, resp.HasMore, lastID, nil
}

// doRequestWithRetry performs an HTTP call with exponential backoff on
// 429 and 5xx.
func (c *Client) doRequestWithRetry(ctx context.Context, method, u string, body io.Reader) ([]byte, error) {
	delay := c.cfg.RetryBaseDelay
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxRetries+1; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u, body)
		if err != nil {
			return nil, fmt.Errorf("billingstripe: build request: %w", err)
		}
		req.SetBasicAuth(c.cfg.SecretKey, "")
		req.Header.Set("Stripe-Version", "2024-11-20.acacia")
		req.Header.Set("Accept", "application/json")
		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("billingstripe: do %s %s: %w", method, u, err)
			if attempt > c.cfg.MaxRetries {
				return nil, lastErr
			}
			if !sleepCtx(ctx, delay) {
				return nil, ctx.Err()
			}
			delay *= 2
			continue
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("billingstripe: read body: %w", readErr)
			if attempt > c.cfg.MaxRetries {
				return nil, lastErr
			}
			if !sleepCtx(ctx, delay) {
				return nil, ctx.Err()
			}
			delay *= 2
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return respBody, nil
		}
		// Retry on 429 + 5xx.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("billingstripe: %s %s: status %d body=%s", method, u, resp.StatusCode, string(respBody))
			if attempt > c.cfg.MaxRetries {
				return nil, lastErr
			}
			if !sleepCtx(ctx, delay) {
				return nil, ctx.Err()
			}
			delay *= 2
			continue
		}
		return nil, fmt.Errorf("billingstripe: %s %s: status %d body=%s", method, u, resp.StatusCode, string(respBody))
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("billingstripe: retries exhausted")
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Compile-time port check.
var _ reconciliation.StripeFeeder = (*Client)(nil)
