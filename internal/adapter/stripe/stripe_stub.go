// Package stripestub is the Stripe payment adapter — STUB only.
//
// Per CLAUDE.md §6 + secrets-and-env: the Stripe API URL + secret key flow
// from the environment via STRIPE_API_URL + STRIPE_API_KEY env vars. The stub
// implements the same interface as the real adapter so
// go-live + payment-method + proration + customer create are testable
// without an external Stripe sandbox.
//
// Real Stripe wiring is M14 — this stub returns deterministic mock IDs
// (cus_mock_N, sched_mock_N, mock_token_N) and an idempotent in-memory
// per-tenant customer cache so go-live retries return the same CustomerID.
package stripestub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrAPIURLMissing   = errors.New("STRIPE_API_URL is required (no inline config)")
	ErrInvalidArgument = errors.New("invalid argument")
)

type Config struct {
	APIURL string
}

type Client struct {
	cfg Config
	seq atomic.Int64

	// per-tenant customer idempotency cache. Stripe's real
	// `idempotency_key` header is keyed on the tenant ID so a duplicate
	// /golive call returns the same Customer object.
	muCust    sync.Mutex
	customers map[string]string // tenantID → customer_id

	// Subscription Schedule store (S6.2 — A-Tenant-Lifecycle).
	muSched   sync.Mutex
	schedules *scheduleStore
}

func New(cfg Config) *Client {
	return &Client{cfg: cfg, customers: make(map[string]string)}
}

func (c *Client) AttachPaymentMethod(ctx context.Context, tenantID, stripePaymentMethodID string) (string, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return "", ErrAPIURLMissing
	}
	if strings.TrimSpace(tenantID) == "" {
		return "", fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(stripePaymentMethodID) == "" {
		return "", fmt.Errorf("%w: stripe_payment_method_id required", ErrInvalidArgument)
	}
	n := c.seq.Add(1)
	return fmt.Sprintf("mock_token_%d", n), nil
}

// CreateCustomerInput captures the fields the real Stripe POST /v1/customers
// endpoint consumes. Per Phyllis MVP §5.2, the tenancy GoLive call passes
// tenant_id (= idempotency key) + display_name + country + currency.
type CreateCustomerInput struct {
	TenantID    string
	DisplayName string
	Country     string // ISO 3166-1 alpha-2 (e.g. "SG")
	Currency    string // ISO 4217 (e.g. "SGD")
	Email       string // optional — admin GCID's email when known
}

// CreateCustomerResult mirrors the relevant Stripe response fields.
type CreateCustomerResult struct {
	CustomerID string
	LiveMode   bool
}

// CreateCustomer creates (or returns the cached) Stripe Customer for the
// supplied tenant. Idempotent: a second call with the same TenantID
// returns the same CustomerID without re-invoking Stripe. Real Stripe
// wiring (M14) will hit /v1/customers with `Stripe-Account` + bearer
// `STRIPE_API_KEY` + `Idempotency-Key: chora.tenancy.golive.<tenant_id>`.
func (c *Client) CreateCustomer(ctx context.Context, in CreateCustomerInput) (*CreateCustomerResult, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return nil, ErrAPIURLMissing
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.DisplayName) == "" {
		return nil, fmt.Errorf("%w: display_name required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.Country) == "" {
		return nil, fmt.Errorf("%w: country required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.Currency) == "" {
		return nil, fmt.Errorf("%w: currency required", ErrInvalidArgument)
	}
	c.muCust.Lock()
	defer c.muCust.Unlock()
	if id, ok := c.customers[in.TenantID]; ok {
		return &CreateCustomerResult{CustomerID: id, LiveMode: false}, nil
	}
	n := c.seq.Add(1)
	id := fmt.Sprintf("cus_mock_%d", n)
	c.customers[in.TenantID] = id
	return &CreateCustomerResult{CustomerID: id, LiveMode: false}, nil
}

type ProrateResult struct {
	BillingDeltaCents int64
	ScheduleID        string
}

func (c *Client) ProrateChange(ctx context.Context, currentPriceCents, targetPriceCents int64, mode string, scheduled bool) (*ProrateResult, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return nil, ErrAPIURLMissing
	}
	res := &ProrateResult{}
	switch mode {
	case "create_prorations":
		res.BillingDeltaCents = targetPriceCents - currentPriceCents
	case "none":
		res.BillingDeltaCents = 0
	default:
		return nil, fmt.Errorf("%w: unknown proration mode %q", ErrInvalidArgument, mode)
	}
	if scheduled {
		n := c.seq.Add(1)
		res.ScheduleID = fmt.Sprintf("sched_mock_%d", n)
	}
	return res, nil
}
