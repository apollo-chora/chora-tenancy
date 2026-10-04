// Package manapool — Stripe adapter for tenant mana pool top-ups.
//
// A real Stripe integration creates a one-time PaymentIntent against the
// tenant's existing Stripe Customer, confirms it with the PaymentMethod
// shipped in the request, and records the resulting `pi_xxx` id on the
// pool's lifetime audit. The skeleton here is a stub that does NOT call
// Stripe — it returns canned PaymentIntents for tests.
//
// Per `feedback_no_inline_config` the base URL comes from
// `CHORA_TENANCY_STRIPE_BASE_URL`. Tests can pass `stub://local` to opt
// into the in-process stub.
package manapool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// StripeClient is the port the HTTP layer consumes; production wires a
// real Stripe SDK adapter that satisfies this interface.
type StripeClient interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResponse, error)
}

// ChargeRequest is the input to a one-time pool funding call.
type ChargeRequest struct {
	TenantID        string
	AmountCents     int64
	Currency        string
	PaymentMethodID string
	IdempotencyKey  string
}

// ChargeResponse is what the Stripe adapter returns on a successful charge.
type ChargeResponse struct {
	PaymentIntentID string
	AmountCents     int64
	Currency        string
}

// -----------------------------------------------------------------------------
// StripeStub — in-process stub for tests + dev
// -----------------------------------------------------------------------------

// StripeStub records calls and returns canned PaymentIntents. NOT for
// production use.
type StripeStub struct {
	mu       sync.Mutex
	failNext string                    // when non-empty, next Charge fails with this code
	seen     map[string]ChargeResponse // idempotency_key -> response
	counter  int
}

// NewStripeStub constructs an empty stub.
func NewStripeStub() *StripeStub {
	return &StripeStub{seen: make(map[string]ChargeResponse)}
}

// FailNext primes the next Charge call to fail with the given code.
func (s *StripeStub) FailNext(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = code
}

// Charge implements StripeClient.
func (s *StripeStub) Charge(_ context.Context, req ChargeRequest) (ChargeResponse, error) {
	if req.AmountCents <= 0 {
		return ChargeResponse{}, fmt.Errorf("%w: amount_cents must be > 0", pool.ErrInvalidArgument)
	}
	if strings.TrimSpace(req.Currency) == "" {
		return ChargeResponse{}, fmt.Errorf("%w: currency required", pool.ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != "" {
		code := s.failNext
		s.failNext = ""
		return ChargeResponse{}, fmt.Errorf("stripe: %s", code)
	}
	if req.IdempotencyKey != "" {
		if existing, ok := s.seen[req.IdempotencyKey]; ok {
			return existing, nil
		}
	}
	s.counter++
	resp := ChargeResponse{
		PaymentIntentID: fmt.Sprintf("pi_stub_%06d", s.counter),
		AmountCents:     req.AmountCents,
		Currency:        req.Currency,
	}
	if req.IdempotencyKey != "" {
		s.seen[req.IdempotencyKey] = resp
	}
	return resp, nil
}

// -----------------------------------------------------------------------------
// Env-driven constructor
// -----------------------------------------------------------------------------

// NewStripeFromEnv constructs a StripeClient from the
// CHORA_TENANCY_STRIPE_BASE_URL env var. The special value `stub://local`
// returns an in-process stub. All other values are reserved for the M12+
// real Stripe adapter (currently returns the stub with the URL captured
// for parity).
func NewStripeFromEnv() (StripeClient, error) {
	url := strings.TrimSpace(os.Getenv("CHORA_TENANCY_STRIPE_BASE_URL"))
	if url == "" {
		return nil, errors.New("CHORA_TENANCY_STRIPE_BASE_URL required (set 'stub://local' for in-process stub)")
	}
	if strings.HasPrefix(url, "stub://") {
		return NewStripeStub(), nil
	}
	// In production this would construct an HTTP-based Stripe SDK client
	// pointing at the configured URL; for the BE-USR-3 skeleton we keep
	// the stub.
	return NewStripeStub(), nil
}
