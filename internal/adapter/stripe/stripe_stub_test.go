// Package stripestub_test holds the RED-phase TDD specs for the Stripe stub
// client. Per the brief: returns a `mock_token` string; NO real Stripe API
// call. The stub URL comes from STRIPE_API_URL env var (no inline config).
package stripestub_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	stripestub "github.com/apollo-chora/chora-tenancy/internal/adapter/stripe"
)

func TestStripeStubClient_AttachPaymentMethod_ReturnsMockToken(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripestub.invalid/v1"})
	tok, err := c.AttachPaymentMethod(context.Background(), "tenant-1", "pm_test_card")
	if err != nil {
		t.Fatalf("AttachPaymentMethod: unexpected error: %v", err)
	}
	if !strings.HasPrefix(tok, "mock_token_") {
		t.Fatalf("expected mock_token_ prefix, got %q", tok)
	}
}

func TestStripeStubClient_RejectsEmptyAPIURL(t *testing.T) {
	t.Parallel()
	if _, err := stripestub.New(stripestub.Config{APIURL: ""}).AttachPaymentMethod(
		context.Background(), "tenant-1", "pm_card",
	); err == nil {
		t.Fatalf("expected error when STRIPE_API_URL not set")
	}
}

func TestStripeStubClient_RejectsEmptyArguments(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripestub.invalid/v1"})
	if _, err := c.AttachPaymentMethod(context.Background(), "", "pm_x"); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
	if _, err := c.AttachPaymentMethod(context.Background(), "t1", ""); err == nil {
		t.Fatalf("expected error for empty stripe payment_method_id")
	}
}

// =============================================================================
// CreateCustomer — Phyllis MVP §5.2 / S2.2 Tenancy go-live wiring.
// =============================================================================

func TestStripeStubClient_CreateCustomer_ReturnsCusPrefix(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripestub.invalid/v1"})
	res, err := c.CreateCustomer(context.Background(), stripestub.CreateCustomerInput{
		TenantID:    "tenant-1",
		DisplayName: "MTM Singapore",
		Country:     "SG",
		Currency:    "SGD",
		Email:       "phyllis@mtm.sg",
	})
	if err != nil {
		t.Fatalf("CreateCustomer: unexpected error: %v", err)
	}
	if !strings.HasPrefix(res.CustomerID, "cus_") {
		t.Fatalf("expected cus_ prefix, got %q", res.CustomerID)
	}
	if res.LiveMode {
		t.Fatalf("stub must always return LiveMode=false")
	}
}

func TestStripeStubClient_CreateCustomer_RejectsEmptyAPIURL(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: ""})
	if _, err := c.CreateCustomer(context.Background(), stripestub.CreateCustomerInput{
		TenantID: "t-1", DisplayName: "X", Country: "SG", Currency: "SGD",
	}); err == nil {
		t.Fatalf("expected error when STRIPE_API_URL not set")
	}
}

func TestStripeStubClient_CreateCustomer_RejectsEmptyArguments(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripestub.invalid/v1"})
	cases := []stripestub.CreateCustomerInput{
		{DisplayName: "X", Country: "SG", Currency: "SGD"},   // missing tenant
		{TenantID: "t-1", Country: "SG", Currency: "SGD"},    // missing display
		{TenantID: "t-1", DisplayName: "X", Currency: "SGD"}, // missing country
		{TenantID: "t-1", DisplayName: "X", Country: "SG"},   // missing currency
	}
	for i, in := range cases {
		if _, err := c.CreateCustomer(context.Background(), in); err == nil {
			t.Fatalf("case %d: expected error for missing required field", i)
		}
	}
}

// Idempotency: calling CreateCustomer twice with the same TenantID returns
// the same CustomerID (so go-live is safe to retry).
func TestStripeStubClient_CreateCustomer_IsIdempotentByTenantID(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripestub.invalid/v1"})
	in := stripestub.CreateCustomerInput{
		TenantID: "tenant-1", DisplayName: "MTM", Country: "SG", Currency: "SGD",
	}
	first, err := c.CreateCustomer(context.Background(), in)
	if err != nil {
		t.Fatalf("first CreateCustomer: %v", err)
	}
	second, err := c.CreateCustomer(context.Background(), in)
	if err != nil {
		t.Fatalf("second CreateCustomer: %v", err)
	}
	if first.CustomerID != second.CustomerID {
		t.Fatalf("expected idempotent customer_id, got %s != %s", first.CustomerID, second.CustomerID)
	}
}

func TestStripeStubClient_ProrateChange(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: "https://stripe.invalid/v1"})
	// create_prorations mode.
	res, err := c.ProrateChange(context.Background(), 4900, 9900, "create_prorations", true)
	if err != nil {
		t.Fatalf("ProrateChange: %v", err)
	}
	if res.BillingDeltaCents != 5000 {
		t.Fatalf("expected delta 5000, got %d", res.BillingDeltaCents)
	}
	if !strings.HasPrefix(res.ScheduleID, "sched_mock_") {
		t.Fatalf("expected schedule id when scheduled, got %q", res.ScheduleID)
	}
	// none mode + non-scheduled.
	res2, err := c.ProrateChange(context.Background(), 4900, 9900, "none", false)
	if err != nil {
		t.Fatalf("ProrateChange(none): %v", err)
	}
	if res2.BillingDeltaCents != 0 || res2.ScheduleID != "" {
		t.Fatalf("unexpected none-mode result %+v", res2)
	}
	// Unknown mode → ErrInvalidArgument.
	if _, err := c.ProrateChange(context.Background(), 1, 2, "bogus", false); !errors.Is(err, stripestub.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
	// Empty API URL.
	c2 := stripestub.New(stripestub.Config{})
	if _, err := c2.ProrateChange(context.Background(), 1, 2, "create_prorations", false); !errors.Is(err, stripestub.ErrAPIURLMissing) {
		t.Fatalf("expected ErrAPIURLMissing, got %v", err)
	}
}
