package manapool_test

import (
	"context"
	"testing"

	manapool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
)

// -----------------------------------------------------------------------------
// Stripe stub
// -----------------------------------------------------------------------------

func TestStripeStub_Charge_ReturnsPaymentIntentID(t *testing.T) {
	t.Parallel()
	s := manapool.NewStripeStub()
	res, err := s.Charge(context.Background(), manapool.ChargeRequest{
		TenantID:        tenantA,
		AmountCents:     1000,
		Currency:        "USD",
		PaymentMethodID: "pm_test_card",
		IdempotencyKey:  "topup-1",
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if res.PaymentIntentID == "" {
		t.Fatalf("expected non-empty payment_intent_id")
	}
	if res.AmountCents != 1000 {
		t.Fatalf("expected echoed amount")
	}
	if res.Currency != "USD" {
		t.Fatalf("currency mismatch")
	}
}

func TestStripeStub_Charge_RejectsZeroAmount(t *testing.T) {
	t.Parallel()
	s := manapool.NewStripeStub()
	if _, err := s.Charge(context.Background(), manapool.ChargeRequest{
		TenantID: tenantA, AmountCents: 0, Currency: "USD",
	}); err == nil {
		t.Fatalf("expected error for zero amount")
	}
}

func TestStripeStub_Charge_IdempotencyDedup(t *testing.T) {
	t.Parallel()
	s := manapool.NewStripeStub()
	r1, err := s.Charge(context.Background(), manapool.ChargeRequest{
		TenantID: tenantA, AmountCents: 1000, Currency: "USD", IdempotencyKey: "topup-1",
	})
	if err != nil {
		t.Fatalf("Charge#1: %v", err)
	}
	r2, err := s.Charge(context.Background(), manapool.ChargeRequest{
		TenantID: tenantA, AmountCents: 1000, Currency: "USD", IdempotencyKey: "topup-1",
	})
	if err != nil {
		t.Fatalf("Charge#2: %v", err)
	}
	if r1.PaymentIntentID != r2.PaymentIntentID {
		t.Fatalf("idempotent re-charge should return same payment_intent_id")
	}
}

// FailNextCharge lets tests simulate a Stripe declination.
func TestStripeStub_FailNextCharge_PropagatesError(t *testing.T) {
	t.Parallel()
	s := manapool.NewStripeStub()
	s.FailNext("card_declined")
	_, err := s.Charge(context.Background(), manapool.ChargeRequest{
		TenantID: tenantA, AmountCents: 500, Currency: "USD",
	})
	if err == nil {
		t.Fatalf("expected declined error")
	}
}

// Resolve URL from env (no inline config).
func TestNewStripeFromEnv_RequiresURL(t *testing.T) {
	t.Setenv("CHORA_TENANCY_STRIPE_BASE_URL", "")
	_, err := manapool.NewStripeFromEnv()
	if err == nil {
		t.Fatalf("expected error when CHORA_TENANCY_STRIPE_BASE_URL is empty")
	}
}

func TestNewStripeFromEnv_StubModeAcceptsStubURL(t *testing.T) {
	t.Setenv("CHORA_TENANCY_STRIPE_BASE_URL", "stub://local")
	s, err := manapool.NewStripeFromEnv()
	if err != nil {
		t.Fatalf("expected stub mode to succeed: %v", err)
	}
	if s == nil {
		t.Fatalf("nil stub")
	}
}

func TestNewStripeFromEnv_MissingURL(t *testing.T) {
	t.Setenv("CHORA_TENANCY_STRIPE_BASE_URL", "")
	if _, err := manapool.NewStripeFromEnv(); err == nil {
		t.Fatal("expected error when env missing")
	}
}

func TestNewStripeFromEnv_StubURL(t *testing.T) {
	t.Setenv("CHORA_TENANCY_STRIPE_BASE_URL", "stub://local")
	c, err := manapool.NewStripeFromEnv()
	if err != nil {
		t.Fatalf("NewStripeFromEnv: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewStripeFromEnv_OtherURL(t *testing.T) {
	t.Setenv("CHORA_TENANCY_STRIPE_BASE_URL", "https://stripe.example.com")
	c, err := manapool.NewStripeFromEnv()
	if err != nil {
		t.Fatalf("NewStripeFromEnv: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}
