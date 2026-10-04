// Unit tests for the column-mapping helpers. These exercise the
// pure-Go transform without needing a live DB — the only behaviour to
// verify is that the args slice matches the canonical column count +
// the state / refund_reason enum mapping is correct.
package main

import (
	"strings"
	"testing"
	"time"
)

func TestMapEggStateToPayments(t *testing.T) {
	cases := map[string]string{
		"checkout_started": "checkout_started",
		"paid":             "payment_captured",
		"payment_failed":   "payment_failed",
		"provisioned":      "provisioned",
		"refunded":         "refunded",
		"expired":          "expired",
		"  PAID ":          "payment_captured",
	}
	for in, want := range cases {
		got, err := MapEggStateToPayments(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("MapEggStateToPayments(%q) = %q want %q", in, got, want)
		}
	}
	if _, err := MapEggStateToPayments("bogus"); err == nil {
		t.Fatalf("expected error for unknown state")
	}
}

func TestMapEggStateToTenancy(t *testing.T) {
	cases := map[string]string{
		"checkout_started": "checkout_started",
		"payment_captured": "paid",
		"payment_failed":   "payment_failed",
		"provisioned":      "provisioned",
		"refunded":         "refunded",
		"expired":          "expired",
	}
	for in, want := range cases {
		got, err := MapEggStateToTenancy(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("MapEggStateToTenancy(%q) = %q want %q", in, got, want)
		}
	}
	if _, err := MapEggStateToTenancy("nope"); err == nil {
		t.Fatalf("expected error for unknown canonical state")
	}
}

func TestMapEggRefundReason(t *testing.T) {
	cases := map[string]string{
		"":                      "",
		"hard_expiry_unhatched": "expired_unhatched",
		"tenant_policy":         "support_initiated",
		"support_initiated":     "support_initiated",
		"customer_request":      "customer_request",
		"duplicate_charge":      "duplicate_charge",
		"fraud":                 "fraud",
	}
	for in, want := range cases {
		if got := MapEggRefundReason(in); got != want {
			t.Errorf("MapEggRefundReason(%q) = %q want %q", in, got, want)
		}
	}
}

func TestMapEggRowToPaymentsArgs_RowCount(t *testing.T) {
	now := time.Now().UTC()
	row := EggRow{
		PurchaseID:           "11111111-1111-7111-8111-111111111111",
		TenantID:             "tenant-1",
		PurchaserGCID:        "gcid-1",
		EggSKU:               "egg.standard.v1",
		SuggestedFocalAtomID: "atom-1",
		State:                "paid",
		AmountCents:          999, AmountCentsPaid: 999, AmountCentsRefunded: 0,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_1",
		RefundCreditOnly:  false,
		CheckoutStartedAt: now, PaidAt: &now,
		SoftExpiryAt: now.Add(30 * 24 * time.Hour),
		HardExpiryAt: now.Add(60 * 24 * time.Hour),
		CreatedAt:    now, UpdatedAt: now,
	}
	args, err := MapEggRowToPaymentsArgs(row)
	if err != nil {
		t.Fatalf("map err: %v", err)
	}
	// Count $1..$N placeholders in SourceInsertEggSQL VALUES clause.
	wantArgs := strings.Count(SourceInsertEggSQL, "$")
	if len(args) != wantArgs {
		t.Fatalf("arg count mismatch: got %d want %d (placeholders in SQL)", len(args), wantArgs)
	}
	// state should be mapped to payment_captured.
	if got := args[5].(string); got != "payment_captured" {
		t.Errorf("state not mapped: %q", got)
	}
}

func TestMapEggRowToPaymentsArgs_NilNullables(t *testing.T) {
	now := time.Now().UTC()
	row := EggRow{
		PurchaseID:           "11111111-1111-7111-8111-111111111111",
		TenantID:             "tenant-1",
		PurchaserGCID:        "gcid-1",
		EggSKU:               "egg.standard.v1",
		SuggestedFocalAtomID: "", // empty → nil
		State:                "checkout_started",
		AmountCents:          999, AmountCentsPaid: 0,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_2",
		CheckoutStartedAt: now,
		SoftExpiryAt:      now.Add(30 * 24 * time.Hour),
		HardExpiryAt:      now.Add(60 * 24 * time.Hour),
		CreatedAt:         now, UpdatedAt: now,
	}
	args, err := MapEggRowToPaymentsArgs(row)
	if err != nil {
		t.Fatalf("map err: %v", err)
	}
	// suggested_focal_atom_id (5th positional, idx 4) should be nil.
	if args[4] != nil {
		t.Errorf("expected nil suggested_focal_atom_id, got %v", args[4])
	}
	// stripe_payment_intent_id (idx 11) should be nil.
	if args[11] != nil {
		t.Errorf("expected nil stripe_payment_intent_id, got %v", args[11])
	}
}

func TestMapEggRowToPaymentsArgs_PropagatesStateError(t *testing.T) {
	row := EggRow{State: "garbage"}
	if _, err := MapEggRowToPaymentsArgs(row); err == nil {
		t.Fatalf("expected state error")
	}
}

func TestMapManaTopUpRowToPaymentsArgs_RowCount(t *testing.T) {
	now := time.Now().UTC()
	row := ManaTopUpRow{
		TopupID:               "22222222-2222-7222-8222-222222222222",
		PoolID:                "pool-1",
		TenantID:              "tenant-1",
		UnitsCredited:         10_000,
		ChargedCents:          9999,
		Currency:              "SGD",
		StripePaymentIntentID: "pi_legacy_1",
		AutoRenew:             false,
		ToppedUpByGCID:        "gcid-admin",
		BalanceAfterUnits:     10_000,
		RecordedAt:            now,
		CreatedAt:             now,
	}
	args := MapManaTopUpRowToPaymentsArgs(row, "mana.tenant_topup.standard_v1")
	wantArgs := strings.Count(SourceInsertManaTopUpSQL, "$")
	if len(args) != wantArgs {
		t.Fatalf("arg count mismatch: got %d want %d", len(args), wantArgs)
	}
	// sku (3rd arg, idx 3).
	if got := args[3].(string); got != "mana.tenant_topup.standard_v1" {
		t.Errorf("sku not propagated: %q", got)
	}
	// admin_gcid (idx 2) — real value passed through.
	if got := args[2].(string); got != "gcid-admin" {
		t.Errorf("admin_gcid not propagated: %q", got)
	}
	// stripe_session_id (idx 10) should be the reconstructed pseudo-id.
	if got := args[10].(string); got != "cs_legacy_pi_legacy_1" {
		t.Errorf("stripe_session_id reconstruction wrong: %q", got)
	}
}

func TestMapManaTopUpRowToPaymentsArgs_SentinelGCID(t *testing.T) {
	now := time.Now().UTC()
	row := ManaTopUpRow{
		TopupID: "22222222-2222-7222-8222-222222222222", TenantID: "tenant-1",
		UnitsCredited:         5,
		ChargedCents:          5,
		Currency:              "SGD",
		StripePaymentIntentID: "pi_auto",
		AutoRenew:             true,
		ToppedUpByGCID:        "", // legacy auto-renew row → sentinel
		RecordedAt:            now, CreatedAt: now,
	}
	args := MapManaTopUpRowToPaymentsArgs(row, "")
	if got := args[2].(string); got != SentinelGCID {
		t.Errorf("expected sentinel admin_gcid, got %q", got)
	}
	if got := args[3].(string); got != "mana.tenant_topup.legacy_v1" {
		t.Errorf("default SKU not applied: %q", got)
	}
}
