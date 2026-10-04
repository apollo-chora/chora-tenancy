// Package main — column-mapping helpers for ADR-164 Wave 1 Stage D
// one-shot extract from chora_tenancy → chora_payments.
//
// The mapping helpers live in their own file (separate from main()) so
// they can be unit-tested without a live DB. The .down direction reverses
// the mapping; both are idempotent on the source-side primary key.
//
// chora_tenancy.familiar_egg_purchases    →    chora_payments.familiar_egg_purchases
// chora_tenancy.tenant_mana_pool_topups   →    chora_payments.tenant_mana_topups
package main

import (
	"fmt"
	"strings"
	"time"
)

// EggRow mirrors the chora_tenancy.familiar_egg_purchases row shape.
type EggRow struct {
	PurchaseID            string
	TenantID              string
	PurchaserGCID         string
	EggSKU                string
	SuggestedFocalAtomID  string // nullable in source — empty = NULL
	State                 string // source legacy values: checkout_started|paid|payment_failed|provisioned|refunded|expired
	AmountCents           int64
	AmountCentsPaid       int64
	AmountCentsRefunded   int64
	Currency              string
	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	StripeRefundID        string
	StripeCheckoutURL     string
	StripeFailureCode     string
	StripeFailureMessage  string
	RefundReason          string
	RefundCreditOnly      bool
	CheckoutStartedAt     time.Time
	PaidAt                *time.Time
	FailedAt              *time.Time
	ProvisionedAt         *time.Time
	RefundedAt            *time.Time
	ExpiredAt             *time.Time
	SoftExpiryAt          time.Time
	HardExpiryAt          time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// ManaTopUpRow mirrors the chora_tenancy.tenant_mana_pool_topups row
// shape (the audit-only append-only Stripe-correlation table).
type ManaTopUpRow struct {
	TopupID               string
	PoolID                string
	TenantID              string
	UnitsCredited         int64
	ChargedCents          int64
	Currency              string
	StripePaymentIntentID string
	AutoRenew             bool
	ToppedUpByGCID        string // nullable for auto-renew — empty = NULL
	BalanceAfterUnits     int64
	EventEnvelopeID       string // nullable
	Traceparent           string // nullable
	RecordedAt            time.Time
	CreatedAt             time.Time
}

// MapEggStateToPayments translates the chora_tenancy legacy state enum
// to the canonical chora_payments PurchaseState wire vocabulary.
//
// Notable rename: chora_tenancy used `paid` for the post-Stripe-capture
// state. chora_payments uses `payment_captured`. The `provisioned` state
// is preserved (it's a payments-side terminal, not a Stripe-side
// transition).
func MapEggStateToPayments(legacy string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(legacy)) {
	case "checkout_started":
		return "checkout_started", nil
	case "paid":
		return "payment_captured", nil
	case "payment_failed":
		return "payment_failed", nil
	case "provisioned":
		return "provisioned", nil
	case "refunded":
		return "refunded", nil
	case "expired":
		return "expired", nil
	default:
		return "", fmt.Errorf("extract: unknown egg state %q", legacy)
	}
}

// MapEggStateToTenancy is the reverse mapping (Stage G rollback).
func MapEggStateToTenancy(canonical string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(canonical)) {
	case "checkout_started":
		return "checkout_started", nil
	case "payment_captured":
		return "paid", nil
	case "payment_failed":
		return "payment_failed", nil
	case "provisioned":
		return "provisioned", nil
	case "refunded":
		return "refunded", nil
	case "expired":
		return "expired", nil
	default:
		return "", fmt.Errorf("extract: unknown canonical state %q", canonical)
	}
}

// MapEggRefundReason maps the chora_tenancy refund_reason enum to the
// chora_payments refund_reason enum. Legacy chora_tenancy had a richer
// set (`hard_expiry_unhatched`, `tenant_policy`); the canonical chora_
// payments enum is smaller (no `tenant_policy`).
//
// hard_expiry_unhatched → expired_unhatched (canonical name)
// tenant_policy         → support_initiated (closest semantic match)
// All others pass through unchanged.
func MapEggRefundReason(legacy string) string {
	switch strings.ToLower(strings.TrimSpace(legacy)) {
	case "":
		return ""
	case "hard_expiry_unhatched":
		return "expired_unhatched"
	case "tenant_policy":
		return "support_initiated"
	default:
		return legacy
	}
}

// SourceInsertEggSQL is the chora_payments.familiar_egg_purchases INSERT
// statement the extract binary runs after pulling rows from
// chora_tenancy.familiar_egg_purchases. ON CONFLICT DO NOTHING makes
// re-runs safe (idempotent on stripe_session_id UNIQUE).
const SourceInsertEggSQL = `
INSERT INTO familiar_egg_purchases (
    purchase_id, tenant_id, learner_gcid, egg_sku,
    suggested_focal_atom_id,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code,
    stripe_failure_message,
    refund_reason, refund_credit_only,
    soft_expiry_at, hard_expiry_at,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    provisioned_at, created_at, updated_at
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
    $20,$21,$22,$23,$24,$25,$26,$27,$28,$29
)
ON CONFLICT (purchase_id) DO NOTHING
`

// SourceInsertManaTopUpSQL is the chora_payments.tenant_mana_topups INSERT
// statement. chora_payments lacks the legacy `pool_id` + `balance_after_
// units` audit fields — those stay with chora_tenancy.tenant_mana_pools
// canonical balance state. The `units_credited` legacy column maps to
// `mana_units` on the canonical side.
//
// admin_gcid is the new mandatory column; the legacy `topped_up_by_gcid`
// maps directly. For auto-renew rows where the legacy column was NULL,
// the extract binary substitutes a sentinel "00000000-0000-0000-0000-
// 000000000000" UUID to satisfy the NOT NULL constraint. Operators can
// post-process these rows if needed.
const SourceInsertManaTopUpSQL = `
INSERT INTO tenant_mana_topups (
    purchase_id, tenant_id, admin_gcid, sku,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    mana_units,
    stripe_session_id, stripe_payment_intent_id,
    checkout_started_at, paid_at, created_at, updated_at
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
)
ON CONFLICT (purchase_id) DO NOTHING
`

// SourceSelectEggSQL is the chora_tenancy.familiar_egg_purchases SELECT.
const SourceSelectEggSQL = `
SELECT purchase_id, tenant_id, purchaser_gcid, egg_sku,
       COALESCE(suggested_focal_atom_id::text, ''),
       state,
       amount_cents, amount_cents_paid, amount_cents_refunded, currency,
       stripe_session_id,
       COALESCE(stripe_payment_intent_id, ''),
       COALESCE(stripe_charge_id, ''),
       COALESCE(stripe_refund_id, ''),
       COALESCE(stripe_checkout_url, ''),
       COALESCE(stripe_failure_code, ''),
       COALESCE(stripe_failure_message, ''),
       COALESCE(refund_reason, ''), refund_credit_only,
       checkout_started_at, paid_at, failed_at, provisioned_at,
       refunded_at, expired_at,
       soft_expiry_at, hard_expiry_at,
       created_at, updated_at
  FROM familiar_egg_purchases
 ORDER BY created_at ASC
`

// SourceSelectManaTopUpSQL is the chora_tenancy.tenant_mana_pool_topups SELECT.
const SourceSelectManaTopUpSQL = `
SELECT topup_id, pool_id, tenant_id, units_credited, charged_cents, currency,
       stripe_payment_intent_id, auto_renew,
       COALESCE(topped_up_by_gcid::text, ''),
       balance_after_units,
       COALESCE(event_envelope_id::text, ''),
       COALESCE(traceparent, ''),
       recorded_at, created_at
  FROM tenant_mana_pool_topups
 ORDER BY created_at ASC
`

// SentinelGCID is the UUID used to satisfy chora_payments.tenant_mana_topups
// admin_gcid NOT NULL when the legacy chora_tenancy auto-renew row had NULL
// topped_up_by_gcid. Operators can post-process / pseudonymise these
// "system" rows during the closure saga audit.
const SentinelGCID = "00000000-0000-0000-0000-000000000000"

// MapEggRowToPaymentsArgs converts an EggRow into the positional
// argument slice for SourceInsertEggSQL. Refund reason is canonicalised
// via MapEggRefundReason; state is canonicalised via MapEggStateToPayments.
func MapEggRowToPaymentsArgs(in EggRow) ([]any, error) {
	state, err := MapEggStateToPayments(in.State)
	if err != nil {
		return nil, err
	}
	reason := MapEggRefundReason(in.RefundReason)

	var focal any
	if strings.TrimSpace(in.SuggestedFocalAtomID) != "" {
		focal = in.SuggestedFocalAtomID
	}
	var reasonArg any
	if reason != "" {
		reasonArg = reason
	}
	return []any{
		in.PurchaseID, in.TenantID, in.PurchaserGCID, in.EggSKU,
		focal,
		state,
		in.AmountCents, in.AmountCentsPaid, in.AmountCentsRefunded, in.Currency,
		in.StripeSessionID,
		nilIfEmpty(in.StripePaymentIntentID),
		nilIfEmpty(in.StripeChargeID),
		nilIfEmpty(in.StripeRefundID),
		nilIfEmpty(in.StripeCheckoutURL),
		nilIfEmpty(in.StripeFailureCode),
		nilIfEmpty(in.StripeFailureMessage),
		reasonArg, in.RefundCreditOnly,
		in.SoftExpiryAt, in.HardExpiryAt,
		in.CheckoutStartedAt, nilTimeIfNil(in.PaidAt),
		nilTimeIfNil(in.FailedAt), nilTimeIfNil(in.RefundedAt),
		nilTimeIfNil(in.ExpiredAt), nilTimeIfNil(in.ProvisionedAt),
		in.CreatedAt, in.UpdatedAt,
	}, nil
}

// MapManaTopUpRowToPaymentsArgs converts a legacy tenant_mana_pool_topups
// row into the positional argument slice for SourceInsertManaTopUpSQL.
//
// The legacy table is audit-only (no FSM state); the extracted
// chora_payments.tenant_mana_topups row is created in the terminal
// `payment_captured` state with paid_at set to the legacy recorded_at.
// The Stripe session_id is reconstructed from the payment_intent_id —
// real Stripe Checkout sessions had a session ID but the legacy
// `tenant_mana_pool_topups` audit row did not persist it. The
// reconstructed pseudo-session-id is `cs_legacy_<payment_intent_id>` so
// the UNIQUE constraint on chora_payments.tenant_mana_topups.stripe_
// session_id is satisfied without falsifying real Stripe handles.
func MapManaTopUpRowToPaymentsArgs(in ManaTopUpRow, defaultSKU string) []any {
	adminGcid := strings.TrimSpace(in.ToppedUpByGCID)
	if adminGcid == "" {
		adminGcid = SentinelGCID
	}
	sku := defaultSKU
	if strings.TrimSpace(sku) == "" {
		sku = "mana.tenant_topup.legacy_v1"
	}
	stripeSession := "cs_legacy_" + in.StripePaymentIntentID
	return []any{
		in.TopupID, in.TenantID, adminGcid, sku,
		"payment_captured",
		in.ChargedCents, in.ChargedCents, int64(0), in.Currency,
		in.UnitsCredited,
		stripeSession,
		in.StripePaymentIntentID,
		in.RecordedAt, in.RecordedAt,
		in.CreatedAt, in.CreatedAt,
	}
}

// nilIfEmpty returns nil for empty strings so pgx writes NULL into the
// nullable text column (rather than an empty string).
func nilIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// nilTimeIfNil unwraps the pointer or returns nil for the pgx driver.
func nilTimeIfNil(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
