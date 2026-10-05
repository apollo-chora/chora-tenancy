// Package transactionledger is the CQRS READ projection for the unified
// transaction timeline (payments + mana), owned by chora_tenancy (Billing)
// per ADR-205. It is a read model, not a rich aggregate — there are no
// invariants beyond "one row per logical entry, idempotently projected from
// events". The write side (this package's WriteRepository port + the pg
// adapter) is driven by the event-bus projection subscriber (B2 / CHO-1938);
// the read side is served scope-aware over gRPC (B3 / CHO-1939).
//
// Canonical-source policy (avoids double-counting the same money across
// domains — the projection subscribes to 3 domains that each observe an
// overlapping slice of a transaction):
//
//   - purchase           ← chora_payments *.payment_captured / *.refunded for
//     the 6 NON-mana aggregates (course / application /
//     familiar_egg / user_subscription / identity_kyc_fee
//     / tenant_addon). Fiat row.
//   - mana_topup (fiat)  ← chora_payments user_mana_topup / tenant_mana_topup
//     captured/refunded — carries BOTH the fiat amount and
//     the mana units bought. (The identity user_mana
//     'credited' with reason=topup is the SAME money seen
//     downstream → NOT projected, to avoid a duplicate.)
//   - mana_topup (grant) ← chora_identity user_mana 'credited' for the
//     non-purchase reasons (subscription_grant /
//     tenant_subsidy / promo / mint). Mana-only, no fiat.
//   - mana_spend_daily   ← chora_observability token_usage.recorded.v1 — the
//     authoritative metered LLM spend (carries model /
//     trace_id / action_code for per-call detail).
//     (chora_identity user_mana 'debited' is the same
//     spend seen on the balance ledger → NOT projected.)
package transactionledger

import (
	"context"
	"time"
)

// Kind discriminates the three unified row types.
const (
	KindPurchase       = "purchase"
	KindManaTopup      = "mana_topup"
	KindManaSpendDaily = "mana_spend_daily"
)

// Status is the normalised surface status across fiat + mana movements.
const (
	StatusCaptured = "captured"
	StatusRefunded = "refunded"
	StatusFailed   = "failed"
	StatusExpired  = "expired"
	StatusPosted   = "posted"
)

// Source domains (for source_domain).
const (
	SourcePayments      = "payments"
	SourceIdentity      = "identity"
	SourceObservability = "observability"
)

// Entry is one unified timeline row (the transaction_ledger projection shape).
// LearnerGCID == "" means tenant-level (NULL learner_gcid). For fiat rows
// Currency+AmountMinor are set and ManaUnits is 0; for mana rows ManaUnits is
// set (signed: topup +, spend −) and Currency is "" — except a fiat mana_topup
// carries both.
type Entry struct {
	OccurredAt   time.Time
	TenantID     string
	LearnerGCID  string
	Kind         string
	SourceDomain string
	SourceRefID  string
	Label        string
	Currency     string
	AmountMinor  int64
	ManaUnits    int64
	Status       string
	Metadata     map[string]any
}

// Detail is one per-call mana-spend row under a mana_spend_daily parent.
type Detail struct {
	OccurredAt  time.Time
	TenantID    string
	LearnerGCID string
	ActionCode  string
	ManaUnits   int64
	Model       string
	TraceID     string
	Metadata    map[string]any
}

// WriteRepository is the projection write port (driven by the subscriber).
//
// Both methods are idempotent on the entry's natural key
// (tenant_id, kind, source_domain, source_ref_id, COALESCE(learner_gcid, nil))
// and run inside a tenant-scoped RLS transaction.
type WriteRepository interface {
	// UpsertPurchase inserts or merges a purchase / mana_topup row. Re-emitting
	// the same captured event is a no-op; a later refunded event flips the
	// status to refunded (refund is sticky — never reverts to captured even on
	// out-of-order delivery).
	UpsertPurchase(ctx context.Context, e Entry) error

	// AccumulateSpend folds a metered spend into the (tenant, learner, day)
	// mana_spend_daily rollup (mana_units accumulate) AND appends the per-call
	// detail row, atomically in one transaction.
	AccumulateSpend(ctx context.Context, e Entry, d Detail) error
}
