// transaction_ledger_subscriber.go — ADR-205 (CHO-1938, Wave B2) event-bus
// projection subscriber. Consumes payments / identity / observability events
// and upserts the unified transaction_ledger read projection (cross-DB-clean:
// subscribe only, never join).
//
// Canonical-source policy (avoids double-counting the same money across the
// three source domains — full rationale in
// internal/domain/transactionledger/ledger.go):
//   - purchase           ← payments *.payment_captured / *.refunded (6 non-mana
//     aggregates).
//   - mana_topup (fiat)  ← payments user_mana_topup / tenant_mana_topup
//     (carries fiat + mana).
//   - mana_topup (grant) ← identity user_mana.credited for NON-purchase reasons
//     (subscription_grant / tenant_subsidy / promo /
//     rollover); reason=topup is the payments dup → skip.
//   - mana_spend_daily   ← observability token_usage.recorded.v1 (authoritative
//     metered spend). identity user_mana.debited is the
//     same spend on the balance ledger → NOT projected.
//
// Resilience (agentic-resilience-d6 Pillar 2): idempotent inbox keyed on
// event_id; handler errors → Nack → broker retry → DLQ; tenant context set
// from the envelope so the RLS write lands under the right tenant.
package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/identity/v1"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TxLedger topics consumed by the projection. Each gets its OWN consumer
// (chora-tenancy-txledger-*) distinct from the payments-notification
// subscriber's, so both process independently. JetStream creates the durable
// consumers on demand.
const (
	topicCoursePurchaseCaptured     = "chora.payments.course_purchase.payment_captured.v1"
	topicCoursePurchaseRefunded     = "chora.payments.course_purchase.refunded.v1"
	topicApplicationPaymentCaptured = "chora.payments.application_payment.payment_captured.v1"
	topicApplicationPaymentRefunded = "chora.payments.application_payment.refunded.v1"
	topicFamiliarEggCaptured        = "chora.payments.familiar_egg_purchase.payment_captured.v1"
	topicFamiliarEggRefunded        = "chora.payments.familiar_egg_purchase.refunded.v1"
	topicIdentityKycFeeCaptured     = "chora.payments.identity_kyc_fee.payment_captured.v1"
	topicIdentityKycFeeRefunded     = "chora.payments.identity_kyc_fee.refunded.v1"
	topicUserSubscriptionCaptured   = "chora.payments.user_subscription.payment_captured.v1"
	topicUserSubscriptionRefunded   = "chora.payments.user_subscription.refunded.v1"
	topicTenantAddonCaptured        = "chora.payments.tenant_addon_purchase.payment_captured.v1"
	topicTenantAddonRefunded        = "chora.payments.tenant_addon_purchase.refunded.v1"
	topicUserManaTopUpCaptured      = "chora.payments.user_mana_topup.payment_captured.v1"
	topicUserManaTopUpRefunded      = "chora.payments.user_mana_topup.refunded.v1"
	topicTenantManaTopUpCaptured    = "chora.payments.tenant_mana_topup.payment_captured.v1"
	topicTenantManaTopUpRefunded    = "chora.payments.tenant_mana_topup.refunded.v1"
	topicUserManaCredited           = "chora.identity.user_mana.credited.v1"
	topicTokenUsageRecorded         = "chora.observability.token_usage.recorded.v1"
)

// txLedgerInboxTTL bounds inbox dedupe — matches the payments subscriber.
const txLedgerInboxTTL = 24 * time.Hour

// TransactionLedgerSubscriber projects source events into transaction_ledger.
type TransactionLedgerSubscriber struct {
	repo  transactionledger.WriteRepository
	inbox idempotent.Store
	ttl   time.Duration
}

// NewTransactionLedgerSubscriber wires the projection subscriber.
func NewTransactionLedgerSubscriber(
	repo transactionledger.WriteRepository, inbox idempotent.Store,
) *TransactionLedgerSubscriber {
	return &TransactionLedgerSubscriber{repo: repo, inbox: inbox, ttl: txLedgerInboxTTL}
}

// Topics is the full set of source topics this projection consumes.
func (s *TransactionLedgerSubscriber) Topics() []string {
	return []string{
		topicCoursePurchaseCaptured, topicCoursePurchaseRefunded,
		topicApplicationPaymentCaptured, topicApplicationPaymentRefunded,
		topicFamiliarEggCaptured, topicFamiliarEggRefunded,
		topicIdentityKycFeeCaptured, topicIdentityKycFeeRefunded,
		topicUserSubscriptionCaptured, topicUserSubscriptionRefunded,
		topicTenantAddonCaptured, topicTenantAddonRefunded,
		topicUserManaTopUpCaptured, topicUserManaTopUpRefunded,
		topicTenantManaTopUpCaptured, topicTenantManaTopUpRefunded,
		topicUserManaCredited,
		topicTokenUsageRecorded,
	}
}

// SubscriptionNameForTopic maps chora.{domain}.{aggregate}.{event}.vN →
// chora-tenancy-txledger-{aggregate}-{event}.
func (s *TransactionLedgerSubscriber) SubscriptionNameForTopic(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 5 {
		return ""
	}
	return "chora-tenancy-txledger-" + parts[2] + "-" + parts[3]
}

// Handle decodes one message, maps it to the projection per the canonical-
// source policy, and applies it idempotently. A nil return Acks; an error
// Nacks (broker retry → DLQ).
func (s *TransactionLedgerSubscriber) Handle(ctx context.Context, topic string, msg eventbus.Message) error {
	env := msg.Envelope
	tenantID := strings.TrimSpace(env.TenantID)
	if tenantID == "" {
		return fmt.Errorf("txledger: empty tenant_id (topic=%s event=%s)", topic, env.EventID)
	}
	if strings.TrimSpace(env.EventID) == "" {
		return fmt.Errorf("txledger: empty event_id (topic=%s)", topic)
	}

	switch topic {
	// ── purchase (learner-scoped) ─────────────────────────────────────
	case topicCoursePurchaseCaptured:
		var ev paymentsv1.CoursePurchasePaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Course purchase", status: transactionledger.StatusCaptured,
		})
	case topicCoursePurchaseRefunded:
		var ev paymentsv1.CoursePurchaseRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Course purchase", status: transactionledger.StatusRefunded,
		})

	case topicApplicationPaymentCaptured:
		var ev paymentsv1.ApplicationPaymentPaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Course application fee", status: transactionledger.StatusCaptured,
		})
	case topicApplicationPaymentRefunded:
		var ev paymentsv1.ApplicationPaymentRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Course application fee", status: transactionledger.StatusRefunded,
		})

	case topicFamiliarEggCaptured:
		var ev paymentsv1.CompanionEggPurchasePaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Familiar egg", status: transactionledger.StatusCaptured,
		})
	case topicFamiliarEggRefunded:
		var ev paymentsv1.CompanionEggPurchaseRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Familiar egg", status: transactionledger.StatusRefunded,
		})

	case topicIdentityKycFeeCaptured:
		var ev paymentsv1.IdentityKycFeePaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Identity verification fee", status: transactionledger.StatusCaptured,
		})
	case topicIdentityKycFeeRefunded:
		var ev paymentsv1.IdentityKycFeeRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Identity verification fee", status: transactionledger.StatusRefunded,
		})

	case topicUserSubscriptionCaptured:
		var ev paymentsv1.UserSubscriptionPaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Subscription", status: transactionledger.StatusCaptured,
		})
	case topicUserSubscriptionRefunded:
		var ev paymentsv1.UserSubscriptionRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Subscription", status: transactionledger.StatusRefunded,
		})

	// ── purchase (tenant-level — admin_gcid → NULL learner) ───────────
	case topicTenantAddonCaptured:
		var ev paymentsv1.TenantAddonPurchasePaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), amount: ev.GetAmountCentsPaid(), currency: ev.GetCurrency(),
			at: tsOr(ev.GetPaidAt(), env.OccurredAt), label: "Add-on: " + ev.GetAddonCode(),
			status: transactionledger.StatusCaptured, meta: adminMeta(ev.GetAdminGcid()),
		})
	case topicTenantAddonRefunded:
		var ev paymentsv1.TenantAddonPurchaseRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindPurchase,
			ref: ev.GetPurchaseId(), amount: ev.GetAmountCentsRefunded(), currency: ev.GetCurrency(),
			at: tsOr(ev.GetRefundedAt(), env.OccurredAt), label: "Add-on",
			status: transactionledger.StatusRefunded, meta: adminMeta(ev.GetAdminGcid()),
		})

	// ── mana_topup (fiat — carries currency + mana) ───────────────────
	case topicUserManaTopUpCaptured:
		var ev paymentsv1.UserManaTopUpPaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindManaTopup,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsPaid(),
			currency: ev.GetCurrency(), mana: ev.GetManaUnits(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Mana top-up", status: transactionledger.StatusCaptured,
		})
	case topicUserManaTopUpRefunded:
		var ev paymentsv1.UserManaTopUpRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindManaTopup,
			ref: ev.GetPurchaseId(), learner: ev.GetLearnerGcid(), amount: ev.GetAmountCentsRefunded(),
			currency: ev.GetCurrency(), at: tsOr(ev.GetRefundedAt(), env.OccurredAt),
			label: "Mana top-up", status: transactionledger.StatusRefunded,
		})
	case topicTenantManaTopUpCaptured:
		var ev paymentsv1.TenantManaTopUpPaymentCaptured
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindManaTopup,
			ref: ev.GetPurchaseId(), amount: ev.GetAmountCentsPaid(), currency: ev.GetCurrency(),
			mana: ev.GetManaUnits(), at: tsOr(ev.GetPaidAt(), env.OccurredAt),
			label: "Tenant mana top-up", status: transactionledger.StatusCaptured,
			meta: adminMeta(ev.GetAdminGcid()),
		})
	case topicTenantManaTopUpRefunded:
		var ev paymentsv1.TenantManaTopUpRefunded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourcePayments, kind: transactionledger.KindManaTopup,
			ref: ev.GetPurchaseId(), amount: ev.GetAmountCentsRefunded(), currency: ev.GetCurrency(),
			at: tsOr(ev.GetRefundedAt(), env.OccurredAt), label: "Tenant mana top-up",
			status: transactionledger.StatusRefunded, meta: adminMeta(ev.GetAdminGcid()),
		})

	// ── mana_topup (identity grant — non-payment credits only) ────────
	case topicUserManaCredited:
		var ev identityv1.UserManaCredited
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		if !isManaGrantReason(ev.GetReason()) {
			return nil // topup is the payments dup; spend/refund/closure not a grant — skip (Ack)
		}
		return s.upsertPurchase(ctx, env.EventID, tenantID, purchaseRow{
			domain: transactionledger.SourceIdentity, kind: transactionledger.KindManaTopup,
			ref: ev.GetEntryId(), learner: ev.GetGcid(), mana: ev.GetUnits(),
			at: tsOr(ev.GetRecordedAt(), env.OccurredAt), label: manaGrantLabel(ev.GetReason()),
			status: transactionledger.StatusPosted,
		})

	// ── mana_spend_daily (authoritative metered spend) ────────────────
	case topicTokenUsageRecorded:
		var ev observabilityv1.TokenUsageRecorded
		if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
			return decodeErr(topic, err)
		}
		cost := ev.GetManaUnits()
		if cost <= 0 {
			return nil // pre-ManaMetering or non-mana call — nothing to project (Ack)
		}
		at := tsOr(ev.GetRecordedAt(), env.OccurredAt)
		day := at.UTC().Format("2006-01-02")
		return s.inbox.Process(ctx, "txledger:"+env.EventID, s.ttl, func() error {
			tctx := tracing.WithTenantID(ctx, tenantID)
			return s.repo.AccumulateSpend(tctx,
				transactionledger.Entry{
					OccurredAt: at, TenantID: tenantID, LearnerGCID: ev.GetGcid(),
					Kind: transactionledger.KindManaSpendDaily, SourceDomain: transactionledger.SourceObservability,
					SourceRefID: day, Label: "Mana spent " + day, ManaUnits: -cost,
					Status: transactionledger.StatusPosted,
				},
				transactionledger.Detail{
					OccurredAt: at, TenantID: tenantID, LearnerGCID: ev.GetGcid(),
					ActionCode: ev.GetActionCode(), ManaUnits: -cost,
					Model: ev.GetModelId(), TraceID: ev.GetInvocationId(),
				})
		})

	default:
		return fmt.Errorf("txledger: unhandled topic %q", topic)
	}
}

// purchaseRow is the decoded, source-agnostic input to a purchase/mana_topup
// upsert.
type purchaseRow struct {
	domain   string
	kind     string
	ref      string
	learner  string
	amount   int64
	currency string
	mana     int64
	at       time.Time
	label    string
	status   string
	meta     map[string]any
}

func (s *TransactionLedgerSubscriber) upsertPurchase(
	ctx context.Context, eventID, tenantID string, in purchaseRow,
) error {
	return s.inbox.Process(ctx, "txledger:"+eventID, s.ttl, func() error {
		tctx := tracing.WithTenantID(ctx, tenantID)
		return s.repo.UpsertPurchase(tctx, transactionledger.Entry{
			OccurredAt: in.at, TenantID: tenantID, LearnerGCID: in.learner,
			Kind: in.kind, SourceDomain: in.domain, SourceRefID: in.ref, Label: in.label,
			Currency: in.currency, AmountMinor: in.amount, ManaUnits: in.mana,
			Status: in.status, Metadata: in.meta,
		})
	})
}

func decodeErr(topic string, err error) error {
	return fmt.Errorf("txledger: decode %s: %w", topic, err)
}

// tsOr returns the proto timestamp as a time, or the fallback when nil/zero.
func tsOr(ts *timestamppb.Timestamp, fallback time.Time) time.Time {
	if ts == nil || !ts.IsValid() {
		return fallback
	}
	return ts.AsTime()
}

func adminMeta(adminGCID string) map[string]any {
	if adminGCID == "" {
		return nil
	}
	return map[string]any{"admin_gcid": adminGCID}
}

// isManaGrantReason is true only for NON-payment mana credits — those without
// a corresponding payments event (so projecting them does not double-count).
// reason=topup is covered by the payments user_mana_topup row; debit/refund/
// closure/familiar_action are not grants.
func isManaGrantReason(r identityv1.ManaReason) bool {
	switch r {
	case identityv1.ManaReason_MANA_REASON_SUBSCRIPTION_GRANT,
		identityv1.ManaReason_MANA_REASON_TENANT_SUBSIDY,
		identityv1.ManaReason_MANA_REASON_PROMO,
		identityv1.ManaReason_MANA_REASON_ROLLOVER:
		return true
	default:
		return false
	}
}

func manaGrantLabel(r identityv1.ManaReason) string {
	switch r {
	case identityv1.ManaReason_MANA_REASON_SUBSCRIPTION_GRANT:
		return "Subscription mana grant"
	case identityv1.ManaReason_MANA_REASON_TENANT_SUBSIDY:
		return "Tenant mana subsidy"
	case identityv1.ManaReason_MANA_REASON_PROMO:
		return "Promotional mana"
	case identityv1.ManaReason_MANA_REASON_ROLLOVER:
		return "Mana rollover"
	default:
		return "Mana grant"
	}
}
