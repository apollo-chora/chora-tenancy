// Payments subscriber for chora-tenancy (ADR-164).
//
// chora-tenancy is the originating service for two Purchase aggregates
// that, post Wave 1 Stage D cutover, live in chora-payments:
//
//   - FamiliarEggPurchase   — ADR-149 Familiar-egg purchases. payment_
//                             captured → emit
//                             chora.consumption.familiar.egg_purchased.v1
//                             so chora-consumption provisions the Stage-0
//                             familiar_instances row. refunded with
//                             credit_only=true → credit a tenant_mana_pool
//                             allocation for the buyer (ADR-149
//                             §"Unhatched egg expiry") + emit
//                             chora.consumption.familiar.egg_revoked.v1.
//                             refunded with credit_only=false → cash-refund
//                             path; emit revocation only.
//   - TenantManaTopUp       — tenant mana pool top-ups. payment_captured →
//                             credit tenant_mana_pool.balance_units +=
//                             mana_units + emit the legacy
//                             chora.tenancy.tenant_mana_pool.topped_up.v1
//                             bridge event for downstream consumers during
//                             the rollback window. refunded → debit
//                             balance_units -= mana_units_to_debit.
//
// All subscriber paths are idempotent on event_id via the shared
// libs/chora-go-common/idempotent.Store. Replays are no-ops.
//
// Topic taxonomy (consumed):
//
//   chora.payments.familiar_egg_purchase.payment_captured.v1
//   chora.payments.familiar_egg_purchase.refunded.v1
//   chora.payments.familiar_egg_purchase.expired.v1
//   chora.payments.tenant_mana_topup.payment_captured.v1
//   chora.payments.tenant_mana_topup.refunded.v1
//
// Subscription naming convention (matches chora-tenancy convention):
//
//   chora-tenancy-payments-{aggregate}-{event_type}
//
// Composes with:
//
//   - libs/chora-go-common/idempotent.Store (Postgres inbox in prod;
//     MemoryStore for dev/tests) — survives pod-death + works across
//     replicas (per `agentic-resilience-d6` skill Pillar 2 step 3b).
//   - events.Recorder / events.CloudPublisher port (in-process vs outbox).
package events

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

// Inbound topic constants — chora-tenancy consumes these from chora-payments.
const (
	TopicFamiliarEggPaymentCaptured = "chora.payments.familiar_egg_purchase.payment_captured.v1"
	TopicFamiliarEggRefunded        = "chora.payments.familiar_egg_purchase.refunded.v1"
	TopicFamiliarEggExpired         = "chora.payments.familiar_egg_purchase.expired.v1"
	TopicManaTopUpPaymentCaptured   = "chora.payments.tenant_mana_topup.payment_captured.v1"
	TopicManaTopUpRefunded          = "chora.payments.tenant_mana_topup.refunded.v1"
	// CHO-1740 — H+ Marketplace Subscribe saga. chora-tenancy consumes
	// payment_captured to ACTIVATE the AddOn Subscription, refunded to
	// DEACTIVATE it.
	TopicTenantAddonPaymentCaptured = "chora.payments.tenant_addon_purchase.payment_captured.v1"
	TopicTenantAddonRefunded        = "chora.payments.tenant_addon_purchase.refunded.v1"
	// CHO-1775 — Stripe Subscription lifecycle Phase 2 (post-CHO-1762).
	// Cancelled drives the AddOn Deactivate path (Stripe Dashboard cancel
	// + smart-retry-exhausted dunning). Subscription-payment-failed +
	// payment-recovered drive the dunning banner + chora-notifications
	// emails (registry past_due flag wired in Phase 2b FE PR).
	TopicTenantAddonSubscriptionCancelled      = "chora.payments.tenant_addon_purchase.cancelled.v1"
	TopicTenantAddonSubscriptionPaymentFailed  = "chora.payments.tenant_addon_purchase.subscription_payment_failed.v1"
	TopicTenantAddonPaymentRecovered           = "chora.payments.tenant_addon_purchase.payment_recovered.v1"
	// CHO-1779 — schedule_released event. chora-payments emits this when
	// Stripe activates the deferred end-of-cycle tier change at cycle
	// anchor. chora-tenancy promotes ScheduledTier → CurrentTier on the
	// in-memory registry so /h/addons reflects the new tier without
	// restart.
	TopicTenantAddonSubscriptionScheduleReleased = "chora.payments.tenant_addon_purchase.subscription_schedule_released.v1"
)

// Outbound topic constants — chora-tenancy emits these as the originating
// service's downstream + legacy-bridge events.
const (
	// ADR-164: after chora-payments captures the egg purchase, chora-tenancy
	// (the FamiliarEgg originating service) re-emits on its OWN canonical topic
	// so chora-consumption provisions the Stage-0 familiar_instances row. The
	// tenancy outbox forbids publishing a foreign-domain topic, and consumption's
	// egg-purchase-provision push sub already listens on THIS topic (its DLQ +
	// IAM exist). The old chora.consumption.familiar.egg_purchased.v1 name was a
	// legacy cross-domain topic the outbox correctly rejects.
	TopicTenancyFamiliarEggPaymentSucceeded = "chora.tenancy.familiar_egg.payment_succeeded.v1"
	TopicConsumptionFamiliarEggRevoked   = "chora.consumption.familiar.egg_revoked.v1"
	TopicLegacyTenantManaPoolToppedUp    = "chora.tenancy.tenant_mana_pool.topped_up.v1"
	// CHO-1740 — emitted by the tenant_addon_purchase.payment_captured
	// handler after a successful subscription Activate. Carries the
	// canonical tenant addon lifecycle event (TenantAddonActivated /
	// Deactivated) for observability + governance + the FE polling-loop
	// that drives the `Installed ✓` flip on /h/marketplace/:planId.
	TopicTenancyTenantAddonActivated   = "chora.tenancy.tenant_addon.activated.v1"
	TopicTenancyTenantAddonDeactivated = "chora.tenancy.tenant_addon.deactivated.v1"
	// CHO-1775 — dunning lifecycle downstream events for chora-notifications
	// + FE banner consumption. Registry past_due flag wiring lands in
	// Phase 2b together with the FE work.
	TopicTenancyTenantAddonPaymentFailed    = "chora.tenancy.tenant_addon.payment_failed.v1"
	TopicTenancyTenantAddonPaymentRecovered = "chora.tenancy.tenant_addon.payment_recovered.v1"
	// CHO-1779 — downstream observability/notifications signal that the
	// end-of-cycle tier change activated. chora-observability records
	// the tier-change ledger entry. chora-notifications sends a
	// "your tier changed" confirmation email (deferred).
	TopicTenancyTenantAddonUpgraded   = "chora.tenancy.tenant_addon.upgraded.v1"
	TopicTenancyTenantAddonDowngraded = "chora.tenancy.tenant_addon.downgraded.v1"
)

// PaymentsInboxTTL is the dedupe-key retention window for chora-tenancy's
// payments subscriber inbox. 24h covers Pub/Sub's max redelivery window for
// these events' typical end-to-end latency. Production may tune via
// WithInboxTTL.
const PaymentsInboxTTL = 24 * time.Hour

// PublisherPort is the minimal publisher surface the subscriber depends on
// at runtime. Both events.Recorder (in-process tests) + outbox.Publisher
// (production outbox dispatcher) satisfy it.
//
// (Test assertions read back recorded events via the concrete Recorder's
// RecordedByTopic — it is NOT part of this port.)
type PublisherPort interface {
	Publish(topic string, h Header, payload map[string]interface{}) PublishedEvent
	PublishWithError(topic string, h Header, payload map[string]interface{}) (PublishedEvent, error)
}

// PoolApplier is the port the subscriber uses to credit / debit a tenant's
// mana pool. Production wires a concrete impl backed by the existing
// manapool.InmemPoolRepo (or a future pgx-backed repo).
type PoolApplier interface {
	// Credit credits units to the named tenant's pool. Implementations
	// MUST persist atomically + emit any depletion / re-fill follow-on
	// events out-of-band.
	Credit(ctx context.Context, tenantID string, units int64, reason string) error
	// Debit debits units from the named tenant's pool. Implementations
	// SHOULD floor at zero (refunds may exceed the original credit if
	// other top-ups have been applied + already drained).
	Debit(ctx context.Context, tenantID string, units int64, reason string) error
}

// -----------------------------------------------------------------------------
// Inbound payload types (ADR-164 events)
// -----------------------------------------------------------------------------

// FamiliarEggPaymentCaptured mirrors
// chora.payments.familiar_egg_purchase.payment_captured.v1 (the subset
// chora-tenancy cares about).
type FamiliarEggPaymentCaptured struct {
	EventID               string
	PurchaseID            string
	LearnerGCID           string
	TargetTenantID        string
	EggSKU                string
	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	AmountCentsPaid       int64
	AmountCentsRefunded   int64
	Currency              string
	SuggestedFocalAtomID  string
	PaidAt                time.Time
	Traceparent           string
}

// FamiliarEggRefunded mirrors
// chora.payments.familiar_egg_purchase.refunded.v1.
type FamiliarEggRefunded struct {
	EventID             string
	PurchaseID          string
	LearnerGCID         string
	TargetTenantID      string
	EggSKU              string
	StripeChargeID      string
	StripeRefundID      string
	AmountCentsRefunded int64
	Currency            string
	Reason              string // expired_unhatched | support_initiated | customer_request | duplicate_charge | fraud
	CreditOnly          bool
	// ManaUnitsCredit is the mana-credit unit count to allocate to the
	// learner's tenant pool when CreditOnly=true. chora-payments
	// computes this at refund-time per the SKU's units-per-cent ratio.
	ManaUnitsCredit int64
	RefundedAt      time.Time
	Traceparent     string
}

// FamiliarEggExpired mirrors
// chora.payments.familiar_egg_purchase.expired.v1.
type FamiliarEggExpired struct {
	EventID         string
	PurchaseID      string
	LearnerGCID     string
	TargetTenantID  string
	EggSKU          string
	StripeSessionID string
	ExpiredAt       time.Time
	Traceparent     string
}

// TenantManaTopUpPaymentCaptured mirrors
// chora.payments.tenant_mana_topup.payment_captured.v1.
type TenantManaTopUpPaymentCaptured struct {
	EventID               string
	PurchaseID            string
	AdminGCID             string
	TargetTenantID        string
	SKU                   string
	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	AmountCentsPaid       int64
	AmountCentsRefunded   int64
	Currency              string
	ManaUnits             int64
	PaidAt                time.Time
	Traceparent           string
}

// TenantManaTopUpRefunded mirrors
// chora.payments.tenant_mana_topup.refunded.v1.
type TenantManaTopUpRefunded struct {
	EventID             string
	PurchaseID          string
	AdminGCID           string
	TargetTenantID      string
	SKU                 string
	StripeChargeID      string
	StripeRefundID      string
	AmountCentsRefunded int64
	Currency            string
	ManaUnitsToDebit    int64
	Reason              string
	RefundedAt          time.Time
	Traceparent         string
}

// TenantAddonPaymentCaptured mirrors
// chora.payments.tenant_addon_purchase.payment_captured.v1 (CHO-1740).
type TenantAddonPaymentCaptured struct {
	EventID               string
	PurchaseID            string
	AdminGCID             string
	TargetTenantID        string
	AddonPlanID           string
	AddonCode             string
	TierCode              string
	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	AmountCentsPaid       int64
	AmountCentsRefunded   int64
	Currency              string
	PaidAt                time.Time
	Traceparent           string
}

// TenantAddonRefunded mirrors
// chora.payments.tenant_addon_purchase.refunded.v1 (CHO-1740).
type TenantAddonRefunded struct {
	EventID             string
	PurchaseID          string
	AdminGCID           string
	TargetTenantID      string
	AddonPlanID         string
	StripeChargeID      string
	StripeRefundID      string
	AmountCentsRefunded int64
	Currency            string
	Reason              string
	RefundedAt          time.Time
	Traceparent         string
}

// -----------------------------------------------------------------------------
// Subscriber
// -----------------------------------------------------------------------------

// AddOnActivator is the hexagonal port the CHO-1740 H+ Marketplace
// payment-captured / refunded handlers call to mutate the
// AddOnSubscription aggregate. The production implementation wraps
// chora-tenancy's *add_on.SubscriptionRegistry — its `Subscribe` +
// `ChangeTier` + `Unsubscribe` primitives compose into the
// activate-after-paid + deactivate-on-refund saga semantics.
//
// Activate is idempotent: re-delivery of the same payment_captured
// event lands in the inbox dedup; the registry-side Subscribe also
// no-ops on an existing ACTIVE row.
type AddOnActivator interface {
	// Activate creates-or-promotes the (tenant, addon) subscription to
	// ACTIVE state. The tierCode (when non-empty + recognised by the
	// catalog) is applied via ChangeTier after Subscribe.
	Activate(ctx context.Context, tenantID, addonCode, adminGCID, tierCode string) error
	// Deactivate transitions the (tenant, addon) subscription to the
	// canonical post-refund state (UNSUBSCRIBED / grace per the
	// existing flow). Called from the refunded handler.
	Deactivate(ctx context.Context, tenantID, addonCode, requesterGCID, reason string) error
}

// RegistryPastDueUpdater is the hexagonal port the CHO-1776 dunning
// subscribers call to flip the AddOn subscription's PastDue flag.
// Production wires the in-memory `*add_on.SubscriptionRegistry` (its
// MarkPastDue / MarkRecovered methods satisfy this interface). Nil is
// permitted — the subscriber falls back to log + ack-only without
// mutating registry state (Phase 2a behaviour). Tests wire a fake.
type RegistryPastDueUpdater interface {
	MarkPastDue(tenantID, addonCode string) error
	MarkRecovered(tenantID, addonCode string) error
}

// RegistryScheduledTierPromoter is the port the
// subscription_schedule.released handler uses to promote the deferred
// tier change on chora-tenancy's in-memory SubscriptionRegistry.
//
// Implementation: SubscriptionRegistry.PromoteScheduledTier (CHO-1779).
// Returns the post-promotion subscription + the recorded lifecycle
// event kind (upgraded / downgraded). When no schedule was pending
// (idempotent duplicate Stripe delivery), kind is empty + the sub is
// unchanged. Tests wire a fake.
type RegistryScheduledTierPromoter interface {
	PromoteScheduledTier(tenantID, addonCode string) (promotedKind string, fromTier string, toTier string, err error)
}

// RegistryDeactivationAnchorer is the port the customer.subscription.
// deleted handler uses to flip the AddOn registry to DEACTIVATED at
// the Stripe cycle anchor.
//
// Implementation: SubscriptionRegistry.AnchorDeactivation (CHO-1783).
// Distinct from the legacy AddOnActivator.Deactivate path which adds
// a 30-day grace window — when Stripe says the subscription is
// cancelled at the cycle anchor, no more grace is meaningful.
// Idempotent on duplicate webhook delivery: returns ok=false when the
// row was already DEACTIVATED so the subscriber can skip the downstream
// emit. Tests wire a fake.
type RegistryDeactivationAnchorer interface {
	AnchorDeactivation(tenantID, addonCode string) (ok bool, err error)
}

// PaymentsSubscriberDeps wires the subscriber.
type PaymentsSubscriberDeps struct {
	Publisher  PublisherPort
	Pool       PoolApplier
	Inbox      idempotent.Store
	// CHO-1740 — required for the tenant_addon_purchase handlers. When
	// nil the H+ Marketplace handlers fail-loud (preventing silent
	// payment-without-activation).
	AddOnActivator AddOnActivator
	// CHO-1776 — optional. When wired, the SubscriptionPaymentFailed /
	// PaymentRecovered handlers flip the AddOn's PastDue flag before
	// emitting downstream. When nil, the handlers log + ack only (the
	// Phase 2a behaviour) — useful for tests that don't care about
	// registry state.
	PastDueRegistry RegistryPastDueUpdater
	// CHO-1779 — optional. When wired, the
	// SubscriptionScheduleReleased handler calls PromoteScheduledTier
	// on the registry so the in-memory CurrentTier reflects the now-
	// active tier without a chora-tenancy restart. When nil, the
	// handler still acks + emits downstream — the registry just won't
	// auto-reconcile until next restart (acceptable for tests).
	ScheduledTierPromoter RegistryScheduledTierPromoter
	// CHO-1783 — optional. When wired, the
	// HandleTenantAddonSubscriptionCancelled handler calls
	// AnchorDeactivation to flip the registry directly to DEACTIVATED
	// at the Stripe cycle anchor — avoiding the legacy 30-day grace
	// stack-up. When nil, the handler falls back to the AddOnActivator.
	// Deactivate path (Unsubscribe → grace) for backwards compatibility.
	DeactivationAnchorer RegistryDeactivationAnchorer
}

// PaymentsSubscriber consumes the 5 chora.payments.* topics for the
// 2 chora-tenancy-originated aggregates + applies the
// originating-service notification work.
type PaymentsSubscriber struct {
	pub           PublisherPort
	pool          PoolApplier
	inbox         idempotent.Store
	ttl           time.Duration
	activator     AddOnActivator
	pastDueReg    RegistryPastDueUpdater
	scheduledReg  RegistryScheduledTierPromoter
	deactAnchorer RegistryDeactivationAnchorer
}

// NewPaymentsSubscriber constructs the subscriber. Inbox defaults to
// MemoryStore when nil for dev / test parity; production callers MUST
// pass a PostgresStore-backed inbox so dedup survives pod-death + works
// across replicas (per `agentic-resilience-d6` Pillar 2 step 3b).
func NewPaymentsSubscriber(deps PaymentsSubscriberDeps) *PaymentsSubscriber {
	inbox := deps.Inbox
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &PaymentsSubscriber{
		pub:           deps.Publisher,
		pool:          deps.Pool,
		inbox:         inbox,
		ttl:           PaymentsInboxTTL,
		activator:     deps.AddOnActivator,
		pastDueReg:    deps.PastDueRegistry,
		scheduledReg:  deps.ScheduledTierPromoter,
		deactAnchorer: deps.DeactivationAnchorer,
	}
}

// WithInboxTTL overrides the default dedupe-key retention window.
func (s *PaymentsSubscriber) WithInboxTTL(ttl time.Duration) *PaymentsSubscriber {
	if s == nil || ttl <= 0 {
		return s
	}
	s.ttl = ttl
	return s
}

// SubscriptionNameForTopic maps an inbound topic to its canonical
// subscription name in the chora-tenancy convention.
func (s *PaymentsSubscriber) SubscriptionNameForTopic(topic string) string {
	// chora.payments.{aggregate}.{event_type}.v1 →
	// chora-tenancy-payments-{aggregate}-{event_type}
	const prefix = "chora.payments."
	if !strings.HasPrefix(topic, prefix) {
		return ""
	}
	body := strings.TrimPrefix(topic, prefix)
	// body = "{aggregate}.{event_type}.v{N}"
	parts := strings.Split(body, ".")
	if len(parts) < 3 {
		return ""
	}
	aggregate := parts[0]
	eventType := parts[1]
	return "chora-tenancy-payments-" + aggregate + "-" + eventType
}

// -----------------------------------------------------------------------------
// FamiliarEgg payment_captured
// -----------------------------------------------------------------------------

// HandleFamiliarEggPaymentCaptured is the entry point for
// chora.payments.familiar_egg_purchase.payment_captured.v1.
//
// Side-effects:
//   - Emit chora.tenancy.familiar_egg.payment_succeeded.v1 so chora-consumption
//     provisions the Stage-0 familiar_instances row (ADR-149). Consumption's
//     egg-purchase-provision push sub already listens on this canonical topic.
//
// Idempotent on event_id via the inbox.
func (s *PaymentsSubscriber) HandleFamiliarEggPaymentCaptured(ctx context.Context, in FamiliarEggPaymentCaptured) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if err := validateEggCaptured(in); err != nil {
		return err
	}
	key := "tenancy.payments.egg.captured:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.LearnerGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":              in.PurchaseID,
			"learner_gcid":             in.LearnerGCID,
			"target_tenant_id":         in.TargetTenantID,
			"egg_sku":                  in.EggSKU,
			"stripe_session_id":        in.StripeSessionID,
			"stripe_payment_intent_id": in.StripePaymentIntentID,
			"stripe_charge_id":         in.StripeChargeID,
			"amount_cents_paid":        in.AmountCentsPaid,
			"currency":                 in.Currency,
			"suggested_focal_atom_id":  in.SuggestedFocalAtomID,
			"paid_at":                  formatTime(in.PaidAt),
			// Free-egg paths (trial / inclusion / grant) emit with
			// source != "purchase"; this subscriber only runs for paid
			// purchases.
			"source":               "purchase",
			"chora_imda_dimension": "accountability",
		}
		_, err := s.pub.PublishWithError(TopicTenancyFamiliarEggPaymentSucceeded, hdr, payload)
		if err != nil {
			return fmt.Errorf("events.PaymentsSubscriber: emit %s: %w", TopicTenancyFamiliarEggPaymentSucceeded, err)
		}
		return nil
	})
}

func validateEggCaptured(in FamiliarEggPaymentCaptured) error {
	if strings.TrimSpace(in.EventID) == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if strings.TrimSpace(in.PurchaseID) == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if strings.TrimSpace(in.LearnerGCID) == "" {
		return errors.New("events.PaymentsSubscriber: learner_gcid required")
	}
	if strings.TrimSpace(in.TargetTenantID) == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if strings.TrimSpace(in.EggSKU) == "" {
		return errors.New("events.PaymentsSubscriber: egg_sku required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// FamiliarEgg refunded
// -----------------------------------------------------------------------------

// HandleFamiliarEggRefunded is the entry point for
// chora.payments.familiar_egg_purchase.refunded.v1.
//
// Side-effects:
//   - CreditOnly=true → credit the tenant_mana_pool by ManaUnitsCredit
//     (ADR-149 §"Unhatched egg expiry" — 50% credit on hard-expiry).
//   - Always emit chora.consumption.familiar.egg_revoked.v1 so
//     chora-consumption frees the FamiliarRoster slot if already
//     provisioned.
//
// Idempotent on event_id via the inbox.
func (s *PaymentsSubscriber) HandleFamiliarEggRefunded(ctx context.Context, in FamiliarEggRefunded) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if err := validateEggRefunded(in); err != nil {
		return err
	}
	key := "tenancy.payments.egg.refunded:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.LearnerGCID, Traceparent: in.Traceparent}
		if in.CreditOnly && in.ManaUnitsCredit > 0 && s.pool != nil {
			reason := "familiar_egg_refund_credit:" + in.PurchaseID
			if err := s.pool.Credit(ctx, in.TargetTenantID, in.ManaUnitsCredit, reason); err != nil {
				return fmt.Errorf("events.PaymentsSubscriber: pool credit: %w", err)
			}
		}
		payload := map[string]interface{}{
			"purchase_id":           in.PurchaseID,
			"learner_gcid":          in.LearnerGCID,
			"target_tenant_id":      in.TargetTenantID,
			"egg_sku":               in.EggSKU,
			"stripe_charge_id":      in.StripeChargeID,
			"stripe_refund_id":      in.StripeRefundID,
			"amount_cents_refunded": in.AmountCentsRefunded,
			"currency":              in.Currency,
			"reason":                in.Reason,
			"credit_only":           in.CreditOnly,
			"mana_units_credit":     in.ManaUnitsCredit,
			"refunded_at":           formatTime(in.RefundedAt),
			"chora_imda_dimension":  "accountability",
		}
		if _, err := s.pub.PublishWithError(TopicConsumptionFamiliarEggRevoked, hdr, payload); err != nil {
			return fmt.Errorf("events.PaymentsSubscriber: emit %s: %w", TopicConsumptionFamiliarEggRevoked, err)
		}
		return nil
	})
}

func validateEggRefunded(in FamiliarEggRefunded) error {
	if strings.TrimSpace(in.EventID) == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if strings.TrimSpace(in.PurchaseID) == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if strings.TrimSpace(in.LearnerGCID) == "" {
		return errors.New("events.PaymentsSubscriber: learner_gcid required")
	}
	if strings.TrimSpace(in.TargetTenantID) == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// FamiliarEgg expired
// -----------------------------------------------------------------------------

// HandleFamiliarEggExpired is the entry point for
// chora.payments.familiar_egg_purchase.expired.v1.
//
// chora-tenancy does not own provisioning state for unhatched-expired
// purchases (those rows live in chora_payments post-Stage D). The
// abandonment-funnel observability event is emitted by chora-observability
// directly subscribing to the source topic; chora-tenancy's role on this
// event is a NO-OP. We still inbox the event_id for trace-correlation +
// to suppress noisy log spam on Pub/Sub redelivery.
func (s *PaymentsSubscriber) HandleFamiliarEggExpired(ctx context.Context, in FamiliarEggExpired) error {
	if s == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if strings.TrimSpace(in.EventID) == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	key := "tenancy.payments.egg.expired:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		// Intentional no-op. The chora-payments row remains in
		// CHECKOUT_STARTED → EXPIRED; no chora-tenancy state to mutate.
		return nil
	})
}

// -----------------------------------------------------------------------------
// TenantManaTopUp payment_captured
// -----------------------------------------------------------------------------

// HandleManaTopUpPaymentCaptured is the entry point for
// chora.payments.tenant_mana_topup.payment_captured.v1.
//
// Side-effects:
//   - Credit tenant_mana_pool.balance_units += mana_units.
//   - Emit the legacy chora.tenancy.tenant_mana_pool.topped_up.v1 bridge
//     event for downstream consumers during the rollback window
//     (per ADR-164 §"Topic taxonomy" + proto comments).
//
// Idempotent on event_id via the inbox.
func (s *PaymentsSubscriber) HandleManaTopUpPaymentCaptured(ctx context.Context, in TenantManaTopUpPaymentCaptured) error {
	if s == nil || s.pub == nil || s.pool == nil {
		return errors.New("events.PaymentsSubscriber: nil deps")
	}
	if err := validateManaCaptured(in); err != nil {
		return err
	}
	key := "tenancy.payments.mana_topup.captured:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		reason := "tenant_mana_topup:" + in.PurchaseID
		if err := s.pool.Credit(ctx, in.TargetTenantID, in.ManaUnits, reason); err != nil {
			return fmt.Errorf("events.PaymentsSubscriber: pool credit: %w", err)
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":              in.PurchaseID,
			"admin_gcid":               in.AdminGCID,
			"target_tenant_id":         in.TargetTenantID,
			"sku":                      in.SKU,
			"stripe_session_id":        in.StripeSessionID,
			"stripe_payment_intent_id": in.StripePaymentIntentID,
			"stripe_charge_id":         in.StripeChargeID,
			"units_credited":           in.ManaUnits,
			"charged_cents":            in.AmountCentsPaid,
			"currency":                 in.Currency,
			"recorded_at":              formatTime(in.PaidAt),
			"chora_imda_dimension":     "accountability",
		}
		if _, err := s.pub.PublishWithError(TopicLegacyTenantManaPoolToppedUp, hdr, payload); err != nil {
			return fmt.Errorf("events.PaymentsSubscriber: emit %s: %w", TopicLegacyTenantManaPoolToppedUp, err)
		}
		return nil
	})
}

func validateManaCaptured(in TenantManaTopUpPaymentCaptured) error {
	if strings.TrimSpace(in.EventID) == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if strings.TrimSpace(in.PurchaseID) == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if strings.TrimSpace(in.TargetTenantID) == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.ManaUnits <= 0 {
		return errors.New("events.PaymentsSubscriber: mana_units must be > 0")
	}
	return nil
}

// -----------------------------------------------------------------------------
// TenantManaTopUp refunded
// -----------------------------------------------------------------------------

// HandleManaTopUpRefunded is the entry point for
// chora.payments.tenant_mana_topup.refunded.v1.
//
// Side-effects:
//   - Debit tenant_mana_pool.balance_units -= mana_units_to_debit. Note
//     that the pool may already have been drained by allocations; the
//     PoolApplier impl is responsible for flooring at zero.
func (s *PaymentsSubscriber) HandleManaTopUpRefunded(ctx context.Context, in TenantManaTopUpRefunded) error {
	if s == nil || s.pool == nil {
		return errors.New("events.PaymentsSubscriber: nil deps")
	}
	if err := validateManaRefunded(in); err != nil {
		return err
	}
	key := "tenancy.payments.mana_topup.refunded:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		reason := "tenant_mana_topup_refund:" + in.PurchaseID
		if err := s.pool.Debit(ctx, in.TargetTenantID, in.ManaUnitsToDebit, reason); err != nil {
			return fmt.Errorf("events.PaymentsSubscriber: pool debit: %w", err)
		}
		return nil
	})
}

func validateManaRefunded(in TenantManaTopUpRefunded) error {
	if strings.TrimSpace(in.EventID) == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if strings.TrimSpace(in.PurchaseID) == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if strings.TrimSpace(in.TargetTenantID) == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.ManaUnitsToDebit <= 0 {
		return errors.New("events.PaymentsSubscriber: mana_units_to_debit must be > 0")
	}
	return nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// -----------------------------------------------------------------------------
// CHO-1740 — H+ Marketplace Subscribe saga (tenant_addon_purchase)
// -----------------------------------------------------------------------------

// HandleTenantAddonPaymentCaptured is the entry point for
// chora.payments.tenant_addon_purchase.payment_captured.v1.
//
// Side-effects:
//   - Calls AddOnActivator.Activate(tenant, addon, admin, tier) to flip
//     the AddOn Subscription aggregate to ACTIVE (creates the row if
//     not yet present; promotes pending / grace / deactivated rows).
//   - Emits chora.tenancy.tenant_addon.activated.v1 for observability +
//     governance + the FE polling-loop that drives the `Installed ✓`
//     flip on /h/marketplace/:planId.
//
// Idempotent on event_id via the inbox; the registry's Subscribe is
// also no-op on an existing ACTIVE row, so re-delivery is safe end-to-end.
func (s *PaymentsSubscriber) HandleTenantAddonPaymentCaptured(ctx context.Context, in TenantAddonPaymentCaptured) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if s.activator == nil {
		return errors.New("events.PaymentsSubscriber: AddOnActivator required for tenant_addon_purchase")
	}
	if err := validateTenantAddonCaptured(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.captured:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		if err := s.activator.Activate(ctx, in.TargetTenantID, in.AddonCode, in.AdminGCID, in.TierCode); err != nil {
			return fmt.Errorf("activate %s/%s: %w", in.TargetTenantID, in.AddonCode, err)
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":              in.PurchaseID,
			"admin_gcid":               in.AdminGCID,
			"target_tenant_id":         in.TargetTenantID,
			"addon_plan_id":            in.AddonPlanID,
			"addon_code":               in.AddonCode,
			"tier_code":                in.TierCode,
			"stripe_session_id":        in.StripeSessionID,
			"stripe_payment_intent_id": in.StripePaymentIntentID,
			"stripe_charge_id":         in.StripeChargeID,
			"amount_cents_paid":        in.AmountCentsPaid,
			"currency":                 in.Currency,
			"activated_at":             formatTime(in.PaidAt),
		}
		s.pub.Publish(TopicTenancyTenantAddonActivated, hdr, payload)
		return nil
	})
}

func validateTenantAddonCaptured(in TenantAddonPaymentCaptured) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonCode == "" {
		return errors.New("events.PaymentsSubscriber: addon_code required")
	}
	if in.AdminGCID == "" {
		return errors.New("events.PaymentsSubscriber: admin_gcid required")
	}
	return nil
}

// HandleTenantAddonRefunded is the entry point for
// chora.payments.tenant_addon_purchase.refunded.v1.
//
// Side-effects:
//   - Calls AddOnActivator.Deactivate(tenant, addon, admin, reason) to
//     transition the AddOn Subscription off ACTIVE (per the existing
//     CHO-1731 deactivation flow).
//   - Emits chora.tenancy.tenant_addon.deactivated.v1 for observability.
//
// Idempotent on event_id via the inbox.
func (s *PaymentsSubscriber) HandleTenantAddonRefunded(ctx context.Context, in TenantAddonRefunded) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if s.activator == nil {
		return errors.New("events.PaymentsSubscriber: AddOnActivator required for tenant_addon_purchase")
	}
	if err := validateTenantAddonRefunded(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.refunded:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		if err := s.activator.Deactivate(ctx, in.TargetTenantID, in.AddonPlanID, in.AdminGCID, in.Reason); err != nil {
			return fmt.Errorf("deactivate %s/%s: %w", in.TargetTenantID, in.AddonPlanID, err)
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":           in.PurchaseID,
			"admin_gcid":            in.AdminGCID,
			"target_tenant_id":      in.TargetTenantID,
			"addon_plan_id":         in.AddonPlanID,
			"stripe_charge_id":      in.StripeChargeID,
			"stripe_refund_id":      in.StripeRefundID,
			"amount_cents_refunded": in.AmountCentsRefunded,
			"currency":              in.Currency,
			"reason":                in.Reason,
			"deactivated_at":        formatTime(in.RefundedAt),
		}
		s.pub.Publish(TopicTenancyTenantAddonDeactivated, hdr, payload)
		return nil
	})
}

func validateTenantAddonRefunded(in TenantAddonRefunded) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonPlanID == "" {
		return errors.New("events.PaymentsSubscriber: addon_plan_id required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// CHO-1775 — Stripe Subscription lifecycle Phase 2 (cancelled / payment_failed /
// payment_recovered).
// -----------------------------------------------------------------------------

// TenantAddonSubscriptionCancelled mirrors
// chora.payments.tenant_addon_purchase.cancelled.v1.
type TenantAddonSubscriptionCancelled struct {
	EventID              string
	PurchaseID           string
	AdminGCID            string
	TargetTenantID       string
	AddonPlanID          string
	AddonCode            string
	TierCode             string
	StripeSubscriptionID string
	StripeCustomerID     string
	CancelledAt          time.Time
	Traceparent          string
}

// TenantAddonSubscriptionPaymentFailed mirrors
// chora.payments.tenant_addon_purchase.subscription_payment_failed.v1.
type TenantAddonSubscriptionPaymentFailed struct {
	EventID              string
	PurchaseID           string
	AdminGCID            string
	TargetTenantID       string
	AddonPlanID          string
	AddonCode            string
	TierCode             string
	StripeSubscriptionID string
	StripeCustomerID     string
	Currency             string
	FailedAt             time.Time
	Traceparent          string
}

// TenantAddonSubscriptionScheduleReleased mirrors
// chora.payments.tenant_addon_purchase.subscription_schedule_released.v1
// (CHO-1779). ToTier is the now-active tier on the row post-release.
// from_tier is NOT carried — derived from the registry's CurrentTier
// at handler dispatch time via PromoteScheduledTier.
type TenantAddonSubscriptionScheduleReleased struct {
	EventID              string
	PurchaseID           string
	AdminGCID            string
	TargetTenantID       string
	AddonPlanID          string
	AddonCode            string
	ToTier               string
	StripeSubscriptionID string
	ReleasedAt           time.Time
	Traceparent          string
}

// TenantAddonPaymentRecovered mirrors
// chora.payments.tenant_addon_purchase.payment_recovered.v1.
type TenantAddonPaymentRecovered struct {
	EventID              string
	PurchaseID           string
	AdminGCID            string
	TargetTenantID       string
	AddonPlanID          string
	AddonCode            string
	TierCode             string
	StripeSubscriptionID string
	StripeCustomerID     string
	Currency             string
	RecoveredAt          time.Time
	Traceparent          string
}

// HandleTenantAddonSubscriptionCancelled handles the Stripe cycle-anchor
// cancellation event. Idempotent on event_id via the inbox.
//
// CHO-1783 — when a DeactivationAnchorer is wired, this flips the
// registry directly to DEACTIVATED via the addon_code (no 30-day grace
// stack-up on rows that were already in PENDING_DEACTIVATION). When
// nil, falls back to the legacy AddOnActivator.Deactivate path (which
// runs Unsubscribe → grace) for backwards compatibility.
//
// Emits chora.tenancy.tenant_addon.deactivated.v1.
func (s *PaymentsSubscriber) HandleTenantAddonSubscriptionCancelled(ctx context.Context, in TenantAddonSubscriptionCancelled) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if s.activator == nil && s.deactAnchorer == nil {
		return errors.New("events.PaymentsSubscriber: AddOnActivator or DeactivationAnchorer required for tenant_addon_purchase")
	}
	if err := validateTenantAddonSubscriptionCancelled(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.subscription_cancelled:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		if s.deactAnchorer != nil && in.AddonCode != "" {
			// CHO-1783 — direct flip to DEACTIVATED at the Stripe anchor.
			// addon_code is the registry key (registry is keyed
			// tenant|addon_code, not by addon_plan_id).
			if _, err := s.deactAnchorer.AnchorDeactivation(in.TargetTenantID, in.AddonCode); err != nil {
				return fmt.Errorf("AnchorDeactivation %s/%s: %w", in.TargetTenantID, in.AddonCode, err)
			}
		} else if s.activator != nil {
			// Legacy path — Unsubscribe → 30-day grace.
			if err := s.activator.Deactivate(ctx, in.TargetTenantID, in.AddonPlanID, in.AdminGCID, "stripe_subscription_cancelled"); err != nil {
				return fmt.Errorf("deactivate %s/%s: %w", in.TargetTenantID, in.AddonPlanID, err)
			}
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":            in.PurchaseID,
			"admin_gcid":             in.AdminGCID,
			"target_tenant_id":       in.TargetTenantID,
			"addon_plan_id":          in.AddonPlanID,
			"addon_code":             in.AddonCode,
			"tier_code":              in.TierCode,
			"stripe_subscription_id": in.StripeSubscriptionID,
			"stripe_customer_id":     in.StripeCustomerID,
			"reason":                 "stripe_subscription_cancelled",
			"deactivated_at":         formatTime(in.CancelledAt),
		}
		s.pub.Publish(TopicTenancyTenantAddonDeactivated, hdr, payload)
		return nil
	})
}

// HandleTenantAddonSubscriptionPaymentFailed — CHO-1776 Phase 2b: flip
// the AddOn registry's PastDue flag → emit downstream
// chora.tenancy.tenant_addon.payment_failed.v1 for chora-notifications
// dunning email + FE banner. Registry mutation is BEST-EFFORT: if the
// registry isn't wired (Phase 2a fallback) OR the (tenant, addon)
// lookup misses, the handler still emits + acks so re-delivery doesn't
// loop. Lookup miss is logged + swallowed for the same reason.
func (s *PaymentsSubscriber) HandleTenantAddonSubscriptionPaymentFailed(ctx context.Context, in TenantAddonSubscriptionPaymentFailed) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if err := validateTenantAddonSubscriptionPaymentFailed(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.subscription_payment_failed:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		if s.pastDueReg != nil && in.AddonCode != "" {
			if err := s.pastDueReg.MarkPastDue(in.TargetTenantID, in.AddonCode); err != nil {
				log.Printf("tenancy/payments: MarkPastDue tenant=%s addon=%s: %v (continuing — best-effort)", in.TargetTenantID, in.AddonCode, err)
			}
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":            in.PurchaseID,
			"admin_gcid":             in.AdminGCID,
			"target_tenant_id":       in.TargetTenantID,
			"addon_plan_id":          in.AddonPlanID,
			"addon_code":             in.AddonCode,
			"tier_code":              in.TierCode,
			"stripe_subscription_id": in.StripeSubscriptionID,
			"stripe_customer_id":     in.StripeCustomerID,
			"currency":               in.Currency,
			"failed_at":              formatTime(in.FailedAt),
		}
		s.pub.Publish(TopicTenancyTenantAddonPaymentFailed, hdr, payload)
		return nil
	})
}

// HandleTenantAddonPaymentRecovered — CHO-1776 Phase 2b: clear the
// PastDue flag → emit downstream chora.tenancy.tenant_addon.payment_recovered.v1
// for recovery email + FE banner clear. Registry mutation is best-effort
// per the same rationale as the payment-failed path.
func (s *PaymentsSubscriber) HandleTenantAddonPaymentRecovered(ctx context.Context, in TenantAddonPaymentRecovered) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if err := validateTenantAddonPaymentRecovered(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.payment_recovered:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		if s.pastDueReg != nil && in.AddonCode != "" {
			if err := s.pastDueReg.MarkRecovered(in.TargetTenantID, in.AddonCode); err != nil {
				log.Printf("tenancy/payments: MarkRecovered tenant=%s addon=%s: %v (continuing — best-effort)", in.TargetTenantID, in.AddonCode, err)
			}
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":            in.PurchaseID,
			"admin_gcid":             in.AdminGCID,
			"target_tenant_id":       in.TargetTenantID,
			"addon_plan_id":          in.AddonPlanID,
			"addon_code":             in.AddonCode,
			"tier_code":              in.TierCode,
			"stripe_subscription_id": in.StripeSubscriptionID,
			"stripe_customer_id":     in.StripeCustomerID,
			"currency":               in.Currency,
			"recovered_at":           formatTime(in.RecoveredAt),
		}
		s.pub.Publish(TopicTenancyTenantAddonPaymentRecovered, hdr, payload)
		return nil
	})
}

// HandleTenantAddonSubscriptionScheduleReleased — CHO-1779. Promotes
// the in-memory registry's ScheduledTier → CurrentTier so /h/addons
// reflects the now-active tier without a chora-tenancy restart. Emits
// chora.tenancy.tenant_addon.upgraded.v1 / downgraded.v1 downstream
// for observability + (deferred) notifications email. Registry
// mutation is BEST-EFFORT: if the promoter isn't wired or the
// (tenant, addon) lookup misses, the handler still emits + acks so
// re-delivery doesn't loop.
func (s *PaymentsSubscriber) HandleTenantAddonSubscriptionScheduleReleased(ctx context.Context, in TenantAddonSubscriptionScheduleReleased) error {
	if s == nil || s.pub == nil {
		return errors.New("events.PaymentsSubscriber: nil")
	}
	if err := validateTenantAddonSubscriptionScheduleReleased(in); err != nil {
		return err
	}
	key := "tenancy.payments.tenant_addon.subscription_schedule_released:" + in.EventID
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		// Default: assume upgrade unless the registry tells us otherwise
		// (e.g. a downgrade scheduled via end-of-cycle).
		promotedKind := "upgraded"
		fromTier := ""
		toTier := in.ToTier
		if s.scheduledReg != nil && in.AddonCode != "" {
			pk, ft, tt, err := s.scheduledReg.PromoteScheduledTier(in.TargetTenantID, in.AddonCode)
			if err != nil {
				log.Printf("tenancy/payments: PromoteScheduledTier tenant=%s addon=%s: %v (continuing — best-effort)", in.TargetTenantID, in.AddonCode, err)
			} else if pk != "" {
				promotedKind = pk
				fromTier = ft
				toTier = tt
			}
		}
		topic := TopicTenancyTenantAddonUpgraded
		if promotedKind == "downgraded" {
			topic = TopicTenancyTenantAddonDowngraded
		}
		hdr := Header{TenantID: in.TargetTenantID, GCID: in.AdminGCID, Traceparent: in.Traceparent}
		payload := map[string]interface{}{
			"purchase_id":            in.PurchaseID,
			"admin_gcid":             in.AdminGCID,
			"target_tenant_id":       in.TargetTenantID,
			"addon_plan_id":          in.AddonPlanID,
			"addon_code":             in.AddonCode,
			"from_tier":              fromTier,
			"to_tier":                toTier,
			"stripe_subscription_id": in.StripeSubscriptionID,
			"released_at":            formatTime(in.ReleasedAt),
		}
		s.pub.Publish(topic, hdr, payload)
		return nil
	})
}

func validateTenantAddonSubscriptionScheduleReleased(in TenantAddonSubscriptionScheduleReleased) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonPlanID == "" {
		return errors.New("events.PaymentsSubscriber: addon_plan_id required")
	}
	return nil
}

func validateTenantAddonSubscriptionCancelled(in TenantAddonSubscriptionCancelled) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonPlanID == "" {
		return errors.New("events.PaymentsSubscriber: addon_plan_id required")
	}
	return nil
}

func validateTenantAddonSubscriptionPaymentFailed(in TenantAddonSubscriptionPaymentFailed) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonPlanID == "" {
		return errors.New("events.PaymentsSubscriber: addon_plan_id required")
	}
	return nil
}

func validateTenantAddonPaymentRecovered(in TenantAddonPaymentRecovered) error {
	if in.EventID == "" {
		return errors.New("events.PaymentsSubscriber: event_id required")
	}
	if in.PurchaseID == "" {
		return errors.New("events.PaymentsSubscriber: purchase_id required")
	}
	if in.TargetTenantID == "" {
		return errors.New("events.PaymentsSubscriber: target_tenant_id required")
	}
	if in.AddonPlanID == "" {
		return errors.New("events.PaymentsSubscriber: addon_plan_id required")
	}
	return nil
}
