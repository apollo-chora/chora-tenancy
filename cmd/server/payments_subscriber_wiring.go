// ADR-164 Wave 1 Stage D — chora-payments Pub/Sub subscriber bootstrap.
//
// chora-tenancy is the originating service for two Purchase aggregates
// extracted to chora-payments:
//
//   - FamiliarEggPurchase  — ADR-149 Familiar-egg purchases.
//   - TenantManaTopUp      — tenant mana pool top-ups.
//
// The chora-payments service emits 5 canonical events on those aggregates
// (post-Stage A); this wiring starts CloudSubscriber goroutines that
// drain those topics + route to events.PaymentsSubscriber for the
// originating-service notification work (provisioning + mana balance
// credit/debit + legacy bridge event re-emit).
//
// On startup:
//
//	startPaymentsSubscribers(ctx, pubsubClient, subscriber, env) spawns
//	one goroutine per topic, each blocking on
//	cgcpubsub.CloudSubscriber.Subscribe(ctx, subscriptionName, handler).
//	Subscriptions follow the chora-tenancy convention:
//
//	  chora-tenancy-payments-{aggregate}-{event_type}
//
//	When the Pub/Sub client is unwired (CHORA_PUBSUB_PROJECT unset / dev
//	mode), the function logs + returns nil — the in-process bus does NOT
//	have a streaming-pull surface in this service, mirroring the
//	chora-observability pattern at token_usage_binding.go.
//
// Idempotency: every handler is wrapped by the PaymentsSubscriber's inbox
// (libs/chora-go-common/idempotent.Store) so Pub/Sub redelivery + multi-
// replica replays are no-ops. Postgres-backed inbox when CHORA_OUTBOX_DSN
// is set; in-memory fallback for dev.
//
// Per `agentic-resilience-d6` skill Pillar 2 (consumer-side dual of the
// outbox).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	tnevents "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"

	paymentsv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/payments/v1"
	"google.golang.org/protobuf/proto"
)

// paymentsTopics are the 7 ADR-164 inbound topics chora-tenancy consumes
// (5 existing + 2 new CHO-1740 tenant_addon_purchase saga topics).
var paymentsTopics = []string{
	tnevents.TopicFamiliarEggPaymentCaptured,
	tnevents.TopicFamiliarEggRefunded,
	tnevents.TopicFamiliarEggExpired,
	tnevents.TopicManaTopUpPaymentCaptured,
	tnevents.TopicManaTopUpRefunded,
	// CHO-1740 — H+ Marketplace Subscribe saga.
	tnevents.TopicTenantAddonPaymentCaptured,
	tnevents.TopicTenantAddonRefunded,
	// CHO-1775 — Stripe Subscription lifecycle Phase 2 (Dashboard-side
	// cancel + dunning + recovery).
	tnevents.TopicTenantAddonSubscriptionCancelled,
	tnevents.TopicTenantAddonSubscriptionPaymentFailed,
	tnevents.TopicTenantAddonPaymentRecovered,
	// CHO-1779 — subscription_schedule.released event for end-of-cycle
	// tier change auto-promotion on the in-memory registry.
	tnevents.TopicTenantAddonSubscriptionScheduleReleased,
}

// startPaymentsSubscribers spawns one goroutine per ADR-164 inbound topic
// + returns a WaitGroup that completes when all goroutines exit. ctx
// cancellation drains every loop.
//
// `client` may be nil in dev (CHORA_PUBSUB_PROJECT unset) — the function
// no-ops + returns nil in that case. `subscriber` MUST be non-nil.
//
// Subscription names follow `chora-tenancy-payments-{aggregate}-{event_
// type}`. Subscriptions are CONSUMER-owned: this code self-provisions any
// missing subscription at boot via EnsureSubscription (CHO-1811 — a rebuilt
// stack previously left these permanently dead because they were never
// provisioned out-of-band, silently breaking paid H+ add-on activation).
func startPaymentsSubscribers(
	ctx context.Context,
	client cgcpubsub.CloudPubSubClient,
	subscriber *tnevents.PaymentsSubscriber,
) (*sync.WaitGroup, error) {
	if subscriber == nil {
		return nil, errors.New("startPaymentsSubscribers: nil PaymentsSubscriber")
	}
	if client == nil {
		log.Printf("tenancy: ADR-164 payments subscribers SKIPPED (CHORA_PUBSUB_PROJECT unset; in-memory bus has no streaming-pull surface)")
		return nil, nil
	}

	sub := cgcpubsub.NewCloudSubscriber(client)
	wg := &sync.WaitGroup{}
	for _, topic := range paymentsTopics {
		topicCopy := topic
		subscription := subscriber.SubscriptionNameForTopic(topicCopy)
		if subscription == "" {
			return wg, fmt.Errorf("startPaymentsSubscribers: empty subscription for topic %q", topicCopy)
		}
		handler := buildPaymentsHandler(subscriber, topicCopy)
		wg.Add(1)
		go func(topic, subscription string) {
			defer wg.Done()
			// Self-provision INSIDE the goroutine (CHO-1811) so the boot loop
			// returns immediately. Doing the ~29 ensure admin calls synchronously
			// delayed server start past the liveness probe; the probe-triggered
			// SIGTERM then cancelled in-flight bootstrap (stripe creds) → fatal
			// exit → crash-loop. Async ensure keeps boot fast; each goroutine
			// ensures its own sub before subscribing.
			ensureSubscription(ctx, client, subscription, topic)
			log.Printf("tenancy: ADR-164 payments subscriber started (topic=%s subscription=%s)", topic, subscription)
			if err := sub.Subscribe(ctx, subscription, handler); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("tenancy: ADR-164 payments subscriber exited (topic=%s): %v", topic, err)
			}
		}(topicCopy, subscription)
	}
	return wg, nil
}

// dlqTopicFor maps a primary topic to its DLQ counterpart
// (chora.payments.X.v1 → chora.dlq.payments.X.v1), matching the
// chora.dlq.{primary-topic-suffix} convention in chora-infra/topics/topics.yaml.
func dlqTopicFor(topic string) string {
	return strings.Replace(topic, "chora.", "chora.dlq.", 1)
}

// ensureSubscription self-provisions the pull subscription if it is missing
// (CHO-1811 self-heal — shared by the ADR-164 payments + ADR-205 txledger
// subscribers). Config matches the chora-tenancy-{payments,txledger}-* subs
// (ack 60s / DLQ max-5 / retry 10s→600s / 7d retention / never-expire). No-op
// when the client does not support admin ops (in-memory/stub clients in
// dev/tests) or when the sub already exists. Never crashes the boot: an ensure
// failure is logged loud and the caller still attempts Subscribe (which
// surfaces the missing sub if creation genuinely failed).
func ensureSubscription(ctx context.Context, client cgcpubsub.CloudPubSubClient, subscription, topic string) {
	ensurer, ok := client.(cgcpubsub.SubscriptionEnsurer)
	if !ok {
		return
	}
	created, err := ensurer.EnsureSubscription(ctx, cgcpubsub.EnsureSubscriptionConfig{
		Subscription:        subscription,
		Topic:               topic,
		DeadLetterTopic:     dlqTopicFor(topic),
		AckDeadline:         60 * time.Second,
		MaxDeliveryAttempts: 5,
		MinBackoff:          10 * time.Second,
		MaxBackoff:          600 * time.Second,
		RetentionDuration:   7 * 24 * time.Hour,
		NeverExpire:         true,
	})
	switch {
	case err != nil:
		log.Printf("tenancy: ensure subscription %s FAILED (continuing to subscribe): %v", subscription, err)
	case created:
		log.Printf("tenancy: self-provisioned subscription %s (topic=%s)", subscription, topic)
	}
}

// buildPaymentsHandler returns a cgcpubsub.Handler that decodes the
// protobuf payload + invokes the right PaymentsSubscriber method for the
// given topic. Decode failures are surfaced as errors so the Cloud Sub
// loop Nacks → broker retries → DLQ.
func buildPaymentsHandler(s *tnevents.PaymentsSubscriber, topic string) cgcpubsub.Handler {
	return func(ctx context.Context, msg *cgcpubsub.Message) error {
		if msg == nil {
			return errors.New("payments handler: nil message")
		}
		env := msg.Envelope
		tenantID := strings.TrimSpace(env.TenantID)
		traceparent := strings.TrimSpace(env.Traceparent)
		switch topic {
		case tnevents.TopicFamiliarEggPaymentCaptured:
			var ev paymentsv1.CompanionEggPurchasePaymentCaptured
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleFamiliarEggPaymentCaptured(ctx, tnevents.FamiliarEggPaymentCaptured{
				EventID:               env.EventID,
				PurchaseID:            ev.GetPurchaseId(),
				LearnerGCID:           ev.GetLearnerGcid(),
				TargetTenantID:        firstNonBlank(ev.GetTargetTenantId(), tenantID),
				EggSKU:                ev.GetEggSku(),
				StripeSessionID:       ev.GetStripeSessionId(),
				StripePaymentIntentID: ev.GetStripePaymentIntentId(),
				StripeChargeID:        ev.GetStripeChargeId(),
				AmountCentsPaid:       ev.GetAmountCentsPaid(),
				AmountCentsRefunded:   ev.GetAmountCentsRefunded(),
				Currency:              ev.GetCurrency(),
				SuggestedFocalAtomID:  ev.GetSuggestedFocalAtomId(),
				PaidAt:                ev.GetPaidAt().AsTime(),
				Traceparent:           traceparent,
			})

		case tnevents.TopicFamiliarEggRefunded:
			var ev paymentsv1.CompanionEggPurchaseRefunded
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			// NOTE: the chora-contracts v1 FamiliarEggPurchaseRefunded proto
			// does NOT carry mana_units_credit (contracts frozen at HEAD
			// 35b072de per ADR-164 Stage D §0 #7). The subscriber's
			// FamiliarEggRefunded shape exposes ManaUnitsCredit because the
			// downstream credit decision belongs to chora-tenancy (it knows
			// the originating purchase amount + the ADR-149 §"Unhatched egg
			// expiry" 50%-credit policy). For Stage D wiring we set the
			// credit to AmountCentsRefunded directly when CreditOnly=true —
			// the 1:1 cents→mana baseline. A follow-on contract bump (Stage
			// E or post-cutover) will replace this with an explicit
			// mana_units_credit field on the event.
			manaCredit := int64(0)
			if ev.GetCreditOnly() {
				manaCredit = ev.GetAmountCentsRefunded()
			}
			return s.HandleFamiliarEggRefunded(ctx, tnevents.FamiliarEggRefunded{
				EventID:             env.EventID,
				PurchaseID:          ev.GetPurchaseId(),
				LearnerGCID:         ev.GetLearnerGcid(),
				TargetTenantID:      tenantID, // refunded proto omits target_tenant_id; envelope.tenant_id is canonical
				EggSKU:              ev.GetEggSku(),
				StripeChargeID:      ev.GetStripeChargeId(),
				StripeRefundID:      ev.GetStripeRefundId(),
				AmountCentsRefunded: ev.GetAmountCentsRefunded(),
				Currency:            ev.GetCurrency(),
				Reason:              ev.GetReason(),
				CreditOnly:          ev.GetCreditOnly(),
				ManaUnitsCredit:     manaCredit,
				RefundedAt:          ev.GetRefundedAt().AsTime(),
				Traceparent:         traceparent,
			})

		case tnevents.TopicFamiliarEggExpired:
			var ev paymentsv1.CompanionEggPurchaseExpired
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleFamiliarEggExpired(ctx, tnevents.FamiliarEggExpired{
				EventID:         env.EventID,
				PurchaseID:      ev.GetPurchaseId(),
				LearnerGCID:     ev.GetLearnerGcid(),
				TargetTenantID:  tenantID, // expired proto omits target_tenant_id; envelope.tenant_id is canonical
				EggSKU:          ev.GetEggSku(),
				StripeSessionID: ev.GetStripeSessionId(),
				ExpiredAt:       ev.GetExpiredAt().AsTime(),
				Traceparent:     traceparent,
			})

		case tnevents.TopicManaTopUpPaymentCaptured:
			var ev paymentsv1.TenantManaTopUpPaymentCaptured
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			// TenantManaTopUp events scope tenant via the canonical
			// envelope.tenant_id (the topup IS the tenant action; no
			// target_tenant_id is carried in the payload).
			return s.HandleManaTopUpPaymentCaptured(ctx, tnevents.TenantManaTopUpPaymentCaptured{
				EventID:               env.EventID,
				PurchaseID:            ev.GetPurchaseId(),
				AdminGCID:             ev.GetAdminGcid(),
				TargetTenantID:        tenantID,
				SKU:                   ev.GetSku(),
				StripeSessionID:       ev.GetStripeSessionId(),
				StripePaymentIntentID: ev.GetStripePaymentIntentId(),
				StripeChargeID:        ev.GetStripeChargeId(),
				AmountCentsPaid:       ev.GetAmountCentsPaid(),
				AmountCentsRefunded:   ev.GetAmountCentsRefunded(),
				Currency:              ev.GetCurrency(),
				ManaUnits:             ev.GetManaUnits(),
				PaidAt:                ev.GetPaidAt().AsTime(),
				Traceparent:           traceparent,
			})

		case tnevents.TopicManaTopUpRefunded:
			var ev paymentsv1.TenantManaTopUpRefunded
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleManaTopUpRefunded(ctx, tnevents.TenantManaTopUpRefunded{
				EventID:             env.EventID,
				PurchaseID:          ev.GetPurchaseId(),
				AdminGCID:           ev.GetAdminGcid(),
				TargetTenantID:      tenantID,
				SKU:                 ev.GetSku(),
				StripeChargeID:      ev.GetStripeChargeId(),
				StripeRefundID:      ev.GetStripeRefundId(),
				AmountCentsRefunded: ev.GetAmountCentsRefunded(),
				Currency:            ev.GetCurrency(),
				ManaUnitsToDebit:    ev.GetManaUnitsToDebit(),
				Reason:              ev.GetReason(),
				RefundedAt:          ev.GetRefundedAt().AsTime(),
				Traceparent:         traceparent,
			})

		case tnevents.TopicTenantAddonPaymentCaptured:
			var ev paymentsv1.TenantAddonPurchasePaymentCaptured
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonPaymentCaptured(ctx, tnevents.TenantAddonPaymentCaptured{
				EventID:               env.EventID,
				PurchaseID:            ev.GetPurchaseId(),
				AdminGCID:             ev.GetAdminGcid(),
				TargetTenantID:        tenantID,
				AddonPlanID:           ev.GetAddonPlanId(),
				AddonCode:             ev.GetAddonCode(),
				TierCode:              ev.GetTierCode(),
				StripeSessionID:       ev.GetStripeSessionId(),
				StripePaymentIntentID: ev.GetStripePaymentIntentId(),
				StripeChargeID:        ev.GetStripeChargeId(),
				AmountCentsPaid:       ev.GetAmountCentsPaid(),
				AmountCentsRefunded:   ev.GetAmountCentsRefunded(),
				Currency:              ev.GetCurrency(),
				PaidAt:                ev.GetPaidAt().AsTime(),
				Traceparent:           traceparent,
			})

		case tnevents.TopicTenantAddonRefunded:
			var ev paymentsv1.TenantAddonPurchaseRefunded
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonRefunded(ctx, tnevents.TenantAddonRefunded{
				EventID:             env.EventID,
				PurchaseID:          ev.GetPurchaseId(),
				AdminGCID:           ev.GetAdminGcid(),
				TargetTenantID:      tenantID,
				AddonPlanID:         ev.GetAddonPlanId(),
				StripeChargeID:      ev.GetStripeChargeId(),
				StripeRefundID:      ev.GetStripeRefundId(),
				AmountCentsRefunded: ev.GetAmountCentsRefunded(),
				Currency:            ev.GetCurrency(),
				Reason:              ev.GetReason(),
				RefundedAt:          ev.GetRefundedAt().AsTime(),
				Traceparent:         traceparent,
			})

		// CHO-1775 — Stripe Subscription lifecycle Phase 2.
		case tnevents.TopicTenantAddonSubscriptionCancelled:
			var ev paymentsv1.TenantAddonPurchaseSubscriptionCancelled
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonSubscriptionCancelled(ctx, tnevents.TenantAddonSubscriptionCancelled{
				EventID:              env.EventID,
				PurchaseID:           ev.GetPurchaseId(),
				AdminGCID:            ev.GetAdminGcid(),
				TargetTenantID:       tenantID,
				AddonPlanID:          ev.GetAddonPlanId(),
				AddonCode:            ev.GetAddonCode(),
				TierCode:             ev.GetTierCode(),
				StripeSubscriptionID: ev.GetStripeSubscriptionId(),
				StripeCustomerID:     ev.GetStripeCustomerId(),
				CancelledAt:          ev.GetCancelledAt().AsTime(),
				Traceparent:          traceparent,
			})

		case tnevents.TopicTenantAddonSubscriptionPaymentFailed:
			var ev paymentsv1.TenantAddonPurchaseSubscriptionPaymentFailed
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonSubscriptionPaymentFailed(ctx, tnevents.TenantAddonSubscriptionPaymentFailed{
				EventID:              env.EventID,
				PurchaseID:           ev.GetPurchaseId(),
				AdminGCID:            ev.GetAdminGcid(),
				TargetTenantID:       tenantID,
				AddonPlanID:          ev.GetAddonPlanId(),
				AddonCode:            ev.GetAddonCode(),
				TierCode:             ev.GetTierCode(),
				StripeSubscriptionID: ev.GetStripeSubscriptionId(),
				StripeCustomerID:     ev.GetStripeCustomerId(),
				Currency:             ev.GetCurrency(),
				FailedAt:             ev.GetFailedAt().AsTime(),
				Traceparent:          traceparent,
			})

		case tnevents.TopicTenantAddonPaymentRecovered:
			var ev paymentsv1.TenantAddonPurchasePaymentRecovered
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonPaymentRecovered(ctx, tnevents.TenantAddonPaymentRecovered{
				EventID:              env.EventID,
				PurchaseID:           ev.GetPurchaseId(),
				AdminGCID:            ev.GetAdminGcid(),
				TargetTenantID:       tenantID,
				AddonPlanID:          ev.GetAddonPlanId(),
				AddonCode:            ev.GetAddonCode(),
				TierCode:             ev.GetTierCode(),
				StripeSubscriptionID: ev.GetStripeSubscriptionId(),
				StripeCustomerID:     ev.GetStripeCustomerId(),
				Currency:             ev.GetCurrency(),
				RecoveredAt:          ev.GetRecoveredAt().AsTime(),
				Traceparent:          traceparent,
			})

		case tnevents.TopicTenantAddonSubscriptionScheduleReleased:
			var ev paymentsv1.TenantAddonPurchaseSubscriptionScheduleReleased
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				return fmt.Errorf("decode %s: %w", topic, err)
			}
			return s.HandleTenantAddonSubscriptionScheduleReleased(ctx, tnevents.TenantAddonSubscriptionScheduleReleased{
				EventID:              env.EventID,
				PurchaseID:           ev.GetPurchaseId(),
				AdminGCID:            ev.GetAdminGcid(),
				TargetTenantID:       tenantID,
				AddonPlanID:          ev.GetAddonPlanId(),
				AddonCode:            ev.GetAddonCode(),
				ToTier:               ev.GetToTier(),
				StripeSubscriptionID: ev.GetStripeSubscriptionId(),
				ReleasedAt:           ev.GetReleasedAt().AsTime(),
				Traceparent:          traceparent,
			})

		default:
			return fmt.Errorf("payments handler: unknown topic %q", topic)
		}
	}
}

func firstNonBlank(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// drainPaymentsSubscribers waits for the goroutines spawned by
// startPaymentsSubscribers to exit. Bounded by ctx + timeout so a stuck
// receive loop doesn't hold up shutdown.
func drainPaymentsSubscribers(wg *sync.WaitGroup, timeout time.Duration) {
	if wg == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("tenancy: ADR-164 payments subscribers drained")
	case <-time.After(timeout):
		log.Printf("tenancy: ADR-164 payments subscribers drain deadline exceeded")
	}
}
