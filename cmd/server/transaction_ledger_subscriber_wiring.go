// transaction_ledger_subscriber_wiring.go — ADR-205 (CHO-1938, Wave B2)
// bootstrap for the transaction_ledger projection subscriber. Spawns one
// event-bus consumer per source topic (chora-tenancy-txledger-* durable
// consumers; JetStream self-provisions them at boot per CHO-1811 so a rebuilt
// stack self-heals instead of leaving dead subscribers). Mirrors the ADR-164
// payments subscriber bootstrap; idempotency + resilience live in the
// subscriber + its inbox (agentic-resilience-d6 Pillar 2).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	tnevents "github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

// startTransactionLedgerSubscribers binds one durable consumer per ADR-205
// source topic + returns a WaitGroup that completes when every goroutine
// exits. ctx cancellation drains every loop. `bus` may be nil in dev (no-op +
// nil).
func startTransactionLedgerSubscribers(
	ctx context.Context,
	bus eventbus.Bus,
	subscriber *tnevents.TransactionLedgerSubscriber,
) (*sync.WaitGroup, error) {
	if subscriber == nil {
		return nil, errors.New("startTransactionLedgerSubscribers: nil subscriber")
	}
	if bus == nil {
		log.Printf("tenancy: ADR-205 transaction-ledger subscribers SKIPPED (NATS_URL unset; no event bus wired)")
		return nil, nil
	}

	wg := &sync.WaitGroup{}
	for _, topic := range subscriber.Topics() {
		topicCopy := topic
		subscription := subscriber.SubscriptionNameForTopic(topicCopy)
		if subscription == "" {
			return wg, fmt.Errorf("startTransactionLedgerSubscribers: empty subscription for topic %q", topicCopy)
		}
		handler := func(hctx context.Context, msg eventbus.Message) error {
			return subscriber.Handle(hctx, topicCopy, msg)
		}
		wg.Add(1)
		go func(topic, subscription string, h eventbus.Handler) {
			defer wg.Done()
			log.Printf("tenancy: ADR-205 transaction-ledger subscriber started (topic=%s subscription=%s)", topic, subscription)
			if err := bus.Subscribe(ctx, consumerConfig(subscription, topic), h); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("tenancy: ADR-205 transaction-ledger subscriber exited (topic=%s): %v", topic, err)
				return
			}
			<-ctx.Done()
		}(topicCopy, subscription, handler)
	}
	return wg, nil
}

// consumerConfig is the shared durable-consumer tuning for every chora-tenancy
// subscriber: at-least-once with a 30s ack window, five delivery attempts, and
// the canonical _dlq.<subject> dead-letter routing.
func consumerConfig(name, subject string) eventbus.ConsumerConfig {
	return eventbus.ConsumerConfig{
		Name:       name,
		Subject:    subject,
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
		Backoff: []time.Duration{
			1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
		},
		DLQSubject: eventbus.DLQSubject(subject),
	}
}
