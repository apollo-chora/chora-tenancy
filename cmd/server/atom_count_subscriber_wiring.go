// atom_count_subscriber_wiring.go — ADR-217 Debt 3 (CHO-2011) bootstrap for the
// atom-count projection subscriber. Spawns one event-bus consumer per atom
// lifecycle topic (chora-tenancy-atomcount-* durable consumers; JetStream
// self-provisions them at boot per CHO-1811 so a rebuilt stack self-heals).
// Mirrors startTransactionLedgerSubscribers; idempotency + resilience live in
// the subscriber + its inbox (agentic-resilience-d6 Pillar 2).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/apollo-chora/chora-common/eventbus"
	tnevents "github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

// startAtomCountSubscribers binds one durable consumer per atom lifecycle
// topic + returns a WaitGroup that completes when every goroutine exits. ctx
// cancellation drains every loop. `bus` may be nil in dev (no-op + nil).
func startAtomCountSubscribers(
	ctx context.Context,
	bus eventbus.Bus,
	subscriber *tnevents.AtomCountSubscriber,
) (*sync.WaitGroup, error) {
	if subscriber == nil {
		return nil, errors.New("startAtomCountSubscribers: nil subscriber")
	}
	if bus == nil {
		log.Printf("tenancy: ADR-217 atom-count subscribers SKIPPED (NATS_URL unset; no event bus wired)")
		return nil, nil
	}

	wg := &sync.WaitGroup{}
	for _, topic := range subscriber.Topics() {
		topicCopy := topic
		subscription := subscriber.SubscriptionNameForTopic(topicCopy)
		if subscription == "" {
			return wg, fmt.Errorf("startAtomCountSubscribers: empty subscription for topic %q", topicCopy)
		}
		handler := func(hctx context.Context, msg eventbus.Message) error {
			return subscriber.Handle(hctx, topicCopy, msg)
		}
		wg.Add(1)
		go func(topic, subscription string, h eventbus.Handler) {
			defer wg.Done()
			log.Printf("tenancy: ADR-217 atom-count subscriber started (topic=%s subscription=%s)", topic, subscription)
			if err := bus.Subscribe(ctx, consumerConfig(subscription, topic), h); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("tenancy: ADR-217 atom-count subscriber exited (topic=%s): %v", topic, err)
				return
			}
			<-ctx.Done()
		}(topicCopy, subscription, handler)
	}
	return wg, nil
}
