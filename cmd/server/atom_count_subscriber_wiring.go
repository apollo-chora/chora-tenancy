// atom_count_subscriber_wiring.go — ADR-217 Debt 3 (CHO-2011) bootstrap for the
// atom-count projection subscriber. Spawns one CloudSubscriber goroutine per
// atom lifecycle topic (chora-tenancy-atomcount-* subscriptions; self-provisioned
// at boot via ensureSubscription per CHO-1811 so a rebuilt stack self-heals).
// Mirrors startTransactionLedgerSubscribers; idempotency + resilience live in
// the subscriber + its inbox (agentic-resilience-d6 Pillar 2).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	tnevents "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
)

// startAtomCountSubscribers spawns one goroutine per atom lifecycle topic +
// returns a WaitGroup that completes when all goroutines exit. ctx cancellation
// drains every loop. `client` may be nil in dev (no-op + nil).
func startAtomCountSubscribers(
	ctx context.Context,
	client cgcpubsub.CloudPubSubClient,
	subscriber *tnevents.AtomCountSubscriber,
) (*sync.WaitGroup, error) {
	if subscriber == nil {
		return nil, errors.New("startAtomCountSubscribers: nil subscriber")
	}
	if client == nil {
		log.Printf("tenancy: ADR-217 atom-count subscribers SKIPPED (CHORA_PUBSUB_PROJECT unset; in-memory bus has no streaming-pull surface)")
		return nil, nil
	}

	sub := cgcpubsub.NewCloudSubscriber(client)
	wg := &sync.WaitGroup{}
	for _, topic := range subscriber.Topics() {
		topicCopy := topic
		subscription := subscriber.SubscriptionNameForTopic(topicCopy)
		if subscription == "" {
			return wg, fmt.Errorf("startAtomCountSubscribers: empty subscription for topic %q", topicCopy)
		}
		handler := func(hctx context.Context, msg *cgcpubsub.Message) error {
			return subscriber.Handle(hctx, topicCopy, msg)
		}
		wg.Add(1)
		go func(topic, subscription string, h cgcpubsub.Handler) {
			defer wg.Done()
			// Self-provision INSIDE the goroutine (CHO-1811) so the boot loop
			// returns immediately (a synchronous ensure delayed server start
			// past the liveness probe → SIGTERM mid-bootstrap → fatal exit).
			ensureSubscription(ctx, client, subscription, topic)
			log.Printf("tenancy: ADR-217 atom-count subscriber started (topic=%s subscription=%s)", topic, subscription)
			if err := sub.Subscribe(ctx, subscription, h); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("tenancy: ADR-217 atom-count subscriber exited (topic=%s): %v", topic, err)
			}
		}(topicCopy, subscription, handler)
	}
	return wg, nil
}
