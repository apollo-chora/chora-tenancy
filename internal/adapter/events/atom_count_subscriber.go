// atom_count_subscriber.go — ADR-217 Debt 3 (CHO-2011) event-bus projection
// subscriber. Folds atom lifecycle events into the per-tenant tenant_atom_counts
// projection: chora.creation.atom.created.v1 → +1, chora.creation.atom.archived.v1
// → -1.
//
// Reads ONLY the envelope (tenant_id + event_id) + the topic — NO payload
// decode — because a per-tenant COUNT needs no atom fields; this decouples the
// projection from the atom payload schema entirely. Resilience
// (agentic-resilience-d6 Pillar 2): idempotent inbox keyed on event_id; a
// handler error Nacks → broker retry → DLQ; the envelope tenant drives the RLS
// write. The counter is a commutative +1/-1 fold, so it is order-independent.
package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-tenancy/internal/domain/atomcount"
)

// Atom lifecycle topics the projection folds into the per-tenant counter.
const (
	topicAtomCreated  = "chora.creation.atom.created.v1"
	topicAtomArchived = "chora.creation.atom.archived.v1"
)

// atomCountInboxTTL bounds inbox dedupe — matches the other tenancy subscribers.
const atomCountInboxTTL = 24 * time.Hour

// AtomCountSubscriber projects atom.created / atom.archived into
// tenant_atom_counts (+1 / -1 per tenant).
type AtomCountSubscriber struct {
	repo  atomcount.WriteRepository
	inbox idempotent.Store
	ttl   time.Duration
}

// NewAtomCountSubscriber wires the projection subscriber.
func NewAtomCountSubscriber(repo atomcount.WriteRepository, inbox idempotent.Store) *AtomCountSubscriber {
	return &AtomCountSubscriber{repo: repo, inbox: inbox, ttl: atomCountInboxTTL}
}

// Topics is the set of atom lifecycle topics this projection consumes.
func (s *AtomCountSubscriber) Topics() []string {
	return []string{topicAtomCreated, topicAtomArchived}
}

// SubscriptionNameForTopic maps chora.creation.atom.{event}.v1 →
// chora-tenancy-atomcount-atom-{event}.
func (s *AtomCountSubscriber) SubscriptionNameForTopic(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 5 {
		return ""
	}
	return "chora-tenancy-atomcount-" + parts[2] + "-" + parts[3]
}

// Handle folds one atom lifecycle event into the counter. A nil return Acks; an
// error Nacks (broker retry → DLQ). Only the envelope tenant_id + event_id + the
// topic are read (no payload decode).
func (s *AtomCountSubscriber) Handle(ctx context.Context, topic string, msg eventbus.Message) error {
	env := msg.Envelope
	tenantID := strings.TrimSpace(env.TenantID)
	if tenantID == "" {
		return fmt.Errorf("atomcount: empty tenant_id (topic=%s event=%s)", topic, env.EventID)
	}
	if strings.TrimSpace(env.EventID) == "" {
		return fmt.Errorf("atomcount: empty event_id (topic=%s)", topic)
	}

	var delta int64
	switch topic {
	case topicAtomCreated:
		delta = 1
	case topicAtomArchived:
		delta = -1
	default:
		return fmt.Errorf("atomcount: unhandled topic %q", topic)
	}

	return s.inbox.Process(ctx, "atomcount:"+env.EventID, s.ttl, func() error {
		tctx := tracing.WithTenantID(ctx, tenantID)
		return s.repo.ApplyDelta(tctx, tenantID, delta)
	})
}
