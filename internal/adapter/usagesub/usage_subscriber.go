// Package usagesub — A-Tenant-Lifecycle (S6.2) live consumption event
// subscriber.
//
// Listens for per-addon consumption events and increments the matching
// addon's UsageSnapshot for the (tenant, addon, day) bucket. The
// addon_usage_events table at migrations/0004_phyllis_mvp.sql is the
// production write target; this in-memory subscriber path mirrors the
// same shape so the nightly rollup job emits chora.tenancy.addon.
// usage_recorded.v1 deterministically.
//
// Idempotent on (tenant_id, addon_code, source_event_id) via the
// chora-common/idempotent.Store inbox — replayed events DO NOT
// double-count, even across pod restarts + multi-replica deployments.
// The old in-process `seenIDs map[string]struct{}` was insufficient
// under chaos (M12.3 inbox audit doc 2026-05-12 §"Why the in-process
// dedupe is insufficient under chaos"); the production PostgresStore
// wiring lives in cmd/server/main.go against the chora_tenancy
// idempotency_keys table (migration 0006_idempotency_keys.up.sql).
//
// Per CLAUDE.md §6 the cross-domain wire is events only.
// Cross-DB queries forbidden — chora-consumption publishes the upstream
// events; chora-tenancy subscribes here.
package usagesub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// InboxTTL is the dedupe-key retention window for the subscriber's inbox.
// 24h covers the broker max redelivery window (7d default) reduced for usage
// events' typical end-to-end latency. Production may tune via
// WithInboxTTL.
const InboxTTL = 24 * time.Hour

// ConsumptionEvent is the input shape for the subscriber. Fields are the
// subset of the upstream event payload that chora-tenancy cares about.
type ConsumptionEvent struct {
	Topic         string // "chora.consumption.daily_dose.served.v1"
	EventID       string // upstream event_id (UUIDv7) — idempotency key seed
	TenantID      string
	GCID          string
	AddonCode     string // optional override; if empty, derived from Topic
	OccurredAt    time.Time
	QuotaConsumed int64 // default 1
}

// AddonUsageSubscriber is the live event consumer.
//
// Inbox dedupe (M12.3 W2b, 2026-05-12): the per-event dedupe key
// (tenant_id :: addon_code :: source_event_id) lives in a
// chora-common/idempotent.Store. Production wires PostgresStore
// against the chora_tenancy database's idempotency_keys table; dev /
// tests use MemoryStore. The store survives pod-death and is shared
// across replicas, which the previous in-process dedup map (`seenIDs`)
// did NOT — critical for the chaos test's (h) "idempotency under
// duplicates" scenario.
type AddonUsageSubscriber struct {
	reg   *addon.SubscriptionRegistry
	inbox idempotent.Store
	ttl   time.Duration

	mu sync.Mutex
}

// NewAddonUsageSubscriber constructs a subscriber wired to the registry +
// inbox. The inbox parameter is REQUIRED in production — bootstrap
// supplies idempotent.PostgresStore. Passing nil triggers a defensive
// MemoryStore fallback; this preserves test fixtures that pre-date the
// inbox refactor (existing tests call NewAddonUsageSubscriber(reg) via
// the convenience constructor below).
func NewAddonUsageSubscriber(reg *addon.SubscriptionRegistry, inbox idempotent.Store) *AddonUsageSubscriber {
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &AddonUsageSubscriber{
		reg:   reg,
		inbox: inbox,
		ttl:   InboxTTL,
	}
}

// WithInboxTTL overrides the default dedupe-key retention window. Mostly
// useful in tests that want a short TTL to exercise expiry / reprocess.
func (s *AddonUsageSubscriber) WithInboxTTL(ttl time.Duration) *AddonUsageSubscriber {
	if s == nil || ttl <= 0 {
		return s
	}
	s.ttl = ttl
	return s
}

// HandleConsumptionEvent records the usage event into the matching
// addon's UsageSnapshot for the (tenant, addon, day) bucket.
//
// Idempotency is enforced by inbox.Process(...) on the
// (tenant_id :: addon_code :: source_event_id) key. A duplicate event
// delivery (pod restart, multi-replica race, retry-after-ack-window)
// hits the same key + skips the handler body, returning nil. The dedup
// token lives for InboxTTL in the domain's idempotency_keys table.
//
// Errors:
//   - empty TenantID → ErrInvalidArgument
//   - empty EventID  → ErrInvalidArgument
//   - replay (same key within TTL) → nil, no-op
//   - tenant has no active subscription → nil, no-op
func (s *AddonUsageSubscriber) HandleConsumptionEvent(ctx context.Context, ev ConsumptionEvent) error {
	if strings.TrimSpace(ev.TenantID) == "" {
		return fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(ev.EventID) == "" {
		return fmt.Errorf("%w: event_id required", ErrInvalidArgument)
	}
	addonCode := ev.AddonCode
	if addonCode == "" {
		addonCode = AddonCodeForTopic(ev.Topic)
	}
	if addonCode == "" {
		return nil // unknown topic — silent no-op
	}

	// Business-natural key: (tenant_id, addon_code, source_event_id).
	// addon_code is included so the same upstream EventID emitted for
	// multiple addons (rare but possible — e.g. cross-cutting events)
	// doesn't collide. tenant_id is included so chaos test scenario (c)
	// cross-tenant collision is impossible.
	key := "tenancy.usage:" + ev.TenantID + ":" + addonCode + ":" + ev.EventID

	return s.inbox.Process(ctx, key, s.ttl, func() error {
		return s.apply(ev, addonCode)
	})
}

// apply does the registry mutation; called at most once per key via
// inbox.Process. Held behind the mu lock so concurrent applies to the
// same (tenant, addon, day) bucket serialize.
func (s *AddonUsageSubscriber) apply(ev ConsumptionEvent, addonCode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, ok := s.reg.GetSubscription(ev.TenantID, addonCode)
	if !ok {
		return nil // no active subscription for this tenant — ignore
	}
	if !sub.IsActive(ev.OccurredAt) {
		return nil // subscription not currently active
	}
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	quota := ev.QuotaConsumed
	if quota <= 0 {
		quota = 1
	}
	existing := s.reg.ListUsage(ev.TenantID, sub.AddOnID,
		bucket, bucket.Add(24*time.Hour))
	var current addon.UsageSnapshot
	if len(existing) > 0 {
		current = *existing[0]
	} else {
		current = addon.UsageSnapshot{
			TenantID:   ev.TenantID,
			AddOnID:    sub.AddOnID,
			BucketDate: bucket,
		}
	}
	current.QuotaConsumed += quota
	current.APICalls += 1
	current.LastRecordedAt = time.Now().UTC()
	s.reg.RecordUsage(&current)
	return nil
}

// AddonCodeForTopic resolves the addon code for an upstream consumption
// topic. Returns the empty string for unknown topics. Production swaps to
// a config-driven map; the static table here covers the Phyllis MVP set
// per docs/design/ux_tenant_addon_lifecycle.md + the seed catalogue.
func AddonCodeForTopic(topic string) string {
	switch topic {
	case "chora.consumption.daily_dose.served.v1":
		return "daily_dose"
	case "chora.consumption.kg_hexagon.expanded.v1",
		"chora.consumption.kg_neighbor.expanded.v1":
		return "kg_hexagonal"
	case "chora.creation.ai_assist.generated.v1",
		"chora.creation.atom.ai_generated.v1":
		return "ai_assist"
	case "chora.delivery.course_application.created.v1",
		"chora.delivery.course_application.confirmed.v1":
		return "course_application"
	case "chora.a2a.mcp_invocation.recorded.v1",
		"chora.a2a.mcp_partner.invoked.v1":
		return "mcp_gateway"
	case "chora.sharing.duel.completed.v1",
		"chora.sharing.duel.started.v1":
		return "pvp-arena"
	case "chora.sharing.familiar_post.created.v1":
		return "familiar"
	}
	return ""
}

// ErrInvalidArgument is the canonical bad-input error.
var ErrInvalidArgument = errors.New("invalid argument")
