// Package usagesub_test — A-Tenant-Lifecycle (S6.2) tests for the usage
// event subscriber. The subscriber consumes per-addon usage events
// (e.g., chora.consumption.daily_dose.served.v1) and increments the
// matching addon's UsageSnapshot for the (tenant, addon, day) bucket.
//
// Idempotent on (tenant_id, addon_code, source_event_id) — replayed
// events must NOT double-count, even across pod restarts + multi-replica
// deployments. The inbox dedup primitive is
// chora-go-common/idempotent.Store; production wires PostgresStore.
package usagesub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/usagesub"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

const (
	subTenantID = "01970000-0000-7000-8000-0000000000ad"
	subGCID     = "gcid-learner-1"
)

func newSeededRegistry(t *testing.T, addonCode string) *addon.SubscriptionRegistry {
	t.Helper()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	if _, err := reg.Subscribe(subTenantID, addonCode, "gcid-admin-seed"); err != nil {
		t.Fatalf("seed subscribe %s: %v", addonCode, err)
	}
	return reg
}

// newSubscriber wires a subscriber with a fresh MemoryStore inbox. Used by
// the existing tests that pre-date the inbox refactor.
func newSubscriber(reg *addon.SubscriptionRegistry) *usagesub.AddonUsageSubscriber {
	return usagesub.NewAddonUsageSubscriber(reg, idempotent.NewMemoryStore())
}

func TestSubscriber_HandleDailyDoseServed_IncrementsUsage(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	sub := newSubscriber(reg)
	now := time.Now().UTC()
	bucket := now.Truncate(24 * time.Hour)
	err := sub.HandleConsumptionEvent(context.Background(), usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-000000000001",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    now,
		QuotaConsumed: 1,
	})
	if err != nil {
		t.Fatalf("HandleConsumptionEvent: %v", err)
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) != 1 {
		t.Fatalf("expected 1 usage snapshot, got %d", len(snaps))
	}
	if snaps[0].QuotaConsumed != 1 {
		t.Fatalf("expected quota_consumed=1, got %d", snaps[0].QuotaConsumed)
	}
	if snaps[0].APICalls != 1 {
		t.Fatalf("expected api_calls=1, got %d", snaps[0].APICalls)
	}
}

func TestSubscriber_HandleConsumptionEvent_IsIdempotentByEventID(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	sub := newSubscriber(reg)
	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-000000000002",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("replay: %v", err)
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 {
		t.Fatalf("expected ≥1 snapshot")
	}
	if snaps[0].QuotaConsumed != 1 {
		t.Fatalf("idempotent replay must not double-count; got quota=%d", snaps[0].QuotaConsumed)
	}
}

func TestSubscriber_HandleConsumptionEvent_AggregatesAcrossEvents(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	sub := newSubscriber(reg)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	for i := 0; i < 5; i++ {
		err := sub.HandleConsumptionEvent(context.Background(), usagesub.ConsumptionEvent{
			Topic:         "chora.consumption.daily_dose.served.v1",
			EventID:       "01970000-aaaa-bbbb-cccc-00000000000" + string(rune('0'+i)),
			TenantID:      subTenantID,
			GCID:          subGCID,
			AddonCode:     "daily_dose",
			OccurredAt:    day.Add(time.Duration(i) * time.Hour),
			QuotaConsumed: 2,
		})
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		day.Add(-1*time.Hour), day.Add(48*time.Hour))
	if len(snaps) != 1 {
		t.Fatalf("expected 1 daily snapshot, got %d", len(snaps))
	}
	// 5 events × 2 quota = 10 quota consumed; 5 api calls.
	if snaps[0].QuotaConsumed != 10 {
		t.Fatalf("expected aggregated quota=10, got %d", snaps[0].QuotaConsumed)
	}
	if snaps[0].APICalls != 5 {
		t.Fatalf("expected api_calls=5, got %d", snaps[0].APICalls)
	}
}

func TestSubscriber_IgnoresEventForInactiveAddon(t *testing.T) {
	t.Parallel()
	// Tenant has NOT subscribed to daily_dose — events should be ignored.
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	sub := newSubscriber(reg)
	err := sub.HandleConsumptionEvent(context.Background(), usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000000a",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	})
	if err != nil {
		t.Fatalf("inactive should not error; got %v", err)
	}
	// No snapshot recorded.
	for _, a := range cat.List() {
		snaps := reg.ListUsage(subTenantID, a.ID,
			time.Now().UTC().Add(-1*time.Hour),
			time.Now().UTC().Add(48*time.Hour))
		if len(snaps) > 0 {
			t.Fatalf("inactive addon must not record usage; got %d snapshots for %s", len(snaps), a.Code)
		}
	}
}

func TestSubscriber_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	sub := newSubscriber(reg)
	err := sub.HandleConsumptionEvent(context.Background(), usagesub.ConsumptionEvent{
		Topic:     "chora.consumption.daily_dose.served.v1",
		EventID:   "01970000-aaaa-bbbb-cccc-00000000000b",
		AddonCode: "daily_dose",
	})
	if err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

func TestSubscriber_RejectsEmptyEventID(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	sub := newSubscriber(reg)
	err := sub.HandleConsumptionEvent(context.Background(), usagesub.ConsumptionEvent{
		Topic:     "chora.consumption.daily_dose.served.v1",
		TenantID:  subTenantID,
		AddonCode: "daily_dose",
	})
	if err == nil {
		t.Fatalf("expected error for empty event_id")
	}
}

func TestSubscriber_TopicResolverMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"chora.consumption.daily_dose.served.v1":       "daily_dose",
		"chora.consumption.kg_hexagon.expanded.v1":     "kg_hexagonal",
		"chora.creation.ai_assist.generated.v1":        "ai_assist",
		"chora.delivery.course_application.created.v1": "course_application",
		"chora.a2a.mcp_invocation.recorded.v1":         "mcp_gateway",
	}
	for topic, expectedCode := range cases {
		got := usagesub.AddonCodeForTopic(topic)
		if got != expectedCode {
			t.Fatalf("topic %s: expected addon=%s, got %q", topic, expectedCode, got)
		}
	}
}

func TestSubscriber_TopicResolverUnknown(t *testing.T) {
	t.Parallel()
	got := usagesub.AddonCodeForTopic("chora.totally.unknown.event.v1")
	if got != "" {
		t.Fatalf("unknown topic must map to empty addon code, got %q", got)
	}
}

// -----------------------------------------------------------------------------
// M12.3 W2b (2026-05-12) — Inbox chaos-readiness tests proving chaos
// scenario (h) "idempotency under duplicates" cannot fire double
// side-effects across the failure modes the in-process map dedup
// couldn't survive:
//
//   1. Pod-death: dedup state must persist outside the subscriber
//      instance. Tested by swapping the subscriber instance between
//      the first delivery and the duplicate redelivery, sharing the
//      same Store.
//   2. Multi-replica: replica A and replica B see the same event; only
//      one of them runs the handler body. Tested by running two
//      subscribers backed by the same Store concurrently.
//   3. Fresh-store negative control: independent stores process the
//      same event twice (documents the contract — chaos test MUST use
//      a shared store).
//   4. TTL expiry: a token outside the TTL allows reprocessing.
//
// Pattern mirrors the canonical W1.7 inbox tests in
// services/chora-creation/internal/adapter/events/closure_subscriber_test.go
// (the W1.7 reference for the inbox swap).
// -----------------------------------------------------------------------------

func TestSubscriber_InboxSurvivesSubscriberRecreation(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	inbox := idempotent.NewMemoryStore()

	sub1 := usagesub.NewAddonUsageSubscriber(reg, inbox)
	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000001a",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}
	if err := sub1.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("sub1: %v", err)
	}

	// Simulate pod restart: drop sub1, recreate sub2 with the SAME inbox.
	sub2 := usagesub.NewAddonUsageSubscriber(reg, inbox)
	if err := sub2.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("sub2 (post-restart): %v", err)
	}

	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 {
		t.Fatalf("expected 1 snapshot")
	}
	if snaps[0].QuotaConsumed != 1 {
		t.Errorf("pod-restart redelivery double-counted: quota=%d; want 1", snaps[0].QuotaConsumed)
	}
}

func TestSubscriber_InboxFreshStoreReprocessesEvent(t *testing.T) {
	t.Parallel()
	// Negative-control: WITHOUT a shared store (each subscriber has its own
	// MemoryStore) the inbox CANNOT dedupe across restarts. This is exactly
	// the failure mode the production PostgresStore wiring fixes.
	reg := newSeededRegistry(t, "daily_dose")

	sub1 := usagesub.NewAddonUsageSubscriber(reg, idempotent.NewMemoryStore())
	sub2 := usagesub.NewAddonUsageSubscriber(reg, idempotent.NewMemoryStore())

	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000001b",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}
	if err := sub1.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("sub1: %v", err)
	}
	if err := sub2.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("sub2: %v", err)
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 {
		t.Fatalf("expected 1 snapshot")
	}
	if snaps[0].QuotaConsumed != 2 {
		t.Errorf("FRESH-store negative control: expected double-count (quota=2) without shared dedup; got %d", snaps[0].QuotaConsumed)
	}
}

func TestSubscriber_InboxConcurrentReplicas(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	inbox := idempotent.NewMemoryStore()
	subA := usagesub.NewAddonUsageSubscriber(reg, inbox)
	subB := usagesub.NewAddonUsageSubscriber(reg, inbox)

	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000001c",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = subA.HandleConsumptionEvent(context.Background(), ev) }()
	go func() { defer wg.Done(); _ = subB.HandleConsumptionEvent(context.Background(), ev) }()
	wg.Wait()

	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 {
		t.Fatalf("multi-replica: expected 1 snapshot")
	}
	if snaps[0].QuotaConsumed != 1 {
		t.Errorf("multi-replica: expected quota=1 (exactly-once); got %d", snaps[0].QuotaConsumed)
	}
}

func TestSubscriber_InboxTTLExpiryReprocesses(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	inbox := idempotent.NewMemoryStore()
	sub := usagesub.NewAddonUsageSubscriber(reg, inbox).WithInboxTTL(50 * time.Millisecond)

	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000001d",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Advance the inbox clock past the TTL.
	inbox.Advance(100 * time.Millisecond)
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("second (post-TTL): %v", err)
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 {
		t.Fatalf("TTL-expiry: expected ≥1 snapshot")
	}
	if snaps[0].QuotaConsumed != 2 {
		t.Errorf("TTL-expiry: expected reprocess (quota=2); got %d", snaps[0].QuotaConsumed)
	}
}

// TestSubscriber_NilInboxFallback exercises the defensive in-memory
// fallback when nil is supplied at construction time.
func TestSubscriber_NilInboxFallback(t *testing.T) {
	t.Parallel()
	reg := newSeededRegistry(t, "daily_dose")
	// nil inbox → defensive MemoryStore.
	sub := usagesub.NewAddonUsageSubscriber(reg, nil)
	ev := usagesub.ConsumptionEvent{
		Topic:         "chora.consumption.daily_dose.served.v1",
		EventID:       "01970000-aaaa-bbbb-cccc-00000000001e",
		TenantID:      subTenantID,
		GCID:          subGCID,
		AddonCode:     "daily_dose",
		OccurredAt:    time.Now().UTC(),
		QuotaConsumed: 1,
	}
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("nil-inbox fallback failed: %v", err)
	}
	if err := sub.HandleConsumptionEvent(context.Background(), ev); err != nil {
		t.Fatalf("nil-inbox replay: %v", err)
	}
	a, _ := reg.GetSubscription(subTenantID, "daily_dose")
	bucket := ev.OccurredAt.Truncate(24 * time.Hour)
	snaps := reg.ListUsage(subTenantID, a.AddOnID,
		bucket.Add(-1*time.Hour), bucket.Add(48*time.Hour))
	if len(snaps) == 0 || snaps[0].QuotaConsumed != 1 {
		t.Errorf("nil-inbox fallback: expected dedupe, got quota=%v", snaps[0].QuotaConsumed)
	}
}

func TestSubscriber_WithInboxTTL_Guard(t *testing.T) {
	t.Parallel()
	s := usagesub.NewAddonUsageSubscriber(nil, nil)
	if got := s.WithInboxTTL(0); got != s {
		t.Fatal("zero TTL must return receiver unchanged")
	}
	var nilS *usagesub.AddonUsageSubscriber
	if got := nilS.WithInboxTTL(time.Hour); got != nil {
		t.Fatal("nil receiver must return nil")
	}
	_ = s.WithInboxTTL(time.Hour)
}

func TestAddonCodeForTopic_AllCodes(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"chora.consumption.daily_dose.served.v1":         "daily_dose",
		"chora.consumption.kg_hexagon.expanded.v1":       "kg_hexagonal",
		"chora.consumption.kg_neighbor.expanded.v1":      "kg_hexagonal",
		"chora.creation.ai_assist.generated.v1":          "ai_assist",
		"chora.creation.atom.ai_generated.v1":            "ai_assist",
		"chora.delivery.course_application.created.v1":   "course_application",
		"chora.delivery.course_application.confirmed.v1": "course_application",
		"chora.a2a.mcp_invocation.recorded.v1":           "mcp_gateway",
		"chora.a2a.mcp_partner.invoked.v1":               "mcp_gateway",
		"chora.sharing.duel.completed.v1":                "pvp-arena",
		"chora.sharing.familiar_post.created.v1":         "familiar",
		"chora.tenancy.unknown.topic.v1":                 "",
	}
	for topic, want := range cases {
		if got := usagesub.AddonCodeForTopic(topic); got != want {
			t.Fatalf("AddonCodeForTopic(%q) = %q want %q", topic, got, want)
		}
	}
}
