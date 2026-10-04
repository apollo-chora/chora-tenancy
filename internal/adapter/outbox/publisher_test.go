// Package outbox_test — OutboxPublisher adapter tests.
//
// OutboxPublisher satisfies the chora-tenancy events.PublisherWithError
// shape (PublishWithError(topic, Header, payload)) by writing the event
// to the outbox_events table (via the Store port) instead of publishing
// directly to Pub/Sub. A separate Dispatcher drains the outbox to Cloud
// Pub/Sub. This decouples tenancy event emission from Pub/Sub
// availability: a crash between domain state-write and Pub/Sub publish
// no longer loses events because the row is durably committed to
// chora_tenancy before the HTTP request returns.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — producer-side durable
// emission for chora-tenancy's `chora.tenancy.{tenant,addon}.*.v1` streams.
package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
)

func canonicalHeader() events.Header {
	return events.Header{
		TenantID:    "01970000-0000-7000-8000-000000000001",
		GCID:        "01970000-0000-7000-8000-000000000bbb",
		Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01",
	}
}

func TestOutboxPublisher_Publish_WritesRowToStore(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-tenancy",
	})

	hdr := canonicalHeader()
	rec, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", hdr, map[string]interface{}{
		"tenant_id": hdr.TenantID,
		"name":      "Acme Pte Ltd",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if rec.EventID == "" {
		t.Errorf("PublishedEvent.EventID empty")
	}
	if rec.IdempotencyKey == "" {
		t.Errorf("PublishedEvent.IdempotencyKey empty")
	}
	if rec.SourceService != "chora-tenancy" {
		t.Errorf("rec.SourceService = %q; want chora-tenancy", rec.SourceService)
	}

	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("store rows = %d; want 1", len(rows))
	}
	row := rows[0]
	if row.Topic != "chora.tenancy.tenant.created.v1" {
		t.Errorf("row.Topic = %q; want chora.tenancy.tenant.created.v1", row.Topic)
	}
	if row.TenantID != hdr.TenantID {
		t.Errorf("row.TenantID = %q; want %q", row.TenantID, hdr.TenantID)
	}
	if row.GCID != hdr.GCID {
		t.Errorf("row.GCID = %q; want %q", row.GCID, hdr.GCID)
	}
	if row.AggregateType != "tenant" {
		t.Errorf("row.AggregateType = %q; want tenant (derived from topic)", row.AggregateType)
	}
	if row.EventType != "tenancy.tenant.created" {
		t.Errorf("row.EventType = %q; want tenancy.tenant.created", row.EventType)
	}
	if row.IdempotencyKey != rec.IdempotencyKey {
		t.Errorf("row.IdempotencyKey = %q; want %q (rec)", row.IdempotencyKey, rec.IdempotencyKey)
	}
}

func TestOutboxPublisher_Publish_StampsEnvelopeFields(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-tenancy",
		Now:           func() time.Time { return now },
	})

	hdr := canonicalHeader()
	if _, err := pub.PublishWithError("chora.tenancy.addon.activated.v1", hdr, map[string]interface{}{
		"addon_code": "daily_dose",
		"tenant_id":  hdr.TenantID,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	env := rows[0].Envelope

	for _, key := range []string{
		"event_id", "idempotency_key", "tenant_id", "occurred_at", "published_at",
		"traceparent", "source_project", "source_service", "schema_version",
	} {
		if env[key] == "" {
			t.Errorf("envelope.%s empty; want non-empty (mandatory per CLAUDE.md §6)", key)
		}
	}
	if env["source_project"] != "chora-489812" {
		t.Errorf("envelope.source_project = %q; want chora-489812", env["source_project"])
	}
	if env["source_service"] != "chora-tenancy" {
		t.Errorf("envelope.source_service = %q; want chora-tenancy", env["source_service"])
	}
	if env["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %q; want 1", env["schema_version"])
	}
	if env["tenant_id"] != hdr.TenantID {
		t.Errorf("envelope.tenant_id = %q; want %q", env["tenant_id"], hdr.TenantID)
	}
}

func TestOutboxPublisher_Publish_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	hdr := events.Header{TenantID: ""}
	_, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", hdr, map[string]interface{}{})
	if err == nil {
		t.Fatalf("expected ErrEmptyTenantID, got nil")
	}
}

func TestOutboxPublisher_Publish_RejectsInvalidTopic(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	hdr := canonicalHeader()
	for _, bad := range []string{
		"not.a.topic",
		"chora.unknown.aggregate.event.v1", // wrong domain
		"chora.tenancy.addon.activated",    // missing version
		"chora.tenancy.addon.activated.v",  // bad version
	} {
		if _, err := pub.PublishWithError(bad, hdr, map[string]interface{}{}); err == nil {
			t.Errorf("topic %q: expected error; got nil", bad)
		}
	}
}

func TestOutboxPublisher_Publish_RejectsNilStore(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{}) // Store nil
	_, err := pub.PublishWithError("chora.tenancy.tenant.created.v1",
		canonicalHeader(), map[string]interface{}{})
	if err == nil {
		t.Errorf("Publish without store = nil err; want error")
	}
}

// TestOutboxPublisher_Publish_PayloadIsBinaryProtoForSchemaAttachedTopic
// asserts that for a topic with a registered Pub/Sub Schema Registry schema
// (BINARY encoding), the OutboxPublisher persists canonical binary protobuf
// wire bytes — NOT JSON. This is the load-bearing invariant from task #33
// (codebase-wide outbox protobuf encoding fix surfaced 2026-05-16).
func TestOutboxPublisher_Publish_PayloadIsBinaryProtoForSchemaAttachedTopic(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	hdr := canonicalHeader()
	body := map[string]interface{}{
		"tenant_id":            hdr.TenantID,
		"addon_plan_id":        "plan-ai-assist-001",
		"from_tier":            "standard",
		"to_tier":              "premium",
		"billing_delta_cents":  int64(1500),
		"requested_by_gcid":    hdr.GCID,
		"chora_imda_dimension": "accountability",
	}
	if _, err := pub.PublishWithError("chora.tenancy.addon.upgraded.v1", hdr, body); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	// JSON parse MUST fail — the payload should now be binary protobuf wire
	// bytes that GCP Pub/Sub Schema Registry accepts.
	var pl map[string]any
	if err := json.Unmarshal(rows[0].Payload, &pl); err == nil {
		t.Fatalf("payload parsed as JSON (%v); want binary protobuf bytes for schema-attached topic", pl)
	}
	// First byte set must be a valid protowire tag (field 1, length-delimited
	// — the envelope submessage). Sanity-check the leading byte without a
	// full proto unmarshal here (wire_compat_test in the protomarshal package
	// already round-trips against the generated bindings).
	if len(rows[0].Payload) < 4 {
		t.Fatalf("payload too short: %d bytes", len(rows[0].Payload))
	}
	if rows[0].Payload[0] != 0x0A { // tag for (field=1, wire-type=2)
		t.Errorf("first byte = 0x%02x; want 0x0a (field 1, length-delimited envelope)", rows[0].Payload[0])
	}
}

// TestOutboxPublisher_Publish_JSONFallbackForUnregisteredTopic asserts that
// topics WITHOUT a binary protobuf encoder in protomarshal fall back to
// JSON encoding with a WARN log so call-sites that emit not-yet-registered
// topics keep working. The dispatcher will dead-letter these rows IF the
// topic later gets a BINARY schema attached — that's the correct loud-
// failure mode per task #33's "no debts" directive.
func TestOutboxPublisher_Publish_JSONFallbackForUnregisteredTopic(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	hdr := canonicalHeader()
	// chora.tenancy.member.invited.v1 has NO entry in protomarshal.MarshalPayload
	// — it must fall back to JSON.
	body := map[string]interface{}{
		"tenant_id":  hdr.TenantID,
		"invitee_id": "01970000-0000-7000-8000-000000000ccc",
	}
	if _, err := pub.PublishWithError("chora.tenancy.member.invited.v1", hdr, body); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	var pl map[string]any
	if err := json.Unmarshal(rows[0].Payload, &pl); err != nil {
		t.Fatalf("JSON fallback path: payload not JSON: %v (%s)", err, string(rows[0].Payload))
	}
	if pl["tenant_id"] != hdr.TenantID {
		t.Errorf("payload.tenant_id = %v; want %q", pl["tenant_id"], hdr.TenantID)
	}
}

func TestOutboxPublisher_Publish_DuplicateIdempotencyKeyError(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	// Inject deterministic idempotency_key via the IdempotencyKeyFn.
	fixedKey := "fixed-idem-001"
	pubFixed := outbox.NewPublisher(outbox.PublisherConfig{
		Store: store,
		IdempotencyKeyFn: func(topic string, h events.Header, body map[string]interface{}) string {
			return fixedKey
		},
	})

	_ = pub // silence unused
	hdr := canonicalHeader()
	if _, err := pubFixed.PublishWithError("chora.tenancy.tenant.created.v1", hdr,
		map[string]interface{}{"k": "v"}); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	// Second publish with same idempotency_key — collides on UNIQUE.
	_, err := pubFixed.PublishWithError("chora.tenancy.tenant.created.v1", hdr,
		map[string]interface{}{"k": "v2"})
	if err == nil {
		t.Errorf("expected duplicate idempotency_key rejection on second Publish")
	}
	if !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

func TestOutboxPublisher_Publish_DefaultsSourceProjectAndService(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store}) // no project/service
	hdr := canonicalHeader()
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", hdr,
		map[string]interface{}{"x": 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].Envelope["source_project"] != "chora-489812" {
		t.Errorf("default source_project = %q; want chora-489812", rows[0].Envelope["source_project"])
	}
	if rows[0].Envelope["source_service"] != "chora-tenancy" {
		t.Errorf("default source_service = %q; want chora-tenancy", rows[0].Envelope["source_service"])
	}
}

// TopicAggregateInference — the Publisher derives AggregateType from the
// 3rd dot-segment of the canonical topic.
func TestOutboxPublisher_Publish_DerivesAggregateTypeFromTopic(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"chora.tenancy.tenant.created.v1":       "tenant",
		"chora.tenancy.tenant.golive.v1":        "tenant",
		"chora.tenancy.addon.activated.v1":      "addon",
		"chora.tenancy.addon.upgraded.v1":       "addon",
		"chora.tenancy.addon.usage_recorded.v1": "addon",
	}
	for topic, wantAggregate := range cases {
		t.Run(topic, func(t *testing.T) {
			store := outbox.NewInMemoryStore()
			pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
			hdr := canonicalHeader()
			if _, err := pub.PublishWithError(topic, hdr, map[string]interface{}{}); err != nil {
				t.Fatalf("Publish %s: %v", topic, err)
			}
			rows, _ := store.FetchPending(context.Background(), 1)
			if rows[0].AggregateType != wantAggregate {
				t.Errorf("topic %s: AggregateType = %q; want %q",
					topic, rows[0].AggregateType, wantAggregate)
			}
		})
	}
}

func TestOutboxPublisher_Publish_AllCanonicalTopics(t *testing.T) {
	t.Parallel()
	// Mirrors the 8 canonical topics tested in outbox_roundtrip_test.go.
	canonical := []string{
		"chora.tenancy.tenant.created.v1",
		"chora.tenancy.tenant.golive.v1",
		"chora.tenancy.addon.activated.v1",
		"chora.tenancy.addon.upgraded.v1",
		"chora.tenancy.addon.downgraded.v1",
		"chora.tenancy.addon.deactivation_requested.v1",
		"chora.tenancy.addon.deactivated.v1",
		"chora.tenancy.addon.usage_recorded.v1",
	}
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	hdr := canonicalHeader()
	for _, topic := range canonical {
		if _, err := pub.PublishWithError(topic, hdr,
			map[string]interface{}{"tenant_id": hdr.TenantID}); err != nil {
			t.Errorf("canonical topic %q: %v", topic, err)
		}
	}
	rows, _ := store.FetchPending(context.Background(), 100)
	if len(rows) != len(canonical) {
		t.Errorf("rows = %d; want %d (all canonical topics)", len(rows), len(canonical))
	}
}

// guard against accidental import-rename surprises.
func TestPublisher_TopicValidator_RejectsLegacy(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	hdr := canonicalHeader()
	// Legacy non-canonical topic (wrong domain).
	_, err := pub.PublishWithError("chora.atomic.events.v1", hdr, map[string]interface{}{})
	if err == nil {
		t.Errorf("legacy topic should be rejected by canonical validator")
	}
	if !strings.Contains(err.Error(), "tenancy") && !strings.Contains(err.Error(), "domain") {
		t.Errorf("err = %v; want domain-mismatch hint", err)
	}
}
