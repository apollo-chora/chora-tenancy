// Package outbox — OutboxPublisher implementation.
//
// OutboxPublisher satisfies the chora-tenancy Recorder-shape API
// (PublishWithError(topic, Header, payload)) by writing the event to the
// outbox_events table instead of publishing directly to Pub/Sub. The
// Dispatcher (see dispatcher.go) drains the table to Cloud Pub/Sub on a
// separate goroutine. This decouples event emission from Pub/Sub
// availability — a crash between the domain state-change and Pub/Sub
// publish no longer loses events.
//
// The wire shape of the payload matches the existing events.Recorder /
// events.CloudPublisher payload (JSON serialisation of the body
// map[string]interface{}) so subscribers see identical bytes whether they
// receive from the legacy direct-publish path or the new outbox path.
// Migration is a constructor swap in cmd/server/main.go.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a producer-side durable
// emission for chora-tenancy's `chora.tenancy.{tenant,addon}.*.v1` streams.
package outbox

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events/protomarshal"
)

// defaultSchemaVersion is the major version of the on-wire payload schema.
// Matches the v{N} suffix in the canonical topic names.
const defaultSchemaVersion = int32(1)

// PublisherConfig wires the OutboxPublisher.
type PublisherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// SourceProject is the GCP project the service runs in (e.g.
	// chora-489812). Defaults to "chora-489812".
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// "chora-tenancy".
	SourceService string

	// IdempotencyKeyFn lets tests inject deterministic idempotency_key
	// values. Production leaves nil — the publisher mints a UUIDv7 per
	// call.
	IdempotencyKeyFn func(topic string, h events.Header, body map[string]interface{}) string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Publisher satisfies the chora-tenancy events.Recorder-shape API by
// enqueueing the event into outbox_events.
type Publisher struct {
	cfg PublisherConfig
}

// NewPublisher constructs an OutboxPublisher.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-tenancy"
	}
	return &Publisher{cfg: cfg}
}

// Publish satisfies the simple Recorder shape. Returns the PublishedEvent
// on success, the zero value on validation failure (without an error —
// keeps drop-in compatibility for callers that ignore the error path).
func (p *Publisher) Publish(topic string, h events.Header, payload map[string]interface{}) events.PublishedEvent {
	rec, _ := p.PublishWithError(topic, h, payload)
	return rec
}

// PublishWithError writes the event as a pending row in outbox_events.
// Returns the canonical PublishedEvent (events.PublishedEvent) on success.
//
// The HTTP request returns successfully once the row is durably committed;
// the Dispatcher publishes to Cloud Pub/Sub asynchronously.
func (p *Publisher) PublishWithError(topic string, h events.Header, payload map[string]interface{}) (events.PublishedEvent, error) {
	if p.cfg.Store == nil {
		return events.PublishedEvent{}, errors.New("outbox: store not wired")
	}
	if err := validateCanonicalTopic(topic); err != nil {
		return events.PublishedEvent{}, err
	}
	if strings.TrimSpace(h.TenantID) == "" {
		return events.PublishedEvent{}, events.ErrEmptyTenantID
	}

	now := p.cfg.Now()
	eventID := newUUIDv7()

	var idemKey string
	if p.cfg.IdempotencyKeyFn != nil {
		idemKey = p.cfg.IdempotencyKeyFn(topic, h, payload)
	}
	if strings.TrimSpace(idemKey) == "" {
		idemKey = newUUIDv7()
	}

	// Build the envelope as a flat string map for the JSONB column. This is
	// the on-wire attribute set published to Pub/Sub (subscribers can
	// filter without parsing the payload).
	envMap := map[string]string{
		"event_id":        eventID,
		"idempotency_key": idemKey,
		"tenant_id":       h.TenantID,
		"gcid":            h.GCID,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"published_at":    now.Format(time.RFC3339Nano),
		"traceparent":     h.Traceparent,
		"tracestate":      "",
		"source_project":  p.cfg.SourceProject,
		"source_service":  p.cfg.SourceService,
		"schema_version":  strconv.Itoa(int(defaultSchemaVersion)),
	}
	// Surface IMDA evidence tags from the payload to the envelope so
	// observability filters can index without parsing the payload.
	if v, ok := payload["chora_imda_dimension"].(string); ok && v != "" {
		envMap["chora_imda_dimension"] = v
	}
	if v, ok := payload["imda_lifecycle_stage"].(string); ok && v != "" {
		envMap["imda_lifecycle_stage"] = v
	}

	// Producer-side encoding: emit canonical binary protobuf for topics whose
	// Pub/Sub Schema Registry schema is BINARY-encoded. JSON-marshalled
	// payloads fail validation at publish time with "Invalid binary proto
	// message" and dead-letter forever. Per the codebase-wide outbox
	// protobuf encoding gap (task #33 surfaced 2026-05-16).
	payloadBytes, err := encodeOutboxPayload(topic, commonEnvelopeFromHeader(eventID, idemKey, h, now, p.cfg.SourceProject, p.cfg.SourceService), payload)
	if err != nil {
		return events.PublishedEvent{}, fmt.Errorf("outbox: marshal payload: %w", err)
	}

	aggregateType := deriveAggregateType(topic)
	aggregateID := h.GCID
	if aggregateID == "" {
		aggregateID = h.TenantID
	}
	if aggregateID == "" {
		aggregateID = eventID
	}

	row := Row{
		ID:             eventID,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		AggregateType:  aggregateType,
		AggregateID:    aggregateID,
		EventType:      deriveEventType(topic),
		Topic:          topic,
		Payload:        payloadBytes,
		Envelope:       envMap,
		IdempotencyKey: idemKey,
		OccurredAt:     now,
	}
	if err := p.cfg.Store.Insert(context.Background(), row); err != nil {
		return events.PublishedEvent{}, err
	}

	rec := events.PublishedEvent{
		Topic:          topic,
		EventID:        eventID,
		IdempotencyKey: idemKey,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		SourceProject:  p.cfg.SourceProject,
		SourceService:  p.cfg.SourceService,
		SchemaVersion:  int(defaultSchemaVersion),
		Payload:        payload,
	}
	return rec, nil
}

// validateCanonicalTopic enforces `chora.tenancy.{aggregate}.{event_type}.v{N}`
// at the publisher boundary. Legacy topic strings (e.g. `chora.atomic.events`)
// MUST migrate to the canonical name before reaching this publisher; the
// outbox `topic` column therefore always holds canonical names.
func validateCanonicalTopic(topic string) error {
	t := strings.TrimSpace(topic)
	if t == "" {
		return errors.New("outbox: topic required")
	}
	parts := strings.Split(t, ".")
	if len(parts) < 5 {
		return fmt.Errorf("outbox: topic %q must follow chora.tenancy.{aggregate}.{event_type}.v{N}", topic)
	}
	if parts[0] != "chora" {
		return fmt.Errorf("outbox: topic %q must start with 'chora.'", topic)
	}
	if parts[1] != TenancyDomain {
		return fmt.Errorf("outbox: topic domain segment = %q; want %q (legacy topics MUST migrate to canonical tenancy)", parts[1], TenancyDomain)
	}
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "v") || len(last) < 2 {
		return fmt.Errorf("outbox: topic %q must end with v{N} version suffix", topic)
	}
	for _, ch := range last[1:] {
		if ch < '0' || ch > '9' {
			return fmt.Errorf("outbox: topic %q version suffix must be numeric", topic)
		}
	}
	return nil
}

// deriveAggregateType returns the 3rd dot-segment of the canonical topic.
// chora.tenancy.{aggregate}.{event_type}.v{N} → {aggregate}.
func deriveAggregateType(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

// deriveEventType returns the topic's aggregate.event_type segments without
// the chora.{domain} prefix or the v{N} version suffix.
// chora.tenancy.tenant.created.v1 → tenancy.tenant.created (matches the
// events_test outbox roundtrip pattern).
func deriveEventType(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 5 {
		return topic
	}
	// Strip the leading "chora." and the trailing version segment.
	return strings.Join(parts[1:len(parts)-1], ".")
}

// newUUIDv7 mints a UUIDv7. Standalone so the package stays independent of
// uuid pkg version drift in services/chora-tenancy. Falls back to time-only
// bytes when crypto/rand fails (vanishingly rare).
func newUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// Payload encoding — binary protobuf for Schema-Registry-attached topics,
// JSON fallback (with WARN log) for topics that don't yet have a binary
// encoder. New topics MUST add a case in protomarshal.MarshalPayload.
//
// JSON fallback exists so call-sites that emit topics without a registered
// Schema Registry schema (e.g. chora.tenancy.member.invited.v1 — not yet
// in the Pub/Sub registry) keep working. Each unknown topic logs a one-time
// WARN so its missing encoder is visible in production logs.
// -----------------------------------------------------------------------------

var (
	warnedUnknownTopicsMu sync.Mutex
	warnedUnknownTopics   = map[string]bool{}
)

// commonEnvelopeFromHeader projects a Header + mint timestamps + source
// provenance onto the encoder's flat envelope type. Keeps the protomarshal
// import free of any events.* dependency.
func commonEnvelopeFromHeader(eventID, idempotencyKey string, h events.Header, now time.Time, sourceProject, sourceService string) protomarshal.Envelope {
	return protomarshal.Envelope{
		EventID:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		Tracestate:     "",
		SourceProject:  sourceProject,
		SourceService:  sourceService,
		SchemaVersion:  defaultSchemaVersion,
	}
}

func encodeOutboxPayload(topic string, env protomarshal.Envelope, payload map[string]any) ([]byte, error) {
	bz, err := protomarshal.MarshalPayload(topic, env, payload)
	if err == nil {
		return bz, nil
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		// Real encoding error (e.g. type mismatch) — fail loud.
		return nil, err
	}

	// Topic has no protobuf encoder yet. Log a one-shot WARN and fall back
	// to JSON. These rows WILL be rejected by Schema Registry on publish if
	// the topic IS schema-attached; if the topic is NOT yet registered, the
	// publish succeeds and the bytes flow through unchanged. Add an encoder
	// case in protomarshal.MarshalPayload before the topic gets a schema.
	warnedUnknownTopicsMu.Lock()
	if !warnedUnknownTopics[topic] {
		warnedUnknownTopics[topic] = true
		log.Printf("WARN outbox: topic %q has no binary protobuf encoder — payload will JSON-marshal and Schema Registry will REJECT at publish if the topic is BINARY-attached. Add a case to internal/adapter/events/protomarshal/MarshalPayload.", topic)
	}
	warnedUnknownTopicsMu.Unlock()

	bz, mErr := json.Marshal(payload)
	if mErr != nil {
		return nil, fmt.Errorf("outbox: json fallback marshal: %w", mErr)
	}
	return bz, nil
}
