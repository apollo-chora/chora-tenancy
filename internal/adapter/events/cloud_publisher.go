// cloud_publisher.go — outbox-backed event publisher for chora-tenancy.
//
// Production drop-in replacement for the in-process Recorder.Atomically
// records events to chora_tenancy.outbox_events; the Relay drains
// pending rows to the event bus.
//
// Same architecture as services/chora-identity/.../events/cloud_publisher.go
// — different Header / Payload shape because the chora-tenancy publisher
// API was designed before chora-identity's. Both ultimately produce a
// chora-common envelope.Envelope + Protobuf-marshalled payload at the
// outbox row.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgcoutbox "github.com/apollo-chora/chora-common/outbox"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events/protomarshal"
)

// CloudPublisherConfig tunes the CloudPublisher.
type CloudPublisherConfig struct {
	// AggregateType labels every outbox row this publisher emits.
	AggregateType string
}

// CloudPublisher writes events to the chora-common outbox recorder.
type CloudPublisher struct {
	rec cgcoutbox.Recorder
	cfg CloudPublisherConfig
}

// NewCloudPublisher constructs a CloudPublisher.
func NewCloudPublisher(rec cgcoutbox.Recorder, cfg CloudPublisherConfig) *CloudPublisher {
	if cfg.AggregateType == "" {
		cfg.AggregateType = "tenant"
	}
	return &CloudPublisher{rec: rec, cfg: cfg}
}

// Publish satisfies the in-process Recorder shape. Returns the
// PublishedEvent on success, the zero value on validation failure
// (without an error — keeps drop-in compatibility for callers that
// ignore the error path).
func (p *CloudPublisher) Publish(topic string, h Header, payload map[string]interface{}) PublishedEvent {
	rec, _ := p.PublishWithError(topic, h, payload)
	return rec
}

// PublishWithError records a new outbox row + returns the PublishedEvent.
func (p *CloudPublisher) PublishWithError(topic string, h Header, payload map[string]interface{}) (PublishedEvent, error) {
	if err := validateTopic(topic); err != nil {
		return PublishedEvent{}, err
	}
	if strings.TrimSpace(h.TenantID) == "" {
		return PublishedEvent{}, ErrEmptyTenantID
	}

	now := time.Now().UTC()
	eventID := newUUIDv7()
	idempotencyKey := newUUIDv7()

	// Producer-side encoding: emit canonical binary protobuf for topics
	// whose flat proto contract is BINARY-encoded. JSON
	// payloads on a schema-attached topic dead-letter forever with
	// "Invalid binary proto message". Per task #33 (2026-05-16) the
	// chora-tenancy slice covers tenant + addon + familiar_egg topics.
	body, err := encodeCloudPublisherPayload(topic, protomarshal.Envelope{
		EventID:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		SourceProject:  sourceProject,
		SourceService:  sourceService,
		SchemaVersion:  1,
	}, payload)
	if err != nil {
		return PublishedEvent{}, fmt.Errorf("events.CloudPublisher: marshal: %w", err)
	}

	commonEnv := cgcenvelope.Envelope{
		EventID:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		SourceProject:  sourceProject,
		SourceService:  sourceService,
		SchemaVersion:  1,
	}

	aggregateID := h.GCID
	if aggregateID == "" {
		aggregateID = h.TenantID
	}
	if aggregateID == "" {
		aggregateID = eventID
	}

	row := &cgcoutbox.Row{
		ID:            newUUIDv7(),
		AggregateType: p.cfg.AggregateType,
		AggregateID:   aggregateID,
		EventType:     deriveEventType(topic),
		Topic:         topic,
		Payload:       body,
		Envelope:      commonEnv,
		OccurredAt:    now,
		Status:        cgcoutbox.StatusPending,
	}
	if err := p.rec.Record(context.Background(), nil, row); err != nil {
		return PublishedEvent{}, fmt.Errorf("events.CloudPublisher: record: %w", err)
	}
	pe := PublishedEvent{
		Topic:          topic,
		EventID:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		SourceProject:  sourceProject,
		SourceService:  sourceService,
		SchemaVersion:  1,
		Payload:        payload,
	}
	return pe, nil
}

// deriveEventType returns the topic's event-type suffix.
// e.g. "chora.tenancy.tenant.created.v1" → "tenant.created.v1".
func deriveEventType(topic string) string {
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) < 3 {
		return topic
	}
	return parts[2]
}

// Ensure unused symbol prevention.
var _ = time.Now

// -----------------------------------------------------------------------------
// Payload encoding — binary protobuf for Schema-Registry-attached topics,
// JSON fallback (with WARN log) for topics without a binary encoder. Mirrors
// the same gating used in internal/adapter/outbox.encodeOutboxPayload — the
// outbox path is the production code path; this CloudPublisher is retained
// for direct-publish call sites (currently unused by main.go but part of
// the package's public API).
// -----------------------------------------------------------------------------

var (
	cloudWarnedUnknownTopicsMu sync.Mutex
	cloudWarnedUnknownTopics   = map[string]bool{}
)

func encodeCloudPublisherPayload(topic string, env protomarshal.Envelope, payload map[string]interface{}) ([]byte, error) {
	bz, err := protomarshal.MarshalPayload(topic, env, payload)
	if err == nil {
		return bz, nil
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		return nil, err
	}

	cloudWarnedUnknownTopicsMu.Lock()
	if !cloudWarnedUnknownTopics[topic] {
		cloudWarnedUnknownTopics[topic] = true
		log.Printf("WARN events.CloudPublisher: topic %q has no binary protobuf encoder — payload will JSON-marshal and a binary-contracted consumer will REJECT it. Add a case to internal/adapter/events/protomarshal/MarshalPayload.", topic)
	}
	cloudWarnedUnknownTopicsMu.Unlock()

	bz, jErr := json.Marshal(payload)
	if jErr != nil {
		return nil, fmt.Errorf("events.CloudPublisher: json fallback marshal: %w", jErr)
	}
	return bz, nil
}
