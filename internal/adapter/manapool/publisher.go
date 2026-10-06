// Package manapool — event-bus publisher for the 7 mana topics.
//
// 7 NEW topics introduced by BE-USR-3:
//
//  1. chora.tenancy.tenant_mana_pool.created.v1
//  2. chora.tenancy.tenant_mana_pool.topped_up.v1
//  3. chora.tenancy.tenant_mana_pool.depleted.v1
//  4. chora.tenancy.tenant_mana_allocation.granted.v1
//  5. chora.tenancy.tenant_mana_allocation.claimed.v1
//  6. chora.tenancy.tenant_mana_allocation.revoked.v1
//  7. chora.tenancy.tenant_mana_allocation.expired.v1
//
// The Publisher port is implemented in production by the event-bus
// adapter (M12+). RecorderPublisher is the in-process test double.
//
// Mandatory envelope fields per CLAUDE.md §6:
//
//	event_id (UUIDv7), idempotency_key, tenant_id, gcid, occurred_at,
//	published_at, traceparent, tracestate, source_project, source_service,
//	schema_version
package manapool

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"time"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

const schemaVersion = "v1"

// EnvelopeHeader is the per-publish input shipped by callers (HTTP layer
// or subscribers). Carries trace context + caller identity so the envelope
// builder can populate the mandatory fields.
type EnvelopeHeader struct {
	TenantID    string
	GCID        string
	Traceparent string
	Tracestate  string
}

// Envelope mirrors chora.common.v1.EventEnvelope (see
// chora-contracts/proto/common/envelope.proto).
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  string
}

// PublishedEvent is the (topic, envelope, payload) triple captured by the
// recorder.
type PublishedEvent struct {
	Topic    string
	Envelope Envelope
	Payload  interface{}
}

// Publisher is the port consumed by the HTTP layer + subscribers.
type Publisher interface {
	PublishPoolCreated(EnvelopeHeader, pool.PoolCreated)
	PublishPoolToppedUp(EnvelopeHeader, pool.PoolToppedUp)
	PublishPoolDepleted(EnvelopeHeader, pool.PoolDepleted)
	PublishAllocationGranted(EnvelopeHeader, allocation.AllocationGranted)
	PublishAllocationClaimed(EnvelopeHeader, allocation.AllocationClaimed)
	PublishAllocationRevoked(EnvelopeHeader, allocation.AllocationRevoked)
	PublishAllocationExpired(EnvelopeHeader, allocation.AllocationExpired)
}

// -----------------------------------------------------------------------------
// RecorderPublisher
// -----------------------------------------------------------------------------

// RecorderPublisher is the in-process stub that records all published events
// for test assertions.
type RecorderPublisher struct {
	mu  sync.Mutex
	out []PublishedEvent
}

// NewRecorderPublisher constructs an empty recorder.
func NewRecorderPublisher() *RecorderPublisher { return &RecorderPublisher{} }

// Recorded returns the slice of published events in publish order.
func (p *RecorderPublisher) Recorded() []PublishedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PublishedEvent, len(p.out))
	copy(out, p.out)
	return out
}

// Reset clears the recorder.
func (p *RecorderPublisher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.out = nil
}

func (p *RecorderPublisher) record(topic string, hdr EnvelopeHeader, payload interface{}) {
	env := buildEnvelope(hdr)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.out = append(p.out, PublishedEvent{Topic: topic, Envelope: env, Payload: payload})
}

// PublishPoolCreated emits chora.tenancy.tenant_mana_pool.created.v1.
func (p *RecorderPublisher) PublishPoolCreated(hdr EnvelopeHeader, ev pool.PoolCreated) {
	p.record("chora.tenancy.tenant_mana_pool.created.v1", hdr, ev)
}

// PublishPoolToppedUp emits chora.tenancy.tenant_mana_pool.topped_up.v1.
func (p *RecorderPublisher) PublishPoolToppedUp(hdr EnvelopeHeader, ev pool.PoolToppedUp) {
	p.record("chora.tenancy.tenant_mana_pool.topped_up.v1", hdr, ev)
}

// PublishPoolDepleted emits chora.tenancy.tenant_mana_pool.depleted.v1.
func (p *RecorderPublisher) PublishPoolDepleted(hdr EnvelopeHeader, ev pool.PoolDepleted) {
	p.record("chora.tenancy.tenant_mana_pool.depleted.v1", hdr, ev)
}

// PublishAllocationGranted emits chora.tenancy.tenant_mana_allocation.granted.v1.
func (p *RecorderPublisher) PublishAllocationGranted(hdr EnvelopeHeader, ev allocation.AllocationGranted) {
	p.record("chora.tenancy.tenant_mana_allocation.granted.v1", hdr, ev)
}

// PublishAllocationClaimed emits chora.tenancy.tenant_mana_allocation.claimed.v1.
func (p *RecorderPublisher) PublishAllocationClaimed(hdr EnvelopeHeader, ev allocation.AllocationClaimed) {
	p.record("chora.tenancy.tenant_mana_allocation.claimed.v1", hdr, ev)
}

// PublishAllocationRevoked emits chora.tenancy.tenant_mana_allocation.revoked.v1.
func (p *RecorderPublisher) PublishAllocationRevoked(hdr EnvelopeHeader, ev allocation.AllocationRevoked) {
	p.record("chora.tenancy.tenant_mana_allocation.revoked.v1", hdr, ev)
}

// PublishAllocationExpired emits chora.tenancy.tenant_mana_allocation.expired.v1.
func (p *RecorderPublisher) PublishAllocationExpired(hdr EnvelopeHeader, ev allocation.AllocationExpired) {
	p.record("chora.tenancy.tenant_mana_allocation.expired.v1", hdr, ev)
}

// -----------------------------------------------------------------------------
// Envelope builder
// -----------------------------------------------------------------------------

func buildEnvelope(hdr EnvelopeHeader) Envelope {
	now := time.Now().UTC()
	tp := hdr.Traceparent
	if tp == "" {
		tp = syntheticTraceparent()
	}
	return Envelope{
		EventID:        pool.NewUUIDv7(),
		IdempotencyKey: pool.NewUUIDv7(),
		TenantID:       hdr.TenantID,
		GCID:           hdr.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    tp,
		Tracestate:     hdr.Tracestate,
		SourceProject:  envOr("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService:  ServiceName,
		SchemaVersion:  schemaVersion,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// syntheticTraceparent generates a W3C-compliant traceparent for events
// triggered without an inbound HTTP request (e.g., monthly tick).
func syntheticTraceparent() string {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	if _, err := rand.Read(traceID); err != nil {
		// Acceptable fallback — generated id will still be unique-ish via time.
		now := time.Now().UnixNano()
		for i := range traceID {
			traceID[i] = byte(now >> uint(8*i))
		}
	}
	if _, err := rand.Read(spanID); err != nil {
		now := time.Now().UnixNano()
		for i := range spanID {
			spanID[i] = byte(now >> uint(8*i))
		}
	}
	return "00-" + hex.EncodeToString(traceID) + "-" + hex.EncodeToString(spanID) + "-01"
}
