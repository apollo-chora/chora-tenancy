package billingpubsub

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events/protodecode"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/reconciliation"
)

// EventLogReplayFeeder is an EventLogFeeder backed by a chora-go-common bus
// subscription on the two payment.captured topics:
//
//	chora.tenancy.payment.captured.v1
//	chora.identity.payment.captured.v1
//
// Implements reconciliation.EventLogFeeder by accumulating capture amounts
// keyed by UTC day. This is the cross-DB-forbidden alternative to direct
// chora_tenancy / chora_identity reads (per .claude/rules/ddd-enforcement.md).
//
// Production: subscribes once at startup; the SumCapturedCentsForDay accessor
// returns the rolling per-day total. Tests can pre-seed via Record().
type EventLogReplayFeeder struct {
	mu      sync.RWMutex
	totals  map[string]int64 // YYYYMMDD UTC -> cents
	cancels []func()
}

// NewEventLogReplayFeeder constructs an empty feeder. Use Subscribe to wire
// it to a bus.
func NewEventLogReplayFeeder() *EventLogReplayFeeder {
	return &EventLogReplayFeeder{totals: map[string]int64{}}
}

// Subscribe registers handlers on the bus for both payment.captured topics.
// Returns an error if either subscription fails. The feeder accumulates
// captures by parsing the payload's `OccurredAt` as the day key.
func (f *EventLogReplayFeeder) Subscribe(ctx context.Context, bus interface {
	Subscribe(ctx context.Context, topic string, handler cgcpubsub.Handler) (func(), error)
}) error {
	topics := []string{
		"chora.tenancy.payment.captured.v1",
		"chora.identity.payment.captured.v1",
	}
	for _, topic := range topics {
		cancel, err := bus.Subscribe(ctx, topic, f.handle)
		if err != nil {
			return fmt.Errorf("event_log_feeder: subscribe %s: %w", topic, err)
		}
		f.mu.Lock()
		f.cancels = append(f.cancels, cancel)
		f.mu.Unlock()
	}
	return nil
}

// Close cancels all active subscriptions.
func (f *EventLogReplayFeeder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.cancels {
		c()
	}
	f.cancels = nil
}

// minimalPaymentPayload is the subset of the chora payment.captured payload
// the feeder needs to do the math. The wire shape is the same one published
// by WebhookPublisher's Payload struct (JSON-marshal compatible).
type minimalPaymentPayload struct {
	AmountCents int64     `json:"AmountCents"`
	OccurredAt  time.Time `json:"OccurredAt"`
}

// handle adds a Pub/Sub message's amount to the day bucket.
//
// Wire-format-tolerant via protodecode (proto.Unmarshal first when registered,
// json.Unmarshal fallback). Today payment.captured topics still emit JSON;
// when chora-tenancy or chora-identity flips its payment payload to binary,
// add an entry to protodecode.binaryDecoders.
//
// Envelope-aware decode: the bus's parsed Envelope is projected back into
// Pub/Sub-attribute shape and threaded through protodecode so envelope
// fields (event_id / tenant_id / gcid) populate even on the JSON-fallback
// path. The minimalPaymentPayload struct itself doesn't reference envelope
// fields today — this seam future-proofs the path when a JSON producer
// adds them OR a publisher flips to binary (#33 / #38).
func (f *EventLogReplayFeeder) handle(_ context.Context, msg *cgcpubsub.Message) error {
	var p minimalPaymentPayload
	if err := protodecode.DecodePayloadIntoWithAttrs(msg.Topic, msg.Payload, attrsFromBusEnvelope(msg), &p); err != nil {
		// Bad payload — bus retries; eventually DLQ. We don't return error
		// here because a malformed payload is a producer bug, not a
		// transient failure. Log + drop.
		return nil
	}
	if p.AmountCents <= 0 || p.OccurredAt.IsZero() {
		return nil
	}
	day := p.OccurredAt.UTC().Format("20060102")
	f.mu.Lock()
	f.totals[day] += p.AmountCents
	f.mu.Unlock()
	return nil
}

// attrsFromBusEnvelope projects a cgcpubsub.Message's parsed envelope back
// into the Pub/Sub-attribute shape expected by protodecode. The bus
// reconstructs Envelope from msg.Attributes on receive (per
// libs/chora-go-common/pubsub.envelopeFromAttributes); this is the inverse.
func attrsFromBusEnvelope(msg *cgcpubsub.Message) map[string]string {
	if msg == nil {
		return nil
	}
	env := msg.Envelope
	attrs := map[string]string{
		"topic":     msg.Topic,
		"event_id":  env.EventID,
		"tenant_id": env.TenantID,
		"gcid":      env.GCID,
	}
	if env.Traceparent != "" {
		attrs["traceparent"] = env.Traceparent
	}
	if env.Tracestate != "" {
		attrs["tracestate"] = env.Tracestate
	}
	if !env.OccurredAt.IsZero() {
		attrs["occurred_at"] = env.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	if !env.PublishedAt.IsZero() {
		attrs["published_at"] = env.PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	if env.ChoraImdaDimension != "" {
		attrs["chora_imda_dimension"] = env.ChoraImdaDimension
	}
	if env.ImdaLifecycleStage != "" {
		attrs["imda_lifecycle_stage"] = env.ImdaLifecycleStage
	}
	return attrs
}

// Record is a test seam — directly add a capture to the feeder's accumulator.
// Use this to pre-load history without going through Pub/Sub.
func (f *EventLogReplayFeeder) Record(day time.Time, cents int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.totals[day.UTC().Format("20060102")] += cents
}

// SumCapturedCentsForDay implements reconciliation.EventLogFeeder.
func (f *EventLogReplayFeeder) SumCapturedCentsForDay(_ context.Context, day time.Time) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.totals[day.UTC().Format("20060102")], nil
}

// Days returns a sorted snapshot of all days currently tracked. Useful in
// audit reports + tests.
func (f *EventLogReplayFeeder) Days() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(f.totals))
	for d := range f.totals {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// Compile-time port check.
var _ reconciliation.EventLogFeeder = (*EventLogReplayFeeder)(nil)
