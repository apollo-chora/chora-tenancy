// Package outbox_test — coverage-uplift tests for branches the canonical
// flow doesn't reach.
//
// Goals:
//   - Cover the convenience Publish(...) wrapper (drops the error).
//   - Cover the Run() loop's empty-drain backoff path.
//   - Cover validateCanonicalTopic's "topic required" empty-string branch.
//   - Cover dispatcher.publishOne's envelope-corrupt fast-path deadletter.
//   - Cover truncate() / indexOf() helpers' boundary branches.
//
// Per the M12.3 W2b deliverable's coverage ≥85% gate.
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
)

// TestPublisher_Publish_ConvenienceWrapper exercises the error-dropping
// Publish(...) wrapper. Publish returns the zero value on validation
// failure (drop-in compatibility with the legacy Recorder shape).
func TestPublisher_Publish_ConvenienceWrapper(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})

	// Happy path — non-zero PublishedEvent.
	rec := pub.Publish("chora.tenancy.tenant.created.v1", events.Header{
		TenantID: "01970000-0000-7000-8000-000000000001",
	}, map[string]interface{}{"x": 1})
	if rec.EventID == "" {
		t.Errorf("Publish convenience: EventID empty on success")
	}

	// Validation failure — empty tenant_id → zero PublishedEvent.
	zero := pub.Publish("chora.tenancy.tenant.created.v1", events.Header{},
		map[string]interface{}{})
	if zero.EventID != "" {
		t.Errorf("Publish convenience: expected zero value on failure, got %+v", zero)
	}
}

// TestPublisher_Publish_EmptyTopicRejected exercises the empty-string
// branch of validateCanonicalTopic.
func TestPublisher_Publish_EmptyTopicRejected(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	_, err := pub.PublishWithError("", events.Header{TenantID: "t"}, map[string]interface{}{})
	if err == nil {
		t.Errorf("empty topic should be rejected")
	}
	if !strings.Contains(err.Error(), "topic") {
		t.Errorf("err = %v; want topic-related complaint", err)
	}
}

// TestPublisher_Publish_PreservesIMDAEnvelopeTags exercises the
// payload-to-envelope IMDA-tag projection path.
func TestPublisher_Publish_PreservesIMDAEnvelopeTags(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	hdr := events.Header{TenantID: "01970000-0000-7000-8000-000000000aa1"}
	body := map[string]interface{}{
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "post_deploy",
	}
	if _, err := pub.PublishWithError("chora.tenancy.tenant.golive.v1", hdr, body); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].Envelope["chora_imda_dimension"] != "accountability" {
		t.Errorf("envelope.chora_imda_dimension missing: %+v", rows[0].Envelope)
	}
	if rows[0].Envelope["imda_lifecycle_stage"] != "post_deploy" {
		t.Errorf("envelope.imda_lifecycle_stage missing: %+v", rows[0].Envelope)
	}
}

// brokenStore returns FetchPending error for the dispatcher to surface.
type brokenStore struct{ outbox.Store }

func (b brokenStore) FetchPending(_ context.Context, _ int) ([]outbox.Row, error) {
	return nil, errors.New("synthetic fetch error")
}

func TestDispatcher_DrainOnce_StoreErrorSurfacedToCaller(t *testing.T) {
	t.Parallel()
	// brokenStore satisfies Store via embedding; override only FetchPending.
	st := brokenStore{Store: outbox.NewInMemoryStore()}
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: st, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	_, err := d.DrainOnce(context.Background(), 10)
	if err == nil {
		t.Errorf("DrainOnce err = nil; want store error surfaced")
	}
	if !strings.Contains(err.Error(), "synthetic fetch error") {
		t.Errorf("err = %v; want contains synthetic fetch error", err)
	}
}

// TestDispatcher_DrainOnce_DefaultsApplied verifies the dispatcher defaults
// kick in when zero values are passed.
func TestDispatcher_DrainOnce_DefaultsApplied(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1",
		// All other fields default
	})
	if d.MaxAttempts() != 5 {
		t.Errorf("MaxAttempts default = %d; want 5", d.MaxAttempts())
	}
}

// TestDispatcher_Run_EmptyDrainSleepsAndExitsOnCancel exercises the
// PollInterval branch + Run's empty-drain backoff path.
func TestDispatcher_Run_EmptyDrainSleepsAndExitsOnCancel(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore() // no rows
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := d.Run(ctx, 10)
	if err == nil {
		t.Errorf("Run err = nil; want context deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Run err = %v; want context cancel/deadline", err)
	}
	if bus.callCount() != 0 {
		t.Errorf("bus.calls = %d; want 0 (no rows to drain)", bus.callCount())
	}
}

// Helper to write coverage for envelope parsing with bad time fields.
func TestDispatcher_DrainOnce_BadEnvelopeTimesFallBack(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	r := outbox.Row{
		ID:             "rBadTime",
		TenantID:       "01970000-0000-7000-8000-0000000000bb",
		AggregateType:  "tenant",
		AggregateID:    "tenant-1",
		EventType:      "tenancy.tenant.created",
		Topic:          "chora.tenancy.tenant.created.v1",
		Payload:        []byte(`{}`),
		Envelope: map[string]string{
			"event_id":        "rBadTime",
			"occurred_at":     "not-a-time",
			"published_at":    "still-not",
			"source_service":  "chora-tenancy",
			"schema_version":  "1",
		},
		IdempotencyKey: "idem-rBadTime",
		OccurredAt:     now,
	}
	_ = store.Insert(context.Background(), r)
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 1); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.calls = %d; want 1", len(bus.calls))
	}
	env := bus.calls[0].Envelope
	// OccurredAt should fall back to the row's occurred_at.
	if env.OccurredAt.IsZero() {
		t.Errorf("envelope.OccurredAt zero; want fallback to row.OccurredAt")
	}
}

// TestPostgresStore_Insert_RealError exercises the non-unique-violation
// error path.
func TestPostgresStore_Insert_RealError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("pq: connection refused")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	r := newRow("rRealErr", "t", time.Now().UTC())
	err := store.Insert(context.Background(), r)
	if err == nil {
		t.Errorf("Insert err = nil; want error")
	}
	if errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert err = ErrDuplicateIdempotencyKey; want real error")
	}
}

// Smoke check: the canonical TenancyDomain constant is "tenancy" and the
// package's domain integration with topic validation is consistent.
func TestTenancyDomainConstant(t *testing.T) {
	t.Parallel()
	if outbox.TenancyDomain != "tenancy" {
		t.Errorf("TenancyDomain = %q; want tenancy", outbox.TenancyDomain)
	}
}
