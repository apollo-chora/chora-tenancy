// Package outbox_test — Dispatcher drain-loop tests.
//
// The Dispatcher drains outbox_events rows to the event bus. It composes
// Store.FetchPending + Bus.Publish + Store.MarkPublished / MarkFailed /
// Deadletter. On max-attempts exhaustion the row lands in
// outbox_dead_letters AND the broker-side DLQ (the _dlq.> stream).
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — dispatcher is the
// retry + DLQ ladder for the producer-side outbox. Subscriber-side
// ack-after-processing is a separate concern (B.6.2.b).
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
)

// recordingBus captures Publish calls.
type recordingBus struct {
	mu      sync.Mutex
	calls   []recordedPub
	fails   int // when >0, fail the first N calls
	failErr error
}

type recordedPub struct {
	Topic    string
	Envelope cgcenvelope.Envelope
	Payload  []byte
}

func (b *recordingBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fails > 0 {
		b.fails--
		return b.failErr
	}
	cp := append([]byte(nil), payload...)
	b.calls = append(b.calls, recordedPub{Topic: topic, Envelope: env, Payload: cp})
	return nil
}

func (b *recordingBus) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

// helper to mint a pending row directly via store.Insert.
func insertRow(t *testing.T, store *outbox.InMemoryStore, id, tenant string) {
	t.Helper()
	now := time.Now().UTC()
	envelope := map[string]string{
		"event_id":        id,
		"idempotency_key": "idem-" + id,
		"tenant_id":       tenant,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"published_at":    now.Format(time.RFC3339Nano),
		"traceparent":     "00-deadbeefdeadbeefdeadbeefdeadbeef-1111111122222222-01",
		"source_project":  "chora-489812",
		"source_service":  "chora-tenancy",
		"schema_version":  "1",
	}
	r := outbox.Row{
		ID:             id,
		TenantID:       tenant,
		GCID:           "00000000-0000-0000-0000-000000000001",
		AggregateType:  "tenant",
		AggregateID:    "tenant-" + id,
		EventType:      "tenancy.tenant.created",
		Topic:          "chora.tenancy.tenant.created.v1",
		Payload:        []byte(`{"id":"` + id + `"}`),
		Envelope:       envelope,
		IdempotencyKey: "idem-" + id,
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestDispatcher_DrainOnce_PublishesAllAndMarksPublished(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	insertRow(t, store, "r2", "t1")
	insertRow(t, store, "r3", "t2")

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 5,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 3 {
		t.Errorf("publish count = %d; want 3", n)
	}
	if bus.callCount() != 3 {
		t.Errorf("bus.Publish calls = %d; want 3", bus.callCount())
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after drain = %d; want 0", len(rows))
	}
	if len(store.Published()) != 3 {
		t.Errorf("Published count = %d; want 3", len(store.Published()))
	}
}

func TestDispatcher_DrainOnce_TransientFailure_IncrementsRetryAndRetainsPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rA", "t1")
	bus := &recordingBus{fails: 1, failErr: errors.New("pubsub blip")}

	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 3,
	})
	n, _ := d.DrainOnce(context.Background(), 10)
	if n != 0 {
		t.Errorf("first drain publish count = %d; want 0 (transient fail)", n)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("pending after transient fail = %d; want 1", len(rows))
	}
	if rows[0].RetryCount != 1 {
		t.Errorf("retry_count = %d; want 1", rows[0].RetryCount)
	}
	// Second drain should succeed.
	n2, _ := d.DrainOnce(context.Background(), 10)
	if n2 != 1 {
		t.Errorf("second drain publish count = %d; want 1", n2)
	}
}

func TestDispatcher_DrainOnce_MaxAttemptsExhausted_DeadletterAndRemoveFromPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rDL", "t1")

	persistentFail := &recordingBus{fails: 10, failErr: errors.New("permanent")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         persistentFail,
		WorkerID:    "w-dlq",
		MaxAttempts: 3,
	})
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce pass %d: %v", i, err)
		}
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after deadletter = %d; want 0", len(rows))
	}
	dl := store.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLetters count = %d; want 1", len(dl))
	}
	if dl[0].RowID != "rDL" {
		t.Errorf("DeadLetter row = %q; want rDL", dl[0].RowID)
	}
	if dl[0].AttemptCount < 3 {
		t.Errorf("DeadLetter attempt_count = %d; want >= 3", dl[0].AttemptCount)
	}
	if !strings.Contains(dl[0].FailureReason, "permanent") {
		t.Errorf("DeadLetter failure_reason = %q; want contains 'permanent'", dl[0].FailureReason)
	}
	if dl[0].WorkerID != "w-dlq" {
		t.Errorf("DeadLetter worker_id = %q; want w-dlq", dl[0].WorkerID)
	}
}

func TestDispatcher_DrainOnce_RespectsContextCancellation(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	for i := 0; i < 5; i++ {
		insertRow(t, store, string(rune('a'+i)), "t")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	_, err := d.DrainOnce(ctx, 10)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("DrainOnce err = %v; want context.Canceled", err)
	}
}

func TestDispatcher_DrainOnce_EnvelopeReconstructedForBus(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rE", "00000000-0000-0000-0000-0000000000aa")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.calls = %d; want 1", len(bus.calls))
	}
	env := bus.calls[0].Envelope
	if env.EventID != "rE" {
		t.Errorf("envelope.EventID = %q; want rE", env.EventID)
	}
	if env.SourceProject != "chora-489812" {
		t.Errorf("envelope.SourceProject = %q; want chora-489812", env.SourceProject)
	}
	if env.SourceService != "chora-tenancy" {
		t.Errorf("envelope.SourceService = %q; want chora-tenancy", env.SourceService)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("envelope.SchemaVersion = %d; want 1", env.SchemaVersion)
	}
	if env.TenantID != "00000000-0000-0000-0000-0000000000aa" {
		t.Errorf("envelope.TenantID = %q; want ...aa", env.TenantID)
	}
}

func TestDispatcher_Validates_RequiresWorkerID(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewDispatcher with empty WorkerID should panic")
		}
	}()
	outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
	})
}

func TestDispatcher_Validates_DefaultsMaxAttempts(t *testing.T) {
	t.Parallel()
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
		WorkerID: "w1",
	})
	if d.MaxAttempts() != 5 {
		t.Errorf("default MaxAttempts = %d; want 5", d.MaxAttempts())
	}
}

func TestDispatcher_Run_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx, 10); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Run err = %v; want context cancel/deadline", err)
	}
	if bus.callCount() < 1 {
		t.Errorf("bus calls = %d; want >= 1 (drained at least once before cancel)", bus.callCount())
	}
}

func TestDispatcher_DrainOnce_SourceServiceDefaultsToTenancy(t *testing.T) {
	t.Parallel()
	// Insert a row without source_service in the envelope (simulating a
	// pre-migration row). The dispatcher must default to chora-tenancy.
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	r := outbox.Row{
		ID:             "rNoSrc",
		TenantID:       "t-1",
		AggregateType:  "tenant",
		AggregateID:    "tenant-1",
		EventType:      "tenancy.tenant.created",
		Topic:          "chora.tenancy.tenant.created.v1",
		Payload:        []byte(`{}`),
		Envelope:       map[string]string{"event_id": "rNoSrc"},
		IdempotencyKey: "idem-rNoSrc",
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
	if env.SourceService != "chora-tenancy" {
		t.Errorf("envelope.SourceService = %q; want chora-tenancy (default)", env.SourceService)
	}
}
