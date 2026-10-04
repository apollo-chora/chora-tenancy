// outbox_extra_coverage_test.go — branch coverage for the Dispatcher's
// failure-ladder logging paths (MarkPublished / MarkFailed / Deadletter
// failures, Run drain-error warn) and the TransactionAuditEmitter's
// filter echoing + unwired/nil handling. Uses the InMemoryStore wrapped
// in failure-injecting decorators.
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// failMarkedStore decorates InMemoryStore to fail a chosen transition.
type failMarkedStore struct {
	*outbox.InMemoryStore
	failMarkPublished bool
	failMarkFailed    bool
	failDeadletter    bool
	failFetch         bool
}

func (f *failMarkedStore) MarkPublished(ctx context.Context, id string) error {
	if f.failMarkPublished {
		return errors.New("mark published boom")
	}
	return f.InMemoryStore.MarkPublished(ctx, id)
}

func (f *failMarkedStore) MarkFailed(ctx context.Context, id, errMsg string) error {
	if f.failMarkFailed {
		return errors.New("mark failed boom")
	}
	return f.InMemoryStore.MarkFailed(ctx, id, errMsg)
}

func (f *failMarkedStore) Deadletter(ctx context.Context, id, reason string, attempt int) error {
	if f.failDeadletter {
		return errors.New("deadletter boom")
	}
	return f.InMemoryStore.Deadletter(ctx, id, reason, attempt)
}

func (f *failMarkedStore) FetchPending(ctx context.Context, limit int) ([]outbox.Row, error) {
	if f.failFetch {
		return nil, errors.New("fetch boom")
	}
	return f.InMemoryStore.FetchPending(ctx, limit)
}

// fakeBus publishes without touching the outside world.
type fakeBus struct {
	err error
}

func (b *fakeBus) Publish(context.Context, string, cgcenvelope.Envelope, []byte) error { return b.err }

func newDispatcher(store outbox.Store, bus outbox.Bus) *outbox.Dispatcher {
	return outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "test-worker",
	})
}

func insertPendingRow(t *testing.T, store outbox.Store) {
	t.Helper()
	err := store.Insert(context.Background(), outbox.Row{
		ID: "row-1", TenantID: "t-1", GCID: "g-1", Topic: "chora.tenancy.tenant.created.v1",
		Payload:    []byte("{}"),
		Envelope:   map[string]string{"tenant_id": "t-1"},
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestDispatcher_MarkPublishedFailure_WarnsAndRetains(t *testing.T) {
	t.Parallel()
	store := &failMarkedStore{InMemoryStore: outbox.NewInMemoryStore(), failMarkPublished: true}
	insertPendingRow(t, store)
	d := newDispatcher(store, &fakeBus{})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 0 {
		t.Fatalf("mark-published failure must not count as published, got %d", n)
	}
	// Row stays pending for the next cycle.
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("expected row still pending, got %d", len(rows))
	}
}

func TestDispatcher_MarkFailedFailure_Warns(t *testing.T) {
	t.Parallel()
	store := &failMarkedStore{InMemoryStore: outbox.NewInMemoryStore(), failMarkFailed: true}
	insertPendingRow(t, store)
	// Bus fails once → transient MarkFailed path → MarkFailed itself fails.
	d := newDispatcher(store, &fakeBus{err: errors.New("bus down")})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
}

func TestDispatcher_DeadletterFailure_Warns(t *testing.T) {
	t.Parallel()
	store := &failMarkedStore{InMemoryStore: outbox.NewInMemoryStore(), failDeadletter: true}
	insertPendingRow(t, store)
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &fakeBus{err: errors.New("bus down")}, WorkerID: "w", MaxAttempts: 1,
	})
	// MaxAttempts=1 → first failure exhausts attempts → Deadletter path.
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
}

func TestDispatcher_Run_DrainErrorWarnsAndRetries(t *testing.T) {
	t.Parallel()
	store := &failMarkedStore{InMemoryStore: outbox.NewInMemoryStore(), failFetch: true}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &fakeBus{}, WorkerID: "w",
		PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := d.Run(ctx, 10)
	if err == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
		t.Fatalf("expected context termination, got %v", err)
	}
}

func TestDispatcher_Run_ExitsOnPreCanceledContext(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	d := newDispatcher(store, &fakeBus{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Run(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDispatcher_EnvelopeDefaultsAndSchemaVersion(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	// Envelope absent → defaults filled from the row + config.
	err := store.Insert(context.Background(), outbox.Row{
		ID: "row-nil-env", TenantID: "t-1", GCID: "g-1",
		Topic: "chora.tenancy.tenant.created.v1", Payload: []byte("{}"),
		OccurredAt: now,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	d := newDispatcher(store, &fakeBus{})
	if n, err := d.DrainOnce(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("DrainOnce: n=%d err=%v", n, err)
	}
	// Envelope with schema_version junk → sanitised to 1; occurred_at in
	// RFC3339 (non-nano) parses via the second time.Parse branch.
	store2 := outbox.NewInMemoryStore()
	err = store2.Insert(context.Background(), outbox.Row{
		ID: "row-ver", TenantID: "t-2", GCID: "g-2",
		Topic: "chora.tenancy.tenant.created.v1", Payload: []byte("{}"),
		OccurredAt: now,
		Envelope: map[string]string{
			"tenant_id":      "t-2",
			"schema_version": "not-a-number",
			"occurred_at":    "2026-07-01T00:00:00Z", // RFC3339, no nanos
		},
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	d2 := newDispatcher(store2, &fakeBus{})
	if n, err := d2.DrainOnce(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("DrainOnce: n=%d err=%v", n, err)
	}
}

// ---------------------------------------------------------------------------
// TransactionAuditEmitter
// ---------------------------------------------------------------------------

func TestTransactionAuditEmitter_EmitCrossTenantViewed(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	e := outbox.NewTransactionAuditEmitter(store)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	now := from.Add(time.Hour)
	err := e.EmitCrossTenantViewed(context.Background(), "gcid-operator", tl.AuditFilter{
		Kind: "purchase", Status: "captured", From: &from, To: &to,
	}, []string{"f-1", "f-2"}, now)
	if err != nil {
		t.Fatalf("EmitCrossTenantViewed: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("expected 1 audit row, got %d", len(rows))
	}
	row := rows[0]
	if row.Topic != "chora.governance.audit.cross_tenant_payments_viewed.v1" {
		t.Fatalf("unexpected topic %q", row.Topic)
	}
	if !strings.Contains(row.TenantID, "00000000-0000-0000-0000-000000000000") {
		t.Fatalf("expected nil-uuid row tenant, got %q", row.TenantID)
	}
	if row.Envelope["tenant_id"] != "platform" || row.Envelope["traceparent"] == "" {
		t.Fatalf("expected platform sentinel + traceparent, got %v", row.Envelope)
	}
	if len(row.Payload) == 0 {
		t.Fatalf("expected binary payload")
	}
	// Nil filter pointers + open span-all (empty tenant set).
	store2 := outbox.NewInMemoryStore()
	e2 := outbox.NewTransactionAuditEmitter(store2)
	if err := e2.EmitCrossTenantViewed(context.Background(), "gcid-2", tl.AuditFilter{}, nil, now); err != nil {
		t.Fatalf("EmitCrossTenantViewed(minimal): %v", err)
	}
	// Insert failure propagates.
	store3 := &failMarkedStore{InMemoryStore: outbox.NewInMemoryStore()}
	_ = store3
	failing := &failInsertStore{InMemoryStore: outbox.NewInMemoryStore()}
	e3 := outbox.NewTransactionAuditEmitter(failing)
	if err := e3.EmitCrossTenantViewed(context.Background(), "g", tl.AuditFilter{}, nil, now); err == nil {
		t.Fatalf("expected insert error")
	}
	// Unwired emitter.
	var nilE *outbox.TransactionAuditEmitter
	if err := nilE.EmitCrossTenantViewed(context.Background(), "g", tl.AuditFilter{}, nil, now); err == nil {
		t.Fatalf("expected unwired error")
	}
	// WithClock returns the receiver for chaining.
	if e.WithClock(func() time.Time { return now }) != e {
		t.Fatalf("WithClock should return receiver")
	}
}

type failInsertStore struct {
	*outbox.InMemoryStore
}

func (f *failInsertStore) Insert(context.Context, outbox.Row) error { return errors.New("insert boom") }
