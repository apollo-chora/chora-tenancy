// Package outbox_test exercises the chora-tenancy D6.2 producer-side
// transactional outbox + DLQ adapter.
//
// The store is the producer-side durable-emission seam. The Publisher
// stamps a pending row inside the request handler; the Dispatcher drains
// pending rows to Cloud Pub/Sub on a separate goroutine.
//
// Two implementations:
//
//   - InMemoryStore — hermetic, used by these tests + by main()'s fallback
//     when CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID is unset.
//   - PostgresStore — production; wraps a database/sql connection pointing
//     at chora_tenancy (where outbox_events lives per
//     migrations/0005_outbox.sql).
//
// Per `feedback_d6_resilience_first_class` B.6.2 sub-deliverable (a) +
// `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2. Mirrors the
// canonical chora-creation + chora-guardrail outbox pattern.
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
)

// -----------------------------------------------------------------------------
// InMemoryStore tests
// -----------------------------------------------------------------------------

func newRow(id, tenant string, occurred time.Time) outbox.Row {
	return outbox.Row{
		ID:             id,
		TenantID:       tenant,
		GCID:           "00000000-0000-0000-0000-000000000001",
		AggregateType:  "tenant",
		AggregateID:    "tenant-" + id,
		EventType:      "tenancy.tenant.created",
		Topic:          "chora.tenancy.tenant.created.v1",
		Payload:        []byte(`{"hello":"world"}`),
		Envelope:       map[string]string{"event_id": id, "source_service": "chora-tenancy"},
		IdempotencyKey: "idem-" + id,
		OccurredAt:     occurred,
	}
}

func TestInMemoryStore_FetchPending_OrdersByOccurredAtAscending(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	store := outbox.NewInMemoryStore()
	_ = store.Insert(context.Background(), newRow("01", "t1", now.Add(2*time.Second)))
	_ = store.Insert(context.Background(), newRow("02", "t1", now.Add(1*time.Second)))
	_ = store.Insert(context.Background(), newRow("03", "t1", now.Add(3*time.Second)))

	rows, err := store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("FetchPending count = %d; want 3", len(rows))
	}
	if rows[0].ID != "02" || rows[1].ID != "01" || rows[2].ID != "03" {
		t.Errorf("ordering = %s,%s,%s; want 02,01,03",
			rows[0].ID, rows[1].ID, rows[2].ID)
	}
}

func TestInMemoryStore_FetchPending_RespectsLimit(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_ = store.Insert(context.Background(),
			newRow(string(rune('a'+i)), "t", now.Add(time.Duration(i)*time.Second)))
	}
	rows, err := store.FetchPending(context.Background(), 3)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("len(rows) = %d; want 3 (limit)", len(rows))
	}
}

func TestInMemoryStore_MarkPublished_RemovesFromPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("X", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.MarkPublished(context.Background(), "X"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after MarkPublished = %d; want 0", len(rows))
	}
	pubs := store.Published()
	if len(pubs) != 1 || pubs[0] != "X" {
		t.Errorf("Published = %v; want [X]", pubs)
	}
}

func TestInMemoryStore_MarkFailed_IncrementsRetryCount(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("Y", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.MarkFailed(context.Background(), "Y", "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("pending after MarkFailed = %d; want 1", len(rows))
	}
	if rows[0].RetryCount != 1 {
		t.Errorf("retry_count = %d; want 1", rows[0].RetryCount)
	}
	if rows[0].LastError != "boom" {
		t.Errorf("last_error = %q; want boom", rows[0].LastError)
	}
}

func TestInMemoryStore_Deadletter_RemovesAndRecords(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("Z", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.Deadletter(context.Background(), "Z", "fatal", 5); err != nil {
		t.Fatalf("Deadletter: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after Deadletter = %d; want 0", len(rows))
	}
	dl := store.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLetters count = %d; want 1", len(dl))
	}
	if dl[0].RowID != "Z" || dl[0].FailureReason != "fatal" || dl[0].AttemptCount != 5 {
		t.Errorf("DeadLetter row = %+v; want {Z fatal 5}", dl[0])
	}
}

func TestInMemoryStore_Insert_RejectsDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r1 := newRow("a", "t", time.Now().UTC())
	r1.IdempotencyKey = "dup"
	r2 := newRow("b", "t", time.Now().UTC())
	r2.IdempotencyKey = "dup"
	if err := store.Insert(context.Background(), r1); err != nil {
		t.Fatalf("Insert r1: %v", err)
	}
	err := store.Insert(context.Background(), r2)
	if err == nil || !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert r2 err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

// -----------------------------------------------------------------------------
// PostgresStore tests — verify SQL shape against a stub *sql.DB-compatible
// surface. Real DB integration is gated behind -short and exercised in the
// chora-tenancy integration suite separately.
// -----------------------------------------------------------------------------

type stubDB struct {
	execStatements []string
	execArgs       [][]any
	execErr        error

	queryStatement string
	queryArgs      []any
	queryRows      []fakePGRow
	queryErr       error
}

type fakePGRow struct {
	id, tenant, gcid, aggregateType, aggregateID, eventType, topic string
	payload                                                        []byte
	envelope                                                       string
	idempotencyKey                                                 string
	retryCount                                                     int
	occurredAt                                                     time.Time
}

func (s *stubDB) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	s.execStatements = append(s.execStatements, q)
	cp := append([]any(nil), args...)
	s.execArgs = append(s.execArgs, cp)
	return nil, s.execErr
}

func (s *stubDB) QueryContext(_ context.Context, q string, args ...any) (outbox.SQLRows, error) {
	s.queryStatement = q
	s.queryArgs = args
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &fakeRows{rows: s.queryRows}, nil
}

type fakeRows struct {
	rows []fakePGRow
	idx  int
	err  error
}

func (r *fakeRows) Next() bool { return r.idx < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("fakeRows: out of range")
	}
	row := r.rows[r.idx]
	r.idx++
	// Order matches PostgresStore.FetchPending SELECT:
	// id, tenant_id, gcid, aggregate_type, aggregate_id, event_type, topic,
	// payload, envelope, idempotency_key, retry_count, occurred_at
	if len(dest) != 12 {
		return errors.New("fakeRows: scan dest mismatch")
	}
	*dest[0].(*string) = row.id
	*dest[1].(*string) = row.tenant
	*dest[2].(*string) = row.gcid
	*dest[3].(*string) = row.aggregateType
	*dest[4].(*string) = row.aggregateID
	*dest[5].(*string) = row.eventType
	*dest[6].(*string) = row.topic
	*dest[7].(*[]byte) = row.payload
	*dest[8].(*string) = row.envelope
	*dest[9].(*string) = row.idempotencyKey
	*dest[10].(*int) = row.retryCount
	*dest[11].(*time.Time) = row.occurredAt
	return nil
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return r.err }

func TestPostgresStore_FetchPending_UsesForUpdateSkipLocked(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryRows: []fakePGRow{
		{
			id: "row-1", tenant: "00000000-0000-0000-0000-000000000aaa",
			gcid:          "00000000-0000-0000-0000-000000000bbb",
			aggregateType: "tenant", aggregateID: "tenant-1",
			eventType:  "tenancy.tenant.created",
			topic:      "chora.tenancy.tenant.created.v1",
			payload:    []byte(`{}`),
			envelope:   `{"event_id":"row-1"}`,
			occurredAt: time.Now().UTC(),
		},
	}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	rows, err := store.FetchPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows count = %d; want 1", len(rows))
	}
	if rows[0].ID != "row-1" {
		t.Errorf("rows[0].ID = %q; want row-1", rows[0].ID)
	}
	if !strings.Contains(db.queryStatement, "FOR UPDATE SKIP LOCKED") {
		t.Errorf("FetchPending SQL missing FOR UPDATE SKIP LOCKED: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "status IN ('pending', 'failed')") {
		t.Errorf("FetchPending SQL missing status filter: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "ORDER BY occurred_at ASC") {
		t.Errorf("FetchPending SQL missing ordering: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "outbox_events") {
		t.Errorf("FetchPending SQL missing table name: %s", db.queryStatement)
	}
	if len(db.queryArgs) != 2 || db.queryArgs[0] != 50 {
		t.Errorf("FetchPending args = %v; want [50]", db.queryArgs)
	}
	if rows[0].Envelope["event_id"] != "row-1" {
		t.Errorf("envelope parsed = %+v; want event_id=row-1", rows[0].Envelope)
	}
}

func TestPostgresStore_Insert_SendsAllColumns(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	r := newRow("row-I", "tenant-A", time.Now().UTC())
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(db.execStatements) != 1 {
		t.Fatalf("Exec calls = %d; want 1", len(db.execStatements))
	}
	stmt := db.execStatements[0]
	for _, col := range []string{"outbox_events", "id", "tenant_id", "gcid", "aggregate_type",
		"aggregate_id", "event_type", "topic", "payload", "envelope", "idempotency_key", "occurred_at"} {
		if !strings.Contains(stmt, col) {
			t.Errorf("Insert SQL missing column %q: %s", col, stmt)
		}
	}
}

func TestPostgresStore_Insert_DetectsUniqueViolation(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New(`pq: duplicate key value violates unique constraint "outbox_events_idempotency_idx" (SQLSTATE 23505)`)}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	r := newRow("dup", "tenant", time.Now().UTC())
	err := store.Insert(context.Background(), r)
	if err == nil || !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

func TestPostgresStore_MarkPublished_SetsStatusPublished(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if err := store.MarkPublished(context.Background(), "row-X"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if len(db.execStatements) != 1 {
		t.Fatalf("Exec calls = %d; want 1", len(db.execStatements))
	}
	stmt := db.execStatements[0]
	if !strings.Contains(stmt, "UPDATE outbox_events") {
		t.Errorf("UPDATE table mismatch: %s", stmt)
	}
	if !strings.Contains(stmt, "status='published'") && !strings.Contains(stmt, "status = 'published'") {
		t.Errorf("status not set to published: %s", stmt)
	}
	if !strings.Contains(stmt, "published_at") {
		t.Errorf("published_at not stamped: %s", stmt)
	}
	if len(db.execArgs[0]) != 1 || db.execArgs[0][0] != "row-X" {
		t.Errorf("MarkPublished args = %v; want [row-X]", db.execArgs[0])
	}
}

func TestPostgresStore_MarkFailed_IncrementsAndStampsError(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if err := store.MarkFailed(context.Background(), "row-Y", "boom-error"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	stmt := db.execStatements[0]
	if !strings.Contains(stmt, "retry_count = retry_count + 1") {
		t.Errorf("MarkFailed missing retry_count increment: %s", stmt)
	}
	if !strings.Contains(stmt, "last_error") {
		t.Errorf("MarkFailed missing last_error: %s", stmt)
	}
	if !strings.Contains(stmt, "last_attempt_at") {
		t.Errorf("MarkFailed missing last_attempt_at: %s", stmt)
	}
	if !strings.Contains(stmt, "status='failed'") && !strings.Contains(stmt, "status = 'failed'") {
		t.Errorf("MarkFailed not setting status to failed: %s", stmt)
	}
}

func TestPostgresStore_Deadletter_TwoStatementInsertPlusUpdate(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "worker-A"})
	if err := store.Deadletter(context.Background(), "row-Z", "fatal-error", 5); err != nil {
		t.Fatalf("Deadletter: %v", err)
	}
	if len(db.execStatements) != 2 {
		t.Fatalf("Deadletter Exec calls = %d; want 2 (INSERT+UPDATE)", len(db.execStatements))
	}
	insert := db.execStatements[0]
	if !strings.Contains(insert, "outbox_dead_letters") {
		t.Errorf("INSERT not into outbox_dead_letters: %s", insert)
	}
	if !strings.Contains(insert, "ON CONFLICT") {
		t.Errorf("Deadletter INSERT missing ON CONFLICT guard: %s", insert)
	}
	update := db.execStatements[1]
	if !strings.Contains(update, "UPDATE outbox_events") {
		t.Errorf("UPDATE not on events table: %s", update)
	}
	if !strings.Contains(update, "status='deadlettered'") && !strings.Contains(update, "status = 'deadlettered'") {
		t.Errorf("Deadletter status update missing: %s", update)
	}
}

func TestPostgresStore_RequiresWorkerID(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewPostgresStore with empty WorkerID should panic")
		}
	}()
	outbox.NewPostgresStore(&stubDB{}, outbox.PostgresStoreOptions{})
}

func TestRow_HasMandatoryFields(t *testing.T) {
	t.Parallel()
	r := newRow("id-1", "tenant-A", time.Now().UTC())
	if r.ID == "" || r.TenantID == "" || r.Topic == "" || len(r.Payload) == 0 || r.IdempotencyKey == "" {
		t.Errorf("Row missing mandatory fields: %+v", r)
	}
}
