// Package outbox is chora-tenancy's D6.2 producer-side transactional outbox
// adapter for the `chora.tenancy.{tenant,addon,member,billing}.*.v1` event
// streams (8 canonical topics enumerated in the events package).
//
// Why it exists
// -------------
// chora-tenancy emits multi-tenant lifecycle events (tenant.created.v1,
// tenant.golive.v1, addon.activated.v1, addon.upgraded.v1, addon.
// downgraded.v1, addon.deactivation_requested.v1, addon.deactivated.v1,
// addon.usage_recorded.v1) whenever the tenancy aggregate or add-on
// subscription state changes. The pre-D6.2 path went through the
// in-process events.Recorder OR the legacy chora-go-common/outbox.Recorder
// — neither of which has a top-level `tenant_id` indexed column (required
// for D6.3 multi-tenant chaos isolation) or RLS.
//
// The canonical D6.2 pattern (per `feedback_d6_resilience_first_class` +
// `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2) replaces this
// with a per-service outbox adapter that:
//
//  1. The OutboxPublisher writes the event to `outbox_events` with
//     `tenant_id` indexed and `idempotency_key` UNIQUE — inside the
//     caller's transaction. The HTTP request returns successfully once
//     the row is durably committed.
//  2. A separate background Dispatcher polls pending rows, publishes
//     them to Cloud Pub/Sub, and marks success/failure/deadletter.
//
// The HTTP handler crash window now never loses an event: on restart
// the dispatcher resumes from the pending row.
//
// Three actors:
//
//   - Store — port that wraps the outbox_events table.
//     InMemoryStore + PostgresStore implementations.
//   - Publisher — satisfies the events.Recorder-shape API by writing to
//     Store. Swap-out for the existing events.Recorder / CloudPublisher.
//   - Dispatcher — drains Store to a `pubsub.Bus` with retry + DLQ.
//
// Mirrored from `services/chora-creation/internal/adapter/outbox/` +
// `services/chora-guardrail/internal/adapter/outbox/`.
//
// Schema in `services/chora-tenancy/migrations/0005_outbox.sql` (D6
// uplift adds tenant_id + idempotency_key UNIQUE + aggregate_type/_id +
// RLS).
//
// Per `feedback_no_inline_config`: no inline URLs/secrets — every
// dependency is injected via the public Config structs.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TenancyDomain is the chora.{domain} segment used by this service's
// canonical topics: chora.tenancy.{aggregate}.{event_type}.v{N}.
const TenancyDomain = "tenancy"

// ErrDuplicateIdempotencyKey is returned by Store.Insert when the supplied
// row collides on idempotency_key. Callers MAY treat this as a no-op
// (the prior emission is already durably queued).
var ErrDuplicateIdempotencyKey = errors.New("outbox: duplicate idempotency_key")

// -----------------------------------------------------------------------------
// Row
// -----------------------------------------------------------------------------

// Row mirrors one row of outbox_events with the D6 alignment columns. Field
// names align with the migration column names so SQL marshalling is
// mechanical.
type Row struct {
	ID             string
	TenantID       string
	GCID           string
	AggregateType  string // "tenant" / "addon_subscription" / "addon_usage" / "billing"
	AggregateID    string // domain entity ID (e.g. tenant_id, subscription_id)
	EventType      string // e.g. "tenancy.tenant.created"
	Topic          string // canonical chora.tenancy.{aggregate}.{event_type}.v{N}
	Payload        []byte
	Envelope       map[string]string // JSONB; subscriber attribute set
	IdempotencyKey string
	OccurredAt     time.Time
	RetryCount     int
	LastError      string
}

// DeadLetter mirrors one row of outbox_dead_letters.
type DeadLetter struct {
	RowID          string
	FailureReason  string
	AttemptCount   int
	WorkerID       string
	DeadletteredAt time.Time
}

// -----------------------------------------------------------------------------
// Store port
// -----------------------------------------------------------------------------

// Store is the port the Publisher + Dispatcher use to talk to outbox_events.
// Two implementations:
//
//   - InMemoryStore — hermetic for tests + local dev.
//   - PostgresStore — production; wraps a database/sql connection on
//     chora_tenancy (where the table lives).
type Store interface {
	// Insert writes a new pending row. Returns ErrDuplicateIdempotencyKey
	// when the row collides on idempotency_key.
	Insert(ctx context.Context, row Row) error

	// FetchPending claims up to limit rows in status='pending' ordered by
	// occurred_at ASC. Postgres uses SELECT ... FOR UPDATE SKIP LOCKED so
	// concurrent workers do not collide.
	FetchPending(ctx context.Context, limit int) ([]Row, error)

	// MarkPublished transitions the row to status='published'.
	MarkPublished(ctx context.Context, id string) error

	// MarkFailed records a transient publish failure: bumps retry_count,
	// stamps last_error + last_attempt_at, sets status='failed'.
	MarkFailed(ctx context.Context, id string, errMsg string) error

	// Deadletter inserts an outbox_dead_letters row + transitions the
	// events row to status='deadlettered'. attemptCount is captured for
	// audit / runbook context.
	Deadletter(ctx context.Context, id string, failureReason string, attemptCount int) error
}

// -----------------------------------------------------------------------------
// InMemoryStore
// -----------------------------------------------------------------------------

// InMemoryStore is the hermetic fixture used by tests + by main()'s fallback
// when CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID is unset. NOT safe for
// production — does not persist across process restart.
type InMemoryStore struct {
	mu                 sync.Mutex
	rows               map[string]*Row // id → row
	idempotency        map[string]string
	published          []string
	deadLetters        []DeadLetter
	deadletterWorkerID string
}

// NewInMemoryStore constructs an empty InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		rows:        make(map[string]*Row),
		idempotency: make(map[string]string),
	}
}

// Insert satisfies Store.
func (s *InMemoryStore) Insert(_ context.Context, row Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.idempotency[row.IdempotencyKey]; dup {
		return fmt.Errorf("%w: %s", ErrDuplicateIdempotencyKey, row.IdempotencyKey)
	}
	cp := row
	if cp.Envelope != nil {
		envCopy := make(map[string]string, len(cp.Envelope))
		for k, v := range cp.Envelope {
			envCopy[k] = v
		}
		cp.Envelope = envCopy
	}
	if cp.Payload != nil {
		cp.Payload = append([]byte(nil), cp.Payload...)
	}
	s.rows[cp.ID] = &cp
	s.idempotency[cp.IdempotencyKey] = cp.ID
	return nil
}

// FetchPending satisfies Store.
func (s *InMemoryStore) FetchPending(_ context.Context, limit int) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := make([]*Row, 0, len(s.rows))
	for _, r := range s.rows {
		pending = append(pending, r)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].OccurredAt.Before(pending[j].OccurredAt)
	})
	if limit > 0 && len(pending) > limit {
		pending = pending[:limit]
	}
	out := make([]Row, 0, len(pending))
	for _, r := range pending {
		out = append(out, *r)
	}
	return out, nil
}

// MarkPublished satisfies Store.
func (s *InMemoryStore) MarkPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return fmt.Errorf("outbox: MarkPublished: row %q not found", id)
	}
	delete(s.rows, id)
	s.published = append(s.published, id)
	return nil
}

// MarkFailed satisfies Store.
func (s *InMemoryStore) MarkFailed(_ context.Context, id string, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return fmt.Errorf("outbox: MarkFailed: row %q not found", id)
	}
	r.RetryCount++
	r.LastError = errMsg
	return nil
}

// Deadletter satisfies Store.
func (s *InMemoryStore) Deadletter(_ context.Context, id, failureReason string, attemptCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return fmt.Errorf("outbox: Deadletter: row %q not found", id)
	}
	s.deadLetters = append(s.deadLetters, DeadLetter{
		RowID:          id,
		FailureReason:  failureReason,
		AttemptCount:   attemptCount,
		WorkerID:       s.deadletterWorkerID,
		DeadletteredAt: time.Now().UTC(),
	})
	delete(s.rows, r.ID)
	return nil
}

// SetWorkerID stamps the worker_id captured in DeadLetters created by this
// in-memory store. The Dispatcher calls this once at construction so the
// store can record the worker on Deadletter() rows without an extra
// parameter on the Store port (PostgresStore stores worker_id on its own
// struct).
func (s *InMemoryStore) SetWorkerID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadletterWorkerID = id
}

// Published returns the IDs of successfully MarkPublished'd rows in order.
func (s *InMemoryStore) Published() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.published...)
	return out
}

// DeadLetters returns the dead-letter rows recorded by Deadletter.
func (s *InMemoryStore) DeadLetters() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]DeadLetter(nil), s.deadLetters...)
	return out
}

// Compile-time check.
var _ Store = (*InMemoryStore)(nil)

// -----------------------------------------------------------------------------
// PostgresStore
// -----------------------------------------------------------------------------

// SQLRows is the minimal database/sql Rows surface used by PostgresStore.
type SQLRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// SQLDB is the minimal database/sql surface required by PostgresStore.
// Production wires `*sql.DB` (or a *sql.Conn / pgxpool bridge) pointing at
// chora_tenancy. Tests stub via the SQLDB interface.
type SQLDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
}

// PostgresStoreOptions tunes a PostgresStore at construction time.
// defaultClaimReservationSeconds is the window during which a claimed-but-not-
// yet-published row is hidden from other workers' FetchPending. It must exceed
// the worst-case time to publish one DrainOnce batch; it also bounds how long a
// row claimed by a crashed worker waits before re-claim (the built-in reaper).
const defaultClaimReservationSeconds = 60

type PostgresStoreOptions struct {
	// WorkerID identifies the dispatcher worker that owns Deadletter rows.
	// Required. Typically derived from the pod name (HOSTNAME).
	WorkerID string

	// ClaimReservationSeconds is the FetchPending claim reservation window (see
	// defaultClaimReservationSeconds). Defaults to 60 when <= 0.
	ClaimReservationSeconds int
}

// PostgresStore is the production Store backed by the outbox_events table
// in chora_tenancy (per migration 0005_outbox.sql).
type PostgresStore struct {
	db              SQLDB
	workerID        string
	claimReservSecs int
}

// NewPostgresStore constructs a PostgresStore. Panics on missing WorkerID.
func NewPostgresStore(db SQLDB, opts PostgresStoreOptions) *PostgresStore {
	if opts.WorkerID == "" {
		panic("outbox: NewPostgresStore: WorkerID required")
	}
	resv := opts.ClaimReservationSeconds
	if resv <= 0 {
		resv = defaultClaimReservationSeconds
	}
	return &PostgresStore{db: db, workerID: opts.WorkerID, claimReservSecs: resv}
}

// Insert satisfies Store.
func (s *PostgresStore) Insert(ctx context.Context, row Row) error {
	envJSON, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	const q = `INSERT INTO outbox_events (
        id, tenant_id, gcid, aggregate_type, aggregate_id, event_type, topic,
        payload, envelope, idempotency_key, occurred_at, status
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending'
    )`
	_, err = s.db.ExecContext(ctx, q,
		row.ID, row.TenantID, row.GCID, row.AggregateType, row.AggregateID,
		row.EventType, row.Topic, row.Payload, string(envJSON),
		row.IdempotencyKey, row.OccurredAt,
	)
	if err != nil {
		// best-effort detection of duplicate idempotency_key — Postgres
		// raises a unique violation (SQLSTATE 23505) when the unique index
		// fires.
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", ErrDuplicateIdempotencyKey, row.IdempotencyKey)
		}
		return fmt.Errorf("outbox: Insert: %w", err)
	}
	return nil
}

// FetchPending satisfies Store.
// FetchPending atomically CLAIMS up to `limit` pending rows and returns them.
// CHO-1615: claiming UPDATE (not a bare SELECT) so two dispatcher workers draining
// the same table (a prod + a *-dev co-deployment, or HPA >1 replica) never both
// publish the same row. The committed last_attempt_at bump + reservation-window
// predicate hide just-claimed rows from a peer; the window doubles as the
// crashed-worker reaper. No migration (last_attempt_at + status already exist).
func (s *PostgresStore) FetchPending(ctx context.Context, limit int) ([]Row, error) {
	const q = `UPDATE outbox_events
        SET last_attempt_at = now()
        WHERE id IN (
            SELECT id FROM outbox_events
            -- includes 'failed' so a transient publish error retries, not strands (see chora-creation outbox)
            WHERE status IN ('pending', 'failed')
              AND (last_attempt_at IS NULL
                   OR last_attempt_at < now() - make_interval(secs => $2))
            ORDER BY occurred_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, tenant_id::TEXT, gcid::TEXT,
               aggregate_type, aggregate_id, event_type, topic,
               payload, envelope::TEXT,
               idempotency_key, retry_count, occurred_at`
	rows, err := s.db.QueryContext(ctx, q, limit, s.claimReservSecs)
	if err != nil {
		return nil, fmt.Errorf("outbox: FetchPending: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var (
			r       Row
			envStr  string
			occured time.Time
		)
		if err := rows.Scan(
			&r.ID, &r.TenantID, &r.GCID,
			&r.AggregateType, &r.AggregateID, &r.EventType, &r.Topic,
			&r.Payload, &envStr,
			&r.IdempotencyKey, &r.RetryCount, &occured,
		); err != nil {
			return nil, fmt.Errorf("outbox: FetchPending scan: %w", err)
		}
		r.OccurredAt = occured
		env := map[string]string{}
		if envStr != "" {
			if err := json.Unmarshal([]byte(envStr), &env); err != nil {
				return nil, fmt.Errorf("outbox: FetchPending envelope: %w", err)
			}
		}
		r.Envelope = env
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: FetchPending iter: %w", err)
	}
	return out, nil
}

// MarkPublished satisfies Store.
func (s *PostgresStore) MarkPublished(ctx context.Context, id string) error {
	const q = `UPDATE outbox_events
        SET status='published', published_at=now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, q, id); err != nil {
		return fmt.Errorf("outbox: MarkPublished: %w", err)
	}
	return nil
}

// MarkFailed satisfies Store.
func (s *PostgresStore) MarkFailed(ctx context.Context, id string, errMsg string) error {
	const q = `UPDATE outbox_events
        SET status='failed',
            retry_count = retry_count + 1,
            last_error = $2,
            last_attempt_at = now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, q, id, truncate(errMsg, 1000)); err != nil {
		return fmt.Errorf("outbox: MarkFailed: %w", err)
	}
	return nil
}

// Deadletter satisfies Store.
func (s *PostgresStore) Deadletter(ctx context.Context, id, failureReason string, attemptCount int) error {
	const insertQ = `INSERT INTO outbox_dead_letters
        (outbox_event_id, failure_reason, attempt_count, worker_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (outbox_event_id) DO NOTHING`
	if _, err := s.db.ExecContext(ctx, insertQ, id, truncate(failureReason, 1000), attemptCount, s.workerID); err != nil {
		return fmt.Errorf("outbox: Deadletter insert: %w", err)
	}
	const updateQ = `UPDATE outbox_events
        SET status='deadlettered',
            last_attempt_at = now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, updateQ, id); err != nil {
		return fmt.Errorf("outbox: Deadletter update: %w", err)
	}
	return nil
}

// Compile-time check.
var _ Store = (*PostgresStore)(nil)

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// isUniqueViolation returns true if the error is a Postgres unique violation.
// The driver-specific check is deliberately loose (string match on the
// SQLSTATE 23505 prefix) so we don't bind to pq/pgx at compile time. Real
// production wiring layers a driver-aware predicate.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "23505") || contains(msg, "duplicate key value violates unique constraint")
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
