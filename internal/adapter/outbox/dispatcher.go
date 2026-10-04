// Package outbox — Dispatcher implementation.
//
// Dispatcher drains pending outbox_events rows to Cloud Pub/Sub. Composes
// Store.FetchPending → Bus.Publish → Store.MarkPublished / MarkFailed /
// Deadletter. On max-attempts exhaustion the row lands in
// outbox_dead_letters AND a Pub/Sub-side DLQ subscription (configured in
// Terraform — see m10-pubsub-dlq).
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — dispatcher is the
// retry + DLQ ladder for the producer-side outbox. Subscriber-side
// ack-after-processing is a separate concern (B.6.2.b).
//
// Concurrency: one Dispatcher per pod is fine — Postgres's
// `SELECT ... FOR UPDATE SKIP LOCKED` (in PostgresStore.FetchPending) lets
// multiple replicas drain the same table without colliding on rows.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
)

// Bus is the Pub/Sub publisher contract the Dispatcher uses. Matches
// `chora-common/pubsub.OutboxCompatible` so the InMemoryBus + the
// CloudPubSub adapter slot in directly.
type Bus interface {
	Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error
}

// DispatcherConfig tunes a Dispatcher.
type DispatcherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// Bus is the Pub/Sub publisher. Required.
	Bus Bus

	// WorkerID identifies the dispatcher worker that records Deadletter
	// rows. Required. Typically derived from the pod name (HOSTNAME).
	WorkerID string

	// MaxAttempts caps the per-row publish retry count. After MaxAttempts
	// the row transitions to status='deadlettered' and an
	// outbox_dead_letters row is inserted. Defaults to 5.
	MaxAttempts int

	// PollInterval is the sleep between empty drain cycles in Run.
	// Defaults to 250ms.
	PollInterval time.Duration

	// BackoffBase + BackoffCap shape the exponential backoff used by Run
	// between non-empty drain cycles when transient failures are
	// accumulating. Defaults: 50ms / 5s.
	BackoffBase time.Duration
	BackoffCap  time.Duration

	// Logger is the structured logger used for outbox_published /
	// outbox_publish_failed / outbox_deadletter lines. nil → stdlib log.
	Logger Logger

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time

	// SourceService stamps the SourceService field on reconstructed
	// envelopes when the persisted row's envelope JSONB does not carry
	// one. Defaults to "chora-tenancy".
	SourceService string
}

// Logger is the minimal contract used by the Dispatcher. The bootstrap
// wiring passes a Cloud Logging adapter in production; tests use the
// default stdlib bridge.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
}

// Dispatcher drains the outbox to a Bus.
type Dispatcher struct {
	cfg DispatcherConfig
}

// NewDispatcher constructs a Dispatcher. Panics on missing WorkerID. Defaults
// MaxAttempts=5, PollInterval=250ms, BackoffBase=50ms, BackoffCap=5s.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.WorkerID == "" {
		panic("outbox: NewDispatcher: WorkerID required")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 50 * time.Millisecond
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger{}
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-tenancy"
	}
	// Bind the worker_id to the in-memory store so Deadletter rows carry
	// the right worker. PostgresStore stores worker_id on its own struct.
	if inmem, ok := cfg.Store.(*InMemoryStore); ok {
		inmem.SetWorkerID(cfg.WorkerID)
	}
	return &Dispatcher{cfg: cfg}
}

// MaxAttempts returns the configured retry ceiling. Exposed for tests.
func (d *Dispatcher) MaxAttempts() int { return d.cfg.MaxAttempts }

// DrainOnce drains one batch of pending rows. Returns the count of
// successfully published rows.
//
// Failures are recorded via Store.MarkFailed (transient) or Store.Deadletter
// (max_attempts exhausted) — they do NOT raise. Callers run this in a loop
// with a backoff between calls (see Run).
//
// Context cancellation aborts the drain partway through; rows already
// published are committed.
func (d *Dispatcher) DrainOnce(ctx context.Context, batchSize int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	rows, err := d.cfg.Store.FetchPending(ctx, batchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox.Dispatcher.DrainOnce: %w", err)
	}

	published := 0
	for _, row := range rows {
		select {
		case <-ctx.Done():
			return published, ctx.Err()
		default:
		}
		if err := d.publishOne(ctx, &row); err == nil {
			published++
		}
	}
	return published, nil
}

// publishOne attempts a single publish + records the outcome via the Store.
func (d *Dispatcher) publishOne(ctx context.Context, row *Row) error {
	env, err := d.reconstructEnvelope(row)
	if err != nil {
		// Envelope reconstruction failure means the row is corrupt
		// — deadletter immediately so a runbook can investigate.
		_ = d.cfg.Store.Deadletter(ctx, row.ID, "envelope reconstruct: "+err.Error(), row.RetryCount+1)
		d.cfg.Logger.Warnf("outbox_deadletter row_id=%s tenant=%s reason=envelope_corrupt err=%v",
			row.ID, row.TenantID, err)
		return err
	}

	pubErr := d.cfg.Bus.Publish(ctx, row.Topic, env, row.Payload)
	if pubErr == nil {
		if mErr := d.cfg.Store.MarkPublished(ctx, row.ID); mErr != nil {
			d.cfg.Logger.Warnf("outbox_mark_published_failed row_id=%s err=%v", row.ID, mErr)
			return mErr
		}
		d.cfg.Logger.Infof("outbox_published row_id=%s tenant=%s topic=%s",
			row.ID, row.TenantID, row.Topic)
		return nil
	}

	attempt := row.RetryCount + 1
	if attempt >= d.cfg.MaxAttempts {
		if dlErr := d.cfg.Store.Deadletter(ctx, row.ID, pubErr.Error(), attempt); dlErr != nil {
			d.cfg.Logger.Warnf("outbox_deadletter_failed row_id=%s err=%v", row.ID, dlErr)
		}
		d.cfg.Logger.Warnf("outbox_deadletter row_id=%s tenant=%s attempts=%d topic=%s err=%v",
			row.ID, row.TenantID, attempt, row.Topic, pubErr)
		return pubErr
	}

	if mErr := d.cfg.Store.MarkFailed(ctx, row.ID, pubErr.Error()); mErr != nil {
		d.cfg.Logger.Warnf("outbox_mark_failed_failed row_id=%s err=%v", row.ID, mErr)
	}
	d.cfg.Logger.Infof("outbox_publish_failed row_id=%s attempts=%d err=%v",
		row.ID, attempt, pubErr)
	return pubErr
}

// Run drives DrainOnce in a loop until ctx is canceled, sleeping
// PollInterval between empty drain cycles. Designed for long-running
// Cloud Run / GKE workers.
//
// Returns context.Canceled / context.DeadlineExceeded when the caller
// stops the dispatcher.
func (d *Dispatcher) Run(ctx context.Context, batchSize int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := d.DrainOnce(ctx, batchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			d.cfg.Logger.Warnf("outbox_drain_error err=%v", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.cfg.PollInterval):
			}
		}
	}
}

// reconstructEnvelope rebuilds the typed cgcenvelope.Envelope from the flat
// string map persisted in the JSONB column. The producer-side Publisher
// persists the same shape; the Dispatcher reverses it.
func (d *Dispatcher) reconstructEnvelope(row *Row) (cgcenvelope.Envelope, error) {
	env := cgcenvelope.Envelope{}
	if row.Envelope != nil {
		env.EventID = row.Envelope["event_id"]
		env.IdempotencyKey = row.Envelope["idempotency_key"]
		env.TenantID = row.Envelope["tenant_id"]
		env.GCID = row.Envelope["gcid"]
		env.Traceparent = row.Envelope["traceparent"]
		env.Tracestate = row.Envelope["tracestate"]
		env.SourceProject = row.Envelope["source_project"]
		env.SourceService = row.Envelope["source_service"]
		env.ChoraImdaDimension = row.Envelope["chora_imda_dimension"]
		env.ImdaLifecycleStage = row.Envelope["imda_lifecycle_stage"]
		if v := row.Envelope["schema_version"]; v != "" {
			var n int32
			_, _ = fmt.Sscanf(v, "%d", &n)
			if n <= 0 {
				n = 1
			}
			env.SchemaVersion = n
		} else {
			env.SchemaVersion = 1
		}
		env.OccurredAt = parseEnvelopeTime(row.Envelope["occurred_at"], row.OccurredAt)
		env.PublishedAt = parseEnvelopeTime(row.Envelope["published_at"], d.cfg.Now())
	} else {
		env.SchemaVersion = 1
		env.OccurredAt = row.OccurredAt
		env.PublishedAt = d.cfg.Now()
	}

	if env.EventID == "" {
		env.EventID = row.ID
	}
	if env.IdempotencyKey == "" {
		env.IdempotencyKey = row.IdempotencyKey
	}
	if env.TenantID == "" {
		env.TenantID = row.TenantID
	}
	if env.GCID == "" {
		env.GCID = row.GCID
	}
	if env.SourceProject == "" {
		env.SourceProject = "chora-489812"
	}
	if env.SourceService == "" {
		env.SourceService = d.cfg.SourceService
	}
	return env, nil
}

func parseEnvelopeTime(s string, fallback time.Time) time.Time {
	if s == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return fallback
}

// -----------------------------------------------------------------------------
// defaultLogger — stdlib log fallback.
// -----------------------------------------------------------------------------

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("INFO  "+format, args...)
}
func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("WARN  "+format, args...)
}
