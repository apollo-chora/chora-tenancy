// Package reconciliation owns the daily Stripe vs chora event-log
// reconciliation domain logic.
//
// Ported from services/chora-billing-webhook/internal/domain/reconciliation
// as part of the M12.2 Batch-1 consolidation. Per locked architecture
// (CLAUDE.md §1) Tenancy + Billing are a single combined supporting domain.
//
// Cross-DB queries forbidden (per .claude/rules/ddd-enforcement.md) — the
// EventLogFeeder port reads via subscription replay from
// chora.tenancy.payment.captured.v1 + chora.identity.payment.captured.v1
// (NOT direct DB access to chora_tenancy / chora_identity).
//
// The Job pulls Stripe Balance API + Charges API for the previous day,
// pulls the event-log sum for the same window, computes drift in basis
// points, and emits one of:
//
//   - chora.governance.payment_reconciliation.completed.v1     (always)
//   - chora.governance.payment_reconciliation.anomaly.v1       (only when drift > tolerance)
//
// IMDA dimension: completed = accountability (D1); anomaly = safety_and_robustness
// (D3 — "anomaly is a robustness signal"). Both per ADR-141 canonical labels.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Inputs are the value-only inputs to the pure-function Reconcile.
type Inputs struct {
	Day                   time.Time
	StripeCapturedCents   int64
	EventLogCapturedCents int64
	// ToleranceBPS is the drift tolerance in basis points. 1 bps = 0.01%.
	// Zero means "use default" (10 bps = 0.1%); use a non-zero value to
	// override.
	ToleranceBPS int
}

// Report is the pure output of Reconcile. Adapters render this onto
// outbound events (completed + anomaly).
type Report struct {
	Day           time.Time
	ExpectedCents int64
	ObservedCents int64
	DeltaCents    int64 // absolute delta
	ToleranceBPS  int
	RunID         string
}

// HasDrift reports whether the absolute delta exceeds the tolerance.
func (r Report) HasDrift() bool {
	if r.ExpectedCents == 0 && r.ObservedCents == 0 {
		return false
	}
	return r.DeltaBPS() > r.ToleranceBPS
}

// DeltaBPS returns the drift in basis points. Defined to be 0 when the
// expected side is 0 (no denominator). 1 bps = 0.01%.
func (r Report) DeltaBPS() int {
	if r.ExpectedCents <= 0 {
		if r.ObservedCents <= 0 {
			return 0
		}
		return 10000
	}
	return int((r.DeltaCents * 10000) / r.ExpectedCents)
}

// DefaultToleranceBPS is 10 bps = 0.10% — a sensible default for daily
// reconciliation drift in payment platforms.
const DefaultToleranceBPS = 10

// Reconcile is the pure-function reconciliation. No I/O. Adapters wire
// Stripe + event-log feeders to gather Inputs.
func Reconcile(in Inputs) Report {
	tolerance := in.ToleranceBPS
	if tolerance == 0 {
		tolerance = DefaultToleranceBPS
	}
	delta := in.StripeCapturedCents - in.EventLogCapturedCents
	if delta < 0 {
		delta = -delta
	}
	return Report{
		Day:           in.Day,
		ExpectedCents: in.StripeCapturedCents,
		ObservedCents: in.EventLogCapturedCents,
		DeltaCents:    delta,
		ToleranceBPS:  tolerance,
	}
}

// -----------------------------------------------------------------------------
// Job — orchestrates Reconcile + emits canonical events.
// -----------------------------------------------------------------------------

// StripeFeeder is the port that fetches the Stripe-side captured-cents total
// for a given day. Production wires the Stripe Go SDK adapter; tests use a
// fake.
//
// Implementations MUST aggregate Stripe Charges with status=succeeded for
// the supplied day window in UTC.
type StripeFeeder interface {
	FetchCapturedCentsForDay(ctx context.Context, day time.Time) (int64, error)
}

// EventLogFeeder is the port that fetches the chora event-log captured
// total for a given day — by replay-subscribing to
// chora.tenancy.payment.captured.v1 + chora.identity.payment.captured.v1.
// Cross-DB queries are forbidden (per .claude/rules/ddd-enforcement.md):
// implementations consume the centralised Pub/Sub history only.
type EventLogFeeder interface {
	SumCapturedCentsForDay(ctx context.Context, day time.Time) (int64, error)
}

// Emitter is the outbound event publisher port. Production wires a
// chora-common/outbox-backed publisher; tests use a fake recorder.
type Emitter interface {
	EmitCompleted(ctx context.Context, r Report) error
	EmitAnomaly(ctx context.Context, r Report) error
}

// JobConfig wires a Job's collaborators.
type JobConfig struct {
	Stripe       StripeFeeder
	EventLog     EventLogFeeder
	Emitter      Emitter
	ToleranceBPS int
	// Now is injectable for tests. Production passes time.Now().UTC().
	// When nil, defaults to time.Now().UTC.
	Now func() time.Time
}

// Job orchestrates a daily reconciliation. Designed to be invoked from
// cmd/billing-reconciliation/main.go (Cloud Run Job) once per day via
// Cloud Scheduler.
type Job struct {
	cfg JobConfig
}

// NewJob constructs a Job. Returns nil-safe panicking on missing collaborators
// is deliberately avoided — the job runs in a Cloud Run Job and we want a
// descriptive runtime error, not a panic.
func NewJob(cfg JobConfig) *Job {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Job{cfg: cfg}
}

// ErrJobMisconfigured is returned by RunDaily when required collaborators are
// nil. (Cloud Run Job will exit non-zero, which Cloud Scheduler retries per
// its policy.)
var ErrJobMisconfigured = errors.New("reconciliation: job misconfigured (missing Stripe / EventLog / Emitter)")

// RunDaily executes one reconciliation pass for the supplied day (UTC).
//
// Always emits the `completed` event so the run is durably recorded —
// regardless of whether drift was detected. When drift exceeds tolerance
// it ALSO emits the `anomaly` event.
//
// Returns the Report on success. Errors surface from the Stripe feeder /
// event-log feeder / emitters; Cloud Run Job non-zero exit triggers
// Cloud Scheduler retry per its policy.
func (j *Job) RunDaily(ctx context.Context, day time.Time) (Report, error) {
	if j.cfg.Stripe == nil || j.cfg.EventLog == nil || j.cfg.Emitter == nil {
		return Report{}, ErrJobMisconfigured
	}

	stripeCents, err := j.cfg.Stripe.FetchCapturedCentsForDay(ctx, day)
	if err != nil {
		return Report{}, fmt.Errorf("reconciliation: stripe feeder: %w", err)
	}
	logCents, err := j.cfg.EventLog.SumCapturedCentsForDay(ctx, day)
	if err != nil {
		return Report{}, fmt.Errorf("reconciliation: event log feeder: %w", err)
	}

	report := Reconcile(Inputs{
		Day:                   day,
		StripeCapturedCents:   stripeCents,
		EventLogCapturedCents: logCents,
		ToleranceBPS:          j.cfg.ToleranceBPS,
	})
	report.RunID = newRunID(j.cfg.Now())

	if report.HasDrift() {
		if err := j.cfg.Emitter.EmitAnomaly(ctx, report); err != nil {
			return report, fmt.Errorf("reconciliation: emit anomaly: %w", err)
		}
	}
	if err := j.cfg.Emitter.EmitCompleted(ctx, report); err != nil {
		return report, fmt.Errorf("reconciliation: emit completed: %w", err)
	}
	return report, nil
}

// newRunID composes a deterministic-ish run ID from the supplied now. We
// deliberately avoid pulling in uuid here to keep the domain dep-free; the
// adapter layer can rewrite this when wiring the outbox.
func newRunID(now time.Time) string {
	return fmt.Sprintf("recon-%s", now.UTC().Format("20060102T150405Z"))
}

// AnomalyTopic + CompletedTopic — canonical chora-event topic names emitted
// by the reconciliation job. Exported so adapters and tests can reference
// them without string duplication.
const (
	CompletedTopic = "chora.governance.payment_reconciliation.completed.v1"
	AnomalyTopic   = "chora.governance.payment_reconciliation.anomaly.v1"
)
