// Package reconciliation tests — domain layer for daily Stripe vs chora event
// log reconciliation.
//
// Per CLAUDE.md (cross-DB queries forbidden) the reconciliation domain MUST
// only consume from event-log feeders + Stripe API; never via direct DB
// connection to chora_tenancy / chora_identity.
//
// Ported from services/chora-billing-webhook/internal/domain/reconciliation
// as part of the M12.2 Batch-1 consolidation.
package reconciliation_test

import (
	"context"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/reconciliation"
)

func TestReconcile_NoDrift(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   day,
		StripeCapturedCents:   1_000_00,
		EventLogCapturedCents: 1_000_00,
		ToleranceBPS:          5,
	})
	if report.HasDrift() {
		t.Fatalf("expected NO drift, got drift cents=%d bps=%d", report.DeltaCents, report.DeltaBPS())
	}
	if report.Day != day {
		t.Errorf("Day = %v; want %v", report.Day, day)
	}
	if report.ExpectedCents != 1_000_00 {
		t.Errorf("ExpectedCents = %d; want 100000", report.ExpectedCents)
	}
	if report.ObservedCents != 1_000_00 {
		t.Errorf("ObservedCents = %d; want 100000", report.ObservedCents)
	}
}

func TestReconcile_WithinTolerance(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   day,
		StripeCapturedCents:   100_000,
		EventLogCapturedCents: 99_999,
		ToleranceBPS:          5,
	})
	if report.HasDrift() {
		t.Fatalf("delta within tolerance must NOT count as drift, got HasDrift=true")
	}
}

func TestReconcile_AboveTolerance(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   day,
		StripeCapturedCents:   100_000,
		EventLogCapturedCents: 99_950,
		ToleranceBPS:          1,
	})
	if !report.HasDrift() {
		t.Fatalf("drift above tolerance must classify as drift; got HasDrift=false")
	}
	if report.DeltaCents != 50 {
		t.Errorf("DeltaCents = %d; want 50", report.DeltaCents)
	}
	if report.DeltaBPS() <= report.ToleranceBPS {
		t.Errorf("DeltaBPS=%d should exceed ToleranceBPS=%d", report.DeltaBPS(), report.ToleranceBPS)
	}
}

func TestReconcile_NegativeDrift(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   day,
		StripeCapturedCents:   100_000,
		EventLogCapturedCents: 100_500,
		ToleranceBPS:          5,
	})
	if !report.HasDrift() {
		t.Fatalf("negative drift must also classify as drift")
	}
	if report.DeltaCents != 500 {
		t.Errorf("DeltaCents = %d; want 500 (absolute delta)", report.DeltaCents)
	}
}

func TestReconcile_ZeroExpected(t *testing.T) {
	t.Parallel()
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		StripeCapturedCents:   0,
		EventLogCapturedCents: 0,
		ToleranceBPS:          5,
	})
	if report.HasDrift() {
		t.Fatalf("0 vs 0 must be clean")
	}
}

func TestReconcile_DefaultToleranceWhenZero(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Reconcile(reconciliation.Inputs{
		Day:                   day,
		StripeCapturedCents:   100_000,
		EventLogCapturedCents: 99_910,
		ToleranceBPS:          0,
	})
	if report.HasDrift() {
		t.Fatalf("default tolerance must absorb 9-bps drift; got HasDrift=true")
	}
}

func TestRunDaily_EmitsCompletedOnCleanRun(t *testing.T) {
	t.Parallel()
	stripe := &fakeStripeFeeder{stripeCents: 100_000}
	eventLog := &fakeEventLogFeeder{cents: 100_000}
	emitter := &fakeEmitter{}

	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       stripe,
		EventLog:     eventLog,
		Emitter:      emitter,
		ToleranceBPS: 5,
	})
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	if _, err := job.RunDaily(context.Background(), day); err != nil {
		t.Fatalf("RunDaily: unexpected error: %v", err)
	}
	if emitter.completedCount != 1 {
		t.Errorf("completed event emitted %d times; want 1", emitter.completedCount)
	}
	if emitter.anomalyCount != 0 {
		t.Errorf("anomaly event emitted %d times on clean run; want 0", emitter.anomalyCount)
	}
}

func TestRunDaily_EmitsAnomalyOnDrift(t *testing.T) {
	t.Parallel()
	stripe := &fakeStripeFeeder{stripeCents: 100_000}
	eventLog := &fakeEventLogFeeder{cents: 99_500}
	emitter := &fakeEmitter{}

	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       stripe,
		EventLog:     eventLog,
		Emitter:      emitter,
		ToleranceBPS: 5,
	})
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report, err := job.RunDaily(context.Background(), day)
	if err != nil {
		t.Fatalf("RunDaily on drift must NOT error (drift is reported, not raised): %v", err)
	}
	if !report.HasDrift() {
		t.Fatalf("expected drift in report")
	}
	if emitter.anomalyCount != 1 {
		t.Errorf("anomaly event emitted %d times on drift; want 1", emitter.anomalyCount)
	}
	if emitter.completedCount != 1 {
		t.Errorf("completed event emitted %d times; want 1 (always emit completion)", emitter.completedCount)
	}
	if emitter.lastAnomalyExpected != 100_000 || emitter.lastAnomalyObserved != 99_500 {
		t.Errorf("anomaly carried (expected,observed)=(%d,%d); want (100000,99500)",
			emitter.lastAnomalyExpected, emitter.lastAnomalyObserved)
	}
}

func TestRunDaily_StripeFeederError(t *testing.T) {
	t.Parallel()
	stripe := &fakeStripeFeeder{err: errBoom}
	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       stripe,
		EventLog:     &fakeEventLogFeeder{},
		Emitter:      &fakeEmitter{},
		ToleranceBPS: 5,
	})
	if _, err := job.RunDaily(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("RunDaily must surface Stripe feeder error")
	}
}

func TestRunDaily_EventLogFeederError(t *testing.T) {
	t.Parallel()
	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       &fakeStripeFeeder{stripeCents: 100},
		EventLog:     &fakeEventLogFeeder{err: errBoom},
		Emitter:      &fakeEmitter{},
		ToleranceBPS: 5,
	})
	if _, err := job.RunDaily(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("RunDaily must surface event-log feeder error")
	}
}

func TestRunDaily_EmitAnomalyError(t *testing.T) {
	t.Parallel()
	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       &fakeStripeFeeder{stripeCents: 100_000},
		EventLog:     &fakeEventLogFeeder{cents: 99_000},
		Emitter:      &fakeEmitter{anomalyErr: errBoom},
		ToleranceBPS: 5,
	})
	if _, err := job.RunDaily(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("RunDaily must surface anomaly emitter error")
	}
}

func TestRunDaily_EmitCompletedError(t *testing.T) {
	t.Parallel()
	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       &fakeStripeFeeder{stripeCents: 100},
		EventLog:     &fakeEventLogFeeder{cents: 100},
		Emitter:      &fakeEmitter{completedErr: errBoom},
		ToleranceBPS: 5,
	})
	if _, err := job.RunDaily(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("RunDaily must surface completed emitter error")
	}
}

func TestRunDaily_Misconfigured(t *testing.T) {
	t.Parallel()
	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe: &fakeStripeFeeder{},
	})
	_, err := job.RunDaily(context.Background(), time.Now().UTC())
	if err == nil {
		t.Fatalf("RunDaily on misconfigured Job must error")
	}
}

func TestDeltaBPS_AllBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		expected int64
		observed int64
		wantBPS  int
	}{
		{"both zero → 0 bps", 0, 0, 0},
		{"expected 0 + observed positive → max 10000 bps", 0, 100, 10000},
		{"both positive equal → 0 bps", 1000, 1000, 0},
		{"100 cents drift on 100000 → 10 bps", 100_000, 99_900, 10},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := reconciliation.Reconcile(reconciliation.Inputs{
				StripeCapturedCents:   tc.expected,
				EventLogCapturedCents: tc.observed,
				ToleranceBPS:          1,
			})
			if got := r.DeltaBPS(); got != tc.wantBPS {
				t.Errorf("DeltaBPS = %d; want %d (expected=%d observed=%d)", got, tc.wantBPS, tc.expected, tc.observed)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Test doubles
// -----------------------------------------------------------------------------

type fakeStripeFeeder struct {
	stripeCents int64
	err         error
}

func (f *fakeStripeFeeder) FetchCapturedCentsForDay(_ context.Context, _ time.Time) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.stripeCents, nil
}

type fakeEventLogFeeder struct {
	cents int64
	err   error
}

func (f *fakeEventLogFeeder) SumCapturedCentsForDay(_ context.Context, _ time.Time) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.cents, nil
}

type fakeEmitter struct {
	completedCount      int
	anomalyCount        int
	lastAnomalyExpected int64
	lastAnomalyObserved int64
	completedErr        error
	anomalyErr          error
}

func (e *fakeEmitter) EmitCompleted(_ context.Context, r reconciliation.Report) error {
	e.completedCount++
	return e.completedErr
}
func (e *fakeEmitter) EmitAnomaly(_ context.Context, r reconciliation.Report) error {
	e.anomalyCount++
	e.lastAnomalyExpected = r.ExpectedCents
	e.lastAnomalyObserved = r.ObservedCents
	return e.anomalyErr
}

type sentinel string

func (s sentinel) Error() string { return string(s) }

const errBoom sentinel = "boom"
