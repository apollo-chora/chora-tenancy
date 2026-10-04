// Package billinginmem provides in-memory adapter doubles for tests + local
// dev of the billing-webhook + reconciliation pipelines.
//
// Includes:
//
//   - InMemoryEmitter — captures reconciliation events the way the
//     production Cloud Pub/Sub emitter would.
//   - InMemoryStripeFeeder — canned Stripe captured-cents per day.
//   - InMemoryEventLogFeeder — canned chora event-log captured-cents per day.
//
// Ported from services/chora-billing-webhook/internal/adapter/inmem as part
// of the M12.2 Batch-1 consolidation.
//
// Package name `billinginmem` (NOT `inmem`) avoids colliding with the
// existing chora-tenancy `inmem` package which owns Tenant + Invoice +
// PaymentMethod repos.
package billinginmem

import (
	"context"
	"sync"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
)

// InMemoryStripeFeeder maps a date (YYYYMMDD UTC) to a captured-cents total.
type InMemoryStripeFeeder struct {
	mu      sync.RWMutex
	totals  map[string]int64
	failErr error
}

// NewInMemoryStripeFeeder returns an empty feeder.
func NewInMemoryStripeFeeder() *InMemoryStripeFeeder {
	return &InMemoryStripeFeeder{totals: map[string]int64{}}
}

// Set seeds the feeder for the supplied day.
func (f *InMemoryStripeFeeder) Set(day time.Time, cents int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.totals[dayKey(day)] = cents
}

// FailWith makes future fetches return the supplied error.
func (f *InMemoryStripeFeeder) FailWith(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failErr = err
}

// FetchCapturedCentsForDay implements reconciliation.StripeFeeder.
func (f *InMemoryStripeFeeder) FetchCapturedCentsForDay(_ context.Context, day time.Time) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failErr != nil {
		return 0, f.failErr
	}
	return f.totals[dayKey(day)], nil
}

// InMemoryEventLogFeeder mirrors InMemoryStripeFeeder for the event-log side.
type InMemoryEventLogFeeder struct {
	mu      sync.RWMutex
	totals  map[string]int64
	failErr error
}

// NewInMemoryEventLogFeeder returns an empty feeder.
func NewInMemoryEventLogFeeder() *InMemoryEventLogFeeder {
	return &InMemoryEventLogFeeder{totals: map[string]int64{}}
}

// Set seeds the feeder for the supplied day.
func (f *InMemoryEventLogFeeder) Set(day time.Time, cents int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.totals[dayKey(day)] = cents
}

// FailWith makes future fetches return the supplied error.
func (f *InMemoryEventLogFeeder) FailWith(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failErr = err
}

// SumCapturedCentsForDay implements reconciliation.EventLogFeeder.
func (f *InMemoryEventLogFeeder) SumCapturedCentsForDay(_ context.Context, day time.Time) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failErr != nil {
		return 0, f.failErr
	}
	return f.totals[dayKey(day)], nil
}

// InMemoryEmitter records reconciliation events for assertion.
type InMemoryEmitter struct {
	mu        sync.Mutex
	completed []reconciliation.Report
	anomaly   []reconciliation.Report
}

// NewInMemoryEmitter returns a fresh recorder.
func NewInMemoryEmitter() *InMemoryEmitter {
	return &InMemoryEmitter{}
}

// EmitCompleted captures a completion event.
func (e *InMemoryEmitter) EmitCompleted(_ context.Context, r reconciliation.Report) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.completed = append(e.completed, r)
	return nil
}

// EmitAnomaly captures an anomaly event.
func (e *InMemoryEmitter) EmitAnomaly(_ context.Context, r reconciliation.Report) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.anomaly = append(e.anomaly, r)
	return nil
}

// Completed returns a snapshot of captured completion events.
func (e *InMemoryEmitter) Completed() []reconciliation.Report {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]reconciliation.Report, len(e.completed))
	copy(out, e.completed)
	return out
}

// Anomaly returns a snapshot of captured anomaly events.
func (e *InMemoryEmitter) Anomaly() []reconciliation.Report {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]reconciliation.Report, len(e.anomaly))
	copy(out, e.anomaly)
	return out
}

// dayKey is the YYYYMMDD UTC bucket.
func dayKey(t time.Time) string { return t.UTC().Format("20060102") }

// Compile-time port checks.
var (
	_ reconciliation.StripeFeeder   = (*InMemoryStripeFeeder)(nil)
	_ reconciliation.EventLogFeeder = (*InMemoryEventLogFeeder)(nil)
	_ reconciliation.Emitter        = (*InMemoryEmitter)(nil)
)
