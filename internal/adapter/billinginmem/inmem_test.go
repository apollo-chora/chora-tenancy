// Package billinginmem_test — in-memory adapter doubles for billing-webhook
// + reconciliation flows. Ported from
// services/chora-billing-webhook/internal/adapter/inmem as part of the
// M12.2 Batch-1 consolidation.
package billinginmem_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/billinginmem"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/reconciliation"
)

func TestInMemoryStripeFeeder_RoundTrip(t *testing.T) {
	t.Parallel()
	feeder := billinginmem.NewInMemoryStripeFeeder()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	feeder.Set(day, 199_900)

	got, err := feeder.FetchCapturedCentsForDay(context.Background(), day)
	if err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
	if got != 199_900 {
		t.Errorf("got=%d want=199900", got)
	}

	otherDay := day.AddDate(0, 0, 1)
	if got, _ := feeder.FetchCapturedCentsForDay(context.Background(), otherDay); got != 0 {
		t.Errorf("other-day fetch got=%d want=0", got)
	}
}

func TestInMemoryStripeFeeder_FailWith(t *testing.T) {
	t.Parallel()
	feeder := billinginmem.NewInMemoryStripeFeeder()
	feeder.FailWith(errors.New("stripe down"))
	if _, err := feeder.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("expected error after FailWith")
	}
}

func TestInMemoryEventLogFeeder_RoundTrip(t *testing.T) {
	t.Parallel()
	feeder := billinginmem.NewInMemoryEventLogFeeder()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	feeder.Set(day, 199_500)

	got, err := feeder.SumCapturedCentsForDay(context.Background(), day)
	if err != nil {
		t.Fatalf("SumCapturedCentsForDay: %v", err)
	}
	if got != 199_500 {
		t.Errorf("got=%d want=199500", got)
	}
}

func TestInMemoryEventLogFeeder_FailWith(t *testing.T) {
	t.Parallel()
	feeder := billinginmem.NewInMemoryEventLogFeeder()
	feeder.FailWith(errors.New("pubsub down"))
	if _, err := feeder.SumCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatalf("expected error after FailWith")
	}
}

func TestInMemoryEmitter_RecordsBothCategories(t *testing.T) {
	t.Parallel()
	em := billinginmem.NewInMemoryEmitter()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)

	clean := reconciliation.Report{Day: day, ExpectedCents: 100_000, ObservedCents: 100_000}
	if err := em.EmitCompleted(context.Background(), clean); err != nil {
		t.Fatalf("EmitCompleted: %v", err)
	}
	drift := reconciliation.Report{Day: day, ExpectedCents: 100_000, ObservedCents: 99_500, DeltaCents: 500, ToleranceBPS: 5}
	if err := em.EmitAnomaly(context.Background(), drift); err != nil {
		t.Fatalf("EmitAnomaly: %v", err)
	}

	if got := em.Completed(); len(got) != 1 {
		t.Errorf("Completed snapshot len=%d want=1", len(got))
	}
	if got := em.Anomaly(); len(got) != 1 {
		t.Errorf("Anomaly snapshot len=%d want=1", len(got))
	}
}
