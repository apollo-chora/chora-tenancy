// Package billingpubsub_test — event-bus publisher + reconciliation emitter
// for the chora-tenancy billing-webhook seam. Ported from
// services/chora-billing-webhook/internal/adapter/pubsub as part of the
// M12.2 Batch-1 consolidation.
package billingpubsub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/billingpubsub"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
)

// recordingBus mirrors the OutboxCompatible interface and captures every
// publish for assertion.
type recordingBus struct {
	mu       sync.Mutex
	captured []capturedPublish
}

type capturedPublish struct {
	Topic    string
	Envelope cgcenvelope.Envelope
	Payload  []byte
}

func (b *recordingBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.captured = append(b.captured, capturedPublish{Topic: topic, Envelope: env, Payload: append([]byte(nil), payload...)})
	return nil
}

func (b *recordingBus) Captured() []capturedPublish {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]capturedPublish, len(b.captured))
	copy(out, b.captured)
	return out
}

func TestReconciliationEmitter_EmitsBothEventsWithIMDADimensions(t *testing.T) {
	t.Parallel()
	bus := &recordingBus{}
	em := billingpubsub.NewReconciliationEmitter(billingpubsub.EmitterConfig{
		Bus:           bus,
		SourceProject: "chora-489812",
	})
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	report := reconciliation.Report{
		Day:           day,
		ExpectedCents: 100_000,
		ObservedCents: 99_500,
		DeltaCents:    500,
		ToleranceBPS:  5,
		RunID:         "recon-20260508T030000Z",
	}
	if err := em.EmitAnomaly(context.Background(), report); err != nil {
		t.Fatalf("EmitAnomaly: %v", err)
	}
	if err := em.EmitCompleted(context.Background(), report); err != nil {
		t.Fatalf("EmitCompleted: %v", err)
	}
	captured := bus.Captured()
	if len(captured) != 2 {
		t.Fatalf("len(captured) = %d; want 2", len(captured))
	}
	if captured[0].Topic != reconciliation.AnomalyTopic {
		t.Errorf("first topic = %q; want %q", captured[0].Topic, reconciliation.AnomalyTopic)
	}
	if captured[0].Envelope.ChoraImdaDimension != "safety_and_robustness" {
		t.Errorf("anomaly IMDA dim = %q; want safety_and_robustness", captured[0].Envelope.ChoraImdaDimension)
	}
	if captured[1].Topic != reconciliation.CompletedTopic {
		t.Errorf("second topic = %q; want %q", captured[1].Topic, reconciliation.CompletedTopic)
	}
	if captured[1].Envelope.ChoraImdaDimension != "accountability" {
		t.Errorf("completed IMDA dim = %q; want accountability", captured[1].Envelope.ChoraImdaDimension)
	}
	if captured[1].Envelope.ImdaLifecycleStage != "post_deploy" {
		t.Errorf("completed lifecycle = %q; want post_deploy", captured[1].Envelope.ImdaLifecycleStage)
	}
}

func TestNewReconciliationEmitter_Defaults(t *testing.T) {
	t.Parallel()
	// Empty SourceService/SourceProject/Now fall back to defaults; the
	// publish still lands on the supplied bus.
	bus := eventbus.NewInMemoryBus()
	defer bus.Close()
	e := billingpubsub.NewReconciliationEmitter(billingpubsub.EmitterConfig{Bus: bus})
	if e == nil {
		t.Fatal("expected non-nil emitter")
	}
	if err := e.EmitCompleted(context.Background(), reconciliationReport()); err != nil {
		t.Fatalf("EmitCompleted with defaults: %v", err)
	}
}

func reconciliationReport() reconciliation.Report {
	return reconciliation.Report{
		RunID:         "run-1",
		Day:           time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		ExpectedCents: 100,
		ObservedCents: 95,
	}
}
