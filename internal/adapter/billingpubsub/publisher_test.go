// Package billingpubsub_test — event-bus publisher + reconciliation emitter
// for the chora-tenancy billing-webhook seam. Ported from
// services/chora-billing-webhook/internal/adapter/pubsub as part of the
// M12.2 Batch-1 consolidation.
package billingpubsub_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/billingpubsub"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/webhook"
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

func TestWebhookPublisher_PublishesTenantPaymentCaptured(t *testing.T) {
	t.Parallel()
	bus := &recordingBus{}
	pub := billingpubsub.NewWebhookPublisher(billingpubsub.EmitterConfig{
		Bus:           bus,
		SourceProject: "chora-489812",
	})
	cls := webhook.Classification{
		Topic:          "chora.tenancy.payment.captured.v1",
		IMDADimension:  "accountability",
		TenantID:       "tnt_acme",
		IdempotencyKey: "stripe:payment.captured:evt_test",
		Payload: &webhook.Payload{
			StripeEventID: "evt_test",
			AmountCents:   199_900,
			Currency:      "USD",
			OccurredAt:    time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		},
	}
	if err := pub.Publish(context.Background(), cls); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	captured := bus.Captured()
	if len(captured) != 1 {
		t.Fatalf("len(captured) = %d; want 1", len(captured))
	}
	got := captured[0]
	if got.Topic != cls.Topic {
		t.Errorf("Topic = %q; want %q", got.Topic, cls.Topic)
	}
	if got.Envelope.TenantID != "tnt_acme" {
		t.Errorf("Envelope.TenantID = %q; want tnt_acme", got.Envelope.TenantID)
	}
	if got.Envelope.SourceService != billingpubsub.DefaultSourceService {
		t.Errorf("Envelope.SourceService = %q; want %q", got.Envelope.SourceService, billingpubsub.DefaultSourceService)
	}
	if got.Envelope.ChoraImdaDimension != "accountability" {
		t.Errorf("Envelope.ChoraImdaDimension = %q; want accountability", got.Envelope.ChoraImdaDimension)
	}
	if got.Envelope.ImdaLifecycleStage != "runtime" {
		t.Errorf("Envelope.ImdaLifecycleStage = %q; want runtime", got.Envelope.ImdaLifecycleStage)
	}
	if got.Envelope.IdempotencyKey != cls.IdempotencyKey {
		t.Errorf("Envelope.IdempotencyKey = %q; want %q", got.Envelope.IdempotencyKey, cls.IdempotencyKey)
	}
	var p webhook.Payload
	if err := json.Unmarshal(got.Payload, &p); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if p.StripeEventID != "evt_test" || p.AmountCents != 199_900 {
		t.Errorf("payload not round-trip: %+v", p)
	}
}

func TestWebhookPublisher_PublishesUserScopedEnvelope(t *testing.T) {
	t.Parallel()
	bus := &recordingBus{}
	pub := billingpubsub.NewWebhookPublisher(billingpubsub.EmitterConfig{Bus: bus})
	cls := webhook.Classification{
		Topic:          "chora.identity.payment.captured.v1",
		IMDADimension:  "accountability",
		TenantID:       "platform",
		GCID:           "0197a-user",
		IdempotencyKey: "stripe:payment.captured:evt_user_1",
		Payload: &webhook.Payload{
			StripeEventID: "evt_user_1",
			AmountCents:   999,
			Currency:      "USD",
			OccurredAt:    time.Now().UTC(),
		},
	}
	if err := pub.Publish(context.Background(), cls); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	captured := bus.Captured()
	if len(captured) != 1 {
		t.Fatalf("len(captured) = %d; want 1", len(captured))
	}
	got := captured[0]
	if got.Envelope.TenantID != "platform" {
		t.Errorf("user-scoped TenantID = %q; want platform", got.Envelope.TenantID)
	}
	if got.Envelope.GCID != "0197a-user" {
		t.Errorf("user-scoped GCID = %q; want 0197a-user", got.Envelope.GCID)
	}
}

func TestWebhookPublisher_RejectsEmptyTopic(t *testing.T) {
	t.Parallel()
	pub := billingpubsub.NewWebhookPublisher(billingpubsub.EmitterConfig{Bus: &recordingBus{}})
	if err := pub.Publish(context.Background(), webhook.Classification{Payload: &webhook.Payload{}}); err == nil {
		t.Fatalf("Publish on empty topic must error")
	}
}

func TestWebhookPublisher_RejectsNilPayload(t *testing.T) {
	t.Parallel()
	pub := billingpubsub.NewWebhookPublisher(billingpubsub.EmitterConfig{Bus: &recordingBus{}})
	if err := pub.Publish(context.Background(), webhook.Classification{
		Topic: "chora.tenancy.payment.captured.v1",
	}); err == nil {
		t.Fatalf("Publish on nil payload must error")
	}
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

func TestRoundtripsThroughInMemoryBus(t *testing.T) {
	t.Parallel()
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()

	received := make(chan eventbus.Message, 1)
	cancel, err := bus.Subscribe(context.Background(), "chora.tenancy.payment.captured.v1", func(_ context.Context, m eventbus.Message) error {
		received <- m
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	pub := billingpubsub.NewWebhookPublisher(billingpubsub.EmitterConfig{Bus: bus})
	cls := webhook.Classification{
		Topic:          "chora.tenancy.payment.captured.v1",
		IMDADimension:  "accountability",
		TenantID:       "tnt_acme",
		IdempotencyKey: "stripe:payment.captured:evt_real",
		Payload: &webhook.Payload{
			StripeEventID: "evt_real",
			AmountCents:   100,
			Currency:      "USD",
			OccurredAt:    time.Now().UTC(),
		},
	}
	if err := pub.Publish(context.Background(), cls); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case m := <-received:
		if m.Envelope.TenantID != "tnt_acme" {
			t.Errorf("received TenantID = %q; want tnt_acme", m.Envelope.TenantID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive the published message")
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
