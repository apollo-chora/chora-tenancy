package billingpubsub_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/billingpubsub"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/webhook"
)

func TestEventLogReplayFeeder_Record(t *testing.T) {
	t.Parallel()
	feeder := billingpubsub.NewEventLogReplayFeeder()
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	feeder.Record(day, 100_000)
	feeder.Record(day, 50_000)
	feeder.Record(day.AddDate(0, 0, 1), 999)

	got, err := feeder.SumCapturedCentsForDay(context.Background(), day)
	if err != nil {
		t.Fatalf("SumCapturedCentsForDay: %v", err)
	}
	if got != 150_000 {
		t.Errorf("day total = %d; want 150000", got)
	}

	got, err = feeder.SumCapturedCentsForDay(context.Background(), day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("SumCapturedCentsForDay: %v", err)
	}
	if got != 999 {
		t.Errorf("next-day total = %d; want 999", got)
	}

	days := feeder.Days()
	if len(days) != 2 {
		t.Errorf("Days() len=%d; want 2", len(days))
	}
}

func TestEventLogReplayFeeder_HandlesPubSubMessage(t *testing.T) {
	t.Parallel()
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	feeder := billingpubsub.NewEventLogReplayFeeder()
	defer feeder.Close()
	if err := feeder.Subscribe(context.Background(), bus); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	day := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	payload, _ := json.Marshal(webhook.Payload{
		StripeEventID: "evt_pubsub_1",
		AmountCents:   199_900,
		Currency:      "USD",
		OccurredAt:    day,
	})

	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     "chora.tenancy.payment.captured.v1",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: billingpubsub.DefaultSourceService,
	})
	if err := bus.Publish(context.Background(), "chora.tenancy.payment.captured.v1", env, payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got, err := feeder.SumCapturedCentsForDay(context.Background(), day)
	if err != nil {
		t.Fatalf("SumCapturedCentsForDay: %v", err)
	}
	if got != 199_900 {
		t.Errorf("day total = %d; want 199900", got)
	}
}

func TestEventLogReplayFeeder_IgnoresMalformedPayload(t *testing.T) {
	t.Parallel()
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	feeder := billingpubsub.NewEventLogReplayFeeder()
	defer feeder.Close()
	_ = feeder.Subscribe(context.Background(), bus)

	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     "chora.identity.payment.captured.v1",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: billingpubsub.DefaultSourceService,
	})
	if err := bus.Publish(context.Background(), "chora.identity.payment.captured.v1", env, []byte("garbage")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got, _ := feeder.SumCapturedCentsForDay(context.Background(), time.Now().UTC()); got != 0 {
		t.Errorf("malformed payload should NOT be accumulated, got %d", got)
	}
}

func TestEventLogReplayFeeder_IgnoresZeroAmountAndZeroTime(t *testing.T) {
	t.Parallel()
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	feeder := billingpubsub.NewEventLogReplayFeeder()
	defer feeder.Close()
	_ = feeder.Subscribe(context.Background(), bus)

	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     "chora.identity.payment.captured.v1",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: billingpubsub.DefaultSourceService,
	})
	payload, _ := json.Marshal(webhook.Payload{StripeEventID: "evt_zero"})
	if err := bus.Publish(context.Background(), "chora.identity.payment.captured.v1", env, payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got, _ := feeder.SumCapturedCentsForDay(context.Background(), time.Now().UTC()); got != 0 {
		t.Errorf("zero-amount/time payload must NOT be accumulated; got %d", got)
	}
}
