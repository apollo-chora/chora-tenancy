// Targeted coverage tests for the ADR-164 PaymentsSubscriber — covers the
// validator-error paths + subscription name edge cases not exercised by the
// happy-path subscriber tests.
package events_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

func newCoverageSub() *events.PaymentsSubscriber {
	return events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: events.NewRecorder(),
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})
}

func TestPaymentsSubscriber_SubscriptionNameForTopic_EdgeCases(t *testing.T) {
	sub := newCoverageSub()
	cases := []struct {
		topic string
		want  string
	}{
		{topic: "", want: ""},
		{topic: "not.payments.thing.v1", want: ""},
		{topic: "chora.payments.invalid", want: ""},
		{topic: "chora.payments.aggregate.event.v1", want: "chora-tenancy-payments-aggregate-event"},
	}
	for _, tc := range cases {
		if got := sub.SubscriptionNameForTopic(tc.topic); got != tc.want {
			t.Errorf("SubscriptionNameForTopic(%q) = %q want %q", tc.topic, got, tc.want)
		}
	}
}

func TestPaymentsSubscriber_FamiliarEggRefunded_RejectsBlankFields(t *testing.T) {
	sub := newCoverageSub()
	ctx := context.Background()
	cases := []struct {
		name string
		in   events.FamiliarEggRefunded
	}{
		{name: "missing event_id", in: events.FamiliarEggRefunded{PurchaseID: "p", LearnerGCID: "g", TargetTenantID: "t"}},
		{name: "missing purchase_id", in: events.FamiliarEggRefunded{EventID: "e", LearnerGCID: "g", TargetTenantID: "t"}},
		{name: "missing gcid", in: events.FamiliarEggRefunded{EventID: "e", PurchaseID: "p", TargetTenantID: "t"}},
		{name: "missing tenant", in: events.FamiliarEggRefunded{EventID: "e", PurchaseID: "p", LearnerGCID: "g"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := sub.HandleFamiliarEggRefunded(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestPaymentsSubscriber_ManaTopUpPaymentCaptured_RejectsBlankFields(t *testing.T) {
	sub := newCoverageSub()
	ctx := context.Background()
	cases := []struct {
		name string
		in   events.TenantManaTopUpPaymentCaptured
	}{
		{name: "missing event_id", in: events.TenantManaTopUpPaymentCaptured{PurchaseID: "p", TargetTenantID: "t", ManaUnits: 1}},
		{name: "missing purchase_id", in: events.TenantManaTopUpPaymentCaptured{EventID: "e", TargetTenantID: "t", ManaUnits: 1}},
		{name: "missing tenant", in: events.TenantManaTopUpPaymentCaptured{EventID: "e", PurchaseID: "p", ManaUnits: 1}},
		{name: "zero mana_units", in: events.TenantManaTopUpPaymentCaptured{EventID: "e", PurchaseID: "p", TargetTenantID: "t", ManaUnits: 0}},
		{name: "negative mana_units", in: events.TenantManaTopUpPaymentCaptured{EventID: "e", PurchaseID: "p", TargetTenantID: "t", ManaUnits: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := sub.HandleManaTopUpPaymentCaptured(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestPaymentsSubscriber_ManaTopUpRefunded_RejectsBlankFields(t *testing.T) {
	sub := newCoverageSub()
	ctx := context.Background()
	cases := []struct {
		name string
		in   events.TenantManaTopUpRefunded
	}{
		{name: "missing event_id", in: events.TenantManaTopUpRefunded{PurchaseID: "p", TargetTenantID: "t", ManaUnitsToDebit: 1}},
		{name: "missing purchase_id", in: events.TenantManaTopUpRefunded{EventID: "e", TargetTenantID: "t", ManaUnitsToDebit: 1}},
		{name: "missing tenant", in: events.TenantManaTopUpRefunded{EventID: "e", PurchaseID: "p", ManaUnitsToDebit: 1}},
		{name: "zero debit", in: events.TenantManaTopUpRefunded{EventID: "e", PurchaseID: "p", TargetTenantID: "t", ManaUnitsToDebit: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := sub.HandleManaTopUpRefunded(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestPaymentsSubscriber_FamiliarEggExpired_RejectsBlankEvent(t *testing.T) {
	sub := newCoverageSub()
	in := events.FamiliarEggExpired{
		PurchaseID:     "p",
		LearnerGCID:    "g",
		TargetTenantID: "t",
		EggSKU:         "s",
	}
	if err := sub.HandleFamiliarEggExpired(context.Background(), in); err == nil {
		t.Fatalf("expected error for blank event_id")
	}
}

func TestPaymentsSubscriber_NilReceiver_FailsLoudly(t *testing.T) {
	var s *events.PaymentsSubscriber
	if err := s.HandleFamiliarEggPaymentCaptured(context.Background(), events.FamiliarEggPaymentCaptured{}); err == nil {
		t.Fatalf("expected error on nil subscriber")
	}
	if err := s.HandleFamiliarEggRefunded(context.Background(), events.FamiliarEggRefunded{}); err == nil {
		t.Fatalf("expected error on nil subscriber")
	}
	if err := s.HandleFamiliarEggExpired(context.Background(), events.FamiliarEggExpired{}); err == nil {
		t.Fatalf("expected error on nil subscriber")
	}
	if err := s.HandleManaTopUpPaymentCaptured(context.Background(), events.TenantManaTopUpPaymentCaptured{}); err == nil {
		t.Fatalf("expected error on nil subscriber")
	}
	if err := s.HandleManaTopUpRefunded(context.Background(), events.TenantManaTopUpRefunded{}); err == nil {
		t.Fatalf("expected error on nil subscriber")
	}
}
