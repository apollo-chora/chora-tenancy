// Package stripestub_test — tests for the Stripe Subscription Schedule
// adapter introduced in S6.2 (A-Tenant-Lifecycle). See subscription.go.
//
// The schedule adapter models phase transitions (current_phase → next_phase)
// as the contract that the H+ preview-tier-change + change-tier endpoints
// consume. Real Stripe wires to subscription_schedules.create with phases.
package stripestub_test

import (
	"context"
	"strings"
	"testing"
	"time"

	stripestub "github.com/apollo-chora/chora-tenancy/internal/adapter/stripe"
)

func newScheduleClient(t *testing.T) *stripestub.Client {
	t.Helper()
	return stripestub.New(stripestub.Config{APIURL: "https://stripe.invalid/v1"})
}

// ---------------------------------------------------------------------------
// CreateSubscriptionSchedule — happy path
// ---------------------------------------------------------------------------

func TestCreateSubscriptionSchedule_ReturnsSchedID(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	cycleEnd := time.Now().UTC().AddDate(0, 0, 14)
	res, err := c.CreateSubscriptionSchedule(context.Background(),
		stripestub.SubscriptionScheduleInput{
			TenantID:          "tenant-1",
			CustomerID:        "cus_mock_1",
			SubscriptionID:    "sub_mock_1",
			FromPriceID:       "price_pro",
			ToPriceID:         "price_starter",
			ProrationBehavior: "create_prorations",
			DeferToCycleEnd:   true,
			CycleEndAt:        cycleEnd,
		})
	if err != nil {
		t.Fatalf("CreateSubscriptionSchedule: %v", err)
	}
	if !strings.HasPrefix(res.ScheduleID, "sched_") {
		t.Fatalf("expected sched_ prefix, got %q", res.ScheduleID)
	}
	if len(res.Phases) != 2 {
		t.Fatalf("expected 2 phases (current + next), got %d", len(res.Phases))
	}
	if res.Phases[0].PriceID != "price_pro" {
		t.Fatalf("expected phase[0].price_id=price_pro, got %q", res.Phases[0].PriceID)
	}
	if res.Phases[1].PriceID != "price_starter" {
		t.Fatalf("expected phase[1].price_id=price_starter, got %q", res.Phases[1].PriceID)
	}
	// Phase 1 must start at the cycle end (deferred).
	if !res.Phases[1].StartDate.Equal(cycleEnd) {
		t.Fatalf("expected phase[1].start_date=%v, got %v", cycleEnd, res.Phases[1].StartDate)
	}
}

// ---------------------------------------------------------------------------
// CreateSubscriptionSchedule — immediate (no defer)
// ---------------------------------------------------------------------------

func TestCreateSubscriptionSchedule_ImmediateNoDeferToCycleEnd(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	res, err := c.CreateSubscriptionSchedule(context.Background(),
		stripestub.SubscriptionScheduleInput{
			TenantID:          "tenant-1",
			CustomerID:        "cus_mock_1",
			SubscriptionID:    "sub_mock_1",
			FromPriceID:       "price_starter",
			ToPriceID:         "price_pro",
			ProrationBehavior: "create_prorations",
			DeferToCycleEnd:   false,
		})
	if err != nil {
		t.Fatalf("CreateSubscriptionSchedule: %v", err)
	}
	if len(res.Phases) != 1 {
		t.Fatalf("expected 1 phase (immediate transition), got %d", len(res.Phases))
	}
	if res.Phases[0].PriceID != "price_pro" {
		t.Fatalf("expected phase[0].price_id=price_pro (the new tier), got %q", res.Phases[0].PriceID)
	}
}

// ---------------------------------------------------------------------------
// CreateSubscriptionSchedule — validation
// ---------------------------------------------------------------------------

func TestCreateSubscriptionSchedule_RejectsEmptyAPIURL(t *testing.T) {
	t.Parallel()
	c := stripestub.New(stripestub.Config{APIURL: ""})
	_, err := c.CreateSubscriptionSchedule(context.Background(),
		stripestub.SubscriptionScheduleInput{
			TenantID: "t-1", CustomerID: "cus_1", SubscriptionID: "sub_1",
			FromPriceID: "p1", ToPriceID: "p2", ProrationBehavior: "none",
		})
	if err == nil {
		t.Fatalf("expected error when STRIPE_API_URL absent")
	}
}

func TestCreateSubscriptionSchedule_RejectsEmptyArguments(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	cases := map[string]stripestub.SubscriptionScheduleInput{
		"missing_tenant":         {CustomerID: "cus", SubscriptionID: "sub", FromPriceID: "p1", ToPriceID: "p2"},
		"missing_customer":       {TenantID: "t", SubscriptionID: "sub", FromPriceID: "p1", ToPriceID: "p2"},
		"missing_subscription":   {TenantID: "t", CustomerID: "cus", FromPriceID: "p1", ToPriceID: "p2"},
		"missing_from_price":     {TenantID: "t", CustomerID: "cus", SubscriptionID: "sub", ToPriceID: "p2"},
		"missing_to_price":       {TenantID: "t", CustomerID: "cus", SubscriptionID: "sub", FromPriceID: "p1"},
		"unknown_proration_mode": {TenantID: "t", CustomerID: "cus", SubscriptionID: "sub", FromPriceID: "p1", ToPriceID: "p2", ProrationBehavior: "weird"},
	}
	for name, in := range cases {
		if _, err := c.CreateSubscriptionSchedule(context.Background(), in); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

// ---------------------------------------------------------------------------
// CreateSubscriptionSchedule — idempotency on (tenant, subscription)
// ---------------------------------------------------------------------------

func TestCreateSubscriptionSchedule_IdempotentByTenantSubscription(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	in := stripestub.SubscriptionScheduleInput{
		TenantID: "tenant-1", CustomerID: "cus_1", SubscriptionID: "sub_1",
		FromPriceID: "p1", ToPriceID: "p2",
		ProrationBehavior: "create_prorations", DeferToCycleEnd: true,
		CycleEndAt: time.Now().UTC().AddDate(0, 0, 7),
	}
	first, err := c.CreateSubscriptionSchedule(context.Background(), in)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := c.CreateSubscriptionSchedule(context.Background(), in)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.ScheduleID != second.ScheduleID {
		t.Fatalf("expected idempotent schedule_id, got %s != %s", first.ScheduleID, second.ScheduleID)
	}
}

// ---------------------------------------------------------------------------
// PreviewSubscriptionScheduleChange — preview-only (no state mutation)
// ---------------------------------------------------------------------------

func TestPreviewSubscriptionScheduleChange_DoesNotPersist(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	preview, err := c.PreviewSubscriptionScheduleChange(context.Background(),
		stripestub.SubscriptionScheduleInput{
			TenantID: "tenant-1", CustomerID: "cus_1", SubscriptionID: "sub_1",
			FromPriceID: "price_starter", ToPriceID: "price_pro",
			ProrationBehavior: "create_prorations",
		})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	// Preview returns a schedule_id that is empty (preview only).
	if preview.ScheduleID != "" {
		t.Fatalf("preview must NOT return a schedule_id; got %q", preview.ScheduleID)
	}
	// Phases must be populated.
	if len(preview.Phases) == 0 {
		t.Fatalf("preview must return phases for UI rendering")
	}
}

func TestPreviewSubscriptionScheduleChange_DeferredDefaultCycleEnd(t *testing.T) {
	t.Parallel()
	c := newScheduleClient(t)
	preview, err := c.PreviewSubscriptionScheduleChange(context.Background(),
		stripestub.SubscriptionScheduleInput{
			TenantID: "tenant-1", CustomerID: "cus_1", SubscriptionID: "sub_1",
			FromPriceID: "price_starter", ToPriceID: "price_pro",
			ProrationBehavior: "none", DeferToCycleEnd: true, // zero CycleEndAt → +30d default
		})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(preview.Phases) != 2 {
		t.Fatalf("deferred preview should have 2 phases, got %d", len(preview.Phases))
	}
	if preview.Phases[0].PriceID != "price_starter" || preview.Phases[1].PriceID != "price_pro" {
		t.Fatalf("unexpected deferred phases %+v", preview.Phases)
	}
	// Unknown proration behaviour rejected.
	if _, err := c.PreviewSubscriptionScheduleChange(context.Background(),
		stripestub.SubscriptionScheduleInput{TenantID: "t", CustomerID: "c", SubscriptionID: "s",
			FromPriceID: "a", ToPriceID: "b", ProrationBehavior: "bogus"}); err == nil {
		t.Fatal("expected invalid proration error")
	}
}
