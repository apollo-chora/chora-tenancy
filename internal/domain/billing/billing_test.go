// Package billing_test holds the RED-phase TDD specs for the Billing
// sub-domain inside chora-tenancy.
//
// Aligned with:
//   - CLAUDE.md §1 — Stripe is the payment processor (here a STUB only)
//   - .claude/skills/secrets-and-env — no inline config; Stripe URL via env
//
// Billing contract:
//   - UsageAggregator collects active add-on subscriptions for a period and
//     produces UsageSummary{tenant_id, period_start, period_end, lines}.
//   - Invoice = sum of (subscription monthly_price_cents) for the period.
//   - Invoices are append-only — Save inserts; never updates.
package billing_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing"
)

const (
	tenantA = "01970000-0000-7000-8000-0000000000aa"
	tenantB = "01970000-0000-7000-8000-0000000000bb"
)

// -----------------------------------------------------------------------------
// UsageAggregator
// -----------------------------------------------------------------------------

func TestUsageAggregator_AggregatesAcrossSubscriptions(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	subs := []billing.SubscriptionUsage{
		{AddOnID: "tms", MonthlyPriceCents: 4900},
		{AddOnID: "cms", MonthlyPriceCents: 2900},
		{AddOnID: "campus-ops", MonthlyPriceCents: 9900},
	}
	summary, err := billing.AggregateUsage(tenantA, periodStart, periodEnd, subs)
	if err != nil {
		t.Fatalf("AggregateUsage: unexpected error: %v", err)
	}
	if summary.TotalCents != 17700 {
		t.Fatalf("expected total 17700 (4900+2900+9900), got %d", summary.TotalCents)
	}
	if len(summary.Lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(summary.Lines))
	}
	if summary.PeriodStart != periodStart {
		t.Fatalf("period_start mismatch")
	}
}

func TestUsageAggregator_HandlesZeroPriceBase(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	subs := []billing.SubscriptionUsage{
		{AddOnID: "base", MonthlyPriceCents: 0},
		{AddOnID: "tms", MonthlyPriceCents: 4900},
	}
	summary, err := billing.AggregateUsage(tenantA, periodStart, periodEnd, subs)
	if err != nil {
		t.Fatalf("AggregateUsage: unexpected error: %v", err)
	}
	if summary.TotalCents != 4900 {
		t.Fatalf("expected total 4900 (base=0 + tms=4900), got %d", summary.TotalCents)
	}
}

func TestUsageAggregator_RejectsInvertedPeriod(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if _, err := billing.AggregateUsage(tenantA, start, end, nil); err == nil {
		t.Fatalf("expected error for inverted period")
	}
}

func TestUsageAggregator_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if _, err := billing.AggregateUsage("", start, end, nil); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

// -----------------------------------------------------------------------------
// Invoice
// -----------------------------------------------------------------------------

func TestNewInvoice_FromUsageSummary(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	subs := []billing.SubscriptionUsage{
		{AddOnID: "tms", MonthlyPriceCents: 4900},
	}
	summary, _ := billing.AggregateUsage(tenantA, periodStart, periodEnd, subs)
	inv, err := billing.NewInvoiceFromSummary(summary)
	if err != nil {
		t.Fatalf("NewInvoiceFromSummary: unexpected error: %v", err)
	}
	if inv.ID == "" {
		t.Fatalf("expected non-empty invoice ID")
	}
	if inv.TotalCents != 4900 {
		t.Fatalf("expected total 4900, got %d", inv.TotalCents)
	}
	if inv.Status != billing.InvoiceStatusIssued {
		t.Fatalf("expected issued status, got %q", inv.Status)
	}
	if len(inv.Lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(inv.Lines))
	}
}

func TestInvoice_AttachStripeID(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	summary, _ := billing.AggregateUsage(tenantA, periodStart, periodEnd, nil)
	inv, _ := billing.NewInvoiceFromSummary(summary)
	inv.AttachStripeID("in_stub_1234")
	if inv.StripeInvoiceID != "in_stub_1234" {
		t.Fatalf("expected stripe id attached")
	}
}

// -----------------------------------------------------------------------------
// InvoiceRepo (in-memory)
// -----------------------------------------------------------------------------

func TestInvoiceRepo_SaveAndListByTenant(t *testing.T) {
	t.Parallel()
	repo := billing.NewInvoiceRepo()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, tenant := range []string{tenantA, tenantA, tenantB} {
		s, _ := billing.AggregateUsage(tenant, periodStart, periodEnd, nil)
		inv, _ := billing.NewInvoiceFromSummary(s)
		repo.Save(inv)
	}
	a := repo.ListByTenant(tenantA)
	if len(a) != 2 {
		t.Fatalf("expected 2 invoices for A, got %d", len(a))
	}
	b := repo.ListByTenant(tenantB)
	if len(b) != 1 {
		t.Fatalf("expected 1 invoice for B, got %d", len(b))
	}
}

func TestInvoiceRepo_SaveIsAppendOnly(t *testing.T) {
	t.Parallel()
	repo := billing.NewInvoiceRepo()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	s, _ := billing.AggregateUsage(tenantA, periodStart, periodEnd, nil)
	inv, _ := billing.NewInvoiceFromSummary(s)
	repo.Save(inv)
	// Try to save the SAME invoice ID again — append-only, no duplicate insert.
	repo.Save(inv)
	all := repo.ListByTenant(tenantA)
	if len(all) != 1 {
		t.Fatalf("expected append-only single record, got %d", len(all))
	}
}

func TestInvoiceRepo_GetByID(t *testing.T) {
	t.Parallel()
	repo := billing.NewInvoiceRepo()
	s, _ := billing.AggregateUsage(tenantA, time.Now(), time.Now().Add(24*time.Hour), nil)
	inv, _ := billing.NewInvoiceFromSummary(s)
	repo.Save(inv)
	got, ok := repo.Get(inv.ID)
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != inv.ID {
		t.Fatalf("ID mismatch")
	}
	if _, ok := repo.Get("no-such-invoice"); ok {
		t.Fatalf("expected miss")
	}
}

// -----------------------------------------------------------------------------
// PaymentMethod (Stripe stub) — separate from Stripe client
// -----------------------------------------------------------------------------

func TestPaymentMethod_New_DefaultsToActive(t *testing.T) {
	t.Parallel()
	pm, err := billing.NewPaymentMethod(tenantA, "pm_stub_1234")
	if err != nil {
		t.Fatalf("NewPaymentMethod: unexpected error: %v", err)
	}
	if pm.Status != billing.PaymentMethodStatusActive {
		t.Fatalf("expected active status, got %q", pm.Status)
	}
	if pm.StripeToken != "pm_stub_1234" {
		t.Fatalf("expected stripe token passthrough")
	}
}

func TestPaymentMethod_RejectsEmpty(t *testing.T) {
	t.Parallel()
	if _, err := billing.NewPaymentMethod("", "pm_x"); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
	if _, err := billing.NewPaymentMethod(tenantA, ""); err == nil {
		t.Fatalf("expected error for empty stripe token")
	}
}

func TestPaymentMethod_Detach_Idempotent(t *testing.T) {
	t.Parallel()
	pm, _ := billing.NewPaymentMethod(tenantA, "pm_x")
	pm.Detach()
	if pm.Status != billing.PaymentMethodStatusDetached {
		t.Fatalf("expected detached")
	}
	prev := pm.UpdatedAt
	pm.Detach()
	if !pm.UpdatedAt.Equal(prev) {
		t.Fatalf("Detach should be idempotent")
	}
}

func TestPaymentMethodRepo_SaveAndList(t *testing.T) {
	t.Parallel()
	repo := billing.NewPaymentMethodRepo()
	pm, _ := billing.NewPaymentMethod(tenantA, "pm_x")
	repo.Save(pm)
	all := repo.ListByTenant(tenantA)
	if len(all) != 1 {
		t.Fatalf("expected 1, got %d", len(all))
	}
}
