// billing_extra_test.go — branch coverage additions to billing_test.go:
// invoice state transitions, constructor guards, repo sorting paths.
package billing_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/billing"
)

func TestNewInvoiceFromSummary_Guards(t *testing.T) {
	t.Parallel()
	if _, err := billing.NewInvoiceFromSummary(nil); err == nil {
		t.Fatalf("expected error for nil summary")
	}
	s, _ := billing.AggregateUsage(tenantA, time.Now().Add(-time.Hour), time.Now(), nil)
	s.TenantID = ""
	if _, err := billing.NewInvoiceFromSummary(s); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

func TestInvoice_StateTransitions(t *testing.T) {
	t.Parallel()
	s, _ := billing.AggregateUsage(tenantA, time.Now().Add(-time.Hour), time.Now(), nil)
	inv, err := billing.NewInvoiceFromSummary(s)
	if err != nil {
		t.Fatalf("NewInvoiceFromSummary: %v", err)
	}
	inv.MarkPaid()
	if inv.Status != billing.InvoiceStatusPaid {
		t.Fatalf("expected paid, got %s", inv.Status)
	}
	inv.MarkFailed()
	if inv.Status != billing.InvoiceStatusFailed {
		t.Fatalf("expected failed, got %s", inv.Status)
	}
	inv.MarkRefunded()
	if inv.Status != billing.InvoiceStatusRefunded {
		t.Fatalf("expected refunded, got %s", inv.Status)
	}
}

func TestInvoiceRepo_SaveMultiple_SortedList(t *testing.T) {
	t.Parallel()
	repo := billing.NewInvoiceRepo()
	// Two invoices with different UUIDs sort stably by ID.
	for i := 0; i < 3; i++ {
		s, _ := billing.AggregateUsage(tenantA, time.Now().Add(-time.Hour), time.Now(), nil)
		inv, _ := billing.NewInvoiceFromSummary(s)
		repo.Save(inv)
	}
	all := repo.ListByTenant(tenantA)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID < all[i-1].ID {
			t.Fatalf("list not sorted: %s after %s", all[i].ID, all[i-1].ID)
		}
	}
}

func TestPaymentMethodRepo_MultipleSorted(t *testing.T) {
	t.Parallel()
	repo := billing.NewPaymentMethodRepo()
	for _, tok := range []string{"pm_a", "pm_b", "pm_c"} {
		pm, _ := billing.NewPaymentMethod(tenantA, tok)
		repo.Save(pm)
	}
	// Overwrite path (same ID re-saved) does not duplicate in byTenant.
	first, _ := billing.NewPaymentMethod(tenantA, "pm_x")
	repo.Save(first)
	repo.Save(first)
	all := repo.ListByTenant(tenantA)
	if len(all) != 4 {
		t.Fatalf("expected 4, got %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID < all[i-1].ID {
			t.Fatalf("list not sorted: %s after %s", all[i].ID, all[i-1].ID)
		}
	}
}
