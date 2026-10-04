// Package inmem_test holds RED-phase TDD specs for the in-memory adapter.
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/inmem"
	domain "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

func TestTenantRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	r := inmem.NewTenantRepo()
	tt, _ := domain.NewTenant("Acme", false, "")
	_ = r.Save(context.Background(), tt)
	got, ok := r.Get(tt.ID)
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != tt.ID {
		t.Fatalf("ID mismatch")
	}
	if _, ok := r.Get("does-not-exist"); ok {
		t.Fatalf("expected miss")
	}
}

func TestTenantRepo_List_PaginatesAndExcludesClosed(t *testing.T) {
	t.Parallel()
	r := inmem.NewTenantRepo()
	for i := 0; i < 5; i++ {
		tt, _ := domain.NewTenant("T", false, "")
		_ = r.Save(context.Background(), tt)
	}
	closed, _ := domain.NewTenant("Closed Co", false, "")
	closed.Close()
	_ = r.Save(context.Background(), closed)

	items, total := r.List(0, 10)
	if total != 5 {
		t.Fatalf("expected total=5 (closed excluded), got %d", total)
	}
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	// Pagination
	page1, _ := r.List(0, 2)
	if len(page1) != 2 {
		t.Fatalf("expected 2 items on page1, got %d", len(page1))
	}
	page2, _ := r.List(2, 2)
	if len(page2) != 2 {
		t.Fatalf("expected 2 items on page2, got %d", len(page2))
	}
	pageOver, _ := r.List(100, 10)
	if len(pageOver) != 0 {
		t.Fatalf("expected 0 items past end, got %d", len(pageOver))
	}
}

func TestInvoiceRepo_SaveAndListByTenant(t *testing.T) {
	t.Parallel()
	r := inmem.NewInvoiceRepo()
	tt, _ := domain.NewTenant("Acme", false, "")
	may := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	jun := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	jul := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	inv1, _ := domain.GenerateInvoice(tt.ID, may, jun, nil)
	inv2, _ := domain.GenerateInvoice(tt.ID, jun, jul, nil)
	r.Save(inv1)
	r.Save(inv2)
	all := r.ListByTenant(tt.ID)
	if len(all) != 2 {
		t.Fatalf("expected 2 invoices, got %d", len(all))
	}
}

func TestPaymentMethodRepo_SaveAndListByTenant(t *testing.T) {
	t.Parallel()
	r := inmem.NewPaymentMethodRepo()
	tt, _ := domain.NewTenant("Acme", false, "")
	pm, _ := domain.NewPaymentMethod(tt.ID, "pm_x")
	r.Save(pm)
	all := r.ListByTenant(tt.ID)
	if len(all) != 1 {
		t.Fatalf("expected 1 payment method, got %d", len(all))
	}
}

func TestInvoiceRepo_Save_AppendOnly(t *testing.T) {
	t.Parallel()
	repo := inmem.NewInvoiceRepo()
	inv := &domain.Invoice{ID: "inv-1", TenantID: "t-1"}
	repo.Save(inv)
	repo.Save(inv) // same ID → no duplicate
	all := repo.ListByTenant("t-1")
	if len(all) != 1 {
		t.Fatalf("expected 1 invoice, got %d", len(all))
	}
}
