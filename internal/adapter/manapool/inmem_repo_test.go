// Package manapool_test — adapter-layer tests for the in-memory pool +
// allocation repos that back the BE-USR-3 endpoints.
package manapool_test

import (
	"context"
	"testing"

	manapool "github.com/apollo-chora/chora-tenancy/internal/adapter/manapool"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

const (
	tenantA   = "01970000-0000-7000-8000-0000000000a1"
	tenantB   = "01970000-0000-7000-8000-0000000000a2"
	gcidL1    = "01970000-0000-7000-8000-0000000000c1"
	gcidL2    = "01970000-0000-7000-8000-0000000000c2"
	gcidAdmin = "01970000-0000-7000-8000-0000000000bb"
)

func TestInmemPoolRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemPoolRepo()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByTenant(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("GetByTenant: %v", err)
	}
	if got.PoolID != p.PoolID {
		t.Fatalf("expected same pool_id")
	}
}

func TestInmemPoolRepo_GetByTenant_NotFound(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemPoolRepo()
	if _, err := repo.GetByTenant(context.Background(), tenantA); err != pool.ErrPoolNotFound {
		t.Fatalf("expected ErrPoolNotFound, got %v", err)
	}
}

func TestInmemPoolRepo_Save_RejectsDuplicateTenant(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemPoolRepo()
	p1, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	if err := repo.Save(context.Background(), p1); err != nil {
		t.Fatalf("Save: %v", err)
	}
	p2, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	err := repo.Save(context.Background(), p2)
	if err != pool.ErrPoolAlreadyExists {
		t.Fatalf("expected ErrPoolAlreadyExists on second insert with same tenant, got %v", err)
	}
}

func TestInmemPoolRepo_UpdateExisting_AllowsResave(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemPoolRepo()
	p, _ := pool.NewPool(tenantA, gcidAdmin, pool.ManualPolicy{})
	_ = repo.Save(context.Background(), p)
	// Update + resave (same pool_id) is fine.
	_ = p.SetMonthlyTopupUnits(500)
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save same pool: %v", err)
	}
	got, _ := repo.GetByTenant(context.Background(), tenantA)
	if got.MonthlyTopupUnits != 500 {
		t.Fatalf("expected updated monthly_topup")
	}
}

// -----------------------------------------------------------------------------
// AllocationRepo
// -----------------------------------------------------------------------------

func TestInmemAllocationRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemAllocationRepo()
	a, _ := allocation.New(allocation.NewParams{
		TenantID: tenantA, GCID: gcidL1, SourcePoolID: "pool-1",
		Units: 250, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.Get(context.Background(), a.AllocationID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AllocationID != a.AllocationID {
		t.Fatalf("ID mismatch")
	}
}

func TestInmemAllocationRepo_ListByTenant_FiltersAndPaginates(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemAllocationRepo()
	for i := 0; i < 5; i++ {
		a, _ := allocation.New(allocation.NewParams{
			TenantID: tenantA, GCID: gcidL1, SourcePoolID: "pool-1",
			Units: 100, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		})
		_ = repo.Save(context.Background(), a)
	}
	// Different tenant should not appear.
	other, _ := allocation.New(allocation.NewParams{
		TenantID: tenantB, GCID: gcidL2, SourcePoolID: "pool-2",
		Units: 100, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
	})
	_ = repo.Save(context.Background(), other)

	got, total, err := repo.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 page items, got %d", len(got))
	}
}

func TestInmemAllocationRepo_ListByTenant_FiltersByGCID(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemAllocationRepo()
	for _, g := range []string{gcidL1, gcidL2, gcidL1} {
		a, _ := allocation.New(allocation.NewParams{
			TenantID: tenantA, GCID: g, SourcePoolID: "pool-1",
			Units: 100, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		})
		_ = repo.Save(context.Background(), a)
	}
	got, total, _ := repo.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{GCID: gcidL1, Limit: 50})
	if total != 2 {
		t.Fatalf("expected total=2 for gcidL1, got %d", total)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	for _, a := range got {
		if a.GCID != gcidL1 {
			t.Fatalf("filter mismatch")
		}
	}
}

func TestInmemAllocationRepo_ListByTenant_FiltersByStatus(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemAllocationRepo()
	for i := 0; i < 3; i++ {
		a, _ := allocation.New(allocation.NewParams{
			TenantID: tenantA, GCID: gcidL1, SourcePoolID: "pool-1",
			Units: 100, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		})
		if i == 0 {
			_, _ = a.Revoke("test", gcidAdmin)
		}
		_ = repo.Save(context.Background(), a)
	}
	got, total, _ := repo.ListByTenant(context.Background(), tenantA, manapool.ListAllocationsFilter{Status: allocation.StatusRevoked, Limit: 50})
	if total != 1 || len(got) != 1 {
		t.Fatalf("expected 1 revoked, got total=%d len=%d", total, len(got))
	}
}

func TestInmemAllocationRepo_Get_NotFound(t *testing.T) {
	t.Parallel()
	repo := manapool.NewInmemAllocationRepo()
	if _, err := repo.Get(context.Background(), "nope"); err != manapool.ErrAllocationNotFound {
		t.Fatalf("expected ErrAllocationNotFound, got %v", err)
	}
}

// -----------------------------------------------------------------------------
func TestInmemAllocationRepo_GetByIdempotencyKey_BlankKey(t *testing.T) {
	repo := manapool.NewInmemAllocationRepo()
	if _, err := repo.GetByIdempotencyKey(context.Background(), ""); err == nil {
		t.Fatal("expected not-found for blank key")
	}
}
