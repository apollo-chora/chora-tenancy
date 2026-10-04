// Tests for the chora-payments PoolApplier adapter.
package manapool_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	pool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

// newTestPool returns a non-zero TenantManaPool with the given starting balance.
func newTestPool(t *testing.T, tenantID string, balance int64) *pool.TenantManaPool {
	t.Helper()
	p, err := pool.NewPool(tenantID, "gcid-test", pool.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	// Force the starting balance to the configured value (test-only seam —
	// the canonical TopUp method is the production seam).
	if balance > 0 {
		if err := p.Credit(balance); err != nil {
			t.Fatalf("seed credit: %v", err)
		}
	}
	return p
}

func TestNewPaymentsPoolApplier_RequiresStore(t *testing.T) {
	if _, err := manapool.NewPaymentsPoolApplier(nil); err == nil {
		t.Fatalf("expected error for nil store")
	}
}

func TestPaymentsPoolApplier_Credit_AddsUnits(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	p := newTestPool(t, "tenant-1", 0)
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("seed: %v", err)
	}
	a, err := manapool.NewPaymentsPoolApplier(repo)
	if err != nil {
		t.Fatalf("NewPaymentsPoolApplier: %v", err)
	}
	if err := a.Credit(context.Background(), "tenant-1", 500, "test"); err != nil {
		t.Fatalf("Credit: %v", err)
	}
	got, err := repo.GetByTenant(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.BalanceUnits != 500 {
		t.Errorf("expected 500, got %d", got.BalanceUnits)
	}
}

func TestPaymentsPoolApplier_Credit_RejectsBlankTenant(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	if err := a.Credit(context.Background(), "", 1, "r"); err == nil {
		t.Fatalf("expected error for blank tenant_id")
	}
}

func TestPaymentsPoolApplier_Credit_PropagatesPoolNotFound(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	err := a.Credit(context.Background(), "tenant-missing", 1, "r")
	if err == nil {
		t.Fatalf("expected error for missing pool")
	}
	if !errors.Is(err, pool.ErrPoolNotFound) && !strings.Contains(err.Error(), "not found") {
		t.Errorf("error did not wrap ErrPoolNotFound: %v", err)
	}
}

func TestPaymentsPoolApplier_Credit_PropagatesInvalidUnits(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	p := newTestPool(t, "tenant-2", 100)
	_ = repo.Save(context.Background(), p)
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	if err := a.Credit(context.Background(), "tenant-2", 0, "r"); err == nil {
		t.Fatalf("expected error for 0-unit credit")
	}
	if err := a.Credit(context.Background(), "tenant-2", -5, "r"); err == nil {
		t.Fatalf("expected error for negative-unit credit")
	}
}

func TestPaymentsPoolApplier_Debit_SubtractsUnits(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	p := newTestPool(t, "tenant-3", 1000)
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("seed: %v", err)
	}
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	if err := a.Debit(context.Background(), "tenant-3", 250, "refund"); err != nil {
		t.Fatalf("Debit: %v", err)
	}
	got, _ := repo.GetByTenant(context.Background(), "tenant-3")
	if got.BalanceUnits != 750 {
		t.Errorf("expected 750, got %d", got.BalanceUnits)
	}
}

func TestPaymentsPoolApplier_Debit_RejectsBlankTenant(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	if err := a.Debit(context.Background(), "", 1, "r"); err == nil {
		t.Fatalf("expected error for blank tenant_id")
	}
}

func TestPaymentsPoolApplier_Debit_PropagatesOverdraw(t *testing.T) {
	repo := manapool.NewInmemPoolRepo()
	p := newTestPool(t, "tenant-4", 100)
	_ = repo.Save(context.Background(), p)
	a, _ := manapool.NewPaymentsPoolApplier(repo)
	err := a.Debit(context.Background(), "tenant-4", 500, "refund")
	if err == nil {
		t.Fatalf("expected overdraw error")
	}
	if !errors.Is(err, pool.ErrInsufficientBalance) {
		t.Errorf("error did not wrap ErrInsufficientBalance: %v", err)
	}
}
