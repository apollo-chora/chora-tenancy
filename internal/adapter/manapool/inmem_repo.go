// Package manapool is the adapter package for BE-USR-3 (Wave 3 of the
// per-user economy / Q2=v1 tenant subsidy track). It exposes:
//
//   - In-memory repositories for TenantManaPool + TenantManaAllocation
//
// All package-level dependencies (Stripe URL, event-bus project) are sourced
// from environment variables — no inline config (per
// `feedback_no_inline_config`).
package manapool

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// ServiceName is the canonical service identifier used in the event
// envelope `source_service` field.
const ServiceName = "chora-tenancy"

// ErrAllocationNotFound is returned by InmemAllocationRepo.Get when the id
// is not in the store.
var ErrAllocationNotFound = errors.New("allocation not found")

// -----------------------------------------------------------------------------
// InmemPoolRepo — implements pool.PoolStore
// -----------------------------------------------------------------------------

// InmemPoolRepo is an in-memory store keyed by tenant_id. Production
// implementations (M12+) will swap in a Postgres adapter against
// chora_tenancy.tenant_mana_pools via PgBouncer; the domain layer is
// unchanged.
type InmemPoolRepo struct {
	mu       sync.RWMutex
	byTenant map[string]*pool.TenantManaPool
}

// NewInmemPoolRepo returns an empty repo.
func NewInmemPoolRepo() *InmemPoolRepo {
	return &InmemPoolRepo{byTenant: make(map[string]*pool.TenantManaPool)}
}

// Save inserts or updates the pool. Returns ErrPoolAlreadyExists when a
// DIFFERENT pool already exists for the tenant (idempotent on same
// pool_id).
func (r *InmemPoolRepo) Save(_ context.Context, p *pool.TenantManaPool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byTenant[p.TenantID]; ok {
		if existing.PoolID != p.PoolID {
			return pool.ErrPoolAlreadyExists
		}
	}
	r.byTenant[p.TenantID] = p
	return nil
}

// GetByTenant returns the pool for the tenant or ErrPoolNotFound.
func (r *InmemPoolRepo) GetByTenant(_ context.Context, tenantID string) (*pool.TenantManaPool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byTenant[tenantID]
	if !ok {
		return nil, pool.ErrPoolNotFound
	}
	return p, nil
}

// -----------------------------------------------------------------------------
// InmemAllocationRepo
// -----------------------------------------------------------------------------

// ListAllocationsFilter is the filter applied by ListByTenant.
type ListAllocationsFilter struct {
	GCID   string
	Status allocation.Status
	Cursor string
	Limit  int
}

// InmemAllocationRepo is an in-memory store keyed by allocation_id.
type InmemAllocationRepo struct {
	mu       sync.RWMutex
	byID     map[string]*allocation.TenantManaAllocation
	byTenant map[string][]string // tenant_id -> ordered allocation_ids
}

// NewInmemAllocationRepo returns an empty repo.
func NewInmemAllocationRepo() *InmemAllocationRepo {
	return &InmemAllocationRepo{
		byID:     make(map[string]*allocation.TenantManaAllocation),
		byTenant: make(map[string][]string),
	}
}

// Save inserts or upserts an allocation.
func (r *InmemAllocationRepo) Save(_ context.Context, a *allocation.TenantManaAllocation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[a.AllocationID]; !ok {
		r.byTenant[a.TenantID] = append(r.byTenant[a.TenantID], a.AllocationID)
	}
	r.byID[a.AllocationID] = a
	return nil
}

// Get returns an allocation by id.
func (r *InmemAllocationRepo) Get(_ context.Context, id string) (*allocation.TenantManaAllocation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.byID[id]
	if !ok {
		return nil, ErrAllocationNotFound
	}
	return a, nil
}

// GetByIdempotencyKey returns an allocation by its IdempotencyKey field.
// Mirrors the 0009 partial UNIQUE INDEX semantic on the pg side: only
// non-empty keys are de-duplicated. Returns ErrAllocationNotFound when
// no row carries the supplied key.
func (r *InmemAllocationRepo) GetByIdempotencyKey(_ context.Context, key string) (*allocation.TenantManaAllocation, error) {
	if key == "" {
		return nil, ErrAllocationNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, a := range r.byID {
		if a != nil && a.IdempotencyKey == key {
			return a, nil
		}
	}
	return nil, ErrAllocationNotFound
}

// ListByTenant returns allocations for a tenant filtered by gcid + status,
// ordered by AllocatedAt asc (UUIDv7 ⇒ creation order). Returns the
// filtered total (pre-pagination) for cursor consumers.
func (r *InmemAllocationRepo) ListByTenant(_ context.Context, tenantID string, f ListAllocationsFilter) ([]*allocation.TenantManaAllocation, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.byTenant[tenantID]
	out := make([]*allocation.TenantManaAllocation, 0, len(ids))
	for _, id := range ids {
		a := r.byID[id]
		if a == nil {
			continue
		}
		if f.GCID != "" && a.GCID != f.GCID {
			continue
		}
		if f.Status != "" && a.Status != f.Status {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].AllocationID, out[j].AllocationID) < 0
	})
	total := len(out)
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > len(out) {
		limit = len(out)
	}
	return out[:limit], total, nil
}
