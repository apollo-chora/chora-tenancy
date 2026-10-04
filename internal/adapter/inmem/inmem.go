// Package inmem holds in-memory repository adapters for the chora-tenancy
// skeleton. Production implementations (M12+) will swap in a Postgres adapter
// against chora_tenancy via PgBouncer; the domain layer is unchanged.
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"

	domain "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
)

// -----------------------------------------------------------------------------
// TenantRepo
// -----------------------------------------------------------------------------

// TenantRepo is an in-memory store for Tenant aggregates.
//
// Tenant is the supporting-domain primary aggregate; the repo is global
// (not tenant-scoped) because list endpoints serve the H+ Hub+ surface
// to platform admins.
type TenantRepo struct {
	mu sync.RWMutex
	by map[string]*domain.Tenant // key = tenant_id
}

// NewTenantRepo returns an empty repo.
func NewTenantRepo() *TenantRepo { return &TenantRepo{by: make(map[string]*domain.Tenant)} }

// Save inserts or upserts a tenant. The in-memory map write cannot fail,
// so it always returns nil — the ctx + error are here to satisfy the
// httpapi.TenantStore port (CHO-2201).
func (r *TenantRepo) Save(_ context.Context, t *domain.Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.by[t.ID] = t
	return nil
}

// Get returns a tenant by ID and a bool ok flag.
func (r *TenantRepo) Get(id string) (*domain.Tenant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.by[id]
	if !ok {
		return nil, false
	}
	return t, true
}

// List returns the non-closed tenants, sorted by ID (UUIDv7 ⇒ creation order).
// Closed tenants (status=closed) are excluded — they are pseudonymised
// per .claude/rules/ddd-enforcement.md soft-delete semantics.
func (r *TenantRepo) List(offset, limit int) ([]*domain.Tenant, int) {
	r.mu.RLock()
	out := make([]*domain.Tenant, 0, len(r.by))
	for _, t := range r.by {
		if t.Status == domain.TenantStatusClosed {
			continue
		}
		out = append(out, t)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	total := len(out)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total
}

// -----------------------------------------------------------------------------
// InvoiceRepo
// -----------------------------------------------------------------------------

// InvoiceRepo is an in-memory store for Invoice aggregates.
type InvoiceRepo struct {
	mu       sync.RWMutex
	byID     map[string]*domain.Invoice
	byTenant map[string][]*domain.Invoice
}

// NewInvoiceRepo returns an empty repo.
func NewInvoiceRepo() *InvoiceRepo {
	return &InvoiceRepo{
		byID:     make(map[string]*domain.Invoice),
		byTenant: make(map[string][]*domain.Invoice),
	}
}

// Save inserts an invoice (invoices are append-only — no upsert).
func (r *InvoiceRepo) Save(inv *domain.Invoice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[inv.ID]; exists {
		return
	}
	r.byID[inv.ID] = inv
	r.byTenant[inv.TenantID] = append(r.byTenant[inv.TenantID], inv)
}

// ListByTenant returns invoices for a tenant, sorted by ID (creation order).
func (r *InvoiceRepo) ListByTenant(tenantID string) []*domain.Invoice {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := r.byTenant[tenantID]
	out := make([]*domain.Invoice, 0, len(all))
	out = append(out, all...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// -----------------------------------------------------------------------------
// PaymentMethodRepo
// -----------------------------------------------------------------------------

// PaymentMethodRepo is an in-memory store for PaymentMethod aggregates.
type PaymentMethodRepo struct {
	mu       sync.RWMutex
	byID     map[string]*domain.PaymentMethod
	byTenant map[string][]*domain.PaymentMethod
}

// NewPaymentMethodRepo returns an empty repo.
func NewPaymentMethodRepo() *PaymentMethodRepo {
	return &PaymentMethodRepo{
		byID:     make(map[string]*domain.PaymentMethod),
		byTenant: make(map[string][]*domain.PaymentMethod),
	}
}

// Save inserts or upserts a payment method.
func (r *PaymentMethodRepo) Save(pm *domain.PaymentMethod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[pm.ID]; !exists {
		r.byTenant[pm.TenantID] = append(r.byTenant[pm.TenantID], pm)
	}
	r.byID[pm.ID] = pm
}

// ListByTenant returns the payment methods for a tenant, sorted by ID.
func (r *PaymentMethodRepo) ListByTenant(tenantID string) []*domain.PaymentMethod {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := r.byTenant[tenantID]
	out := make([]*domain.PaymentMethod, 0, len(all))
	out = append(out, all...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}
