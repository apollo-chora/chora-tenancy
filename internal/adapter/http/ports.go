// ports.go — hexagonal persistence ports for the legacy /api/* HTTP layer.
//
// A7 (docs/m13/handoff-fe-to-be-service-2026-05-14.md §A7): the A6-wired
// gateway routes `GET /api/tenants/{id}` and `GET /api/feature-flags`
// (→ chora-tenancy `GET /api/tenants/{id}/entitlements`) reached this
// service but returned 404 / empty because `httpapi.Deps` was wired to
// in-memory stores ONLY — the demo tenant rows + entitlements live in the
// `chora_tenancy` Postgres tables, never in the in-memory registry.
//
// Per the no-stubs / no-debts directive (and mirroring the D0.4 chora-
// delivery CataloguePort pattern) the HTTP handlers now depend on these
// interface PORTS, not on concrete stores. Two adapters satisfy each port:
//
//   - the in-memory adapters (internal/adapter/inmem + the registries in
//     internal/domain/tenancy) — kept for unit tests + local dev when no
//     Postgres pool is wired.
//   - the pgx adapters (internal/adapter/pg, the Legacy* types) — the
//     production adapters backed by chora_tenancy. Wired in cmd/server
//     when CHORA_DB_DSN(_SECRET_ID) is set.
//
// The handler signatures in handlers.go are unchanged — only the `Deps`
// struct field types move from concrete to interface. The domain layer
// (internal/domain/tenancy) is NEVER imported by an adapter in the reverse
// direction; these ports are defined in the adapter layer and the domain
// types they traffic in (tenancy.Tenant, tenancy.TenantEntitlement,
// tenancy.AddOn, tenancy.Invoice, tenancy.PaymentMethod) are pure-domain
// aggregates.
package httpapi

import (
	"context"

	domain "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
)

// TenantStore is the persistence port for the Tenant aggregate as the
// legacy /api/tenants* handlers use it.
//
// Read methods (Get, List) back the gateway-facing `GET /api/tenants/{id}`
// route + the H+ Hub+ admin list. Save backs `POST /api/tenants` +
// `POST /api/tenants/{id}/parent`.
type TenantStore interface {
	// Save inserts or upserts a tenant by ID. It carries the request ctx
	// (cancellation / trace propagation) and RETURNS the persistence error
	// — a failed write MUST NOT be reported to the caller as success (the
	// H+ admin UI would otherwise show a tenant the DB never accepted).
	Save(ctx context.Context, t *domain.Tenant) error
	// Get returns a tenant by ID and a bool ok flag. Soft-deleted /
	// unknown tenants return ok=false.
	Get(id string) (*domain.Tenant, bool)
	// List returns the non-closed tenants for the H+ admin marketplace,
	// 0-indexed offset + limit pagination, plus the total pre-pagination
	// count.
	List(offset, limit int) ([]*domain.Tenant, int)
}

// AddOnStore is the persistence port for the AddOn catalog (flat,
// composable — NO tier hierarchy per CLAUDE.md §1).
type AddOnStore interface {
	// Register adds an AddOn to the catalog.
	Register(a *domain.AddOn)
	// Get returns an AddOn by ID and a bool ok flag.
	Get(id string) (*domain.AddOn, bool)
	// List returns all add-ons sorted by ID (creation order).
	List() []*domain.AddOn
}

// EntitlementStore is the persistence port for TenantEntitlement records
// (the composable add-on subscriptions).
//
// ListActiveByTenant backs the gateway-facing `GET /api/tenants/{id}/
// entitlements` route — the TenantEntitlement list IS the feature-flag
// set the FE consumes via `GET /api/feature-flags`.
type EntitlementStore interface {
	// Subscribe creates a new active TenantEntitlement OR reactivates a
	// previously-cancelled one. Idempotent for already-active entries.
	Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*domain.TenantEntitlement, error)
	// Cancel marks the (tenant_id, add_on_id) entitlement cancelled.
	// Returns domain.ErrEntitlementNotFound for an unknown pair.
	Cancel(tenantID, addOnID string) (*domain.TenantEntitlement, error)
	// ListActiveByTenant returns all active entitlements for a tenant,
	// sorted by ID (creation order).
	ListActiveByTenant(tenantID string) []*domain.TenantEntitlement
}

// InvoiceStore is the persistence port for period billing snapshots.
type InvoiceStore interface {
	// Save inserts an invoice (invoices are append-only — no upsert).
	Save(inv *domain.Invoice)
	// ListByTenant returns invoices for a tenant, sorted by ID.
	ListByTenant(tenantID string) []*domain.Invoice
}

// PaymentMethodStore is the persistence port for Stripe payment-method
// handles.
type PaymentMethodStore interface {
	// Save inserts or upserts a payment method.
	Save(pm *domain.PaymentMethod)
	// ListByTenant returns the payment methods for a tenant, sorted by ID.
	ListByTenant(tenantID string) []*domain.PaymentMethod
}
