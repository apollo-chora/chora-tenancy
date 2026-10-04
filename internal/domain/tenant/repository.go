// repository.go — persistence ports for the tenant lifecycle sub-domain.
//
// Adapters (in-memory Registry, Postgres pg.TenantRepository) implement
// these interfaces; the domain MUST NOT import any adapter package.
package tenant

import (
	"context"
	"errors"
)

// ErrTenantSlugExists is returned by Repository.Save when a non-deleted
// tenant with the same slug already exists. The Phyllis light-wizard
// idempotency contract handles this at the service layer (look up by
// slug first, return existing on hit).
var ErrTenantSlugExists = errors.New("tenant: slug already exists")

// Repository is the persistence port for the Tenant aggregate.
type Repository interface {
	// Save upserts a tenant by ID.
	Save(ctx context.Context, t *Tenant) error
	// Get returns a tenant by ID and a bool ok flag.
	Get(ctx context.Context, id string) (*Tenant, bool, error)
	// FindBySlug returns the (single) non-deleted tenant with the given
	// slug, false if absent.
	FindBySlug(ctx context.Context, slug string) (*Tenant, bool, error)
}
