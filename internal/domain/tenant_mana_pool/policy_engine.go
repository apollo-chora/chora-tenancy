// Package pool — PoolStore is the repository port for tenant mana pools.
package pool

import (
	"context"
)

// PoolStore is the repository port for tenant mana pools. The HTTP layer
// supplies an in-memory or Postgres-backed implementation; the engine
// itself does not care.
type PoolStore interface {
	GetByTenant(ctx context.Context, tenantID string) (*TenantManaPool, error)
	Save(ctx context.Context, p *TenantManaPool) error
}
