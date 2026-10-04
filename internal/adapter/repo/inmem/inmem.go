// Package inmem provides in-memory repository adapters for the new
// chora-tenancy sub-domains (subscription store passthrough wrapper).
package inmem

import (
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// SubscriptionStore wraps an in-memory SubscriptionRegistry for hexagonal
// port substitution. Production swaps to Postgres.
type SubscriptionStore struct {
	*addon.SubscriptionRegistry
}

// NewSubscriptionStore returns a passthrough store wired to the catalogue.
func NewSubscriptionStore(cat *addon.Catalogue) *SubscriptionStore {
	return &SubscriptionStore{SubscriptionRegistry: addon.NewSubscriptionRegistry(cat)}
}
