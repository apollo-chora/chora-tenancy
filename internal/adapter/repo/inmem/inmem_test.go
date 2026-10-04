// Package inmem_test holds the RED-phase TDD specs for the new in-memory
// repositories used by the extended chora-tenancy domain (tenant, member,
// add-on, billing).
package inmem_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/repo/inmem"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

func TestSubscriptionStore_PassthroughOfRegistry(t *testing.T) {
	t.Parallel()
	cat := addon.NewSeedCatalogue()
	store := inmem.NewSubscriptionStore(cat)
	tenantID := "01970000-0000-7000-8000-0000000000aa"
	if _, err := store.Subscribe(tenantID, "tms", "owner-1"); err != nil {
		t.Fatalf("Subscribe: unexpected error: %v", err)
	}
	all := store.ListActiveByTenant(tenantID)
	if len(all) != 1 {
		t.Fatalf("expected 1 active sub, got %d", len(all))
	}
}
