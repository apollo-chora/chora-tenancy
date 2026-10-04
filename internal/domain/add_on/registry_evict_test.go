package addon_test

// registry_evict_test.go: the eviction that makes memory unable to outrun the
// database (UX Track U, row E4 slice 2).
//
// # WHY EVICTION, AND WHAT IT BUYS
//
// Row E4 makes PostgreSQL the source of truth for entitlements. The rule is
// that memory must never be ahead of the database. The registry cannot honour
// that by writing PG first, because its mutation methods take the mutex, apply
// the state machine and return the mutated subscription in one step: there is
// no way to ask what the result WOULD be, and the only insert path is the
// domain `Subscribe` itself.
//
// So the call site mutates, persists, and on a persist FAILURE evicts the
// tenant. Memory is then not ahead, it is ABSENT, and `KnowsTenant` already
// reports false for an unknown tenant, which is what makes the read fall
// through to the durable table. The failure path therefore lands on the source
// of truth rather than serving a state PG does not hold.
//
// ⚠ WHAT THIS DOES NOT CLOSE, stated here rather than discovered later:
// between the in-memory mutation and the commit, a same-pod reader can observe
// a state that is not yet durable. That window is one write long and is closed
// only by making the lock or the transaction span both, which is the
// load-modify-save end state slice 3 points at and is NOT this row.
//
// Eviction is TENANT-WIDE on purpose. Evicting only the failed (tenant, code)
// pair would leave the registry authoritative for that tenant's other add-ons
// and stale for one, which is exactly the half-truth row E4 exists to remove.
// The cost is honest and small: a cache refill for that tenant on the next
// read.

import (
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// evictFixture builds a registry holding two live subscriptions for one tenant
// and one for a second tenant, so the tests can prove eviction is scoped to a
// tenant rather than global.
func evictFixture(t *testing.T) (reg *addon.SubscriptionRegistry, tenantA, tenantB string, codes []string) {
	t.Helper()
	reg = addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	reg.SetClock(func() time.Time { return time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC) })

	tenantA = "01990000-0000-7000-8000-00000000000a"
	tenantB = "01990000-0000-7000-8000-00000000000b"
	codes = []string{"tms", "cms"}

	for _, c := range codes {
		if _, err := reg.Subscribe(tenantA, c, "gcid-a"); err != nil {
			t.Fatalf("seed %s for tenant A: %v", c, err)
		}
	}
	if _, err := reg.Subscribe(tenantB, "tms", "gcid-b"); err != nil {
		t.Fatalf("seed tms for tenant B: %v", err)
	}
	return reg, tenantA, tenantB, codes
}

func TestSubscriptionRegistry_Evict_RemovesTheWholeTenant(t *testing.T) {
	reg, tenantA, _, _ := evictFixture(t)

	if !reg.KnowsTenant(tenantA) {
		t.Fatal("fixture is wrong: the registry should know tenant A before eviction")
	}
	if got := len(reg.ListAllByTenant(tenantA)); got != 2 {
		t.Fatalf("tenant A holds %d subscriptions, want 2", got)
	}

	reg.Evict(tenantA)

	// KnowsTenant reporting false is the load-bearing part: it is what makes
	// the durable fall-through fire on the next read.
	if reg.KnowsTenant(tenantA) {
		t.Fatal("KnowsTenant still true after Evict; the read would be served from memory instead of falling through to the durable table, which is the whole point")
	}
	if got := len(reg.ListAllByTenant(tenantA)); got != 0 {
		t.Fatalf("tenant A still holds %d subscriptions after Evict, want 0", got)
	}
	if _, ok := reg.GetSubscription(tenantA, "tms"); ok {
		t.Error("GetSubscription still resolves an evicted tenant's subscription")
	}
}

// TestSubscriptionRegistry_Evict_LeavesOtherTenantsAlone is the arm that stops
// Evict being a global flush. Without it, a registry that simply cleared
// everything would satisfy the test above.
func TestSubscriptionRegistry_Evict_LeavesOtherTenantsAlone(t *testing.T) {
	reg, tenantA, tenantB, _ := evictFixture(t)

	reg.Evict(tenantA)

	if !reg.KnowsTenant(tenantB) {
		t.Fatal("evicting tenant A also evicted tenant B; Evict is tenant-scoped, not a flush")
	}
	if got := len(reg.ListAllByTenant(tenantB)); got != 1 {
		t.Fatalf("tenant B holds %d subscriptions after A was evicted, want 1", got)
	}
}

// TestSubscriptionRegistry_Evict_IsIdempotentAndSafeOnUnknown pins the two
// boring cases, because the caller invokes this on a FAILURE path where the
// tenant may already be gone and an extra panic would turn a recoverable write
// error into a crash.
func TestSubscriptionRegistry_Evict_IsIdempotentAndSafeOnUnknown(t *testing.T) {
	reg, tenantA, _, _ := evictFixture(t)

	reg.Evict(tenantA)
	reg.Evict(tenantA) // second time must be a no-op, not a panic
	reg.Evict("01990000-0000-7000-8000-0000000000ff")

	if reg.KnowsTenant(tenantA) {
		t.Fatal("tenant A came back")
	}
}

// TestSubscriptionRegistry_Evict_AllowsAFreshSubscribeAfterwards proves the
// eviction leaves the registry usable rather than poisoned: the tenant can be
// re-hydrated, which is exactly what the next read does.
func TestSubscriptionRegistry_Evict_AllowsAFreshSubscribeAfterwards(t *testing.T) {
	reg, tenantA, _, _ := evictFixture(t)

	reg.Evict(tenantA)

	if _, err := reg.Subscribe(tenantA, "tms", "gcid-a"); err != nil {
		t.Fatalf("re-subscribing an evicted tenant failed: %v; the next read could never refill the cache", err)
	}
	if !reg.KnowsTenant(tenantA) {
		t.Fatal("KnowsTenant still false after a fresh Subscribe")
	}
	if got := len(reg.ListAllByTenant(tenantA)); got != 1 {
		t.Fatalf("tenant A holds %d after refill, want 1", got)
	}
}
