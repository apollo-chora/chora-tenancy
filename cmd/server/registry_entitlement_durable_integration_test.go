//go:build integration

// registry_entitlement_durable_integration_test.go: the E1 acceptance, end to
// end. A tenant created with chosen add-ons must read back its entitlements in
// a process that NEVER hydrated the in-memory registry, which is every pod that
// booted before the tenant existed.
//
// Run against the local Postgres 18 mirror while Cloud SQL is cost-paused:
//
//	CHORA_TEST_DSN='postgres://chora_tenancy_migrate:dev@localhost:5432/chora_tenancy?sslmode=disable' \
//	  go test -tags integration ./cmd/server/ -run Integration_EntitlementRead
//
// The unit tests beside this one prove the wrapper's branching against a fake
// delegate. This one proves the branch is wired to the real RLS-protected
// table through the real store, which a fake cannot show.
package main

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/apollo-chora/chora-common/db"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

func liveEntitlementDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CHORA_TEST_DSN")
	if dsn == "" {
		t.Skip("set CHORA_TEST_DSN to run integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := cgcdb.Bootstrap(ctx, cgcdb.BootstrapOptions{
		DSN:             dsn,
		RewriteFromPort: 6432,
		RewriteToPort:   5432,
		AppName:         "chora-tenancy-entitlement-read-integration-test",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestIntegration_EntitlementRead_NeverHydratedProcessSeesTheDurableGrant(t *testing.T) {
	pool := liveEntitlementDB(t)
	querier := pg.NewPgxPoolQuerier(pool)

	b, err := bootstrap.New(bootstrap.Input{
		Name:       "E1 Read Through " + uuid.NewString()[:8],
		OwnerGCID:  uuid.NewString(),
		AddOnCodes: []string{"tms", "cms"},
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	if err := pg.NewBootstrapRepository(querier).Persist(context.Background(), b); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// A registry as empty as it is in a pod that booted before this tenant
	// existed. The CHO-1752 hydrator is deliberately NOT run.
	reg := addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
	if reg.KnowsTenant(b.Tenant.ID) {
		t.Fatalf("the fixture is wrong: a fresh registry already knows the tenant")
	}
	store := newRegistryEntitlementStore(reg, pg.NewLegacyEntitlementStore(querier))

	got := []string{}
	for _, e := range store.ListActiveByTenant(b.Tenant.ID) {
		got = append(got, e.AddOnCode)
	}

	// Sorted because the durable read orders by subscription_id, a UUIDv7, so
	// the order follows insertion rather than code. base rather than core: the
	// durable row carries the legacy code and the reader must see the one
	// vocabulary a hydrated pod would have served.
	const want = "base,cms,tms"
	sort.Strings(got)
	if strings.Join(got, ",") != want {
		t.Fatalf("entitlements read in a never-hydrated process = %v, want [%s]. Before this fix the "+
			"registry-backed wrapper served its empty boot snapshot and the durable grant was "+
			"invisible until the pod restarted", got, want)
	}
}
