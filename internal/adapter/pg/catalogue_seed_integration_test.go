//go:build integration

package pg_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// TestCatalogueSeed_FullMigrationSetLeavesNoDrift is the acceptance test for
// migration 0035. It runs against a database with the WHOLE migration set
// applied and asserts the running catalogue and the `add_ons` table agree.
//
// Before 0035 it fails naming eleven missing plans, which is the RED: a
// fresh database holds exactly two rows, `core` from 0018 and `cplus_social`
// from 0021, while the binary offers thirteen.
//
// Run it against a throwaway PostgreSQL 18:
//
//	docker run -d --name b2pg -e POSTGRES_PASSWORD=b2pass \
//	    -e POSTGRES_DB=chora_tenancy -p 55435:5432 postgres:18-alpine
//	# create the three chora_tenancy_* roles, apply migrations/*.sql in order
//	CHORA_TEST_DSN='postgres://postgres:b2pass@127.0.0.1:55435/chora_tenancy' \
//	    go test ./internal/adapter/pg/ -run TestCatalogueSeed -v
//
// It skips without a DSN, matching every other integration test in this
// package.
func TestCatalogueSeed_FullMigrationSetLeavesNoDrift(t *testing.T) {
	pool := liveDB(t) // t.Skip()s when no DSN configured
	ctx := context.Background()

	rows, err := pg.NewCatalogueReader(pg.NewPgxPoolQuerier(pool)).ReadCatalogue(ctx)
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("read zero add_ons rows; the migration set was not applied to this database")
	}

	if drift := addon.ReconcileCatalogue(addon.NewSeedCatalogue(), rows); drift != nil {
		t.Fatalf("catalogue drift after the full migration set: %v", drift)
	}
}

// TestCatalogueSeed_PvpArenaCodeIsRepaired pins the one slug migration 0033
// deliberately left alone. The row's display name is "PvP Arena"; its code
// was `pvp_arena`, a slug of that name written by 0021's backfill, while the
// catalogue says `pvp-arena`. A hyphen and an underscore are one character
// apart and the failure is silent: the hydrator skips the row and a paying
// tenant's plan simply does not appear.
func TestCatalogueSeed_PvpArenaCodeIsRepaired(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()

	rows, err := pg.NewCatalogueReader(pg.NewPgxPoolQuerier(pool)).ReadCatalogue(ctx)
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	var found bool
	for _, r := range rows {
		if r.Code == "pvp_arena" {
			t.Errorf("row %q still carries the slug code pvp_arena; 0035 must repair it to pvp-arena", r.Name)
		}
		if r.Code == "pvp-arena" {
			found = true
		}
	}
	if !found {
		t.Error("no row carries the canonical code pvp-arena")
	}
}

// TestCatalogueSeed_BaselineRowIsStillNamedCore pins the exemption. Renaming
// it would break pg.BootstrapRepository, which resolves the baseline inside
// the bootstrap transaction with `SELECT add_on_id FROM add_ons WHERE name =
// 'core'`, and would strand every existing subscription to it.
func TestCatalogueSeed_BaselineRowIsStillNamedCore(t *testing.T) {
	pool := liveDB(t)
	ctx := context.Background()

	rows, err := pg.NewCatalogueReader(pg.NewPgxPoolQuerier(pool)).ReadCatalogue(ctx)
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	var core, base int
	for _, r := range rows {
		switch r.Code {
		case "core":
			core++
		case "base":
			base++
		}
	}
	if core != 1 {
		t.Errorf("expected exactly one row coded core, got %d", core)
	}
	if base != 0 {
		t.Errorf("expected no row coded base (core IS the baseline row), got %d", base)
	}
}
