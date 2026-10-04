// catalogue_reader.go: reads the `add_ons` catalogue for the boot-time
// drift guard (B2 day zero).
//
// Pairs with addon.ReconcileCatalogue, which owns the meaning. This file
// owns only the SQL.
//
// NO TENANT PREDICATE, DELIBERATELY. `add_ons` is a global catalogue:
// 0001_initial enables RLS on `members` and `add_on_subscriptions` and never
// on `add_ons`, and migration 0033's header says the same ("add_ons is a
// global catalogue with no RLS, and the runner connects as the owning
// migrate role, so no SET LOCAL chora.tenant_id is required"). Wrapping this
// read in RunInTenantTx would not make it safer; against a table with no
// policy it would only add a transaction, and adding a `tenant_id` predicate
// to a table that has no such column would fail loudly at boot. The test
// pins both halves so a future edit cannot quietly empty the guard.
package pg

import (
	"context"
	"fmt"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// CatalogueReader reads every row of the global `add_ons` catalogue.
type CatalogueReader struct {
	q QueryRunner
}

// NewCatalogueReader binds the reader to a querier. Panics on nil so a
// wiring bug fails at boot rather than at the first drift check.
func NewCatalogueReader(q QueryRunner) *CatalogueReader {
	if q == nil {
		panic("pg.NewCatalogueReader: nil querier")
	}
	return &CatalogueReader{q: q}
}

// catalogueSQL selects the two columns the reconciler needs.
//
// COALESCE on `code` rather than a nullable scan: `add_ons.code` is
// `VARCHAR(64) NULL` (migration 0021 added it nullable and backfilled), and
// the domain already treats an empty code as a row with no code, reporting
// it by product name. Collapsing NULL to the empty string here keeps the
// scan destinations plain strings and keeps the two layers agreeing on one
// representation of missing.
const catalogueSQL = `
    SELECT COALESCE(code, ''), name
    FROM   add_ons
    ORDER  BY name`

// ReadCatalogue returns every `add_ons` row as a CatalogueRow.
//
// An error is ALWAYS returned, never collapsed into an empty slice: an
// empty catalogue and an unreachable database are different failures with
// different fixes, and reporting a read error as "every plan is missing"
// would send an operator to the migrations when the problem is the network.
func (r *CatalogueReader) ReadCatalogue(ctx context.Context) ([]addon.CatalogueRow, error) {
	rows, err := r.q.Query(ctx, catalogueSQL)
	if err != nil {
		return nil, fmt.Errorf("pg.CatalogueReader: query add_ons: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []addon.CatalogueRow
	for rows.Next() {
		var code, name string
		if err := rows.Scan(&code, &name); err != nil {
			return nil, fmt.Errorf("pg.CatalogueReader: scan add_ons row: %w", err)
		}
		out = append(out, addon.CatalogueRow{Code: code, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.CatalogueReader: iterate add_ons: %w", err)
	}
	return out, nil
}
