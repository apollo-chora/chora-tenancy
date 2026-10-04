// addon_catalogue_guard.go: the boot-time add-on catalogue drift guard
// (UX refactor Phase B, package B2 day zero).
//
// The catalogue exists twice: in the binary (addon.NewSeedCatalogue) and in
// the `add_ons` table. Nothing reconciled them, and the failure mode when
// they disagree is SILENT. An unknown code raises nothing: the marketplace
// renders the plan as unavailable, and the boot hydrator skips a paying
// tenant's subscription with a log line nobody reads. Migration 0033 found
// four such rows on the live estate; 0035 seeds the missing plans and
// repairs the last slug. This guard is what keeps them agreeing afterwards.
//
// FAIL-CLOSED BY DEFAULT. An unset or unrecognised
// CHORA_ADDON_CATALOGUE_RECONCILE enforces, because the whole point is that
// nobody notices this drift on their own. The single documented downgrade is
// the literal value `warn`, for the case an operator has to boot a drifted
// estate in order to repair it. That asymmetry is deliberate: a typo in the
// variable must never quietly disarm the guard.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// catalogueReader is the port the guard needs: pg.CatalogueReader in
// production, a stub in tests.
type catalogueReader interface {
	ReadCatalogue(ctx context.Context) ([]addon.CatalogueRow, error)
}

// reconcileAddOnCatalogue reads `add_ons` and compares it against the
// running catalogue. Returns nil when they agree.
//
// The two failure kinds stay DISTINCT in the returned error. A read failure
// is wrapped as a read failure and never rendered as drift, because "the
// database would not answer" and "the catalogue is missing eleven plans"
// send an operator to entirely different places.
func reconcileAddOnCatalogue(ctx context.Context, r catalogueReader, cat *addon.Catalogue) error {
	if r == nil {
		return errors.New("add-on catalogue guard: no reader wired; refusing to certify the catalogue")
	}
	rows, err := r.ReadCatalogue(ctx)
	if err != nil {
		return fmt.Errorf("add-on catalogue guard: could not read add_ons: %w", err)
	}
	if drift := addon.ReconcileCatalogue(cat, rows); drift != nil {
		return drift
	}
	return nil
}

// addOnCatalogueGuardEnforces reports whether drift should stop the boot.
//
// Trimmed and case-folded so a manifest's trailing newline cannot turn an
// intended downgrade into an enforcement, and so `WARN` behaves like `warn`.
// Everything that is not exactly `warn` enforces.
func addOnCatalogueGuardEnforces(env string) bool {
	return !strings.EqualFold(strings.TrimSpace(env), "warn")
}
