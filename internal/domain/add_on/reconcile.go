// reconcile.go: the pure catalogue-drift diff (B2 day zero).
//
// Why this exists.
//
// The add-on catalogue lives in TWO places that nothing reconciles:
//
//   - the running binary, in NewSeedCatalogue() above, which is what the
//     marketplace, the H+ setup wizard and the SubscriptionRegistry all
//     read;
//   - the `add_ons` table, which is what add_on_subscriptions references
//     and what the boot hydrator joins against.
//
// Migrations seeded exactly TWO of those rows before 0035: `core`
// (0018_seed_core_add_on) and `cplus_social` (0021_chora_master_public_tenant
// item 5). Every other row on the live estate was created out of band, and
// migration 0021's backfill then wrote `add_ons.code` as a SLUG OF THE
// DISPLAY NAME, so the stored code disagreed with the canonical code in 4 of
// 7 rows. Migration 0033 repaired those four; 0035 seeds the missing plans and
// repairs the last slug (`pvp_arena` to `pvp-arena`).
//
// A repair is a one-off. This function is the standing guard: it runs at
// boot, compares the two sides, and NAMES every disagreement. Without it the
// failure mode is silent, because a code the runtime cannot key on does not
// raise anything: the marketplace simply renders a plan as unavailable and
// the hydrator simply skips a paying tenant's subscription.
//
// PURE DOMAIN: no SQL, no pgx, no logging. The adapter reads the rows; this
// decides what they mean.
package addon

import (
	"fmt"
	"sort"
	"strings"
)

// LegacyBaseCode is the code the baseline add-on row actually carries in
// `add_ons`. The seed catalogue calls the same plan `base`.
//
// The row is deliberately NOT renamed, for two reasons that both still
// hold: pg.BootstrapRepository resolves the baseline inside the bootstrap
// transaction with `SELECT add_on_id FROM add_ons WHERE name = 'core'`, and
// pg.TranslateAddOnCode already maps the code to `base` at read time. So the
// bridge is the established behaviour, and the reconciler has to honour it
// or it would report a permanent false drift on every healthy deployment.
const LegacyBaseCode = "core"

// SeedBaseCode is the catalogue-side name for the same plan.
const SeedBaseCode = "base"

// CatalogueRow is one `add_ons` row as the reconciler needs to see it.
// Name is carried for diagnostics only: a drift line that prints a code and
// no product name cannot tell an operator which add-on stopped working, and
// a row whose code is NULL has no code to print at all.
type CatalogueRow struct {
	Code string
	Name string
}

// Drift is the disagreement between the running catalogue and the table.
// A nil *Drift means they agree.
type Drift struct {
	// Missing holds catalogue codes with no row, sorted. These are the
	// plans the marketplace offers and the database cannot represent.
	Missing []string
	// Unknown holds rows the catalogue does not know, sorted, each rendered
	// as "Display Name (code=x)" or "Display Name (code is NULL)". These are
	// the rows the hydrator will skip.
	Unknown []string
}

// Error renders every drifted item by name. This string is the whole point
// of the guard: an operator reading it at 3am must not need a database
// prompt to find out what broke.
func (d *Drift) Error() string {
	var b strings.Builder
	b.WriteString("add-on catalogue drift between the running binary and the add_ons table")
	if len(d.Missing) > 0 {
		b.WriteString("; catalogue codes with no row: ")
		b.WriteString(strings.Join(d.Missing, ", "))
	}
	if len(d.Unknown) > 0 {
		b.WriteString("; rows the catalogue cannot key on: ")
		b.WriteString(strings.Join(d.Unknown, ", "))
	}
	return b.String()
}

// Codes returns every code registered in the catalogue, sorted. Sorting is
// not cosmetic: it makes the drift report and its tests deterministic.
func (c *Catalogue) Codes() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	codes := make([]string, 0, len(c.by))
	for code := range c.by {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// ReconcileCatalogue compares the running catalogue against the rows read
// from `add_ons` and returns the drift, or nil when they agree.
//
// A nil catalogue is drift, not a pass. Certifying a match against nothing
// is exactly the false green this guard exists to prevent.
func ReconcileCatalogue(cat *Catalogue, rows []CatalogueRow) *Drift {
	if cat == nil {
		return &Drift{Missing: []string{"<the running catalogue is nil>"}}
	}

	// Index the rows by code. The legacy `core` row counts as `base`.
	haveCode := make(map[string]bool, len(rows))
	var unknown []string
	known := make(map[string]bool)
	for _, code := range cat.Codes() {
		known[code] = true
	}

	for _, row := range rows {
		code := strings.TrimSpace(row.Code)
		name := strings.TrimSpace(row.Name)
		if name == "" {
			name = "<unnamed row>"
		}
		if code == "" {
			unknown = append(unknown, fmt.Sprintf("%s (code is NULL)", name))
			continue
		}
		if code == LegacyBaseCode {
			haveCode[SeedBaseCode] = true
			continue
		}
		haveCode[code] = true
		if !known[code] {
			unknown = append(unknown, fmt.Sprintf("%s (code=%s)", name, code))
		}
	}

	var missing []string
	for _, code := range cat.Codes() {
		if !haveCode[code] {
			missing = append(missing, code)
		}
	}

	if len(missing) == 0 && len(unknown) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unknown)
	return &Drift{Missing: missing, Unknown: unknown}
}
