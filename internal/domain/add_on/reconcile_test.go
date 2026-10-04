package addon

import (
	"sort"
	"strings"
	"testing"
)

// rowsFromCodes is a fixture helper: one CatalogueRow per code, with the
// display name derived so a failure message names something a human
// recognises.
func rowsFromCodes(codes ...string) []CatalogueRow {
	rows := make([]CatalogueRow, 0, len(codes))
	for _, c := range codes {
		rows = append(rows, CatalogueRow{Code: c, Name: strings.ToUpper(c)})
	}
	return rows
}

// allSeedCodes returns every code NewSeedCatalogue registers, so a test
// can build a no-drift row set without hard-coding the list twice.
func allSeedCodes() []string {
	cat := NewSeedCatalogue()
	codes := cat.Codes()
	sort.Strings(codes)
	return codes
}

func TestReconcileCatalogue_NoDriftWhenEveryCodeHasARow(t *testing.T) {
	cat := NewSeedCatalogue()
	drift := ReconcileCatalogue(cat, rowsFromCodes(allSeedCodes()...))
	if drift != nil {
		t.Fatalf("expected no drift for a complete row set, got %v", drift)
	}
}

func TestReconcileCatalogue_CoreSatisfiesBase(t *testing.T) {
	// The live baseline row is named and coded `core`, not `base`:
	// pg.BootstrapRepository resolves it by `name = 'core'` and
	// pg.TranslateAddOnCode maps the code to `base` at read time. The
	// reconciler must honour that same bridge or it would report a
	// permanent false drift on every healthy deployment.
	codes := allSeedCodes()
	swapped := make([]string, 0, len(codes))
	for _, c := range codes {
		if c == "base" {
			swapped = append(swapped, "core")
			continue
		}
		swapped = append(swapped, c)
	}

	drift := ReconcileCatalogue(NewSeedCatalogue(), rowsFromCodes(swapped...))
	if drift != nil {
		t.Fatalf("core must satisfy base, got drift %v", drift)
	}
}

func TestReconcileCatalogue_MissingCodesAreNamed(t *testing.T) {
	// A fresh database holds only the two rows migrations seed before
	// 0035: `core` (0018) and `cplus_social` (0021).
	drift := ReconcileCatalogue(NewSeedCatalogue(), rowsFromCodes("core", "cplus_social"))
	if drift == nil {
		t.Fatal("expected drift on a fresh two-row catalogue, got nil")
	}
	want := []string{
		"ai_assist", "campus-ops", "cms", "course_application", "daily_dose",
		"familiar", "governance_dashboard", "kg_hexagonal", "marketplace",
		"pvp-arena", "tms",
	}
	if len(drift.Missing) != len(want) {
		t.Fatalf("missing count = %d, want %d (%v)", len(drift.Missing), len(want), drift.Missing)
	}
	for i, code := range want {
		if drift.Missing[i] != code {
			t.Errorf("Missing[%d] = %q, want %q", i, drift.Missing[i], code)
		}
	}
	if len(drift.Unknown) != 0 {
		t.Errorf("Unknown = %v, want none", drift.Unknown)
	}
}

func TestReconcileCatalogue_UnknownCodeIsNamedWithItsProduct(t *testing.T) {
	// The live slug drift this migration repairs: the row for PvP Arena
	// carries `pvp_arena` while the catalogue says `pvp-arena`. Before
	// the repair the reconciler must report BOTH halves: the catalogue
	// code has no row, and the row's code is not in the catalogue.
	rows := rowsFromCodes(allSeedCodes()...)
	for i := range rows {
		if rows[i].Code == "pvp-arena" {
			rows[i] = CatalogueRow{Code: "pvp_arena", Name: "PvP Arena"}
		}
	}

	drift := ReconcileCatalogue(NewSeedCatalogue(), rows)
	if drift == nil {
		t.Fatal("expected drift for the pvp_arena slug, got nil")
	}
	if len(drift.Missing) != 1 || drift.Missing[0] != "pvp-arena" {
		t.Errorf("Missing = %v, want [pvp-arena]", drift.Missing)
	}
	if len(drift.Unknown) != 1 || drift.Unknown[0] != "PvP Arena (code=pvp_arena)" {
		t.Errorf("Unknown = %v, want [PvP Arena (code=pvp_arena)]", drift.Unknown)
	}
}

func TestReconcileCatalogue_EmptyCodeReportsTheProductNotTheBlank(t *testing.T) {
	// A row that predates the migration-0021 backfill has a NULL code.
	// Reporting `code=` tells an operator nothing; the product name is
	// the only actionable half.
	rows := append(rowsFromCodes(allSeedCodes()...), CatalogueRow{Code: "", Name: "Legacy Bundle"})

	drift := ReconcileCatalogue(NewSeedCatalogue(), rows)
	if drift == nil {
		t.Fatal("expected drift for a row with no code, got nil")
	}
	if len(drift.Unknown) != 1 || drift.Unknown[0] != "Legacy Bundle (code is NULL)" {
		t.Errorf("Unknown = %v, want [Legacy Bundle (code is NULL)]", drift.Unknown)
	}
}

func TestReconcileCatalogue_ErrorNamesEveryDriftedCode(t *testing.T) {
	// The whole value of the guard is the log line. A message that says
	// "catalogue drift" without naming anything sends an operator to a
	// database prompt to find out what broke.
	drift := ReconcileCatalogue(NewSeedCatalogue(), rowsFromCodes("core"))
	if drift == nil {
		t.Fatal("expected drift, got nil")
	}
	msg := drift.Error()
	for _, code := range []string{"tms", "cms", "familiar", "cplus_social"} {
		if !strings.Contains(msg, code) {
			t.Errorf("Error() must name %q, got %q", code, msg)
		}
	}
}

func TestReconcileCatalogue_NilCatalogueIsDriftNotAPanic(t *testing.T) {
	drift := ReconcileCatalogue(nil, rowsFromCodes("core"))
	if drift == nil {
		t.Fatal("a nil catalogue must report drift rather than certify a match")
	}
}

func TestCatalogue_CodesReturnsEverySeededCode(t *testing.T) {
	codes := NewSeedCatalogue().Codes()
	if len(codes) != 13 {
		t.Fatalf("seed catalogue holds %d codes, want 13: %v", len(codes), codes)
	}
	seen := map[string]bool{}
	for _, c := range codes {
		if seen[c] {
			t.Errorf("duplicate code %q", c)
		}
		seen[c] = true
	}
	for _, want := range []string{"base", "tms", "cms", "cplus_social", "pvp-arena", "campus-ops"} {
		if !seen[want] {
			t.Errorf("Codes() omits %q", want)
		}
	}
}

func TestDrift_ErrorRendersEachHalfIndependently(t *testing.T) {
	// Missing-only and unknown-only are both real states: the first is a
	// fresh database, the second is a live row the catalogue dropped.
	// Neither may render the other half's clause.
	missingOnly := (&Drift{Missing: []string{"tms"}}).Error()
	if !strings.Contains(missingOnly, "codes with no row: tms") {
		t.Errorf("missing-only message = %q", missingOnly)
	}
	if strings.Contains(missingOnly, "cannot key on") {
		t.Errorf("missing-only message must not mention unknown rows: %q", missingOnly)
	}

	unknownOnly := (&Drift{Unknown: []string{"Old Plan (code=old)"}}).Error()
	if !strings.Contains(unknownOnly, "cannot key on: Old Plan (code=old)") {
		t.Errorf("unknown-only message = %q", unknownOnly)
	}
	if strings.Contains(unknownOnly, "no row") {
		t.Errorf("unknown-only message must not mention missing codes: %q", unknownOnly)
	}
}

func TestCatalogue_CodesOnNilIsEmptyNotAPanic(t *testing.T) {
	var c *Catalogue
	if got := c.Codes(); got != nil {
		t.Errorf("nil catalogue Codes() = %v, want nil", got)
	}
}
