package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

type stubCatalogueReader struct {
	rows []addon.CatalogueRow
	err  error
}

func (s stubCatalogueReader) ReadCatalogue(ctx context.Context) ([]addon.CatalogueRow, error) {
	return s.rows, s.err
}

func completeRows(t *testing.T) []addon.CatalogueRow {
	t.Helper()
	var rows []addon.CatalogueRow
	for _, code := range addon.NewSeedCatalogue().Codes() {
		if code == addon.SeedBaseCode {
			rows = append(rows, addon.CatalogueRow{Code: addon.LegacyBaseCode, Name: "core"})
			continue
		}
		rows = append(rows, addon.CatalogueRow{Code: code, Name: strings.ToUpper(code)})
	}
	return rows
}

func TestReconcileAddOnCatalogue_NilOnAgreement(t *testing.T) {
	err := reconcileAddOnCatalogue(context.Background(),
		stubCatalogueReader{rows: completeRows(t)}, addon.NewSeedCatalogue())
	if err != nil {
		t.Fatalf("expected agreement, got %v", err)
	}
}

func TestReconcileAddOnCatalogue_DriftIsAnErrorThatNamesThePlans(t *testing.T) {
	err := reconcileAddOnCatalogue(context.Background(),
		stubCatalogueReader{rows: []addon.CatalogueRow{{Code: "core", Name: "core"}}},
		addon.NewSeedCatalogue())
	if err == nil {
		t.Fatal("a one-row catalogue must be reported as drift")
	}
	for _, code := range []string{"tms", "cms", "cplus_social"} {
		if !strings.Contains(err.Error(), code) {
			t.Errorf("error must name %q, got %q", code, err)
		}
	}
}

func TestReconcileAddOnCatalogue_ReadFailureIsNotReportedAsDrift(t *testing.T) {
	// A database that will not answer and a catalogue that is genuinely
	// missing eleven plans need different fixes. Collapsing the first into
	// the second sends an operator to the migrations during an outage.
	err := reconcileAddOnCatalogue(context.Background(),
		stubCatalogueReader{err: errors.New("connection refused")}, addon.NewSeedCatalogue())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("must carry the read cause, got %q", err)
	}
	if strings.Contains(err.Error(), "codes with no row") {
		t.Errorf("a read failure must not be dressed up as drift, got %q", err)
	}
}

func TestReconcileAddOnCatalogue_NilReaderIsAnErrorNotAPass(t *testing.T) {
	if err := reconcileAddOnCatalogue(context.Background(), nil, addon.NewSeedCatalogue()); err == nil {
		t.Fatal("a nil reader must not certify the catalogue")
	}
}

func TestAddOnCatalogueGuardEnforces_FailsClosedByDefault(t *testing.T) {
	// Unset is the production case. The guard exists because the failure it
	// catches is silent, so an unset variable must mean STOP, not shrug.
	if !addOnCatalogueGuardEnforces("") {
		t.Error("an unset variable must enforce")
	}
	if !addOnCatalogueGuardEnforces("enforce") {
		t.Error("enforce must enforce")
	}
	if !addOnCatalogueGuardEnforces("anything-else") {
		t.Error("an unrecognised value must enforce, not silently downgrade")
	}
}

func TestAddOnCatalogueGuardEnforces_WarnIsTheOnlyDowngrade(t *testing.T) {
	// One documented escape hatch, for the case where an operator has to
	// boot a drifted estate to fix it. Spelled exactly, and case-insensitive
	// plus trimmed so a Kubernetes manifest's trailing newline cannot turn
	// an intended downgrade into an enforcement.
	for _, v := range []string{"warn", "WARN", " warn ", "Warn"} {
		if addOnCatalogueGuardEnforces(v) {
			t.Errorf("%q must downgrade to a warning", v)
		}
	}
}
