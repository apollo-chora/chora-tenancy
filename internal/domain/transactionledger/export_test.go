package transactionledger_test

import (
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

func sampleRows() []tl.LedgerRow {
	at := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	return []tl.LedgerRow{
		{LedgerID: "L1", OccurredAt: at, TenantID: "t1", LearnerGCID: "g1", Kind: tl.KindPurchase,
			SourceDomain: "payments", SourceRefID: "pur1", Label: "Course, with comma", Currency: "sgd",
			AmountMinor: 4999, Status: tl.StatusCaptured},
		{LedgerID: "L2", OccurredAt: at, TenantID: "t1", Kind: tl.KindManaSpendDaily,
			SourceDomain: "observability", SourceRefID: "2026-06-20", Label: "Mana spent", ManaUnits: -30,
			Status: tl.StatusPosted},
	}
}

func TestSerializeExport_CSV(t *testing.T) {
	b, ct, err := tl.SerializeExport(tl.FormatCSV, sampleRows())
	if err != nil {
		t.Fatalf("SerializeExport csv: %v", err)
	}
	if !strings.HasPrefix(ct, "text/csv") {
		t.Errorf("content type = %q", ct)
	}
	recs, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	if len(recs) != 3 { // header + 2 rows
		t.Fatalf("csv rows = %d; want 3", len(recs))
	}
	if recs[0][0] != "ledger_id" || recs[0][9] != "amount_minor" {
		t.Errorf("csv header wrong: %v", recs[0])
	}
	// comma in label survived quoting (csv reader split it back cleanly).
	if recs[1][7] != "Course, with comma" {
		t.Errorf("csv label quoting wrong: %q", recs[1][7])
	}
	if recs[1][9] != "4999" || recs[2][10] != "-30" {
		t.Errorf("csv amount/mana wrong: %v / %v", recs[1][9], recs[2][10])
	}
}

func TestSerializeExport_JSON(t *testing.T) {
	b, ct, err := tl.SerializeExport(tl.FormatJSON, sampleRows())
	if err != nil {
		t.Fatalf("SerializeExport json: %v", err)
	}
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q", ct)
	}
	var arr []map[string]any
	if err := json.Unmarshal(b, &arr); err != nil {
		t.Fatalf("json parse: %v", err)
	}
	if len(arr) != 2 {
		t.Fatalf("json rows = %d; want 2", len(arr))
	}
	if arr[0]["ledger_id"] != "L1" || arr[0]["kind"] != "purchase" {
		t.Errorf("json row0 wrong: %v", arr[0])
	}
	// numeric fields are JSON numbers
	if arr[1]["mana_units"].(float64) != -30 {
		t.Errorf("json mana_units wrong: %v", arr[1]["mana_units"])
	}
}

func TestSerializeExport_UnknownFormat(t *testing.T) {
	if _, _, err := tl.SerializeExport("xml", sampleRows()); err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestGUCForTenant(t *testing.T) {
	if got := tl.GUCForTenant(tl.NilTenantUUID); got != tl.PlatformScope {
		t.Errorf("nil-uuid → %q; want platform sentinel", got)
	}
	const tenant = "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b"
	if got := tl.GUCForTenant(tenant); got != tenant {
		t.Errorf("concrete tenant → %q; want itself", got)
	}
}

func TestExportJob_AttemptExceeds(t *testing.T) {
	if (tl.ExportJob{AttemptCount: 2}).AttemptExceeds(3) {
		t.Error("2 should not exceed 3")
	}
	if !(tl.ExportJob{AttemptCount: 4}).AttemptExceeds(3) {
		t.Error("4 should exceed 3")
	}
}

func TestSerializeExport_Empty(t *testing.T) {
	b, _, err := tl.SerializeExport(tl.FormatCSV, nil)
	if err != nil {
		t.Fatalf("empty csv: %v", err)
	}
	if !strings.HasPrefix(string(b), "ledger_id,") {
		t.Errorf("empty csv should still have a header: %q", string(b))
	}
	bj, _, err := tl.SerializeExport(tl.FormatJSON, nil)
	if err != nil {
		t.Fatalf("empty json: %v", err)
	}
	if strings.TrimSpace(string(bj)) != "[]" {
		t.Errorf("empty json should be []: %q", string(bj))
	}
}
