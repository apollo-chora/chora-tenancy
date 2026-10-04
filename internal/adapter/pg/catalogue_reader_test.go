package pg

import (
	"context"
	"errors"
	"strings"
	"testing"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// --- stub QueryRunner -------------------------------------------------

type catRow struct {
	code any
	name any
}

type catRows struct {
	rows   []catRow
	i      int
	err    error
	closed bool
}

func (r *catRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *catRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	if len(dest) != 2 {
		return errors.New("catRows.Scan: want 2 destinations")
	}
	// code is *string or *sql-null-ish; the reader must cope with NULL.
	switch d := dest[0].(type) {
	case *string:
		if row.code == nil {
			*d = ""
		} else {
			*d = row.code.(string)
		}
	case **string:
		if row.code == nil {
			*d = nil
		} else {
			s := row.code.(string)
			*d = &s
		}
	default:
		return errors.New("catRows.Scan: unexpected code destination")
	}
	switch d := dest[1].(type) {
	case *string:
		if row.name == nil {
			*d = ""
		} else {
			*d = row.name.(string)
		}
	default:
		return errors.New("catRows.Scan: unexpected name destination")
	}
	return nil
}

func (r *catRows) Close() error { r.closed = true; return nil }
func (r *catRows) Err() error   { return r.err }

type catQuerier struct {
	rows     *catRows
	queryErr error
	lastSQL  string
}

func (q *catQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	q.lastSQL = sql
	if q.queryErr != nil {
		return nil, q.queryErr
	}
	return q.rows, nil
}

func (q *catQuerier) Exec(ctx context.Context, sql string, args ...any) error { return nil }

func (q *catQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row { return nil }

// --- tests ------------------------------------------------------------

func TestCatalogueReader_ReadsCodeAndName(t *testing.T) {
	q := &catQuerier{rows: &catRows{rows: []catRow{
		{code: "core", name: "core"},
		{code: "tms", name: "Training Management Suite"},
	}}}
	r := NewCatalogueReader(q)

	rows, err := r.ReadCatalogue(context.Background())
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[1].Code != "tms" || rows[1].Name != "Training Management Suite" {
		t.Errorf("row 1 = %+v, want tms / Training Management Suite", rows[1])
	}
}

func TestCatalogueReader_NullCodeBecomesEmptyString(t *testing.T) {
	// A row that predates the migration-0021 backfill has code NULL. The
	// reader must surface it as an empty code so the domain reconciler
	// reports it by product name, not drop it.
	q := &catQuerier{rows: &catRows{rows: []catRow{
		{code: nil, name: "Legacy Bundle"},
	}}}
	rows, err := NewCatalogueReader(q).ReadCatalogue(context.Background())
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	if len(rows) != 1 || rows[0].Code != "" || rows[0].Name != "Legacy Bundle" {
		t.Fatalf("got %+v, want one row with an empty code named Legacy Bundle", rows)
	}
}

func TestCatalogueReader_QueryErrorIsReturnedNotSwallowed(t *testing.T) {
	// A read failure must never be reported as an empty catalogue: that
	// would turn a database outage into a drift report naming every plan
	// as missing, and the operator would go looking in the wrong place.
	q := &catQuerier{queryErr: errors.New("connection refused")}
	_, err := NewCatalogueReader(q).ReadCatalogue(context.Background())
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error must carry the cause, got %q", err)
	}
}

func TestCatalogueReader_RowsErrIsReturned(t *testing.T) {
	q := &catQuerier{rows: &catRows{
		rows: []catRow{{code: "core", name: "core"}},
		err:  errors.New("scan aborted"),
	}}
	_, err := NewCatalogueReader(q).ReadCatalogue(context.Background())
	if err == nil {
		t.Fatal("expected the rows error to surface, got nil")
	}
}

func TestCatalogueReader_ReadsFromAddOnsWithNoTenantPredicate(t *testing.T) {
	// add_ons is a GLOBAL catalogue with no RLS (0001_initial: RLS is
	// enabled on members and add_on_subscriptions, never on add_ons), so
	// this read correctly runs outside RunInTenantTx. Pin that, because
	// adding a tenant predicate here would silently empty the guard.
	q := &catQuerier{rows: &catRows{}}
	if _, err := NewCatalogueReader(q).ReadCatalogue(context.Background()); err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	// Collapse whitespace: the repo formats SQL with aligned keywords, and
	// this test is about which table and which predicates, not spacing.
	flat := strings.Join(strings.Fields(q.lastSQL), " ")
	if !strings.Contains(flat, "FROM add_ons") {
		t.Errorf("query must read add_ons, got %q", flat)
	}
	if strings.Contains(flat, "tenant_id") {
		t.Errorf("add_ons is global; the query must carry no tenant predicate, got %q", flat)
	}
}

func TestNewCatalogueReader_PanicsOnNilQuerier(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic on a nil querier so wiring bugs fail at boot")
		}
	}()
	NewCatalogueReader(nil)
}

func TestCatalogueReader_FeedsTheDomainReconciler(t *testing.T) {
	// The seam that matters: rows out of the adapter go straight into the
	// pure reconciler with no translation in between.
	q := &catQuerier{rows: &catRows{rows: []catRow{
		{code: "core", name: "core"},
		{code: "cplus_social", name: "ChoraCircle Social"},
	}}}
	rows, err := NewCatalogueReader(q).ReadCatalogue(context.Background())
	if err != nil {
		t.Fatalf("ReadCatalogue: %v", err)
	}
	drift := addon.ReconcileCatalogue(addon.NewSeedCatalogue(), rows)
	if drift == nil {
		t.Fatal("a two-row catalogue must drift against the 13-code seed")
	}
	if !strings.Contains(drift.Error(), "tms") {
		t.Errorf("drift must name the missing plans, got %q", drift.Error())
	}
}
