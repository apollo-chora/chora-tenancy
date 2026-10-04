// transaction_ledger_read_internal_test.go — white-box (package pg) unit
// coverage for the ADR-205 read repo's pure helpers: cursor codecs, SQL
// WHERE / ORDER BY builders, the row scanners and the summary map-folder.
// The SQL-emit side (List/GetDetail/GetSummary/ListFranchisees) is
// exercised through the ScopeTxQuerier seam; the live-DB round-trips live
// in the //go:build integration sibling.
package pg

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// ----------------------------------------------------------------------------
// Local white-box stubs (the shared pg_test stubs live in the external
// package and are out of reach here).
// ----------------------------------------------------------------------------

type wbRow struct{ fn func(dest ...any) error }

func (r *wbRow) Scan(dest ...any) error { return r.fn(dest...) }

type wbRows struct {
	rows []func(dest ...any) error
	idx  int
	err  error
}

func (r *wbRows) Next() bool { return r.idx < len(r.rows) }
func (r *wbRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("wbRows: out of range")
	}
	fn := r.rows[r.idx]
	r.idx++
	return fn(dest...)
}
func (r *wbRows) Close() error { return nil }
func (r *wbRows) Err() error   { return r.err }

// wbScan fills dest positionally from vals, supporting the scan types the
// read repo uses.
func wbScan(vals ...any) func(dest ...any) error {
	return func(dest ...any) error {
		for i := range dest {
			if i >= len(vals) {
				break
			}
			switch d := dest[i].(type) {
			case *string:
				if v, ok := vals[i].(string); ok {
					*d = v
				}
			case **string:
				if vals[i] == nil {
					*d = nil
				} else if v, ok := vals[i].(string); ok {
					*d = &v
				}
			case *int64:
				if v, ok := vals[i].(int64); ok {
					*d = v
				}
			case **int64:
				if vals[i] == nil {
					*d = nil
				} else if v, ok := vals[i].(int64); ok {
					*d = &v
				}
			case *bool:
				if v, ok := vals[i].(bool); ok {
					*d = v
				}
			case *time.Time:
				if v, ok := vals[i].(time.Time); ok {
					*d = v
				}
			case **time.Time:
				if vals[i] == nil {
					*d = nil
				} else if v, ok := vals[i].(time.Time); ok {
					*d = &v
				}
			}
		}
		return nil
	}
}

// wbTx is a programmable Tx for the read repo's multi-query methods.
type wbTx struct {
	rowFns     []func(dest ...any) error
	rowIdx     int
	rowsSeq    []*wbRows
	rowsIdx    int
	queryErr   error
	queryCount int
	execErr    error
}

func (t *wbTx) Exec(context.Context, string, ...any) error { return t.execErr }
func (t *wbTx) Query(_ context.Context, _ string, _ ...any) (Rows, error) {
	t.queryCount++
	if t.queryErr != nil {
		return nil, t.queryErr
	}
	if t.rowsIdx >= len(t.rowsSeq) {
		return &wbRows{}, nil
	}
	r := t.rowsSeq[t.rowsIdx]
	t.rowsIdx++
	return r, nil
}
func (t *wbTx) QueryRow(_ context.Context, _ string, _ ...any) Row {
	if t.rowIdx >= len(t.rowFns) {
		return &wbRow{fn: func(dest ...any) error { return ErrNoRows }}
	}
	fn := t.rowFns[t.rowIdx]
	t.rowIdx++
	return &wbRow{fn: fn}
}

// wbScope satisfies ScopeTxQuerier, recording every scope opened.
type wbScope struct {
	tx     *wbTx
	scopes []string
}

func (s *wbScope) RunInScopeTx(_ context.Context, scopeID string, fn func(context.Context, Tx) error) error {
	s.scopes = append(s.scopes, scopeID)
	if s.tx == nil {
		s.tx = &wbTx{}
	}
	return fn(context.Background(), s.tx)
}

// aLedgerRow models one ledgerSelectCols scan for GetDetail/List parents.
func aLedgerRow() []any {
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	return []any{
		"ledger-1", ts, "tenant-1", "learner-1", "purchase", "payments",
		"pur_1", "TMS", "SGD", int64(4900), int64(0), "captured",
		true, `{"k":"v"}`, int64(4900),
	}
}

// ----------------------------------------------------------------------------
// Cursor codecs
// ----------------------------------------------------------------------------

func TestSortToken(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sort tl.SortSpec
		want string
	}{
		{tl.SortSpec{Field: tl.SortByOccurredAt}, "oa:a"},
		{tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true}, "oa:d"},
		{tl.SortSpec{Field: tl.SortByAmount}, "amt:a"},
		{tl.SortSpec{Field: tl.SortByAmount, Desc: true}, "amt:d"},
	}
	for _, tc := range cases {
		if got := sortToken(tc.sort); got != tc.want {
			t.Fatalf("sortToken(%+v) = %q want %q", tc.sort, got, tc.want)
		}
	}
}

func TestEncodeDecodeLedgerCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	// occurred-at sort.
	oaSort := tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true}
	tok, err := encodeLedgerCursor(oaSort, tl.LedgerRow{LedgerID: "l-1", OccurredAt: ts}, 0)
	if err != nil {
		t.Fatalf("encodeLedgerCursor: %v", err)
	}
	if tok == "" {
		t.Fatalf("expected non-empty token")
	}
	cur, err := decodeLedgerCursor(tok, oaSort)
	if err != nil {
		t.Fatalf("decodeLedgerCursor: %v", err)
	}
	if cur == nil || cur.ID != "l-1" || cur.OA == nil || !cur.OA.Equal(ts) {
		t.Fatalf("unexpected cursor %+v", cur)
	}
	// amount sort keys on the passed amountKey, not the row.
	amtSort := tl.SortSpec{Field: tl.SortByAmount, Desc: false}
	tok2, err := encodeLedgerCursor(amtSort, tl.LedgerRow{LedgerID: "l-2"}, 77)
	if err != nil {
		t.Fatalf("encodeLedgerCursor(amount): %v", err)
	}
	cur2, err := decodeLedgerCursor(tok2, amtSort)
	if err != nil {
		t.Fatalf("decodeLedgerCursor(amount): %v", err)
	}
	if cur2 == nil || cur2.Amt == nil || *cur2.Amt != 77 {
		t.Fatalf("unexpected amount cursor %+v", cur2)
	}
}

func TestDecodeLedgerCursor_Errors(t *testing.T) {
	t.Parallel()
	sort := tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true}
	if cur, err := decodeLedgerCursor("", sort); err != nil || cur != nil {
		t.Fatalf("empty cursor should be nil,nil; got %v %v", cur, err)
	}
	// Malformed base64.
	if _, err := decodeLedgerCursor("%%%", sort); err == nil {
		t.Fatalf("expected decode error for malformed token")
	}
	// Valid JSON but missing id.
	good := base64.URLEncoding.EncodeToString([]byte(`{"s":"oa:d"}`))
	if _, err := decodeLedgerCursor(good, sort); err == nil || !strings.Contains(err.Error(), "missing id") {
		t.Fatalf("expected missing-id error, got %v", err)
	}
	// Sort mismatch.
	c := ledgerCursor{Sort: "oa:d", ID: "l-1", OA: ptrTime(time.Time{})}
	enc, _ := encodeJSONCursor(c)
	if _, err := decodeLedgerCursor(enc, tl.SortSpec{Field: tl.SortByOccurredAt, Desc: false}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected sort-mismatch error, got %v", err)
	}
	// Missing amt for amount sort.
	c2 := ledgerCursor{Sort: "amt:d", ID: "l-1"}
	enc2, _ := encodeJSONCursor(c2)
	if _, err := decodeLedgerCursor(enc2, tl.SortSpec{Field: tl.SortByAmount, Desc: true}); err == nil || !strings.Contains(err.Error(), "missing amt") {
		t.Fatalf("expected missing-amt error, got %v", err)
	}
	// Missing oa for occurred_at sort.
	c3 := ledgerCursor{Sort: "oa:d", ID: "l-1"}
	enc3, _ := encodeJSONCursor(c3)
	if _, err := decodeLedgerCursor(enc3, sort); err == nil || !strings.Contains(err.Error(), "missing oa") {
		t.Fatalf("expected missing-oa error, got %v", err)
	}
}

func TestEncodeDecodeDetailCursor(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	tok, err := encodeDetailCursor(tl.DetailRow{DetailID: "d-1", OccurredAt: ts})
	if err != nil {
		t.Fatalf("encodeDetailCursor: %v", err)
	}
	d, err := decodeDetailCursor(tok)
	if err != nil {
		t.Fatalf("decodeDetailCursor: %v", err)
	}
	if d == nil || d.ID != "d-1" || !d.OA.Equal(ts) {
		t.Fatalf("unexpected detail cursor %+v", d)
	}
	if cur, err := decodeDetailCursor(""); cur != nil || err != nil {
		t.Fatalf("empty detail cursor should be nil,nil")
	}
	if _, err := decodeDetailCursor("%%%"); err == nil {
		t.Fatalf("expected decode error for malformed token")
	}
	c := detailCursor{}
	enc, _ := encodeJSONCursor(c)
	if _, err := decodeDetailCursor(enc); err == nil || !strings.Contains(err.Error(), "missing id") {
		t.Fatalf("expected missing-id error, got %v", err)
	}
}

func TestJSONCursorBuffer(t *testing.T) {
	t.Parallel()
	type wire struct {
		A string
		B int
	}
	enc, err := encodeJSONCursor(wire{A: "x", B: 3})
	if err != nil {
		t.Fatalf("encodeJSONCursor: %v", err)
	}
	var out wire
	if err := decodeJSONCursor(enc, &out); err != nil {
		t.Fatalf("decodeJSONCursor: %v", err)
	}
	if out.A != "x" || out.B != 3 {
		t.Fatalf("unexpected round-trip %+v", out)
	}
	if err := decodeJSONCursor("!!!", &out); err == nil {
		t.Fatalf("expected decode error for bad base64")
	}
	if _, err := encodeJSONCursor(make(chan int)); err == nil {
		t.Fatalf("expected marshal error for channel value")
	}
}

// ----------------------------------------------------------------------------
// SQL builder helpers
// ----------------------------------------------------------------------------

func TestWhereClauseAndCondJoin(t *testing.T) {
	t.Parallel()
	if got := whereClause(nil); got != "" {
		t.Fatalf("empty conds should yield empty string, got %q", got)
	}
	if got := whereClause([]string{"a", "b"}); got != "WHERE a AND b" {
		t.Fatalf("unexpected where %q", got)
	}
	if got := condJoin("", "x"); got != " WHERE x" {
		t.Fatalf("condJoin(empty) = %q", got)
	}
	if got := condJoin("WHERE a", "x"); got != " AND x" {
		t.Fatalf("condJoin(non-empty) = %q", got)
	}
}

func TestOrderByClause(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sort tl.SortSpec
		want string
	}{
		{tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true}, "ORDER BY l.occurred_at DESC, l.ledger_id DESC"},
		{tl.SortSpec{Field: tl.SortByOccurredAt, Desc: false}, "ORDER BY l.occurred_at ASC, l.ledger_id ASC"},
		{tl.SortSpec{Field: tl.SortByAmount, Desc: true}, "ORDER BY COALESCE(l.amount_minor, ABS(l.mana_units), 0) DESC, l.ledger_id DESC"},
		{tl.SortSpec{Field: tl.SortByAmount, Desc: false}, "ORDER BY COALESCE(l.amount_minor, ABS(l.mana_units), 0) ASC, l.ledger_id ASC"},
	}
	for _, tc := range cases {
		if got := orderByClause(tc.sort); got != tc.want {
			t.Fatalf("orderByClause(%+v) = %q want %q", tc.sort, got, tc.want)
		}
	}
}

func TestAppendCursorCond(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	oa := tl.SortSpec{Field: tl.SortByOccurredAt}
	conds, args := appendCursorCond(nil, nil, oa, &ledgerCursor{OA: &ts, ID: "l-1"})
	if len(conds) != 1 || !strings.Contains(conds[0], "(l.occurred_at, l.ledger_id) >") {
		t.Fatalf("unexpected oa cond %v", conds)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 cursor args, got %d", len(args))
	}
	amtCond, amtArgs := appendCursorCond(nil, nil, tl.SortSpec{Field: tl.SortByAmount, Desc: false}, &ledgerCursor{Amt: ptrInt64(50), ID: "l-2"})
	if len(amtCond) != 1 || !strings.Contains(amtCond[0], amountSortExpr) || !strings.Contains(amtCond[0], ">") {
		t.Fatalf("unexpected amount cond %v", amtCond)
	}
	if len(amtArgs) != 2 {
		t.Fatalf("expected 2 amount args, got %d", len(amtArgs))
	}
}

func TestCommonFilterConds(t *testing.T) {
	t.Parallel()
	f := tl.ReadFilters{
		Kind: "purchase", Status: "captured",
		From:             time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		To:               time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		LearnerGCIDs:     []string{"l-1", "l-2"},
		ManagedTenantIDs: []string{"m-1"},
	}
	conds, args := commonFilterConds(f)
	if len(conds) != 6 {
		t.Fatalf("expected 6 conds, got %v", conds)
	}
	joined := strings.Join(conds, " ")
	for _, want := range []string{"l.occurred_at >= $1", "l.occurred_at < $2", "l.kind = $3", "l.status = $4", "l.learner_gcid = ANY($5::uuid[])", "l.tenant_id = ANY($6::uuid[])"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing predicate %q in %s", want, joined)
		}
	}
	if len(args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(args))
	}
	base, baseArgs := commonFilterConds(tl.ReadFilters{})
	if len(base) != 2 || len(baseArgs) != 2 {
		t.Fatalf("window-only filters should yield 2 conds, got %v %v", base, baseArgs)
	}
}

// ----------------------------------------------------------------------------
// Row scanners
// ----------------------------------------------------------------------------

func TestScanLedgerRow_HappyAndNilFields(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	row, key, err := scanLedgerRow(wbScan(
		"ledger-1", ts, "tenant-1", nil, "mana_spend_daily", "creation",
		"atom_1", "mint", nil, nil, int64(-5), "posted",
		false, nil, int64(5),
	))
	if err != nil {
		t.Fatalf("scanLedgerRow: %v", err)
	}
	if row.LedgerID != "ledger-1" || row.LearnerGCID != "" || row.Currency != "" || row.MetadataJSON != "" {
		t.Fatalf("nil fields not dereferenced: %+v", row)
	}
	if row.AmountMinor != 0 || row.ManaUnits != -5 || row.HasDetail {
		t.Fatalf("scalar fields wrong: %+v", row)
	}
	if key != 5 {
		t.Fatalf("amount key not returned: %d", key)
	}
	if !row.OccurredAt.Equal(ts) {
		t.Fatalf("occurred_at not UTC-normalised: %v", row.OccurredAt)
	}
	// Scan error maps to ErrNoRows passthrough / wrapped err.
	if _, _, err := scanLedgerRow(func(dest ...any) error { return ErrNoRows }); !errors.Is(err, ErrNoRows) {
		t.Fatalf("expected ErrNoRows passthrough, got %v", err)
	}
	if _, _, err := scanLedgerRow(func(dest ...any) error { return errors.New("boom") }); !strings.Contains(err.Error(), "scan ledger row") {
		t.Fatalf("expected wrapped scan error, got %v", err)
	}
}

func TestScanDetailRow(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	d, err := scanDetailRow(wbScan(
		"detail-1", "ledger-1", "tenant-1", "learner-1", ts, "mint", int64(-3), "gpt-x", "tr-1", nil,
	))
	if err != nil {
		t.Fatalf("scanDetailRow: %v", err)
	}
	if d.DetailID != "detail-1" || d.ActionCode != "mint" || d.ManaUnits != -3 || d.Model != "gpt-x" {
		t.Fatalf("fields wrong: %+v", d)
	}
	if d.TraceID != "tr-1" || d.MetadataJSON != "" {
		t.Fatalf("nil/pointer fields wrong: %+v", d)
	}
	if _, err := scanDetailRow(func(dest ...any) error { return errors.New("boom") }); !strings.Contains(err.Error(), "scan detail row") {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

func TestScanMapInt64(t *testing.T) {
	t.Parallel()
	tx := &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{
			wbScan("SGD", int64(100)),
			wbScan("USD", int64(50)),
			wbScan(nil, int64(7)), // NULL key skipped
		}},
	}}
	dest := map[string]int64{}
	if err := scanMapInt64(context.Background(), tx, "SELECT currency, count(*)...", nil, dest); err != nil {
		t.Fatalf("scanMapInt64: %v", err)
	}
	if dest["SGD"] != 100 || dest["USD"] != 50 || len(dest) != 2 {
		t.Fatalf("unexpected fold %v", dest)
	}
	// Query error.
	tx2 := &wbTx{queryErr: errors.New("db down")}
	if err := scanMapInt64(context.Background(), tx2, "q", nil, map[string]int64{}); err == nil {
		t.Fatalf("expected query error")
	}
	// Scan error.
	tx3 := &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}},
	}}
	if err := scanMapInt64(context.Background(), tx3, "q", nil, map[string]int64{}); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected scan error, got %v", err)
	}
	// rows.Err.
	tx4 := &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{wbScan("SGD", int64(1))}, err: errors.New("iter boom")},
	}}
	if err := scanMapInt64(context.Background(), tx4, "q", nil, map[string]int64{}); !strings.Contains(err.Error(), "iter boom") {
		t.Fatalf("expected rows.Err error, got %v", err)
	}
}

func strPtr(s string) *string { return &s }
func ptrTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
func ptrInt64(v int64) *int64 { return &v }

// ----------------------------------------------------------------------------
// DB-path methods through the ScopeTxQuerier seam
// ----------------------------------------------------------------------------

func TestReadRepo_ListFranchisees(t *testing.T) {
	t.Parallel()
	s := &wbScope{tx: &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{
			wbScan("f-1", "Alpha"),
			wbScan("f-2", "Beta"),
		}},
	}}}
	repo := NewTransactionLedgerReadRepo(s)
	out, err := repo.ListFranchisees(context.Background(), "alp", 10, 0)
	if err != nil {
		t.Fatalf("ListFranchisees: %v", err)
	}
	if len(out) != 2 || out[0].TenantID != "f-1" || out[1].Name != "Beta" {
		t.Fatalf("unexpected franchisees %+v", out)
	}
	if len(s.scopes) != 1 || s.scopes[0] != tl.PlatformScope {
		t.Fatalf("expected platform scope, got %v", s.scopes)
	}
	// Default limit/offset normalisation + query error + scan error paths.
	s2 := &wbScope{tx: &wbTx{queryErr: errors.New("db down")}}
	r2 := NewTransactionLedgerReadRepo(s2)
	if _, err := r2.ListFranchisees(context.Background(), "", 0, -1); !strings.Contains(err.Error(), "list franchisees") {
		t.Fatalf("expected query error, got %v", err)
	}
	s3 := &wbScope{tx: &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}},
	}}}
	r3 := NewTransactionLedgerReadRepo(s3)
	if _, err := r3.ListFranchisees(context.Background(), "", 5, 0); !strings.Contains(err.Error(), "scan franchisee") {
		t.Fatalf("expected scan error, got %v", err)
	}
	var nilRepo *TransactionLedgerReadRepo
	if _, err := nilRepo.ListFranchisees(context.Background(), "", 5, 0); err == nil {
		t.Fatalf("expected unwired error")
	}
}

func TestReadRepo_GetDetail(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	parent := aLedgerRow()
	s := &wbScope{tx: &wbTx{
		rowFns: []func(dest ...any) error{wbScan(parent...)},
		rowsSeq: []*wbRows{
			{rows: []func(dest ...any) error{
				wbScan("detail-1", "ledger-1", "tenant-1", "learner-1", ts, "mint", int64(-3), "gpt-x", "tr-1", `{}`),
				wbScan("detail-2", "ledger-1", "tenant-1", "learner-1", ts, "mint", int64(-2), "gpt-x", "tr-2", `{}`),
			}},
		},
	}}
	repo := NewTransactionLedgerReadRepo(s)
	page, err := repo.GetDetail(context.Background(), tl.DetailQuery{LedgerID: "ledger-1", GUCTenantID: "tenant-1", Limit: 1})
	if err != nil {
		t.Fatalf("GetDetail: %v", err)
	}
	if !page.Found || page.Parent.LedgerID != "ledger-1" {
		t.Fatalf("parent not found %+v", page)
	}
	// limit=1 with 2 detail rows → truncate + next-page token.
	if len(page.Details) != 1 || page.NextPageToken == "" {
		t.Fatalf("expected 1 detail + cursor, got %+v", page)
	}
	// Parent not found.
	s2 := &wbScope{tx: &wbTx{}}
	r2 := NewTransactionLedgerReadRepo(s2)
	p2, err := r2.GetDetail(context.Background(), tl.DetailQuery{LedgerID: "nope", GUCTenantID: "tenant-1", Limit: 5})
	if err != nil {
		t.Fatalf("GetDetail(notfound): %v", err)
	}
	if p2.Found {
		t.Fatalf("expected not-found")
	}
	// Missing ledger id + invalid cursor + unwired repo errors.
	if _, err := r2.GetDetail(context.Background(), tl.DetailQuery{GUCTenantID: "t", Limit: 5}); err == nil {
		t.Fatalf("expected ledger_id error")
	}
	s3 := &wbScope{tx: &wbTx{
		rowFns: []func(dest ...any) error{wbScan(parent...)},
		rowsSeq: []*wbRows{
			{rows: []func(dest ...any) error{wbScan("d-1", "l-1", "t", "l", ts, "mint", int64(-1), nil, nil, nil)}},
		},
	}}
	r3 := NewTransactionLedgerReadRepo(s3)
	if _, err := r3.GetDetail(context.Background(), tl.DetailQuery{LedgerID: "l-1", GUCTenantID: "t", Limit: 5, Cursor: "%%%"}); err == nil {
		t.Fatalf("expected invalid-cursor error")
	}
	var nilRepo *TransactionLedgerReadRepo
	if _, err := nilRepo.GetDetail(context.Background(), tl.DetailQuery{LedgerID: "x"}); err == nil {
		t.Fatalf("expected unwired error")
	}
}

func TestReadRepo_GetSummary(t *testing.T) {
	t.Parallel()
	s := &wbScope{tx: &wbTx{
		rowFns: []func(dest ...any) error{
			wbScan(int64(3), int64(1500), int64(200)), // totals
		},
		rowsSeq: []*wbRows{
			{rows: []func(dest ...any) error{wbScan("SGD", int64(9800))}},
			{rows: []func(dest ...any) error{wbScan("purchase", int64(2))}},
			{rows: []func(dest ...any) error{wbScan("captured", int64(2))}},
		},
	}}
	repo := NewTransactionLedgerReadRepo(s)
	sum, err := repo.GetSummary(context.Background(), tl.SummaryQuery{
		GUCTenantID: "tenant-1",
		Filters: tl.ReadFilters{
			From: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		},
	})
	if err != nil {
		t.Fatalf("GetSummary: %v", err)
	}
	if sum.TotalCount != 3 || sum.TotalManaToppedUp != 1500 || sum.TotalManaSpent != 200 {
		t.Fatalf("totals wrong: %+v", sum)
	}
	if sum.AmountMinorByCurrency["SGD"] != 9800 || sum.CountByKind["purchase"] != 2 || sum.CountByStatus["captured"] != 2 {
		t.Fatalf("kpis wrong: %+v", sum)
	}
	if sum.WindowFrom.IsZero() || sum.WindowTo.IsZero() {
		t.Fatalf("window not propagated: %+v", sum)
	}
	// Totals-scan failure propagates.
	s2 := &wbScope{tx: &wbTx{rowFns: []func(dest ...any) error{func(dest ...any) error { return errors.New("db down") }}}}
	r2 := NewTransactionLedgerReadRepo(s2)
	if _, err := r2.GetSummary(context.Background(), tl.SummaryQuery{GUCTenantID: "t"}); !strings.Contains(err.Error(), "summary totals") {
		t.Fatalf("expected totals error, got %v", err)
	}
	// Currency-map folder failure.
	s3 := &wbScope{tx: &wbTx{
		rowFns:  []func(dest ...any) error{wbScan(int64(0), int64(0), int64(0))},
		rowsSeq: []*wbRows{{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("cur boom") }}}},
	}}
	r3 := NewTransactionLedgerReadRepo(s3)
	if _, err := r3.GetSummary(context.Background(), tl.SummaryQuery{GUCTenantID: "t"}); !strings.Contains(err.Error(), "summary by currency") {
		t.Fatalf("expected currency error, got %v", err)
	}
	// Unwired repo.
	var nilRepo *TransactionLedgerReadRepo
	if _, err := nilRepo.GetSummary(context.Background(), tl.SummaryQuery{}); err == nil {
		t.Fatalf("expected unwired error")
	}
}

func TestReadRepo_List_InvalidCursor(t *testing.T) {
	t.Parallel()
	s := &wbScope{tx: &wbTx{}}
	repo := NewTransactionLedgerReadRepo(s)
	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: "tenant-1", Cursor: "%%%", Limit: 20,
		Filters: tl.ReadFilters{From: time.Now().Add(-time.Hour), To: time.Now()},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid cursor") {
		t.Fatalf("expected invalid-cursor error, got %v", err)
	}
}

func TestReadRepo_List_HasMore_WithCursor(t *testing.T) {
	t.Parallel()
	rowsVals := [][]any{
		aLedgerRow(),
		func() []any {
			r := aLedgerRow()
			r[0] = "ledger-2"
			return r
		}(),
	}
	s := &wbScope{tx: &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{wbScan(rowsVals[0]...), wbScan(rowsVals[1]...)}},
	}}}
	repo := NewTransactionLedgerReadRepo(s)
	page, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: "tenant-1", Limit: 1,
		Filters: tl.ReadFilters{
			From: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			Sort: tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true},
		},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Items) != 1 || page.NextPageToken == "" {
		t.Fatalf("expected 1 item + next token, got %+v", page)
	}
	// Feed the token back through the repo with a cursor → keyset predicate.
	s2 := &wbScope{tx: &wbTx{rowsSeq: []*wbRows{
		{rows: []func(dest ...any) error{wbScan(rowsVals[1]...)}},
	}}}
	r2 := NewTransactionLedgerReadRepo(s2)
	page2, err := r2.List(context.Background(), tl.ListQuery{
		GUCTenantID: "tenant-1", Limit: 20, Cursor: page.NextPageToken,
		Filters: tl.ReadFilters{
			From: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			Sort: tl.SortSpec{Field: tl.SortByOccurredAt, Desc: true},
		},
	})
	if err != nil {
		t.Fatalf("List(cursor): %v", err)
	}
	if len(page2.Items) != 1 || page2.Items[0].LedgerID != "ledger-2" {
		t.Fatalf("unexpected page2 %+v", page2)
	}
}
