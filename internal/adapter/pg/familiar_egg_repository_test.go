// familiar_egg_repository_test.go — unit tests for the pgx FamiliarEgg
// repositories (Iter G.3, ADR-149).
//
// Stubs the QueryRunner seam to avoid a live Cloud SQL dependency. The
// SQL emit + scan paths are exercised; the actual Postgres semantics are
// covered by integration tests at M14.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

// catalogTestTenantID is the RLS scope the catalog SQL-emit tests run under.
const catalogTestTenantID = "01970000-0000-7000-8000-000000000010"

// stubQueryRunner satisfies pg.QueryRunner, pg.Tx and pg.TxQuerier with
// recording.
type stubQueryRunner struct {
	execCalls     []stubExecCall
	rowCalls      []stubRowCall
	queryCalls    []stubExecCall
	rowResponses  []func(dest ...any) error
	rowsResponses []*stubRows
	execErr       error
	queryErr      error
	txTenants     []string
}

// RunInTenantTx satisfies pg.TxQuerier. This stub does NOT model RLS: it
// records the scope and runs fn against itself, keeping the SQL-emit and
// scan assertions below focused on the statement rather than the policy.
// Policy behaviour is covered by familiar_egg_catalog_rls_test.go.
func (s *stubQueryRunner) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	s.txTenants = append(s.txTenants, tenantID)
	return fn(ctx, s)
}

func (s *stubQueryRunner) Exec(_ context.Context, sql string, args ...any) error {
	s.execCalls = append(s.execCalls, stubExecCall{sql: sql, args: args})
	return s.execErr
}
func (s *stubQueryRunner) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	s.rowCalls = append(s.rowCalls, stubRowCall{sql: sql, args: args})
	if len(s.rowResponses) == 0 {
		return &stubRow{scanFn: func(dest ...any) error { return pg.ErrNoRows }}
	}
	r := s.rowResponses[0]
	s.rowResponses = s.rowResponses[1:]
	return &stubRow{scanFn: r}
}
func (s *stubQueryRunner) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	s.queryCalls = append(s.queryCalls, stubExecCall{sql: sql, args: args})
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	if len(s.rowsResponses) == 0 {
		return &stubRows{}, nil
	}
	r := s.rowsResponses[0]
	s.rowsResponses = s.rowsResponses[1:]
	return r, nil
}

type stubRows struct {
	rows []func(dest ...any) error
	idx  int
	err  error
}

func (r *stubRows) Next() bool {
	return r.idx < len(r.rows)
}
func (r *stubRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("stubRows: out of range")
	}
	fn := r.rows[r.idx]
	r.idx++
	return fn(dest...)
}
func (r *stubRows) Close() error { return nil }
func (r *stubRows) Err() error   { return r.err }

// -----------------------------------------------------------------------------

func TestPurchaseRepository_Insert_EmitsExpectedSQL(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewPurchaseRepository(q)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "01970000-0000-7000-8000-000000000001",
		TenantID:          "01970000-0000-7000-8000-000000000010",
		PurchaserGCID:     "01970000-0000-7000-8000-000000000020",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_1",
		StripeCheckoutURL: "https://stripe.example/c1",
		Now:               time.Now().UTC(),
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
	if err := r.Insert(context.Background(), p); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(q.execCalls))
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "INSERT INTO familiar_egg_purchases") {
		t.Errorf("expected INSERT INTO familiar_egg_purchases; got %q", got)
	}
}

func TestPurchaseRepository_Insert_NilReturnsError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewPurchaseRepository(q)
	if err := r.Insert(context.Background(), nil); err == nil {
		t.Fatalf("expected nil purchase error")
	}
}

func TestPurchaseRepository_Insert_DetectsDuplicateStripeSession(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{execErr: errors.New("ERROR: duplicate key value violates unique constraint \"familiar_egg_purchases_stripe_session_id_key\" (SQLSTATE 23505)")}
	r := pg.NewPurchaseRepository(q)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:      "01970000-0000-7000-8000-000000000001",
		TenantID:        "01970000-0000-7000-8000-000000000010",
		PurchaserGCID:   "01970000-0000-7000-8000-000000000020",
		EggSKU:          "egg.standard.v1",
		AmountCents:     999,
		Currency:        "SGD",
		StripeSessionID: "cs_dup",
		Now:             time.Now().UTC(),
		SoftExpiryDays:  30,
		HardExpiryDays:  60,
	})
	err := r.Insert(context.Background(), p)
	if !errors.Is(err, pg.ErrDuplicateStripeSession) {
		t.Fatalf("expected ErrDuplicateStripeSession; got %v", err)
	}
}

func TestPurchaseRepository_UpdateState_EmitsUpdateSQL(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewPurchaseRepository(q)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:      "01970000-0000-7000-8000-000000000001",
		TenantID:        "01970000-0000-7000-8000-000000000010",
		PurchaserGCID:   "01970000-0000-7000-8000-000000000020",
		EggSKU:          "egg.standard.v1",
		AmountCents:     999,
		Currency:        "SGD",
		StripeSessionID: "cs_test_2",
		Now:             time.Now().UTC(),
		SoftExpiryDays:  30,
		HardExpiryDays:  60,
	})
	_ = p.MarkPaid(time.Now().UTC().Add(time.Minute), "pi_x", "ch_x", 999)
	if err := r.UpdateState(context.Background(), p); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "UPDATE familiar_egg_purchases") {
		t.Errorf("expected UPDATE familiar_egg_purchases; got %q", got)
	}
}

func TestPurchaseRepository_UpdateState_NilReturnsError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewPurchaseRepository(q)
	if err := r.UpdateState(context.Background(), nil); err == nil {
		t.Fatalf("expected nil purchase error")
	}
}

func TestPurchaseRepository_GetByStripeSessionID_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewPurchaseRepository(q)
	_, ok, err := r.GetByStripeSessionID(context.Background(), "cs_unknown")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false")
	}
}

func TestPurchaseRepository_GetByStripeSessionID_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{purchaseScanFn("cs_known", "paid")},
	}
	r := pg.NewPurchaseRepository(q)
	p, ok, err := r.GetByStripeSessionID(context.Background(), "cs_known")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok || p == nil {
		t.Fatalf("ok=%v p=%v", ok, p)
	}
	if p.StripeSessionID != "cs_known" {
		t.Errorf("session_id: got %q", p.StripeSessionID)
	}
	if p.State != familiareag.StatePaid {
		t.Errorf("state: got %q", p.State)
	}
}

func TestPurchaseRepository_FindHardExpiryCandidates_ReturnsRows(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{
			rows: []func(dest ...any) error{
				purchaseScanFn("cs_a", "paid"),
				purchaseScanFn("cs_b", "paid"),
			},
		}},
	}
	r := pg.NewPurchaseRepository(q)
	rows, err := r.FindHardExpiryCandidates(context.Background(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2", len(rows))
	}
}

func TestCatalogRepository_GetBySKU_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{catalogScanFn("egg.test.v1", "")},
	}
	r := pg.NewCatalogRepository(q)
	e, ok, err := r.GetBySKU(context.Background(), catalogTestTenantID, "egg.test.v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok || e == nil {
		t.Fatalf("ok=%v entry=%v", ok, e)
	}
	if e.SKU != "egg.test.v1" {
		t.Errorf("sku: got %q", e.SKU)
	}
}

func TestCatalogRepository_GetBySKU_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewCatalogRepository(q)
	_, ok, err := r.GetBySKU(context.Background(), catalogTestTenantID, "egg.missing.v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false")
	}
}

func TestCatalogRepository_ListForTenant_QueriesWithTenant(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{
			rows: []func(dest ...any) error{
				catalogScanFn("egg.trial.v1", ""),                                         // platform
				catalogScanFn("egg.skillflow.v1", "01970000-0000-7000-8000-000000000010"), // tenant
			},
		}},
	}
	r := pg.NewCatalogRepository(q)
	list, err := r.ListForTenant(context.Background(), "01970000-0000-7000-8000-000000000010", time.Now().UTC())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("got %d entries, want 2", len(list))
	}
	got := q.queryCalls[0].sql
	if !strings.Contains(got, "tenant_id IS NULL OR tenant_id = $1") {
		t.Errorf("expected platform OR tenant filter; got %q", got)
	}
}

func TestCatalogRepository_Upsert_EmitsOnConflictDoUpdate(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewCatalogRepository(q)
	now := time.Now().UTC()
	e := &familiareag.CatalogEntry{
		SKU:               "egg.test.v1",
		TenantID:          catalogTestTenantID,
		DisplayName:       "Test Egg",
		PriceCents:        1500,
		Currency:          "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 50, "fox": 50},
		Purchasable:       true,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := r.Upsert(context.Background(), e); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "INSERT INTO familiar_egg_catalog") {
		t.Errorf("expected INSERT INTO familiar_egg_catalog; got %q", got)
	}
	if !strings.Contains(got, "ON CONFLICT (sku) DO UPDATE") {
		t.Errorf("expected ON CONFLICT DO UPDATE; got %q", got)
	}
}

func TestCatalogRepository_Upsert_NilReturnsError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewCatalogRepository(q)
	if err := r.Upsert(context.Background(), nil); err == nil {
		t.Fatalf("expected nil entry error")
	}
}

func TestCatalogRepository_SoftDelete(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewCatalogRepository(q)
	if err := r.SoftDelete(context.Background(), catalogTestTenantID, "egg.test.v1"); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "UPDATE familiar_egg_catalog") || !strings.Contains(got, "deleted_at = now()") {
		t.Errorf("expected soft-delete UPDATE; got %q", got)
	}
}

// -----------------------------------------------------------------------------
// scan helpers
// -----------------------------------------------------------------------------

func purchaseScanFn(sessionID string, state string) func(dest ...any) error {
	now := time.Now().UTC()
	soft := now.AddDate(0, 0, 30)
	hard := now.AddDate(0, 0, 60)
	return func(dest ...any) error {
		// columns: 29
		mustString(dest[0], "01970000-0000-7000-8000-000000000001")
		mustString(dest[1], "01970000-0000-7000-8000-000000000010")
		mustString(dest[2], "01970000-0000-7000-8000-000000000020")
		mustString(dest[3], "egg.standard.v1")
		mustString(dest[4], "")
		mustString(dest[5], state)
		mustInt64(dest[6], 999)
		mustInt64(dest[7], 999)
		mustInt64(dest[8], 0)
		mustString(dest[9], "SGD")
		mustString(dest[10], sessionID)
		mustString(dest[11], "")
		mustString(dest[12], "")
		mustString(dest[13], "")
		mustString(dest[14], "")
		mustString(dest[15], "")
		mustString(dest[16], "")
		mustString(dest[17], "")
		mustBool(dest[18], false)
		mustTime(dest[19], now)
		mustNullTime(dest[20])
		mustNullTime(dest[21])
		mustNullTime(dest[22])
		mustNullTime(dest[23])
		mustNullTime(dest[24])
		mustTime(dest[25], soft)
		mustTime(dest[26], hard)
		mustTime(dest[27], now)
		mustTime(dest[28], now)
		return nil
	}
}

func catalogScanFn(sku, tenantID string) func(dest ...any) error {
	now := time.Now().UTC()
	return func(dest ...any) error {
		mustString(dest[0], sku)
		mustString(dest[1], tenantID)
		mustString(dest[2], "Test")
		mustString(dest[3], "")
		mustInt64(dest[4], 1500)
		mustString(dest[5], "SGD")
		mustString(dest[6], "")
		mustString(dest[7], `{"owl":50,"fox":50}`)
		mustTime(dest[8], now)
		mustBool(dest[9], true)
		mustBool(dest[10], false)
		mustInt(dest[11], 30)
		mustInt(dest[12], 60)
		mustNullTime(dest[13])
		mustNullTime(dest[14])
		mustTime(dest[15], now)
		mustTime(dest[16], now)
		mustNullTime(dest[17])
		return nil
	}
}

func mustString(dst any, v string) {
	switch p := dst.(type) {
	case *string:
		*p = v
	}
}
func mustInt64(dst any, v int64) {
	switch p := dst.(type) {
	case *int64:
		*p = v
	}
}
func mustInt(dst any, v int) {
	switch p := dst.(type) {
	case *int:
		*p = v
	}
}
func mustBool(dst any, v bool) {
	switch p := dst.(type) {
	case *bool:
		*p = v
	}
}
func mustTime(dst any, v time.Time) {
	switch p := dst.(type) {
	case *time.Time:
		*p = v
	}
}
func mustNullTime(dst any) {
	// already zero-valued; pgx-like sql.NullTime stays Valid=false.
	_ = dst
}
