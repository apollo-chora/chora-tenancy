// legacy_tenancy_repository_test.go — unit tests for the pgx adapters that
// back the legacy /api/* HTTP layer (A7).
//
// These adapters satisfy the httpapi.TenantStore / AddOnStore /
// EntitlementStore ports and read/write the real chora_tenancy tables
// (tenants, add_ons, add_on_subscriptions). Stub queriers (no live DB)
// exercise the SQL emit + row scan + RLS-tx wiring; live Cloud SQL proof
// is in legacy_tenancy_integration_test.go.
//
// Reuses stubQuerier (tenant_repository_test.go) + stubQueryRunner /
// stubRows (familiar_egg_repository_test.go). Adds stubTxQuerier for the
// RLS-aware add_on_subscriptions path.
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	tenancy "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

// ----------------------------------------------------------------------------
// stub TxQuerier — records that a tenant-scoped tx was opened (SET LOCAL
// chora.tenant_id) and forwards Exec/Query/QueryRow to a recording stubTx.
// ----------------------------------------------------------------------------

// stubTx records the statements run inside a transaction. It satisfies pg.Tx.
type stubTx struct {
	execSQL  []string
	querySQL []string
	rowsByQ  map[string]*stubRows
	rowFn    func(sql string, args ...any) pg.Row
}

func (t *stubTx) Exec(_ context.Context, sql string, _ ...any) error {
	t.execSQL = append(t.execSQL, sql)
	return nil
}
func (t *stubTx) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	t.querySQL = append(t.querySQL, sql)
	for k, v := range t.rowsByQ {
		if strings.Contains(sql, k) {
			return v, nil
		}
	}
	return &stubRows{}, nil
}
func (t *stubTx) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	t.querySQL = append(t.querySQL, sql)
	if t.rowFn != nil {
		return t.rowFn(sql, args...)
	}
	return &stubRow{scanFn: func(dest ...any) error { return pg.ErrNoRows }}
}

// stubTxQuerier hands out a single stubTx and records that a tx was opened.
// It satisfies pg.TxQuerier.
type stubTxQuerier struct {
	tx       *stubTx
	beginErr error
	opened   int
}

func (q *stubTxQuerier) RunInTenantTx(ctx context.Context, _ string, fn func(context.Context, pg.Tx) error) error {
	if q.beginErr != nil {
		return q.beginErr
	}
	q.opened++
	if q.tx == nil {
		q.tx = &stubTx{}
	}
	return fn(ctx, q.tx)
}

// scanRow builds a stubRows row-fn from a positional value slice. Supports
// the dest types the legacy adapters scan into.
func scanRow(vals ...any) func(dest ...any) error {
	return func(dest ...any) error {
		for i := range dest {
			if i >= len(vals) {
				break
			}
			switch d := dest[i].(type) {
			case *string:
				*d = vals[i].(string)
			case *int64:
				*d = vals[i].(int64)
			case *int:
				*d = vals[i].(int)
			case *bool:
				*d = vals[i].(bool)
			case *[]byte:
				*d = vals[i].([]byte)
			case *time.Time:
				*d = vals[i].(time.Time)
			case **time.Time:
				if vals[i] == nil {
					*d = nil
				} else {
					v := vals[i].(time.Time)
					*d = &v
				}
			case **string:
				if vals[i] == nil {
					*d = nil
				} else {
					v := vals[i].(string)
					*d = &v
				}
			}
		}
		return nil
	}
}

// ----------------------------------------------------------------------------
// LegacyTenantStore — tenants table (NOT RLS-protected)
// ----------------------------------------------------------------------------

func TestLegacyTenantStore_Get_HappyPath(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{
			scanRow(
				"11111111-1111-7111-8111-111111111111",
				"Mighty Mind Tuition Agency",
				"", // parent_tenant_id
				[]byte(`{"primary_color":"#5B7FFF"}`),
				false,    // self_hosted
				"active", // status
				now, now, // created_at, updated_at
				nil, // deleted_at
			),
		},
	}
	store := pg.NewLegacyTenantStore(q)
	got, ok := store.Get("11111111-1111-7111-8111-111111111111")
	if !ok {
		t.Fatalf("expected ok=true for seeded tenant")
	}
	if got.DisplayName != "Mighty Mind Tuition Agency" {
		t.Errorf("DisplayName = %q", got.DisplayName)
	}
	if got.Status != tenancy.TenantStatusActive {
		t.Errorf("Status = %q", got.Status)
	}
	if len(q.rowCalls) != 1 || !strings.Contains(q.rowCalls[0].sql, "FROM tenants") {
		t.Errorf("expected SELECT FROM tenants, got %+v", q.rowCalls)
	}
	if !strings.Contains(q.rowCalls[0].sql, "deleted_at IS NULL") {
		t.Errorf("expected soft-delete filter, got %q", q.rowCalls[0].sql)
	}
}

func TestLegacyTenantStore_Get_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{} // empty → ErrNoRows
	store := pg.NewLegacyTenantStore(q)
	_, ok := store.Get("00000000-0000-7000-8000-000000000bad")
	if ok {
		t.Fatalf("expected ok=false for missing tenant")
	}
}

func TestLegacyTenantStore_Save_Upserts(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	store := pg.NewLegacyTenantStore(q)
	tn, err := tenancy.NewTenant("New School", false, "")
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	if err := store.Save(context.Background(), tn); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(q.execCalls))
	}
	if !strings.Contains(q.execCalls[0].sql, "INSERT INTO tenants") ||
		!strings.Contains(q.execCalls[0].sql, "ON CONFLICT") {
		t.Errorf("expected UPSERT INTO tenants, got %q", q.execCalls[0].sql)
	}
}

func TestLegacyTenantStore_List_ReturnsRows(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{
			{rows: []func(dest ...any) error{
				scanRow("11111111-1111-7111-8111-111111111111", "MTM", "", []byte(`{}`), false, "active", now, now, nil),
				scanRow("22222222-2222-7222-8222-222222222222", "Chen", "", []byte(`{}`), false, "active", now, now, nil),
			}},
		},
	}
	store := pg.NewLegacyTenantStore(q)
	items, total := store.List(0, 50)
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(items) != 2 {
		t.Errorf("len(items) = %d, want 2", len(items))
	}
}

// ----------------------------------------------------------------------------
// LegacyAddOnStore — add_ons table (NOT RLS-protected)
// ----------------------------------------------------------------------------

func TestLegacyAddOnStore_List_ReturnsRows(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{
			{rows: []func(dest ...any) error{
				scanRow("aaaa1111-0000-7000-8000-000000000001", "AI Assist", "AI-powered authoring", int64(4900), now),
			}},
		},
	}
	store := pg.NewLegacyAddOnStore(q)
	items := store.List()
	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	if items[0].Name != "AI Assist" {
		t.Errorf("Name = %q", items[0].Name)
	}
	if items[0].MonthlyPriceCents != 4900 {
		t.Errorf("MonthlyPriceCents = %d", items[0].MonthlyPriceCents)
	}
}

func TestLegacyAddOnStore_Get_HappyPath(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{
			scanRow("aaaa1111-0000-7000-8000-000000000001", "AI Assist", "AI-powered authoring", int64(4900), now),
		},
	}
	store := pg.NewLegacyAddOnStore(q)
	got, ok := store.Get("aaaa1111-0000-7000-8000-000000000001")
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if got.Name != "AI Assist" {
		t.Errorf("Name = %q", got.Name)
	}
}

func TestLegacyAddOnStore_Get_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{} // empty → ErrNoRows
	store := pg.NewLegacyAddOnStore(q)
	_, ok := store.Get("aaaa1111-0000-7000-8000-000000000bad")
	if ok {
		t.Fatalf("expected ok=false for missing add-on")
	}
}

func TestLegacyAddOnStore_Register_EmitsUpsert(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	store := pg.NewLegacyAddOnStore(q)
	a, err := tenancy.NewAddOn("AI Assist", "AI-powered authoring", 4900, []string{"ai.assist"})
	if err != nil {
		t.Fatalf("NewAddOn: %v", err)
	}
	store.Register(a)
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(q.execCalls))
	}
	if !strings.Contains(q.execCalls[0].sql, "INSERT INTO add_ons") ||
		!strings.Contains(q.execCalls[0].sql, "ON CONFLICT") {
		t.Errorf("expected UPSERT INTO add_ons, got %q", q.execCalls[0].sql)
	}
}

func TestLegacyAddOnStore_Register_NilNoop(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	store := pg.NewLegacyAddOnStore(q)
	store.Register(nil)
	if len(q.execCalls) != 0 {
		t.Errorf("expected nil add-on to be a no-op, got %d Exec calls", len(q.execCalls))
	}
}

func TestLegacyTenantStore_Save_NilNoop(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	store := pg.NewLegacyTenantStore(q)
	if err := store.Save(context.Background(), nil); err != nil {
		t.Fatalf("Save(nil) must be a no-op returning nil, got %v", err)
	}
	if len(q.execCalls) != 0 {
		t.Errorf("expected nil tenant to be a no-op, got %d Exec calls", len(q.execCalls))
	}
}

// ----------------------------------------------------------------------------
// LegacyEntitlementStore — add_on_subscriptions (RLS-protected → tenant tx)
// ----------------------------------------------------------------------------

func TestLegacyEntitlementStore_ListActiveByTenant_RunsInTenantTx(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tx := &stubTx{
		rowsByQ: map[string]*stubRows{
			"add_on_subscriptions": {rows: []func(dest ...any) error{
				// subscription_id, tenant_id, add_on_id,
				// monthly_price_cents_snap, status, started_at,
				// updated_at, cancelled_at, code (CHO-1717 §4.6)
				scanRow(
					"50b00001-0000-7000-8000-000000000001",
					"11111111-1111-7111-8111-111111111111",
					"aaaa1111-0000-7000-8000-000000000001",
					int64(4900), "active", now, now, nil,
					"cplus_social",
				),
			}},
		},
	}
	q := &stubTxQuerier{tx: tx}
	store := pg.NewLegacyEntitlementStore(q)

	items := store.ListActiveByTenant("11111111-1111-7111-8111-111111111111")
	if q.opened != 1 {
		t.Fatalf("expected exactly 1 tenant tx opened, got %d", q.opened)
	}
	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	if items[0].TenantID != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("TenantID = %q", items[0].TenantID)
	}
	if items[0].AddOnID != "aaaa1111-0000-7000-8000-000000000001" {
		t.Errorf("AddOnID = %q", items[0].AddOnID)
	}
	if items[0].Status != tenancy.EntitlementStatusActive {
		t.Errorf("Status = %q", items[0].Status)
	}
	if items[0].MonthlyPriceCentsSnapshot != 4900 {
		t.Errorf("MonthlyPriceCentsSnapshot = %d", items[0].MonthlyPriceCentsSnapshot)
	}
	// CHO-1717 §4.6 — the read JOINs add_ons to surface the stable
	// machine code (migration 0021 added add_ons.code).
	if items[0].AddOnCode != "cplus_social" {
		t.Errorf("AddOnCode = %q, want cplus_social", items[0].AddOnCode)
	}
	joined := strings.Join(tx.querySQL, " ")
	if !strings.Contains(joined, "add_on_subscriptions") {
		t.Errorf("expected query on add_on_subscriptions, got %q", joined)
	}
	if !strings.Contains(joined, "JOIN add_ons") {
		t.Errorf("expected JOIN add_ons in the read query, got %q", joined)
	}
	if !strings.Contains(joined, "code") {
		t.Errorf("expected add-on code column in the read query, got %q", joined)
	}
}

// NULL code (pre-backfill / legacy add-on rows) scans to an empty
// AddOnCode — the wire DTO pin allows addon_code:"" for legacy rows.
func TestLegacyEntitlementStore_ListActiveByTenant_NullCodeScansEmpty(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tx := &stubTx{
		rowsByQ: map[string]*stubRows{
			"add_on_subscriptions": {rows: []func(dest ...any) error{
				scanRow(
					"50b00001-0000-7000-8000-000000000002",
					"11111111-1111-7111-8111-111111111111",
					"aaaa1111-0000-7000-8000-000000000002",
					int64(900), "active", now, now, nil,
					nil, // code IS NULL
				),
			}},
		},
	}
	q := &stubTxQuerier{tx: tx}
	store := pg.NewLegacyEntitlementStore(q)

	items := store.ListActiveByTenant("11111111-1111-7111-8111-111111111111")
	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	if items[0].AddOnCode != "" {
		t.Errorf("AddOnCode = %q, want empty for NULL code", items[0].AddOnCode)
	}
}

func TestLegacyEntitlementStore_ListActiveByTenant_EmptyOnNoRows(t *testing.T) {
	t.Parallel()
	q := &stubTxQuerier{tx: &stubTx{}} // no rows configured
	store := pg.NewLegacyEntitlementStore(q)
	items := store.ListActiveByTenant("22222222-2222-7222-8222-222222222222")
	if len(items) != 0 {
		t.Errorf("expected empty slice, got %d items", len(items))
	}
	if items == nil {
		t.Errorf("expected non-nil empty slice (handler json-encodes it)")
	}
}

func TestLegacyEntitlementStore_Subscribe_RunsInTenantTx(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	// The INSERT ... ON CONFLICT ... RETURNING emits via QueryRow; the
	// stub returns the post-write row so Subscribe can scan it back.
	tx := &stubTx{
		rowFn: func(_ string, _ ...any) pg.Row {
			return &stubRow{scanFn: scanRow(
				"50b00001-0000-7000-8000-000000000099",
				"11111111-1111-7111-8111-111111111111",
				"aaaa1111-0000-7000-8000-000000000001",
				int64(4900), "active", now, now, nil,
				"cplus_social",
			)}
		},
	}
	q := &stubTxQuerier{tx: tx}
	store := pg.NewLegacyEntitlementStore(q)

	ent, err := store.Subscribe(
		"11111111-1111-7111-8111-111111111111",
		"aaaa1111-0000-7000-8000-000000000001",
		4900,
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if ent == nil {
		t.Fatalf("expected entitlement, got nil")
	}
	if q.opened != 1 {
		t.Errorf("expected 1 tenant tx, got %d", q.opened)
	}
	// CHO-1717 §4.6 — the write-path RETURNING also carries the add-on
	// code so the POST 201 response DTO includes addon_code.
	if ent.AddOnCode != "cplus_social" {
		t.Errorf("AddOnCode = %q, want cplus_social", ent.AddOnCode)
	}
	// The INSERT ... ON CONFLICT ... RETURNING runs via QueryRow inside
	// the tenant tx (it must read back the post-write row). Assert the
	// INSERT statement landed on the tx, regardless of Exec vs QueryRow.
	allTxSQL := append(append([]string{}, tx.execSQL...), tx.querySQL...)
	insertSeen := false
	for _, s := range allTxSQL {
		if strings.Contains(s, "INSERT INTO add_on_subscriptions") &&
			strings.Contains(s, "ON CONFLICT") {
			insertSeen = true
		}
	}
	if !insertSeen {
		t.Errorf("expected INSERT INTO add_on_subscriptions ON CONFLICT in the tx, got %v", allTxSQL)
	}
}

func TestLegacyEntitlementStore_Cancel_NotFound(t *testing.T) {
	t.Parallel()
	// stubTx with no rowFn → QueryRow returns ErrNoRows → Cancel maps to
	// domain.ErrEntitlementNotFound.
	q := &stubTxQuerier{tx: &stubTx{}}
	store := pg.NewLegacyEntitlementStore(q)
	_, err := store.Cancel(
		"11111111-1111-7111-8111-111111111111",
		"unknown-addon-id",
	)
	if err != tenancy.ErrEntitlementNotFound {
		t.Errorf("err = %v, want ErrEntitlementNotFound", err)
	}
}
