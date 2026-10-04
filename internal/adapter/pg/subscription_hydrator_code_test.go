// subscription_hydrator_code_test.go: CHO-2414 RED tests for the boot
// hydrator's add-on identity.
//
// The defect: LoadActive selected `a.name` (the human display name) and fed
// it to a SubscriptionRegistry keyed on add-on CODE, so only the rows whose
// display name happened to coincide with a valid code replayed. On the live
// estate that was 2 of 9, and /h/addons rendered empty for the four
// subscriptions Mighty Mind is billed for.
//
// Note the second half of the defect, which these tests pin but cannot fix
// on their own: migration 0021 backfilled add_ons.code as
// lower(regexp_replace(name,'[^a-zA-Z0-9]+','_','g')), a slug of the display
// name, so the stored code disagrees with the canonical catalogue code
// (training_management_suite vs tms). Selecting the code column is necessary
// but not sufficient; migration 0033 repairs the data.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

// hydStub satisfies both pg.QueryRunner and pg.TxQuerier so the hydrator's
// enumerate-then-per-tenant-tx shape is exercised without a live DB.
type hydStub struct {
	tenantIDs []string
	// subsByTenant maps tenant id to the (code, name, owner, pastDue) tuples
	// the per-tenant SELECT returns.
	subsByTenant map[string][][4]any
	txTenants    []string
	querySQL     []string
}

func (s *hydStub) Exec(context.Context, string, ...any) error { return nil }

func (s *hydStub) QueryRow(_ context.Context, _ string, _ ...any) pg.Row {
	return &hydRow{fn: func(...any) error { return pg.ErrNoRows }}
}

func (s *hydStub) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	s.txTenants = append(s.txTenants, tenantID)
	return fn(ctx, &hydTx{parent: s, tenantID: tenantID})
}

// Query serves the tenant enumeration (the non-tenant-scoped leg).
func (s *hydStub) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	s.querySQL = append(s.querySQL, sql)
	rows := make([]func(dest ...any) error, 0, len(s.tenantIDs))
	for _, id := range s.tenantIDs {
		id := id
		rows = append(rows, func(dest ...any) error {
			*(dest[0].(*string)) = id
			return nil
		})
	}
	return &hydRows{rows: rows}, nil
}

type hydTx struct {
	parent   *hydStub
	tenantID string
}

func (t *hydTx) Exec(context.Context, string, ...any) error { return nil }
func (t *hydTx) QueryRow(context.Context, string, ...any) pg.Row {
	return &hydRow{fn: func(...any) error { return pg.ErrNoRows }}
}

func (t *hydTx) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	t.parent.querySQL = append(t.parent.querySQL, sql)
	tuples := t.parent.subsByTenant[t.tenantID]
	rows := make([]func(dest ...any) error, 0, len(tuples))
	for _, tup := range tuples {
		tup := tup
		rows = append(rows, func(dest ...any) error {
			if len(dest) != 5 {
				return errors.New("hydTx: the subscription SELECT must project tenant, code, name, owner, past_due")
			}
			*(dest[0].(*string)) = t.tenantID
			*(dest[1].(*string)) = tup[0].(string)
			*(dest[2].(*string)) = tup[1].(string)
			*(dest[3].(*string)) = tup[2].(string)
			*(dest[4].(*bool)) = tup[3].(bool)
			return nil
		})
	}
	return &hydRows{rows: rows}, nil
}

type hydRow struct{ fn func(dest ...any) error }

func (r *hydRow) Scan(dest ...any) error { return r.fn(dest...) }

type hydRows struct {
	rows []func(dest ...any) error
	idx  int
}

func (r *hydRows) Next() bool { return r.idx < len(r.rows) }
func (r *hydRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("hydRows: out of range")
	}
	fn := r.rows[r.idx]
	r.idx++
	return fn(dest...)
}
func (r *hydRows) Close() error { return nil }
func (r *hydRows) Err() error   { return nil }

// ----------------------------------------------------------------------------

const (
	hydTenantMM  = "11111111-1111-7111-8111-111111111111"
	hydTenantACE = "019e899d-99d7-7064-a732-6c9ca6955cda"
	hydOwner     = "44444444-4444-7444-8444-444444444444"
)

// The defect, stated as a test: the row's AddOnCode must come from the code
// column, never from the display name.
func TestSubscriptionHydrator_LoadActive_UsesCodeNotDisplayName(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs: []string{hydTenantMM},
		subsByTenant: map[string][][4]any{
			hydTenantMM: {
				{"tms", "Training Management Suite", hydOwner, false},
				{"ai_assist", "AI Assist (Tier-4 Guardrail)", hydOwner, false},
			},
		},
	}
	rows, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background())
	if err != nil {
		t.Fatalf("LoadActive: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d (%+v)", len(rows), rows)
	}
	got := map[string]bool{rows[0].AddOnCode: true, rows[1].AddOnCode: true}
	for _, want := range []string{"tms", "ai_assist"} {
		if !got[want] {
			t.Errorf("AddOnCode %q missing; hydrator returned %+v", want, rows)
		}
	}
	for _, r := range rows {
		if strings.Contains(r.AddOnCode, " ") {
			t.Errorf("AddOnCode %q carries a display name, not a code", r.AddOnCode)
		}
	}
}

// A skip has to be diagnosable: the operator needs to know WHICH product
// failed to replay, and the code alone does not say that.
func TestSubscriptionHydrator_LoadActive_CarriesDisplayNameForDiagnostics(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs: []string{hydTenantMM},
		subsByTenant: map[string][][4]any{
			hydTenantMM: {{"pvp_arena", "PvP Arena", hydOwner, false}},
		},
	}
	rows, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background())
	if err != nil {
		t.Fatalf("LoadActive: %v", err)
	}
	if len(rows) != 1 || rows[0].AddOnName != "PvP Arena" {
		t.Fatalf("want the display name carried alongside the code, got %+v", rows)
	}
}

func TestSubscriptionHydrator_LoadActive_TranslatesLegacyCore(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs: []string{hydTenantACE},
		subsByTenant: map[string][][4]any{
			hydTenantACE: {{"core", "core", hydOwner, false}},
		},
	}
	rows, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background())
	if err != nil {
		t.Fatalf("LoadActive: %v", err)
	}
	if len(rows) != 1 || rows[0].AddOnCode != "base" {
		t.Fatalf("legacy core must translate to base, got %+v", rows)
	}
}

// add_ons.code is nullable pre-0021-backfill. An empty code must not be
// handed to the registry as if it were a real code.
func TestSubscriptionHydrator_LoadActive_FlagsEmptyCode(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs: []string{hydTenantMM},
		subsByTenant: map[string][][4]any{
			hydTenantMM: {{"", "Legacy Unbackfilled Add-On", hydOwner, false}},
		},
	}
	rows, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background())
	if err != nil {
		t.Fatalf("LoadActive must return the row for a loud skip, not fail the boot: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want the row surfaced, got %+v", rows)
	}
	if rows[0].AddOnCode != "" {
		t.Fatalf("an unbackfilled code must stay empty so the caller can skip it by name, got %q", rows[0].AddOnCode)
	}
	if rows[0].AddOnName != "Legacy Unbackfilled Add-On" {
		t.Fatalf("the display name must still identify the row: %+v", rows[0])
	}
}

func TestSubscriptionHydrator_LoadActive_RunsOneTenantScopedTxPerTenant(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs: []string{hydTenantMM, hydTenantACE},
		subsByTenant: map[string][][4]any{
			hydTenantMM:  {{"tms", "Training Management Suite", hydOwner, false}},
			hydTenantACE: {{"core", "core", hydOwner, true}},
		},
	}
	rows, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background())
	if err != nil {
		t.Fatalf("LoadActive: %v", err)
	}
	if len(s.txTenants) != 2 || s.txTenants[0] != hydTenantMM || s.txTenants[1] != hydTenantACE {
		t.Fatalf("want one tenant-scoped tx per tenant in order, got %v", s.txTenants)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %+v", rows)
	}
	for _, r := range rows {
		if r.AddOnCode == "core" && !r.PastDue {
			t.Errorf("past_due must survive hydration: %+v", r)
		}
	}
}

// Guard the defect class directly: the projection must not read the display
// name into the code position again.
func TestSubscriptionHydrator_LoadActive_ProjectsTheCodeColumn(t *testing.T) {
	t.Parallel()
	s := &hydStub{
		tenantIDs:    []string{hydTenantMM},
		subsByTenant: map[string][][4]any{hydTenantMM: {}},
	}
	if _, err := pg.NewSubscriptionHydratorFor(s, s).LoadActive(context.Background()); err != nil {
		t.Fatalf("LoadActive: %v", err)
	}
	var subSQL string
	for _, q := range s.querySQL {
		if strings.Contains(q, "add_on_subscriptions") {
			subSQL = q
		}
	}
	if subSQL == "" {
		t.Fatal("no add_on_subscriptions query was emitted")
	}
	if !strings.Contains(subSQL, "a.code") {
		t.Errorf("subscription projection must select a.code; SQL was:\n%s", subSQL)
	}
}
