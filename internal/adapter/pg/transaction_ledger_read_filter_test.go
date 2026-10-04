// transaction_ledger_read_filter_test.go — unit coverage (no live DB) for the
// SQL predicate shaping in commonFilterConds via List(). Fakes the scope-tx
// seam so we can assert the emitted SQL + bound args for the multi-id filters
// (ADR-205 / CHO-1930) without a Postgres round-trip. The full write→read
// round-trip lives in the //go:build integration sibling.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// ---- fakes for ScopeTxQuerier / Tx / Rows ----------------------------------

type captureTx struct {
	sql  string
	args []any
}

func (c *captureTx) Exec(context.Context, string, ...any) error { return nil }
func (c *captureTx) QueryRow(context.Context, string, ...any) pg.Row {
	return emptyRow{}
}
func (c *captureTx) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	c.sql, c.args = sql, args
	return emptyRows{}, nil
}

type emptyRow struct{}

func (emptyRow) Scan(...any) error { return pg.ErrNoRows }

type emptyRows struct{}

func (emptyRows) Next() bool        { return false }
func (emptyRows) Scan(...any) error { return nil }
func (emptyRows) Close() error      { return nil }
func (emptyRows) Err() error        { return nil }

// captureScopeTx runs fn against a shared captureTx, recording the scope id.
type captureScopeTx struct {
	scopeID string
	tx      *captureTx
}

func (c *captureScopeTx) RunInScopeTx(ctx context.Context, scopeID string, fn func(context.Context, pg.Tx) error) error {
	c.scopeID = scopeID
	c.tx = &captureTx{}
	return fn(ctx, c.tx)
}

// argContainsStringSlice reports whether any bound arg equals want (order-sensitive).
func argContainsStringSlice(args []any, want []string) bool {
	for _, a := range args {
		if s, ok := a.([]string); ok && len(s) == len(want) {
			eq := true
			for i := range s {
				if s[i] != want[i] {
					eq = false
					break
				}
			}
			if eq {
				return true
			}
		}
	}
	return false
}

// ---- tests -----------------------------------------------------------------

func TestReadRepo_List_MultiLearner_BindsAnyArray(t *testing.T) {
	t.Parallel()
	stx := &captureScopeTx{}
	repo := pg.NewTransactionLedgerReadRepo(stx)

	want := []string{"11111111-1111-7111-8111-111111111111", "22222222-2222-7222-8222-222222222222"}
	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b",
		Filters:     tl.ReadFilters{LearnerGCIDs: want},
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !strings.Contains(stx.tx.sql, "l.learner_gcid = ANY(") || !strings.Contains(stx.tx.sql, "::uuid[])") {
		t.Errorf("expected learner ANY(::uuid[]) predicate, sql=%q", stx.tx.sql)
	}
	if !argContainsStringSlice(stx.tx.args, want) {
		t.Errorf("learner id slice not bound as an arg: %#v", stx.tx.args)
	}
}

func TestReadRepo_List_SingleLearner_IsOneElementSet(t *testing.T) {
	t.Parallel()
	stx := &captureScopeTx{}
	repo := pg.NewTransactionLedgerReadRepo(stx)

	want := []string{"11111111-1111-7111-8111-111111111111"}
	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b",
		Filters:     tl.ReadFilters{LearnerGCIDs: want},
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Back-compat: a single id still routes through the ANY predicate (identical
	// result set to the former `= $N::uuid`).
	if !strings.Contains(stx.tx.sql, "l.learner_gcid = ANY(") {
		t.Errorf("single-id should still bind ANY, sql=%q", stx.tx.sql)
	}
	if !argContainsStringSlice(stx.tx.args, want) {
		t.Errorf("single learner id not bound: %#v", stx.tx.args)
	}
}

func TestReadRepo_List_MultiFranchisee_BindsTenantAnyArray(t *testing.T) {
	t.Parallel()
	stx := &captureScopeTx{}
	repo := pg.NewTransactionLedgerReadRepo(stx)

	// Master multi-select runs span-all ('platform') + the tenant_id predicate.
	want := []string{"0197aaaa-bbbb-7ccc-8ddd-eeeeffff0000", "0197bbbb-cccc-7ddd-8eee-ffff11112222"}
	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: tl.PlatformScope,
		Filters:     tl.ReadFilters{ManagedTenantIDs: want},
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !strings.Contains(stx.tx.sql, "l.tenant_id = ANY(") || !strings.Contains(stx.tx.sql, "::uuid[])") {
		t.Errorf("expected tenant_id ANY(::uuid[]) predicate, sql=%q", stx.tx.sql)
	}
	if !argContainsStringSlice(stx.tx.args, want) {
		t.Errorf("franchisee id slice not bound as an arg: %#v", stx.tx.args)
	}
	if stx.scopeID != tl.PlatformScope {
		t.Errorf("scope id = %q; want platform span-all", stx.scopeID)
	}
}

func TestReadRepo_List_NoFranchisee_NoTenantPredicate(t *testing.T) {
	t.Parallel()
	stx := &captureScopeTx{}
	repo := pg.NewTransactionLedgerReadRepo(stx)

	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: tl.PlatformScope,
		Filters:     tl.ReadFilters{},
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// tenant isolation is RLS — with no franchisee selection there must be NO
	// tenant_id SQL predicate (the deliberate span-all).
	if strings.Contains(stx.tx.sql, "l.tenant_id = ANY") {
		t.Errorf("no franchisee filter should emit no tenant predicate, sql=%q", stx.tx.sql)
	}
}

func TestReadRepo_List_NoLearner_NoLearnerPredicate(t *testing.T) {
	t.Parallel()
	stx := &captureScopeTx{}
	repo := pg.NewTransactionLedgerReadRepo(stx)

	_, err := repo.List(context.Background(), tl.ListQuery{
		GUCTenantID: tl.PlatformScope,
		Filters:     tl.ReadFilters{},
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// The SELECT list always contains the learner_gcid column; assert only that
	// no learner *predicate* (= ANY) was appended.
	if strings.Contains(stx.tx.sql, "learner_gcid = ANY") {
		t.Errorf("no learner filter should emit no learner predicate, sql=%q", stx.tx.sql)
	}
	if stx.scopeID != tl.PlatformScope {
		t.Errorf("scope id = %q; want platform span-all", stx.scopeID)
	}
}
