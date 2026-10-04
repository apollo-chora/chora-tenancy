// extra_repos_test.go — unit coverage (no live DB) for the pg repos that
// were previously only integration-tested: AllocationRepository, the
// Setup-Wizard add-on selections repo, the atom_count projection write
// repo and the tenant-hierarchy repo. All four are exercised through the
// package's existing Querier / QueryRunner / TxQuerier / ScopeTxQuerier
// seams; see *-repository_test.go siblings for the same pattern.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
)

// ----------------------------------------------------------------------------
// AllocationRepository — Querier-backed (0009 schema)
// ----------------------------------------------------------------------------

func TestAllocationRepository_Get_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowResponses = []func(dest ...any) error{
		scanRow(
			"alloc-1", "tenant-1", "pool-1", "gcid-1", int64(500), "pending",
			"egg_hard_expiry_refund", "platform",
			time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
			time.Time{}, time.Time{}, "", "platform", "entry-1", int64(500),
			time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), "expiry-p1",
		),
	}
	repo := pg.NewAllocationRepository(q)
	a, err := repo.Get(context.Background(), "alloc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if a == nil || a.AllocationID != "alloc-1" || a.TenantID != "tenant-1" || a.Units != 500 {
		t.Fatalf("unexpected allocation %+v", a)
	}
	if a.Status != allocation.StatusPending || a.Reason != allocation.ReasonEggHardExpiryRefund {
		t.Fatalf("status/reason not mapped: %+v", a)
	}
	if a.IdempotencyKey != "expiry-p1" || a.FirstDebitEntryID != "entry-1" {
		t.Fatalf("idempotency fields not mapped: %+v", a)
	}
	if a.ExpiresAt == nil || a.ExpiresAt.Year() != 2026 {
		t.Fatalf("expires_at not scanned: %+v", a)
	}
}

func TestAllocationRepository_Get_NotfoundAndValidation(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default QueryRow → pg.ErrNoRows
	repo := pg.NewAllocationRepository(q)
	_, err := repo.Get(context.Background(), "no-such-alloc")
	if !errors.Is(err, pg.ErrNoRows) {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
	if _, err := repo.Get(context.Background(), "   "); err == nil {
		t.Fatalf("expected error for blank allocation id")
	}
	// Non-not-found scan error wraps.
	q2 := &stubQuerier{}
	q2.rowResponses = []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}
	r2 := pg.NewAllocationRepository(q2)
	if _, err := r2.Get(context.Background(), "alloc-2"); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected scan error to wrap, got %v", err)
	}
}

func TestAllocationRepository_GetByIdempotencyKey(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowResponses = []func(dest ...any) error{
		scanRow("alloc-9", "tenant-9", "", "gcid-9", int64(100), "pending",
			"egg_hard_expiry_refund", "platform", time.Now(), nil, nil, nil,
			"", "", "", int64(100), time.Now(), "expiry-p9"),
	}
	repo := pg.NewAllocationRepository(q)
	a, err := repo.GetByIdempotencyKey(context.Background(), "expiry-p9")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if a == nil || a.AllocationID != "alloc-9" {
		t.Fatalf("unexpected allocation %+v", a)
	}
	if !strings.Contains(q.rowCalls[0].sql, "idempotency_key = $1") {
		t.Fatalf("expected idempotency-key lookup, sql=%q", q.rowCalls[0].sql)
	}
	// Blank key short-circuits to ErrNoRows without a query.
	q2 := &stubQuerier{}
	repo2 := pg.NewAllocationRepository(q2)
	if _, err := repo2.GetByIdempotencyKey(context.Background(), "  "); !errors.Is(err, pg.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for blank key, got %v", err)
	}
	if len(q2.rowCalls) != 0 {
		t.Fatalf("expected no query for blank key")
	}
	if _, err := repo2.GetByIdempotencyKey(context.Background(), "not-found"); !errors.Is(err, pg.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for unknown key, got %v", err)
	}
}

func TestAllocationRepository_Save(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	base := &allocation.TenantManaAllocation{
		AllocationID:    "alloc-s1",
		TenantID:        "tenant-1",
		SourcePoolID:    "pool-1",
		GCID:            "gcid-1",
		Units:           250,
		Status:          allocation.StatusPending,
		Reason:          allocation.ReasonEggHardExpiryRefund,
		AllocatedByGCID: "platform",
		AllocatedAt:     now,
		UpdatedAt:       now,
		IdempotencyKey:  "expiry-p1",
	}
	q := &stubQuerier{}
	repo := pg.NewAllocationRepository(q)
	if err := repo.Save(context.Background(), base); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(q.execCalls))
	}
	got := q.execCalls[0]
	if !strings.Contains(got.sql, "ON CONFLICT (idempotency_key)") {
		t.Fatalf("expected idempotency-key conflict clause, sql=%q", got.sql)
	}
	if len(got.args) != 18 {
		t.Fatalf("expected 18 bound args, got %d", len(got.args))
	}
	// RemainingUnits == 0 → NULL placeholder.
	if got.args[15] != nil {
		t.Fatalf("expected nil remaining_units placeholder, got %#v", got.args[15])
	}
	// Nil allocation guard.
	if err := repo.Save(context.Background(), nil); err == nil {
		t.Fatalf("expected error for nil allocation")
	}
	// Positive remaining units → concrete value bound.
	base.RemainingUnits = 120
	q2 := &stubQuerier{}
	repo2 := pg.NewAllocationRepository(q2)
	if err := repo2.Save(context.Background(), base); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := q2.execCalls[0].args[15]; got != int64(120) {
		t.Fatalf("expected remaining_units bound, got %#v", got)
	}
}

// ----------------------------------------------------------------------------
// SetupWizardAddonRepository — TxQuerier-backed (CHO-1692)
// ----------------------------------------------------------------------------

func TestSetupWizardAddonRepository_Upsert(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	repo := pg.NewSetupWizardAddonRepository(q)
	if err := repo.Upsert(context.Background(), "tenant-1", "tms", "pending_activation"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if len(q.txTenants) != 1 || q.txTenants[0] != "tenant-1" {
		t.Fatalf("expected tenant-scoped tx, got %v", q.txTenants)
	}
	if len(q.execCalls) != 1 || !strings.Contains(q.execCalls[0].sql, "setup_wizard_addon_selections") {
		t.Fatalf("expected selection upsert, got %v", q.execCalls)
	}
	if len(q.execCalls) != 1 || q.execCalls[0].args[0] != "tenant-1" || q.execCalls[0].args[1] != "tms" || q.execCalls[0].args[2] != "pending_activation" {
		t.Fatalf("unexpected upsert args %v", q.execCalls)
	}
	// Missing tenant / code rejected before any tx.
	q2 := &stubQueryRunner{}
	r2 := pg.NewSetupWizardAddonRepository(q2)
	if err := r2.Upsert(context.Background(), "", "tms", "pending_activation"); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
	if err := r2.Upsert(context.Background(), "tenant-1", "", "pending_activation"); err == nil {
		t.Fatalf("expected error for empty code")
	}
	if len(q2.txTenants) != 0 {
		t.Fatalf("expected no tx for invalid input")
	}
	// Exec failure propagates through the tx.
	q3 := &stubQueryRunner{execErr: errors.New("db down")}
	r3 := pg.NewSetupWizardAddonRepository(q3)
	if err := r3.Upsert(context.Background(), "tenant-1", "tms", "pending_activation"); !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected db error to propagate, got %v", err)
	}
}

func TestSetupWizardAddonRepository_ReplaceSet(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	repo := pg.NewSetupWizardAddonRepository(q)
	if err := repo.ReplaceSet(context.Background(), "tenant-1", []string{"tms", "cms"}); err != nil {
		t.Fatalf("ReplaceSet: %v", err)
	}
	// 2 upserts + 1 delete-with-IN.
	if len(q.execCalls) != 3 {
		t.Fatalf("expected 3 execs (2 upsert + 1 delete), got %d", len(q.execCalls))
	}
	del := q.execCalls[2]
	if !strings.Contains(del.sql, "add_on_code NOT IN") {
		t.Fatalf("expected NOT IN delete, sql=%q", del.sql)
	}
	if !strings.Contains(del.sql, "$2") || !strings.Contains(del.sql, "$3") {
		t.Fatalf("expected parameterised IN placeholders, sql=%q", del.sql)
	}
	if got := len(del.args); got != 3 {
		t.Fatalf("expected 3 delete args (tenant + 2 codes), got %d", got)
	}
	// Empty codes → DELETE-all branch.
	q2 := &stubQueryRunner{}
	r2 := pg.NewSetupWizardAddonRepository(q2)
	if err := r2.ReplaceSet(context.Background(), "tenant-1", nil); err != nil {
		t.Fatalf("ReplaceSet(nil): %v", err)
	}
	if len(q2.execCalls) != 1 || !strings.Contains(q2.execCalls[0].sql, "DELETE FROM setup_wizard_addon_selections WHERE tenant_id = $1") {
		t.Fatalf("expected delete-all, got %v", q2.execCalls)
	}
	// Empty-string codes skipped; a lone valid code still executes.
	q3 := &stubQueryRunner{}
	r3 := pg.NewSetupWizardAddonRepository(q3)
	if err := r3.ReplaceSet(context.Background(), "tenant-1", []string{"tms", "", "cms"}); err != nil {
		t.Fatalf("ReplaceSet with blanks: %v", err)
	}
	if len(q3.execCalls) != 3 {
		t.Fatalf("expected 3 execs (blank skipped), got %d", len(q3.execCalls))
	}
	// Missing tenant rejected.
	if err := r3.ReplaceSet(context.Background(), "", []string{"tms"}); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
}

func TestSetupWizardAddonRepository_ListByTenant(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	q.rowsResponses = []*stubRows{
		{rows: []func(dest ...any) error{
			scanRow("tms", "pending_activation"),
			scanRow("cms", "pending_activation"),
		}},
	}
	repo := pg.NewSetupWizardAddonRepository(q)
	got, err := repo.ListByTenant(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(got) != 2 || got[0].AddOnCode != "tms" || got[1].Status != "pending_activation" {
		t.Fatalf("unexpected selections %+v", got)
	}
	if len(q.txTenants) != 1 || q.txTenants[0] != "tenant-1" {
		t.Fatalf("expected tenant-scoped read, got %v", q.txTenants)
	}
	// Empty result set is a valid empty slice.
	q2 := &stubQueryRunner{}
	r2 := pg.NewSetupWizardAddonRepository(q2)
	out, err := r2.ListByTenant(context.Background(), "tenant-2")
	if err != nil || len(out) != 0 {
		t.Fatalf("expected empty list, got %v err %v", out, err)
	}
	// Scan failure propagates.
	q3 := &stubQueryRunner{}
	q3.rowsResponses = []*stubRows{
		{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}},
	}
	r3 := pg.NewSetupWizardAddonRepository(q3)
	if _, err := r3.ListByTenant(context.Background(), "tenant-3"); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected scan error, got %v", err)
	}
	// rows.Err propagates.
	q4 := &stubQueryRunner{}
	q4.rowsResponses = []*stubRows{
		{rows: []func(dest ...any) error{scanRow("tms", "pending_activation")}, err: errors.New("iter boom")},
	}
	r4 := pg.NewSetupWizardAddonRepository(q4)
	if _, err := r4.ListByTenant(context.Background(), "tenant-4"); !strings.Contains(err.Error(), "iter boom") {
		t.Fatalf("expected rows.Err error, got %v", err)
	}
	// Missing tenant rejected.
	if _, err := r4.ListByTenant(context.Background(), ""); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
}

// ----------------------------------------------------------------------------
// AtomCountWriteRepo — TxQuerier-backed (CHO-2011)
// ----------------------------------------------------------------------------

func TestAtomCountWriteRepo_ApplyDelta(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	repo := pg.NewAtomCountWriteRepo(q)
	if err := repo.ApplyDelta(context.Background(), "tenant-1", 1); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if len(q.txTenants) != 1 || q.txTenants[0] != "tenant-1" {
		t.Fatalf("expected tenant-scoped tx under tenant RLS, got %v", q.txTenants)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(q.execCalls))
	}
	ec := q.execCalls[0]
	if !strings.Contains(ec.sql, "tenant_atom_counts") || !strings.Contains(ec.sql, "ON CONFLICT") {
		t.Fatalf("expected counter upsert, sql=%q", ec.sql)
	}
	if ec.args[0] != "tenant-1" || ec.args[1] != int64(1) {
		t.Fatalf("unexpected args %v", ec.args)
	}
	// Negative delta folds the same way.
	q2 := &stubQueryRunner{}
	r2 := pg.NewAtomCountWriteRepo(q2)
	if err := r2.ApplyDelta(context.Background(), "tenant-1", -1); err != nil {
		t.Fatalf("ApplyDelta(-1): %v", err)
	}
	if q2.execCalls[0].args[1] != int64(-1) {
		t.Fatalf("expected -1 delta, got %v", q2.execCalls[0].args)
	}
	// Exec failure inside the tx propagates.
	q3 := &stubQueryRunner{execErr: errors.New("db down")}
	r3 := pg.NewAtomCountWriteRepo(q3)
	if err := r3.ApplyDelta(context.Background(), "tenant-1", 1); !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected db error, got %v", err)
	}
}

// ----------------------------------------------------------------------------
// TenantHierarchyRepo — ScopeTxQuerier-backed (CHO-2010)
// ----------------------------------------------------------------------------

// hierTx is a programmable Tx for the hierarchy two-phase read.
type hierTx struct {
	queryRows []*stubRows
	queryIdx  int
	rowFns    []func(dest ...any) error
	rowIdx    int
	queryErr  error
	execSQL   []string
}

func (t *hierTx) Exec(_ context.Context, sql string, _ ...any) error {
	t.execSQL = append(t.execSQL, sql)
	return nil
}
func (t *hierTx) Query(context.Context, string, ...any) (pg.Rows, error) {
	if t.queryErr != nil {
		return nil, t.queryErr
	}
	if t.queryIdx >= len(t.queryRows) {
		return &stubRows{}, nil
	}
	r := t.queryRows[t.queryIdx]
	t.queryIdx++
	return r, nil
}
func (t *hierTx) QueryRow(context.Context, string, ...any) pg.Row {
	if t.rowIdx >= len(t.rowFns) {
		return &stubRow{scanFn: func(dest ...any) error { return errors.New("unexpected QueryRow") }}
	}
	fn := t.rowFns[t.rowIdx]
	t.rowIdx++
	return &stubRow{scanFn: fn}
}

// hierScopeTx satisfies pg.ScopeTxQuerier, recording every scope opened.
type hierScopeTx struct {
	scopes []string
	tx     *hierTx
}

func (s *hierScopeTx) RunInScopeTx(_ context.Context, scopeID string, fn func(context.Context, pg.Tx) error) error {
	s.scopes = append(s.scopes, scopeID)
	return fn(context.Background(), s.tx)
}

func TestTenantHierarchyRepo_GetHierarchy_HappyPath(t *testing.T) {
	t.Parallel()
	stx := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{
			// children of the parent
			{rows: []func(dest ...any) error{
				scanRow("child-1", "Alpha Child"),
				scanRow("child-2", "Beta Child"),
			}},
		},
		// per child: (member count) then (atom count) → 2 QueryRow calls per child
		rowFns: []func(dest ...any) error{
			scanRow(int64(3)),
			scanRow(int64(14)),
			scanRow(int64(0)),
			scanRow(int64(0)),
		},
	}}
	repo := pg.NewTenantHierarchyRepo(stx)
	h, err := repo.GetHierarchy(context.Background(), "parent-1")
	if err != nil {
		t.Fatalf("GetHierarchy: %v", err)
	}
	if !h.IsParent || len(h.Children) != 2 {
		t.Fatalf("expected 2 children, got %+v", h)
	}
	if h.Children[0].TenantName != "Alpha Child" || h.Children[1].TenantName != "Beta Child" {
		t.Fatalf("child names not mapped: %+v", h.Children)
	}
	if h.Children[0].UserCount != 3 || h.Children[0].AtomCount != 14 {
		t.Fatalf("child 0 counts not mapped: %+v", h.Children[0])
	}
	if h.Children[1].UserCount != 0 || h.Children[1].AtomCount != 0 {
		t.Fatalf("child 1 counts not mapped: %+v", h.Children[1])
	}
	// Scopes: parent first, then each child.
	if len(stx.scopes) != 3 || stx.scopes[0] != "parent-1" || stx.scopes[1] != "child-1" || stx.scopes[2] != "child-2" {
		t.Fatalf("unexpected scope sequence %v", stx.scopes)
	}
}

func TestTenantHierarchyRepo_GetHierarchy_NoChildren(t *testing.T) {
	t.Parallel()
	stx := &hierScopeTx{tx: &hierTx{}}
	repo := pg.NewTenantHierarchyRepo(stx)
	h, err := repo.GetHierarchy(context.Background(), "leaf-1")
	if err != nil {
		t.Fatalf("GetHierarchy: %v", err)
	}
	if h.IsParent || len(h.Children) != 0 {
		t.Fatalf("expected leaf tenant, got %+v", h)
	}
	if len(stx.scopes) != 1 || stx.scopes[0] != "leaf-1" {
		t.Fatalf("expected single parent scope, got %v", stx.scopes)
	}
}

func TestTenantHierarchyRepo_GetHierarchy_Errors(t *testing.T) {
	t.Parallel()
	// Children query error.
	stx := &hierScopeTx{tx: &hierTx{queryErr: errors.New("db down")}}
	repo := pg.NewTenantHierarchyRepo(stx)
	if _, err := repo.GetHierarchy(context.Background(), "parent-1"); !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected children-query error, got %v", err)
	}
	// Child scan error.
	stx2 := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{
			{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}},
		},
	}}
	repo2 := pg.NewTenantHierarchyRepo(stx2)
	if _, err := repo2.GetHierarchy(context.Background(), "parent-1"); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected child-scan error, got %v", err)
	}
	// rows.Err after iteration.
	stx3 := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{
			{rows: []func(dest ...any) error{scanRow("child-1", "Alpha")}, err: errors.New("iter boom")},
		},
	}}
	repo3 := pg.NewTenantHierarchyRepo(stx3)
	if _, err := repo3.GetHierarchy(context.Background(), "parent-1"); !strings.Contains(err.Error(), "iter boom") {
		t.Fatalf("expected rows.Err error, got %v", err)
	}
	// Per-child count QueryRow error.
	stx4 := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{
			{rows: []func(dest ...any) error{scanRow("child-1", "Alpha")}},
		},
		rowFns: []func(dest ...any) error{func(dest ...any) error { return errors.New("count boom") }},
	}}
	repo4 := pg.NewTenantHierarchyRepo(stx4)
	if _, err := repo4.GetHierarchy(context.Background(), "parent-1"); !strings.Contains(err.Error(), "count boom") {
		t.Fatalf("expected member-count error, got %v", err)
	}
	// Unwired repo.
	var nilRepo *pg.TenantHierarchyRepo
	if _, err := nilRepo.GetHierarchy(context.Background(), "parent-1"); err == nil {
		t.Fatalf("expected unwired-repo error")
	}
}
