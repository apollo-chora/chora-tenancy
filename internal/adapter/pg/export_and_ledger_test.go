// export_and_ledger_test.go — no-live-DB unit coverage for the ADR-205
// async-export job repo (transaction_export_jobs) and the transaction_ledger
// CQRS write repo. Both are driven through the package's ScopeTxQuerier /
// TxQuerier seams using the stubs shared across the pg_test package.
package pg_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// ----------------------------------------------------------------------------
// TransactionExportRepo — ScopeTxQuerier-backed (CHO-1941)
// ----------------------------------------------------------------------------

func TestTransactionExportRepo_Create(t *testing.T) {
	t.Parallel()
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	stx := &hierScopeTx{tx: &hierTx{
		rowFns: []func(dest ...any) error{scanRow("job-1", created)},
	}}
	repo := pg.NewTransactionExportRepo(stx)
	job, err := repo.Create(context.Background(), tl.NewExportJob{
		TenantID: "tenant-1", OwnerGCID: "owner-1", LearnerGCID: "learner-1",
		Scope: "learner", ManagedTenantID: "", Format: tl.FormatCSV,
		Filters: tl.ReadFilters{Kind: "purchase"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if job.JobID != "job-1" || job.TenantID != "tenant-1" || job.Format != tl.FormatCSV {
		t.Fatalf("unexpected job %+v", job)
	}
	if job.Status != tl.ExportPending || !job.CreatedAt.Equal(created) {
		t.Fatalf("lifecycle fields wrong: %+v", job)
	}
	if len(stx.scopes) != 1 || stx.scopes[0] != "tenant-1" {
		t.Fatalf("expected tenant scope, got %v", stx.scopes)
	}
	// Span-all master job → platform sentinel scope.
	stx2 := &hierScopeTx{tx: &hierTx{
		rowFns: []func(dest ...any) error{scanRow("job-2", created)},
	}}
	repo2 := pg.NewTransactionExportRepo(stx2)
	if _, err := repo2.Create(context.Background(), tl.NewExportJob{
		TenantID: tl.NilTenantUUID, OwnerGCID: "owner-1", Scope: "master",
		Format: tl.FormatJSON,
	}); err != nil {
		t.Fatalf("Create(master): %v", err)
	}
	if len(stx2.scopes) != 1 || stx2.scopes[0] != tl.PlatformScope {
		t.Fatalf("expected platform scope for master, got %v", stx2.scopes)
	}
}

func TestTransactionExportRepo_Get(t *testing.T) {
	t.Parallel()
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	// Full export row scan: job_id, tenant_id, owner_gcid, learner_gcid, scope,
	// franchisee_tenant_id, filters_jsonb, format, status, gcs_object,
	// download_url, expires_at, error, attempt_count, created_at.
	rowVals := func() []any {
		return []any{
			"job-1", "tenant-1", "owner-1", "learner-1", "learner",
			nil, `{"kind":"purchase"}`, "csv", "ready",
			"gs://b/obj", "https://signed/url", time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
			nil, 2, created,
		}
	}
	stx := &hierScopeTx{tx: &hierTx{rowFns: []func(dest ...any) error{scanRow(rowVals()...)}}}
	repo := pg.NewTransactionExportRepo(stx)
	job, found, err := repo.Get(context.Background(), "tenant-1", "job-1", "learner-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatalf("expected found")
	}
	if job.JobID != "job-1" || job.LearnerGCID != "learner-1" || job.Status != "ready" {
		t.Fatalf("unexpected job %+v", job)
	}
	if job.GCSObject != "gs://b/obj" || job.DownloadURL != "https://signed/url" || job.AttemptCount != 2 {
		t.Fatalf("worker fields not mapped: %+v", job)
	}
	if job.Filters.Kind != "purchase" {
		t.Fatalf("filters not round-tripped: %+v", job.Filters)
	}
	// Not-found → found=false with nil error.
	stx2 := &hierScopeTx{tx: &hierTx{rowFns: []func(dest ...any) error{func(dest ...any) error { return pg.ErrNoRows }}}}
	repo2 := pg.NewTransactionExportRepo(stx2)
	if _, found, err := repo2.Get(context.Background(), "tenant-1", "job-x", ""); err != nil || found {
		t.Fatalf("expected not-found, err=%v found=%v", err, found)
	}
	// Scan error propagates.
	stx3 := &hierScopeTx{tx: &hierTx{rowFns: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}}}
	repo3 := pg.NewTransactionExportRepo(stx3)
	if _, _, err := repo3.Get(context.Background(), "tenant-1", "job-y", ""); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected scan error, got %v", err)
	}
}

func TestTransactionExportRepo_ClaimPending(t *testing.T) {
	t.Parallel()
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	oneRow := func() []any {
		return []any{
			"job-1", "tenant-1", "owner-1", nil, "tenant",
			nil, "", "csv", "building",
			nil, nil, nil, nil, 1, created,
		}
	}
	stx := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{{rows: []func(dest ...any) error{
			scanRow(oneRow()...), scanRow(append(oneRow()[:0], []any{
				"job-2", "tenant-2", "owner-2", nil, "tenant",
				nil, "", "json", "building", nil, nil, nil, nil, 1, created,
			}...)...),
		}}},
	}}
	repo := pg.NewTransactionExportRepo(stx)
	jobs, err := repo.ClaimPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if jobs[0].JobID != "job-1" || jobs[1].JobID != "job-2" {
		t.Fatalf("unexpected jobs %+v", jobs)
	}
	if len(stx.scopes) != 1 || stx.scopes[0] != tl.PlatformScope {
		t.Fatalf("expected platform scope, got %v", stx.scopes)
	}
	// Empty claim window → nil slice, no error.
	stx2 := &hierScopeTx{tx: &hierTx{}}
	repo2 := pg.NewTransactionExportRepo(stx2)
	jobs, err = repo2.ClaimPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no jobs, got %v", jobs)
	}
	// Row scan error propagates.
	stx3 := &hierScopeTx{tx: &hierTx{
		queryRows: []*stubRows{{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("scan boom") }}}},
	}}
	repo3 := pg.NewTransactionExportRepo(stx3)
	if _, err := repo3.ClaimPending(context.Background(), 10); !strings.Contains(err.Error(), "scan boom") {
		t.Fatalf("expected scan error, got %v", err)
	}
	// Query failure propagates.
	stx4 := &hierScopeTx{tx: &hierTx{queryErr: errors.New("db down")}}
	repo4 := pg.NewTransactionExportRepo(stx4)
	if _, err := repo4.ClaimPending(context.Background(), 10); !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected query error, got %v", err)
	}
	// Unwired repo.
	var nilRepo *pg.TransactionExportRepo
	if _, err := nilRepo.ClaimPending(context.Background(), 10); err == nil {
		t.Fatalf("expected unwired-repo error")
	}
	if _, err := nilRepo.Create(context.Background(), tl.NewExportJob{}); err == nil {
		t.Fatalf("expected unwired-repo error on Create")
	}
	if _, _, err := nilRepo.Get(context.Background(), "t", "j", ""); err == nil {
		t.Fatalf("expected unwired-repo error on Get")
	}
}

func TestTransactionExportRepo_MarkReadyAndFailed(t *testing.T) {
	t.Parallel()
	stx := &hierScopeTx{tx: &hierTx{}}
	repo := pg.NewTransactionExportRepo(stx)
	expires := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	if err := repo.MarkReady(context.Background(), "job-1", "gs://b/o", "https://dl/1", expires); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	if len(stx.tx.execSQL) == 0 || !strings.Contains(stx.tx.execSQL[0], "status = 'ready'") {
		t.Fatalf("expected ready UPDATE, got %v", stx.tx.execSQL)
	}
	if err := repo.MarkFailed(context.Background(), "job-1", "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if len(stx.tx.execSQL) < 2 || !strings.Contains(stx.tx.execSQL[1], "status = 'failed'") {
		t.Fatalf("expected failed UPDATE, got %v", stx.tx.execSQL)
	}
	// Long error truncated to 1000 chars.
	long := strings.Repeat("x", 2000)
	if err := repo.MarkFailed(context.Background(), "job-1", long); err != nil {
		t.Fatalf("MarkFailed(long): %v", err)
	}
	if len(stx.tx.execSQL) != 3 {
		t.Fatalf("expected 3rd UPDATE, got %d", len(stx.tx.execSQL))
	}
	// Unwired repo short-circuits before update.
	var nilRepo *pg.TransactionExportRepo
	if err := nilRepo.MarkReady(context.Background(), "j", "o", "u", expires); err == nil {
		t.Fatalf("expected unwired-repo error")
	}
	if err := nilRepo.MarkFailed(context.Background(), "j", "e"); err == nil {
		t.Fatalf("expected unwired-repo error")
	}
}

// ----------------------------------------------------------------------------
// TransactionLedgerWriteRepo — TxQuerier-backed (CHO-1938)
// ----------------------------------------------------------------------------

func TestTransactionLedgerWriteRepo_UpsertPurchase(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	repo := pg.NewTransactionLedgerWriteRepo(q)
	entry := tl.Entry{
		OccurredAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		TenantID:   "tenant-1", LearnerGCID: "learner-1", Kind: tl.KindPurchase,
		SourceDomain: tl.SourcePayments, SourceRefID: "pur_1", Label: "TMS",
		Currency: "SGD", AmountMinor: 4900, Status: tl.StatusCaptured,
		Metadata: map[string]any{"k": "v"},
	}
	if err := repo.UpsertPurchase(context.Background(), entry); err != nil {
		t.Fatalf("UpsertPurchase: %v", err)
	}
	if len(q.txTenants) != 1 || q.txTenants[0] != "tenant-1" {
		t.Fatalf("expected tenant scope, got %v", q.txTenants)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(q.execCalls))
	}
	ec := q.execCalls[0]
	if !strings.Contains(ec.sql, "ON CONFLICT (tenant_id, kind, source_domain, source_ref_id") {
		t.Fatalf("expected natural-key ON CONFLICT, sql=%q", ec.sql)
	}
	// metadata $12 is the JSON string (empty map → "{}").
	if ec.args[11] != `{"k":"v"}` {
		t.Fatalf("expected marshalled metadata, got %#v", ec.args[11])
	}
	// Empty metadata + nil learner/currency → JSON "{}" + NULL binds.
	q2 := &stubQueryRunner{}
	repo2 := pg.NewTransactionLedgerWriteRepo(q2)
	entry2 := entry
	entry2.Metadata = nil
	entry2.LearnerGCID = ""
	entry2.Currency = ""
	entry2.AmountMinor = 0
	if err := repo2.UpsertPurchase(context.Background(), entry2); err != nil {
		t.Fatalf("UpsertPurchase: %v", err)
	}
	ec2 := q2.execCalls[0]
	if ec2.args[11] != "{}" {
		t.Fatalf("expected empty metadata JSON, got %#v", ec2.args[11])
	}
	// learner_gcid / currency / amount bind as typed-nil pointers → SQL NULL.
	for i, wantNil := range map[int]bool{2: true, 7: true, 8: true} {
		got := ec2.args[i]
		gotNil := false
		if v := reflect.ValueOf(got); v.IsValid() && v.Kind() == reflect.Ptr {
			gotNil = v.IsNil()
		}
		if gotNil != wantNil {
			t.Fatalf("arg[%d] = %#v, expected nil pointer? %v", i, got, wantNil)
		}
	}
	// Non-empty learner/currency/amount are concrete values (first call).
	if got := *ec.args[2].(*string); got != "learner-1" {
		t.Fatalf("learner bind = %q", got)
	}
	if got := *ec.args[7].(*string); got != "SGD" {
		t.Fatalf("currency bind = %q", got)
	}
	if got := *ec.args[8].(*int64); got != 4900 {
		t.Fatalf("amount bind = %d", got)
	}
	// Marshal error short-circuits before the tx.
	entry3 := entry
	entry3.Metadata = map[string]any{"bad": make(chan int)}
	if err := repo.UpsertPurchase(context.Background(), entry3); err == nil {
		t.Fatalf("expected metadata marshal error")
	}
}

func TestTransactionLedgerWriteRepo_AccumulateSpend(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	q.rowResponses = []func(dest ...any) error{scanRow("ledger-1")}
	repo := pg.NewTransactionLedgerWriteRepo(q)
	entry := tl.Entry{
		OccurredAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		TenantID:   "tenant-1", LearnerGCID: "learner-1", Kind: tl.KindManaSpendDaily,
		SourceDomain: "creation", SourceRefID: "atom_1", Label: "mint",
		ManaUnits: -5,
	}
	detail := tl.Detail{
		OccurredAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		TenantID:   "tenant-1", LearnerGCID: "learner-1", ActionCode: "mint",
		ManaUnits: -5, Model: "gpt-x", TraceID: "tr-1",
	}
	if err := repo.AccumulateSpend(context.Background(), entry, detail); err != nil {
		t.Fatalf("AccumulateSpend: %v", err)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 detail exec, got %d", len(q.execCalls))
	}
	if !strings.Contains(q.execCalls[0].sql, "transaction_ledger_detail") {
		t.Fatalf("expected detail insert, sql=%q", q.execCalls[0].sql)
	}
	if q.execCalls[0].args[0] != "ledger-1" {
		t.Fatalf("expected ledger_id bind, got %#v", q.execCalls[0].args)
	}
	// Rollup upsert QueryRow failure.
	q2 := &stubQueryRunner{} // default QueryRow → pg.ErrNoRows
	repo2 := pg.NewTransactionLedgerWriteRepo(q2)
	if err := repo2.AccumulateSpend(context.Background(), entry, detail); !strings.Contains(err.Error(), "upsert day rollup") {
		t.Fatalf("expected rollup scan error, got %v", err)
	}
	// Detail append failure.
	q3 := &stubQueryRunner{execErr: errors.New("detail db down")}
	q3.rowResponses = []func(dest ...any) error{scanRow("ledger-2")}
	repo3 := pg.NewTransactionLedgerWriteRepo(q3)
	if err := repo3.AccumulateSpend(context.Background(), entry, detail); !strings.Contains(err.Error(), "append detail") {
		t.Fatalf("expected detail error, got %v", err)
	}
	// Metadata marshal failure on either leg.
	bad := map[string]any{"bad": make(chan int)}
	entry2 := entry
	entry2.Metadata = bad
	if err := repo.AccumulateSpend(context.Background(), entry2, detail); err == nil {
		t.Fatalf("expected entry-metadata marshal error")
	}
	detail2 := detail
	detail2.Metadata = bad
	if err := repo.AccumulateSpend(context.Background(), entry, detail2); err == nil {
		t.Fatalf("expected detail-metadata marshal error")
	}
}
