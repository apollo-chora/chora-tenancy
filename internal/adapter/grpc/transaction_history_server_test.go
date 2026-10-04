package grpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

const (
	tenantUUID      = "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b"
	franchiseeUUID  = "0197aaaa-bbbb-7ccc-8ddd-eeeeffff0000"
	franchiseeUUID2 = "0197bbbb-cccc-7ddd-8eee-ffff11112222"
	gcidUUID        = "0197a0b1-2c3d-7e4f-8a9b-111122223333"
)

// stubReadRepo records the last query per method + returns canned results.
type stubReadRepo struct {
	lastList    tl.ListQuery
	lastDetail  tl.DetailQuery
	lastSummary tl.SummaryQuery
	listPage    tl.ListPage
	detailPage  tl.DetailPage
	summary     tl.Summary
	err         error
}

func (s *stubReadRepo) List(_ context.Context, q tl.ListQuery) (tl.ListPage, error) {
	s.lastList = q
	return s.listPage, s.err
}
func (s *stubReadRepo) GetDetail(_ context.Context, q tl.DetailQuery) (tl.DetailPage, error) {
	s.lastDetail = q
	return s.detailPage, s.err
}
func (s *stubReadRepo) GetSummary(_ context.Context, q tl.SummaryQuery) (tl.Summary, error) {
	s.lastSummary = q
	return s.summary, s.err
}

// auditSpy records EmitCrossTenantViewed calls + can force an error.
type auditSpy struct {
	calls           int
	lastActor       string
	lastFilter      tl.AuditFilter
	lastTenantsView []string
	err             error
}

func (a *auditSpy) EmitCrossTenantViewed(_ context.Context, actorGCID string, f tl.AuditFilter, tenantIDsInView []string, _ time.Time) error {
	a.calls++
	a.lastActor = actorGCID
	a.lastFilter = f
	a.lastTenantsView = tenantIDsInView
	return a.err
}

func ctxWith(pairs ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
}

func fixedClock() time.Time { return time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC) }

func newServer(repo tl.ReadRepository, audit tenancygrpc.CrossTenantAuditEmitter) *tenancygrpc.TransactionHistoryServer {
	return tenancygrpc.NewTransactionHistoryServer(repo, audit).WithClock(fixedClock)
}

func codeOf(t *testing.T, err error) codes.Code {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	return st.Code()
}

// ── LEARNER ──────────────────────────────────────────────────────────────

func TestList_LearnerScope_BindsCallerGCIDAndTenant_NoAudit(t *testing.T) {
	repo := &stubReadRepo{}
	audit := &auditSpy{}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-gcid", gcidUUID, "chora-tenant-id", tenantUUID)
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER,
		// a learner trying to pass franchisee/learner_gcids must be ignored:
		Filters:  &tenancyv1.TransactionFilters{ManagedTenantIds: []string{franchiseeUUID}, LearnerGcids: []string{"someone-else", "another"}},
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.GUCTenantID != tenantUUID {
		t.Errorf("guc = %q; want caller tenant", repo.lastList.GUCTenantID)
	}
	if len(repo.lastList.Filters.LearnerGCIDs) != 1 || repo.lastList.Filters.LearnerGCIDs[0] != gcidUUID {
		t.Errorf("learner filter = %v; want forced [caller gcid] (client set ignored)", repo.lastList.Filters.LearnerGCIDs)
	}
	if repo.lastList.Limit != 50 {
		t.Errorf("limit = %d; want 50", repo.lastList.Limit)
	}
	if audit.calls != 0 {
		t.Errorf("learner scope emitted %d audits; want 0", audit.calls)
	}
}

func TestList_LearnerScope_MissingClaims_Unauthenticated(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	_, err := srv.ListTransactions(ctxWith("chora-tenant-id", tenantUUID), &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER,
	})
	if codeOf(t, err) != codes.Unauthenticated {
		t.Fatalf("code = %v; want Unauthenticated", codeOf(t, err))
	}
}

// ── TENANT ───────────────────────────────────────────────────────────────

func TestList_TenantScope_HonoursLearnerDrill_NoAudit(t *testing.T) {
	repo := &stubReadRepo{}
	audit := &auditSpy{}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
		Filters: &tenancyv1.TransactionFilters{LearnerGcids: []string{"drill-gcid"}},
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.GUCTenantID != tenantUUID {
		t.Errorf("guc = %q; want tenant", repo.lastList.GUCTenantID)
	}
	if len(repo.lastList.Filters.LearnerGCIDs) != 1 || repo.lastList.Filters.LearnerGCIDs[0] != "drill-gcid" {
		t.Errorf("learner drill = %v; want [drill-gcid]", repo.lastList.Filters.LearnerGCIDs)
	}
	if audit.calls != 0 {
		t.Errorf("tenant scope emitted %d audits; want 0", audit.calls)
	}
}

func TestList_TenantScope_MultiLearnerDrill_PassesSet(t *testing.T) {
	repo := &stubReadRepo{}
	srv := newServer(repo, &auditSpy{})

	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
		Filters: &tenancyv1.TransactionFilters{LearnerGcids: []string{"g-a", " ", "g-b"}}, // blank dropped
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	got := repo.lastList.Filters.LearnerGCIDs
	if len(got) != 2 || got[0] != "g-a" || got[1] != "g-b" {
		t.Errorf("learner set = %v; want [g-a g-b] (blank sanitized)", got)
	}
}

// ── MASTER ───────────────────────────────────────────────────────────────

func TestList_MasterScope_NonOperator_PermissionDenied(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "tenant_admin,learner")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
	})
	if codeOf(t, err) != codes.PermissionDenied {
		t.Fatalf("code = %v; want PermissionDenied", codeOf(t, err))
	}
}

func TestList_MasterScope_SpanAll_EmitsAudit_UsesPlatformGUC(t *testing.T) {
	repo := &stubReadRepo{}
	audit := &auditSpy{}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-gcid", gcidUUID, "chora-tenant-id", tenantUUID, "x-mesh-user-roles", "PLATFORM_OPERATOR")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.GUCTenantID != tl.PlatformScope {
		t.Errorf("guc = %q; want platform sentinel", repo.lastList.GUCTenantID)
	}
	if audit.calls != 1 {
		t.Fatalf("expected 1 audit, got %d", audit.calls)
	}
	if audit.lastActor != gcidUUID {
		t.Errorf("audit actor = %q; want operator gcid", audit.lastActor)
	}
	if len(audit.lastTenantsView) != 0 {
		t.Errorf("span-all tenant_ids_in_view = %v; want empty", audit.lastTenantsView)
	}
}

func TestList_MasterScope_SingleFranchisee_UsesGUC_NoPredicate_Audits(t *testing.T) {
	// A single franchisee keeps the RLS-GUC path (back-compat): guc = the
	// franchisee, NO tenant_id predicate, audit fed the 1-element set.
	repo := &stubReadRepo{}
	audit := &auditSpy{}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
		Filters: &tenancyv1.TransactionFilters{ManagedTenantIds: []string{franchiseeUUID}},
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.GUCTenantID != franchiseeUUID {
		t.Errorf("guc = %q; want franchisee (single-select RLS-GUC path)", repo.lastList.GUCTenantID)
	}
	if len(repo.lastList.Filters.ManagedTenantIDs) != 0 {
		t.Errorf("single franchisee must NOT add a tenant_id predicate, got %v", repo.lastList.Filters.ManagedTenantIDs)
	}
	if len(audit.lastTenantsView) != 1 || audit.lastTenantsView[0] != franchiseeUUID {
		t.Errorf("tenant_ids_in_view = %v; want [franchisee]", audit.lastTenantsView)
	}
}

func TestList_MasterScope_MultiFranchisee_SpanAllPlusPredicate_Audits(t *testing.T) {
	// 2+ franchisees: span-all ('platform') GUC + the new tenant_id ANY
	// predicate carries the selected set, and the IMDA-D1 audit is fed the
	// FULL selected list BEFORE the read.
	repo := &stubReadRepo{}
	audit := &auditSpy{}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
		Filters: &tenancyv1.TransactionFilters{ManagedTenantIds: []string{franchiseeUUID, franchiseeUUID2}},
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.GUCTenantID != tl.PlatformScope {
		t.Errorf("guc = %q; want platform span-all for multi-franchisee", repo.lastList.GUCTenantID)
	}
	got := repo.lastList.Filters.ManagedTenantIDs
	if len(got) != 2 || got[0] != franchiseeUUID || got[1] != franchiseeUUID2 {
		t.Errorf("franchisee predicate set = %v; want [%s %s]", got, franchiseeUUID, franchiseeUUID2)
	}
	if len(audit.lastTenantsView) != 2 || audit.lastTenantsView[0] != franchiseeUUID || audit.lastTenantsView[1] != franchiseeUUID2 {
		t.Errorf("audit tenant_ids_in_view = %v; want the full selected set", audit.lastTenantsView)
	}
}

func TestList_MasterScope_BadFranchisee_InvalidArgument(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
		Filters: &tenancyv1.TransactionFilters{ManagedTenantIds: []string{"platform"}}, // sentinel must be rejected
	})
	if codeOf(t, err) != codes.InvalidArgument {
		t.Fatalf("code = %v; want InvalidArgument", codeOf(t, err))
	}
}

func TestList_MasterScope_MultiFranchisee_MalformedElement_InvalidArgument(t *testing.T) {
	// Every element of a multi-select must be a tenant UUID; one malformed id
	// fails the whole request (fail-closed, no partial span).
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
		Filters: &tenancyv1.TransactionFilters{ManagedTenantIds: []string{franchiseeUUID, "not-a-uuid"}},
	})
	if codeOf(t, err) != codes.InvalidArgument {
		t.Fatalf("code = %v; want InvalidArgument", codeOf(t, err))
	}
}

func TestList_MasterScope_AuditFailure_AbortsRead_FailClosed(t *testing.T) {
	repo := &stubReadRepo{}
	audit := &auditSpy{err: errors.New("pubsub down")}
	srv := newServer(repo, audit)

	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER,
	})
	if codeOf(t, err) != codes.Internal {
		t.Fatalf("code = %v; want Internal", codeOf(t, err))
	}
	// ListQuery now carries slices (non-comparable); a non-empty GUCTenantID is
	// the "List was invoked" signal — it must stay zero when the audit aborts.
	if repo.lastList.GUCTenantID != "" {
		t.Fatal("repo.List was called despite audit failure (fail-closed violated)")
	}
}

// ── filters / window / page-size ───────────────────────────────────────────

func TestList_DefaultWindowAndPageSize(t *testing.T) {
	repo := &stubReadRepo{}
	srv := newServer(repo, &auditSpy{})

	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin")
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope:    tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
		PageSize: 7, // out-of-set → default 20
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	if repo.lastList.Limit != tl.DefaultListPageSize {
		t.Errorf("limit = %d; want %d", repo.lastList.Limit, tl.DefaultListPageSize)
	}
	wantTo := fixedClock()
	wantFrom := wantTo.AddDate(0, 0, -tl.DefaultWindowDays)
	if !repo.lastList.Filters.To.Equal(wantTo) {
		t.Errorf("window To = %v; want %v", repo.lastList.Filters.To, wantTo)
	}
	if !repo.lastList.Filters.From.Equal(wantFrom) {
		t.Errorf("window From = %v; want %v (90d default)", repo.lastList.Filters.From, wantFrom)
	}
}

func TestList_ScopeUnspecified_InvalidArgument(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	_, err := srv.ListTransactions(ctxWith("chora-tenant-id", tenantUUID), &tenancyv1.ListTransactionsRequest{})
	if codeOf(t, err) != codes.InvalidArgument {
		t.Fatalf("code = %v; want InvalidArgument", codeOf(t, err))
	}
}

// ── summary ────────────────────────────────────────────────────────────────

func TestGetSummary_MapsKPIsWithEnumNameKeys(t *testing.T) {
	repo := &stubReadRepo{summary: tl.Summary{
		TotalCount:            3,
		AmountMinorByCurrency: map[string]int64{"sgd": 8998},
		TotalManaToppedUp:     500,
		TotalManaSpent:        30,
		CountByKind:           map[string]int64{tl.KindPurchase: 2, tl.KindManaSpendDaily: 1},
		CountByStatus:         map[string]int64{tl.StatusCaptured: 2, tl.StatusPosted: 1},
		WindowFrom:            fixedClock().AddDate(0, 0, -90),
		WindowTo:              fixedClock(),
	}}
	srv := newServer(repo, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin")
	resp, err := srv.GetSummary(ctx, &tenancyv1.GetSummaryRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT})
	if err != nil {
		t.Fatalf("GetSummary: %v", err)
	}
	sm := resp.GetSummary()
	if sm.GetTotalCount() != 3 || sm.GetTotalManaSpent() != 30 || sm.GetAmountMinorByCurrency()["sgd"] != 8998 {
		t.Fatalf("summary scalars wrong: %+v", sm)
	}
	if sm.GetCountByKind()["TRANSACTION_KIND_PURCHASE"] != 2 {
		t.Errorf("count_by_kind keys not enum names: %v", sm.GetCountByKind())
	}
	if sm.GetCountByStatus()["TRANSACTION_STATUS_POSTED"] != 1 {
		t.Errorf("count_by_status keys not enum names: %v", sm.GetCountByStatus())
	}
}

// ── detail ───────────────────────────────────────────────────────────────

func TestGetDetail_NotFound(t *testing.T) {
	repo := &stubReadRepo{detailPage: tl.DetailPage{Found: false}}
	srv := newServer(repo, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin")
	_, err := srv.GetTransactionDetail(ctx, &tenancyv1.GetTransactionDetailRequest{
		Scope:    tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
		LedgerId: gcidUUID,
	})
	if codeOf(t, err) != codes.NotFound {
		t.Fatalf("code = %v; want NotFound", codeOf(t, err))
	}
}

func TestGetDetail_LearnerScope_ConstrainsLedgerToCaller(t *testing.T) {
	repo := &stubReadRepo{detailPage: tl.DetailPage{Found: true, Parent: tl.LedgerRow{LedgerID: "L1", Kind: tl.KindManaSpendDaily}}}
	srv := newServer(repo, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID)
	_, err := srv.GetTransactionDetail(ctx, &tenancyv1.GetTransactionDetailRequest{
		Scope:    tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER,
		LedgerId: "L1",
	})
	if err != nil {
		t.Fatalf("GetTransactionDetail: %v", err)
	}
	if repo.lastDetail.LearnerGCID != gcidUUID {
		t.Errorf("detail learner constraint = %q; want caller gcid", repo.lastDetail.LearnerGCID)
	}
	if repo.lastDetail.Limit != tl.DefaultDetailPageSize {
		t.Errorf("detail limit = %d; want %d", repo.lastDetail.Limit, tl.DefaultDetailPageSize)
	}
}

func TestGetDetail_MissingLedgerID_InvalidArgument(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID)
	_, err := srv.GetTransactionDetail(ctx, &tenancyv1.GetTransactionDetailRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
	})
	if codeOf(t, err) != codes.InvalidArgument {
		t.Fatalf("code = %v; want InvalidArgument", codeOf(t, err))
	}
}

// ── async export (B5) ───────────────────────────────────────────────────────

type fakeExportRepo struct {
	created   tl.NewExportJob
	createOut tl.ExportJob
	getJob    tl.ExportJob
	getFound  bool
}

func (f *fakeExportRepo) Create(_ context.Context, in tl.NewExportJob) (tl.ExportJob, error) {
	f.created = in
	out := f.createOut
	out.Status = tl.ExportPending
	out.Format = in.Format
	if out.JobID == "" {
		out.JobID = "JOB1"
	}
	return out, nil
}
func (f *fakeExportRepo) Get(_ context.Context, _, _, _ string) (tl.ExportJob, bool, error) {
	return f.getJob, f.getFound, nil
}
func (f *fakeExportRepo) ClaimPending(context.Context, int) ([]tl.ExportJob, error) { return nil, nil }
func (f *fakeExportRepo) MarkReady(context.Context, string, string, string, time.Time) error {
	return nil
}
func (f *fakeExportRepo) MarkFailed(context.Context, string, string) error { return nil }

func TestCreateExport_NotWired_Unavailable(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{}) // no WithExports
	_, err := srv.CreateTransactionExport(ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID),
		&tenancyv1.CreateTransactionExportRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT, Format: tenancyv1.ExportFormat_EXPORT_FORMAT_CSV})
	if codeOf(t, err) != codes.Unavailable {
		t.Fatalf("code = %v; want Unavailable", codeOf(t, err))
	}
}

func TestCreateExport_Tenant_PersistsPendingJob(t *testing.T) {
	exp := &fakeExportRepo{}
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithExports(exp)
	resp, err := srv.CreateTransactionExport(
		ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin"),
		&tenancyv1.CreateTransactionExportRequest{
			Scope:   tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
			Format:  tenancyv1.ExportFormat_EXPORT_FORMAT_JSON,
			Filters: &tenancyv1.TransactionFilters{Kind: tenancyv1.TransactionKind_TRANSACTION_KIND_PURCHASE},
		})
	if err != nil {
		t.Fatalf("CreateTransactionExport: %v", err)
	}
	if exp.created.TenantID != tenantUUID || exp.created.Scope != "tenant" || exp.created.Format != tl.FormatJSON {
		t.Fatalf("created job wrong: %+v", exp.created)
	}
	if exp.created.OwnerGCID != gcidUUID {
		t.Errorf("owner = %q; want caller gcid", exp.created.OwnerGCID)
	}
	if exp.created.Filters.Kind != tl.KindPurchase {
		t.Errorf("frozen filters wrong: %+v", exp.created.Filters)
	}
	if resp.GetStatus() != tenancyv1.ExportStatus_EXPORT_STATUS_PENDING || resp.GetFormat() != tenancyv1.ExportFormat_EXPORT_FORMAT_JSON {
		t.Errorf("resp wrong: %+v", resp)
	}
}

func TestCreateExport_BadFormat_InvalidArgument(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithExports(&fakeExportRepo{})
	_, err := srv.CreateTransactionExport(ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID),
		&tenancyv1.CreateTransactionExportRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT})
	if codeOf(t, err) != codes.InvalidArgument {
		t.Fatalf("code = %v; want InvalidArgument", codeOf(t, err))
	}
}

func TestCreateExport_MasterSpanAll_NilTenantAndAudit(t *testing.T) {
	exp := &fakeExportRepo{}
	audit := &auditSpy{}
	srv := newServer(&stubReadRepo{}, audit).WithExports(exp)
	_, err := srv.CreateTransactionExport(
		ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator"),
		&tenancyv1.CreateTransactionExportRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER, Format: tenancyv1.ExportFormat_EXPORT_FORMAT_CSV})
	if err != nil {
		t.Fatalf("CreateTransactionExport: %v", err)
	}
	if exp.created.TenantID != tl.NilTenantUUID || exp.created.Scope != "master" {
		t.Fatalf("span-all job tenant = %q (want nil-uuid)", exp.created.TenantID)
	}
	if audit.calls != 1 {
		t.Errorf("master export should emit 1 audit, got %d", audit.calls)
	}
}

func TestGetExport_NotFound(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithExports(&fakeExportRepo{getFound: false})
	_, err := srv.GetTransactionExport(ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin"),
		&tenancyv1.GetTransactionExportRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT, JobId: "JOBX"})
	if codeOf(t, err) != codes.NotFound {
		t.Fatalf("code = %v; want NotFound", codeOf(t, err))
	}
}

func TestGetExport_Ready_MapsURL(t *testing.T) {
	exp := &fakeExportRepo{getFound: true, getJob: tl.ExportJob{
		JobID: "JOBR", Status: tl.ExportReady, Format: tl.FormatCSV,
		DownloadURL: "https://signed/x", ExpiresAt: fixedClock(), CreatedAt: fixedClock(),
	}}
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithExports(exp)
	resp, err := srv.GetTransactionExport(ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "admin"),
		&tenancyv1.GetTransactionExportRequest{Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT, JobId: "JOBR"})
	if err != nil {
		t.Fatalf("GetTransactionExport: %v", err)
	}
	if resp.GetStatus() != tenancyv1.ExportStatus_EXPORT_STATUS_READY || resp.GetDownloadUrl() != "https://signed/x" {
		t.Errorf("ready job mapping wrong: %+v", resp)
	}
}

// ── stream ─────────────────────────────────────────────────────────────────

func TestStream_Unimplemented(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{})
	err := srv.StreamTransactionEvents(&tenancyv1.StreamTransactionEventsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
	}, nil)
	if codeOf(t, err) != codes.Unimplemented {
		t.Fatalf("code = %v; want Unimplemented", codeOf(t, err))
	}
}

// fakeFranchiseeLister records the query it received + returns canned rows.
type fakeFranchiseeLister struct {
	items   []tl.Franchisee
	lastQ   string
	lastLim int
	lastOff int
	err     error
}

func (f *fakeFranchiseeLister) ListFranchisees(_ context.Context, q string, limit, offset int) ([]tl.Franchisee, error) {
	f.lastQ, f.lastLim, f.lastOff = q, limit, offset
	if f.err != nil {
		return nil, f.err
	}
	if limit > 0 && len(f.items) > limit {
		return f.items[:limit], nil
	}
	return f.items, nil
}

func TestListFranchisees_NonOperator_PermissionDenied(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithFranchisees(&fakeFranchiseeLister{})
	ctx := ctxWith("chora-gcid", gcidUUID, "chora-tenant-id", tenantUUID) // no operator role
	_, err := srv.ListFranchisees(ctx, &tenancyv1.ListFranchiseesRequest{})
	if codeOf(t, err) != codes.PermissionDenied {
		t.Fatalf("code = %v; want PermissionDenied", codeOf(t, err))
	}
}

func TestListFranchisees_Unwired_Unavailable(t *testing.T) {
	srv := newServer(&stubReadRepo{}, &auditSpy{}) // no WithFranchisees
	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")
	_, err := srv.ListFranchisees(ctx, &tenancyv1.ListFranchiseesRequest{})
	if codeOf(t, err) != codes.Unavailable {
		t.Fatalf("code = %v; want Unavailable", codeOf(t, err))
	}
}

func TestListFranchisees_OperatorMapsAndPaginates(t *testing.T) {
	// page_size=10 → the server fetches 11 to detect a next page; 11 available
	// → 10 returned + next_page_token "10".
	items := make([]tl.Franchisee, 11)
	for i := range items {
		id := "t-" + string(rune('a'+i))
		items[i] = tl.Franchisee{TenantID: id, Name: "F-" + string(rune('a'+i))}
	}
	lister := &fakeFranchiseeLister{items: items}
	srv := newServer(&stubReadRepo{}, &auditSpy{}).WithFranchisees(lister)
	ctx := ctxWith("chora-gcid", gcidUUID, "x-mesh-user-roles", "platform_operator")

	resp, err := srv.ListFranchisees(ctx, &tenancyv1.ListFranchiseesRequest{Q: "f", PageSize: 10})
	if err != nil {
		t.Fatalf("ListFranchisees: %v", err)
	}
	if len(resp.GetFranchisees()) != 10 {
		t.Fatalf("returned %d; want 10 (page clamped)", len(resp.GetFranchisees()))
	}
	if resp.GetNextPageToken() != "10" {
		t.Errorf("next_page_token = %q; want 10", resp.GetNextPageToken())
	}
	if lister.lastQ != "f" || lister.lastLim != 11 {
		t.Errorf("lister got q=%q lim=%d; want f / 11 (limit+1)", lister.lastQ, lister.lastLim)
	}
	if resp.GetFranchisees()[0].GetTenantId() != "t-a" || resp.GetFranchisees()[0].GetName() != "F-a" {
		t.Errorf("row 0 mapping wrong: %+v", resp.GetFranchisees()[0])
	}
}

// ── TENANT-scope role gate (CHO-2288) ────────────────────────────────────
//
// The live contract (chora-contracts/openapi/transaction-history.yaml §490-492)
// requires the caller to hold at least one of
// TENANT_ADMIN / OWNER / AUDITOR / PLATFORM_OPERATOR, case-insensitively, and
// defines a 403 for "Caller lacks the required role". Before CHO-2288 the
// TENANT branch of resolveScope bound the GUC off the mesh tenant claim
// WITHOUT ever reading the roles — so a plain learner could read the whole
// tenant's purchase ledger via GET /api/v1/admin/transactions. Verified
// against deployed reality 2026-07-18: a JWT with roles exactly
// ["author","learner"] returned 200 + the tenant's ledger rows.
//
// Roles are driven through the real wire header (x-mesh-user-roles,
// comma-separated) rather than a hand-built claims struct, so the test
// exercises the same decode path production uses.

func TestList_TenantScope_NonPrivilegedRole_PermissionDenied(t *testing.T) {
	for _, roles := range []string{
		"learner",
		"author,learner",
		"instructor",
		"training_admin,assessment_author",
		"support_agent",
	} {
		t.Run(roles, func(t *testing.T) {
			repo := &stubReadRepo{}
			srv := newServer(repo, &auditSpy{})

			ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", roles)
			_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
				Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
			})
			if got := codeOf(t, err); got != codes.PermissionDenied {
				t.Errorf("code = %v; want PermissionDenied for roles %q", got, roles)
			}
			// Fail-closed: the repository must never be reached.
			if repo.lastList.GUCTenantID != "" {
				t.Errorf("repo was queried (GUC %q) despite a denied caller", repo.lastList.GUCTenantID)
			}
		})
	}
}

func TestList_TenantScope_NoRolesClaim_PermissionDenied(t *testing.T) {
	repo := &stubReadRepo{}
	srv := newServer(repo, &auditSpy{})

	// No x-mesh-user-roles header at all — must fail CLOSED, not open.
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID)
	_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
	})
	if got := codeOf(t, err); got != codes.PermissionDenied {
		t.Errorf("code = %v; want PermissionDenied when the roles claim is absent", got)
	}
	if repo.lastList.GUCTenantID != "" {
		t.Errorf("repo was queried (GUC %q) despite an absent roles claim", repo.lastList.GUCTenantID)
	}
}

func TestList_TenantScope_PrivilegedRoles_Allowed(t *testing.T) {
	// Both the JWT-extension shape (TENANT_ADMIN/OWNER) and the PG
	// membership_role shape (admin/auditor, migration 0001_initial.sql), in
	// both cases — the contract mandates a case-insensitive check.
	for _, roles := range []string{
		"tenant_admin", "TENANT_ADMIN", "admin", "ADMIN",
		"owner", "OWNER", "auditor", "AUDITOR",
		"learner,admin",     // privileged role alongside a base role
		"platform_operator", // operator may also read tenant scope
	} {
		t.Run(roles, func(t *testing.T) {
			repo := &stubReadRepo{}
			srv := newServer(repo, &auditSpy{})

			ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", roles)
			_, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
				Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
			})
			if err != nil {
				t.Fatalf("ListTransactions with roles %q: %v", roles, err)
			}
			if repo.lastList.GUCTenantID != tenantUUID {
				t.Errorf("guc = %q; want caller tenant %q", repo.lastList.GUCTenantID, tenantUUID)
			}
		})
	}
}

// The gate must cover every RPC that resolves a scope — not just list.
func TestTenantScope_RoleGate_AppliesToSummaryAndDetail(t *testing.T) {
	repo := &stubReadRepo{}
	srv := newServer(repo, &auditSpy{})
	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "learner")

	if _, err := srv.GetSummary(ctx, &tenancyv1.GetSummaryRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
	}); codeOf(t, err) != codes.PermissionDenied {
		t.Errorf("GetSummary: want PermissionDenied for a learner")
	}
	if _, err := srv.GetTransactionDetail(ctx, &tenancyv1.GetTransactionDetailRequest{
		Scope:    tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT,
		LedgerId: "0197a0b1-2c3d-7e4f-8a9b-999988887777",
	}); codeOf(t, err) != codes.PermissionDenied {
		t.Errorf("GetTransactionDetail: want PermissionDenied for a learner")
	}
}

// A LEARNER-scope caller reads only their OWN rows, so the tenant-admin role
// gate must NOT apply there — guarding against an over-broad fix.
func TestList_LearnerScope_PlainLearner_StillAllowed(t *testing.T) {
	repo := &stubReadRepo{}
	srv := newServer(repo, &auditSpy{})

	ctx := ctxWith("chora-tenant-id", tenantUUID, "chora-gcid", gcidUUID, "x-mesh-user-roles", "learner")
	if _, err := srv.ListTransactions(ctx, &tenancyv1.ListTransactionsRequest{
		Scope: tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER,
	}); err != nil {
		t.Fatalf("learner scope must stay open to a plain learner: %v", err)
	}
	if repo.lastList.Filters.LearnerGCIDs[0] != gcidUUID {
		t.Errorf("learner filter = %v; want forced to caller", repo.lastList.Filters.LearnerGCIDs)
	}
}
