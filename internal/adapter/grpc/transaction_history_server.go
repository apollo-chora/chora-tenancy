// transaction_history_server.go — gRPC adapter for the ADR-205 contextual
// Transaction History read surface (B3 / CHO-1939). Serves the
// chora.services.tenancy.v1.TransactionHistoryService contract over the
// transaction_ledger CQRS projection (read repo) for all three ADR-205 D1
// scopes.
//
// Scope → isolation is EXPLICIT per request; the binding identity is resolved
// server-side from the mesh-trust claims (servicemesh: chora-gcid /
// chora-tenant-id / x-mesh-user-roles, asserted by Cloud Service Mesh mTLS from
// the BFF peer — NEVER client-spoofable), not from which roles a JWT happens to
// carry:
//
//   - LEARNER → caller GCID + active tenant; RLS pins the tenant, a learner_gcid
//     predicate pins the learner. franchisee/learner_gcid filters ignored.
//   - TENANT  → caller tenant (all members); optional learner_gcid drill.
//   - MASTER  → PLATFORM_OPERATOR only (case-insensitive role gate). Span-all
//     ("platform" RLS sentinel) by default, or a single franchisee via
//     filters.managed_tenant_id. EVERY master read emits the IMDA-D1
//     cross_tenant_payments_viewed audit BEFORE the read (fail-closed: an audit
//     emit failure aborts the read — no un-audited cross-tenant access).
//
// StreamTransactionEvents (SSE live tail) is wired by the BFF in B4/CHO-1940;
// this server returns Unimplemented for it (fail-loud, not a fake success).
package grpc

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/servicemesh"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

const txHistoryTracerName = "chora-tenancy.transaction_history"

// roleOperator is the canonical PLATFORM_OPERATOR role token (compared
// case-insensitively — JWT/mesh roles are lowercase, callers vary).
const roleOperator = "platform_operator"

// tenantReadRoles is the TENANT-scope authorisation set mandated by the
// contract (chora-contracts/openapi/transaction-history.yaml §490-492): the
// caller must hold at least one of TENANT_ADMIN / OWNER / AUDITOR /
// PLATFORM_OPERATOR, matched case-insensitively.
//
// `admin` is the PG membership_role twin of the JWT-extension TENANT_ADMIN
// (ADR-165 §59: the ENUM is learner/instructor/admin/auditor and the mint flow
// forwards it verbatim), so both shapes must resolve or a real tenant admin is
// denied. Deliberately EXCLUDED: learner / author / instructor /
// training_admin / support_agent — transaction history is tenant billing, not
// teaching or authoring (mirrors the chora-web capability map, where
// tenant:view_payments is held only by TENANT_ADMIN_CAPS + AUDITOR_CAPS).
var tenantReadRoles = map[string]struct{}{
	"tenant_admin": {},
	"admin":        {},
	"owner":        {},
	"auditor":      {},
	roleOperator:   {},
}

// normaliseRole lower-cases and canonicalises '-' to '_' so PLATFORM-OPERATOR,
// platform_operator and Platform_Operator all compare equal (mirrors
// gatewayproxy.canonicaliseAdminRole's liberal-in posture).
func normaliseRole(r string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(r), "-", "_"))
}

// hasTenantReadRole reports whether the caller may read TENANT-scope
// transaction history. Fails CLOSED: nil claims or an absent roles claim deny.
func hasTenantReadRole(c *servicemesh.MeshClaims) bool {
	if c == nil {
		return false
	}
	for _, r := range c.Roles {
		if _, ok := tenantReadRoles[normaliseRole(r)]; ok {
			return true
		}
	}
	return false
}

// CrossTenantAuditEmitter records the IMDA-D1 cross-tenant access audit BEFORE
// a master-scope read. Implemented by
// outbox.TransactionAuditEmitter (structural). Stubbed in tests.
type CrossTenantAuditEmitter interface {
	EmitCrossTenantViewed(ctx context.Context, actorGCID string, filter tl.AuditFilter, tenantIDsInView []string, occurredAt time.Time) error
}

// TransactionHistoryServer is the gRPC server adapter.
type TransactionHistoryServer struct {
	tenancyv1.UnimplementedTransactionHistoryServiceServer
	repo        tl.ReadRepository
	audit       CrossTenantAuditEmitter
	exports     tl.ExportJobRepository // nil = async export not wired (RPCs → Unavailable)
	franchisees tl.FranchiseeLister    // nil = franchisee directory not wired (ListFranchisees → Unavailable)
	now         func() time.Time
}

// NewTransactionHistoryServer constructs the server. now defaults to
// time.Now().UTC when nil (override in tests for a deterministic window).
func NewTransactionHistoryServer(repo tl.ReadRepository, audit CrossTenantAuditEmitter) *TransactionHistoryServer {
	return &TransactionHistoryServer{
		repo:  repo,
		audit: audit,
		now:   func() time.Time { return time.Now().UTC() },
	}
}

// WithExports wires the async-export job repository (B5/CHO-1941). When unset,
// CreateTransactionExport / GetTransactionExport return Unavailable (the
// feature is gated on the GCS export bucket + worker being configured).
func (s *TransactionHistoryServer) WithExports(exports tl.ExportJobRepository) *TransactionHistoryServer {
	s.exports = exports
	return s
}

// WithFranchisees wires the master franchisee directory (S3 / CHO-1930). When
// unset, ListFranchisees returns Unavailable (the picker is a master-only
// affordance, gated on the directory being wired).
func (s *TransactionHistoryServer) WithFranchisees(l tl.FranchiseeLister) *TransactionHistoryServer {
	s.franchisees = l
	return s
}

// WithClock overrides the time source (tests).
func (s *TransactionHistoryServer) WithClock(now func() time.Time) *TransactionHistoryServer {
	s.now = now
	return s
}

func txTracer() trace.Tracer { return otel.GetTracerProvider().Tracer(txHistoryTracerName) }

// resolvedScope is the authorised, isolation-resolved request context.
type resolvedScope struct {
	gucTenantID      string   // tenant UUID | "platform"
	learnerGCID      string   // bound learner (learner scope) | "" — for detail/export own-row constraint
	learnerGCIDs     []string // learner filter SET for list/summary (forced [caller] for learner scope; drill set for tenant/master)
	managedTenantIDs []string // franchisee tenant_id SQL predicate SET — populated ONLY for a MASTER 2+ multi-select (span-all + narrow); empty for len 0/1
	isMaster         bool
	actorGCID        string
	tenantIDsInView  []string // for the master audit: single/selected franchisee set, or empty span-all
}

// ListTransactions implements the contract.
func (s *TransactionHistoryServer) ListTransactions(
	ctx context.Context, req *tenancyv1.ListTransactionsRequest,
) (*tenancyv1.ListTransactionsResponse, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.ListTransactions")
	defer span.End()

	rs, err := s.resolveScope(ctx, req.GetScope(), req.GetFilters())
	if err != nil {
		return nil, err
	}
	filters := s.buildReadFilters(req.GetFilters(), rs)
	span.SetAttributes(
		attribute.String("chora.tx.scope", req.GetScope().String()),
		attribute.Bool("chora.tx.master", rs.isMaster),
	)

	if err := s.auditIfMaster(ctx, rs, filters); err != nil {
		return nil, err
	}

	page, err := s.repo.List(ctx, tl.ListQuery{
		GUCTenantID: rs.gucTenantID,
		Filters:     filters,
		Cursor:      req.GetPageToken(),
		Limit:       tl.NormalizePageSize(int(req.GetPageSize()), tl.DefaultListPageSize),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list transactions: %v", err)
	}

	out := &tenancyv1.ListTransactionsResponse{
		Items:         make([]*tenancyv1.TransactionLedgerItem, 0, len(page.Items)),
		NextPageToken: page.NextPageToken,
		HasMore:       page.NextPageToken != "",
	}
	for i := range page.Items {
		out.Items = append(out.Items, toLedgerItemProto(page.Items[i]))
	}
	return out, nil
}

// GetTransactionDetail implements the contract.
func (s *TransactionHistoryServer) GetTransactionDetail(
	ctx context.Context, req *tenancyv1.GetTransactionDetailRequest,
) (*tenancyv1.GetTransactionDetailResponse, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.GetTransactionDetail")
	defer span.End()

	if strings.TrimSpace(req.GetLedgerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "ledger_id required")
	}
	rs, err := s.resolveScope(ctx, req.GetScope(), nil)
	if err != nil {
		return nil, err
	}
	if err := s.auditIfMaster(ctx, rs, tl.ReadFilters{}); err != nil {
		return nil, err
	}

	page, err := s.repo.GetDetail(ctx, tl.DetailQuery{
		GUCTenantID: rs.gucTenantID,
		LedgerID:    req.GetLedgerId(),
		LearnerGCID: rs.learnerGCID,
		Cursor:      req.GetPageToken(),
		Limit:       tl.NormalizePageSize(int(req.GetPageSize()), tl.DefaultDetailPageSize),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get transaction detail: %v", err)
	}
	if !page.Found {
		return nil, status.Errorf(codes.NotFound, "transaction %s not found", req.GetLedgerId())
	}

	out := &tenancyv1.GetTransactionDetailResponse{
		Parent:        toLedgerItemProto(page.Parent),
		Details:       make([]*tenancyv1.TransactionDetailItem, 0, len(page.Details)),
		NextPageToken: page.NextPageToken,
		HasMore:       page.NextPageToken != "",
	}
	for i := range page.Details {
		out.Details = append(out.Details, toDetailItemProto(page.Details[i]))
	}
	return out, nil
}

// GetSummary implements the contract.
func (s *TransactionHistoryServer) GetSummary(
	ctx context.Context, req *tenancyv1.GetSummaryRequest,
) (*tenancyv1.GetSummaryResponse, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.GetSummary")
	defer span.End()

	rs, err := s.resolveScope(ctx, req.GetScope(), req.GetFilters())
	if err != nil {
		return nil, err
	}
	filters := s.buildReadFilters(req.GetFilters(), rs)
	if err := s.auditIfMaster(ctx, rs, filters); err != nil {
		return nil, err
	}

	sum, err := s.repo.GetSummary(ctx, tl.SummaryQuery{GUCTenantID: rs.gucTenantID, Filters: filters})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get summary: %v", err)
	}
	return &tenancyv1.GetSummaryResponse{Summary: toSummaryProto(sum)}, nil
}

// CreateTransactionExport enqueues an async export (B5/CHO-1941). Same scope
// authorisation + MASTER cross-tenant audit as the reads (the worker WILL read
// cross-tenant data), then persists a PENDING job a worker builds.
func (s *TransactionHistoryServer) CreateTransactionExport(
	ctx context.Context, req *tenancyv1.CreateTransactionExportRequest,
) (*tenancyv1.TransactionExportJob, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.CreateTransactionExport")
	defer span.End()

	if s.exports == nil {
		return nil, status.Error(codes.Unavailable, "async export is not configured on this deployment")
	}
	format, err := protoFormatToDomain(req.GetFormat())
	if err != nil {
		return nil, err
	}
	rs, err := s.resolveScope(ctx, req.GetScope(), req.GetFilters())
	if err != nil {
		return nil, err
	}
	filters := s.buildReadFilters(req.GetFilters(), rs)
	if err := s.auditIfMaster(ctx, rs, filters); err != nil {
		return nil, err
	}

	// Job tenant_id: concrete tenant for learner/tenant/franchisee; the
	// nil-uuid span-all sentinel when the read runs under 'platform'.
	jobTenantID := rs.gucTenantID
	if jobTenantID == tl.PlatformScope {
		jobTenantID = tl.NilTenantUUID
	}
	// learner_gcid column = the bound learner (own-job constraint) for LEARNER
	// scope only; the tenant/master drill learner lives in the frozen filters.
	jobLearnerGCID := ""
	if req.GetScope() == tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER {
		jobLearnerGCID = rs.learnerGCID
	}

	job, err := s.exports.Create(ctx, tl.NewExportJob{
		TenantID:    jobTenantID,
		OwnerGCID:   rs.actorGCID,
		LearnerGCID: jobLearnerGCID,
		Scope:       scopeName(req.GetScope()),
		// The job's single franchisee_tenant_id column is populated ONLY for a
		// len==1 select (which runs under that franchisee's RLS GUC). A multi
		// (2+) select spans-all (nil-uuid TenantID) and rides the selected set
		// in Filters.ManagedTenantIDs — replayed by the worker via
		// GUCForTenant(TenantID)+Filters (see exportworker.buildRows).
		ManagedTenantID: singleFranchisee(rs),
		Format:          format,
		Filters:         filters,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create export job: %v", err)
	}
	return toExportJobProto(job), nil
}

// GetTransactionExport polls one export job (own jobs only, scope-checked).
func (s *TransactionHistoryServer) GetTransactionExport(
	ctx context.Context, req *tenancyv1.GetTransactionExportRequest,
) (*tenancyv1.TransactionExportJob, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.GetTransactionExport")
	defer span.End()

	if s.exports == nil {
		return nil, status.Error(codes.Unavailable, "async export is not configured on this deployment")
	}
	if strings.TrimSpace(req.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}
	rs, err := s.resolveScope(ctx, req.GetScope(), nil)
	if err != nil {
		return nil, err
	}
	learnerFilter := ""
	if req.GetScope() == tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER {
		learnerFilter = rs.learnerGCID
	}
	job, found, err := s.exports.Get(ctx, rs.gucTenantID, req.GetJobId(), learnerFilter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get export job: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "export job %s not found", req.GetJobId())
	}
	return toExportJobProto(job), nil
}

// StreamTransactionEvents — the SSE live tail is wired by the chora-gateway BFF
// in B4/CHO-1940. Fail loud rather than fake an empty stream.
func (s *TransactionHistoryServer) StreamTransactionEvents(
	req *tenancyv1.StreamTransactionEventsRequest, _ tenancyv1.TransactionHistoryService_StreamTransactionEventsServer,
) error {
	return status.Error(codes.Unimplemented, "live tail is served by the chora-gateway BFF (B4/CHO-1940); the projection read side does not push events")
}

// ListFranchisees returns the franchisee tenants a MASTER may narrow the ledger
// to (the children of chora-master), name-searchable + offset-paginated.
// PLATFORM_OPERATOR only. No cross-tenant audit here — this reads the RLS-free
// tenant registry, NOT any tenant's ledger rows (contrast the ListTransactions
// master path, which audits before the cross-tenant SQL).
func (s *TransactionHistoryServer) ListFranchisees(
	ctx context.Context, req *tenancyv1.ListFranchiseesRequest,
) (*tenancyv1.ListFranchiseesResponse, error) {
	ctx, span := txTracer().Start(ctx, "TransactionHistory.ListFranchisees")
	defer span.End()

	if !hasOperatorRole(meshClaims(ctx)) {
		return nil, status.Error(codes.PermissionDenied, "listing franchisees requires the platform_operator role")
	}
	if s.franchisees == nil {
		return nil, status.Error(codes.Unavailable, "franchisee directory is not wired on this deployment")
	}

	limit := tl.NormalizePageSize(int(req.GetPageSize()), tl.DefaultFranchiseePageSize)
	offset := parsePageOffset(req.GetPageToken())

	// Fetch limit+1 to detect a next page without a second round trip.
	items, err := s.franchisees.ListFranchisees(ctx, strings.TrimSpace(req.GetQ()), limit+1, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list franchisees: %v", err)
	}

	next := ""
	if len(items) > limit {
		items = items[:limit]
		next = strconv.Itoa(offset + limit)
	}

	out := &tenancyv1.ListFranchiseesResponse{
		Franchisees:   make([]*tenancyv1.Franchisee, 0, len(items)),
		NextPageToken: next,
	}
	for i := range items {
		out.Franchisees = append(out.Franchisees, &tenancyv1.Franchisee{
			TenantId: items[i].TenantID,
			Name:     items[i].Name,
		})
	}
	return out, nil
}

// parsePageOffset decodes the opaque franchisee page_token (a decimal offset).
// A blank / malformed token starts at offset 0 — a bad cursor never errors the
// read (it just re-serves the first page).
func parsePageOffset(token string) int {
	if strings.TrimSpace(token) == "" {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(token))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// -----------------------------------------------------------------------------
// Scope resolution + authorization
// -----------------------------------------------------------------------------

func (s *TransactionHistoryServer) resolveScope(
	ctx context.Context, scope tenancyv1.TransactionScope, pf *tenancyv1.TransactionFilters,
) (resolvedScope, error) {
	claims := meshClaims(ctx)

	switch scope {
	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER:
		if claims.GCID == "" || claims.TenantID == "" {
			return resolvedScope{}, status.Error(codes.Unauthenticated, "learner scope requires mesh gcid + tenant_id claims")
		}
		// Learner scope forces the learner filter to the caller's OWN gcid —
		// any client-supplied learner_gcids are ignored (a learner cannot drill
		// into another learner's rows).
		return resolvedScope{
			gucTenantID:  claims.TenantID,
			learnerGCID:  claims.GCID,
			learnerGCIDs: []string{claims.GCID},
			actorGCID:    claims.GCID,
		}, nil

	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT:
		if claims.TenantID == "" {
			return resolvedScope{}, status.Error(codes.Unauthenticated, "tenant scope requires mesh tenant_id claim")
		}
		// Tenant scope exposes EVERY learner's purchases in the tenant, so it
		// is tenant-admin territory — not something a member of the tenant may
		// read by virtue of membership alone. Denied in the domain (not the
		// BFF) so the gate holds for any caller of this RPC; the adapter maps
		// PermissionDenied → 403 per the contract's documented response.
		if !hasTenantReadRole(claims) {
			return resolvedScope{}, status.Error(codes.PermissionDenied,
				"tenant scope requires one of the tenant_admin / owner / auditor / platform_operator roles")
		}
		return resolvedScope{
			gucTenantID:  claims.TenantID,
			learnerGCIDs: sanitizeIDs(pf.GetLearnerGcids()),
			actorGCID:    claims.GCID,
		}, nil

	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER:
		if !hasOperatorRole(claims) {
			return resolvedScope{}, status.Error(codes.PermissionDenied, "master scope requires the platform_operator role")
		}
		rs := resolvedScope{isMaster: true, learnerGCIDs: sanitizeIDs(pf.GetLearnerGcids()), actorGCID: claims.GCID}
		frs := sanitizeIDs(pf.GetManagedTenantIds())
		for _, fr := range frs {
			if !looksLikeUUID(fr) {
				return resolvedScope{}, status.Error(codes.InvalidArgument, "managed_tenant_id must be a tenant UUID")
			}
		}
		switch len(frs) {
		case 0:
			// span all franchisees.
			rs.gucTenantID = tl.PlatformScope
		case 1:
			// single franchisee — keep the RLS-GUC path (no SQL predicate),
			// back-compat with the former singular selector.
			rs.gucTenantID = frs[0]
			rs.tenantIDsInView = frs
		default:
			// multi-franchisee — span-all GUC + the tenant_id ANY predicate,
			// audit fed the FULL selected set.
			rs.gucTenantID = tl.PlatformScope
			rs.tenantIDsInView = frs
			rs.managedTenantIDs = frs
		}
		return rs, nil

	default:
		return resolvedScope{}, status.Error(codes.InvalidArgument, "scope is required (learner | tenant | master)")
	}
}

// auditIfMaster emits the IMDA-D1 cross-tenant audit BEFORE the read for
// master-scope callers. Fail-closed: a failed emit aborts the read.
func (s *TransactionHistoryServer) auditIfMaster(ctx context.Context, rs resolvedScope, f tl.ReadFilters) error {
	if !rs.isMaster {
		return nil
	}
	if s.audit == nil {
		return status.Error(codes.Internal, "cross-tenant audit emitter not wired")
	}
	af := tl.AuditFilter{Kind: f.Kind, Status: f.Status}
	if !f.From.IsZero() {
		from := f.From
		af.From = &from
	}
	if !f.To.IsZero() {
		to := f.To
		af.To = &to
	}
	if err := s.audit.EmitCrossTenantViewed(ctx, rs.actorGCID, af, rs.tenantIDsInView, s.now()); err != nil {
		return status.Errorf(codes.Internal, "cross-tenant audit emit failed (read aborted, IMDA D1): %v", err)
	}
	return nil
}

// buildReadFilters maps proto filters → the resolved domain ReadFilters,
// applying the server-bounded default window (ADR-205 D5) + the scope-resolved
// learner set (rs.learnerGCIDs — forced to the caller for learner scope, the
// validated drill set for tenant/master).
func (s *TransactionHistoryServer) buildReadFilters(pf *tenancyv1.TransactionFilters, rs resolvedScope) tl.ReadFilters {
	now := s.now().UTC()
	f := tl.ReadFilters{
		Kind:             protoKindToDomain(pf.GetKind()),
		Status:           protoStatusToDomain(pf.GetStatus()),
		Sort:             tl.ParseSort(pf.GetSort()),
		LearnerGCIDs:     rs.learnerGCIDs,
		ManagedTenantIDs: rs.managedTenantIDs,
		To:               now,
	}
	if to := pf.GetTo(); to != nil && to.IsValid() && !to.AsTime().IsZero() {
		f.To = to.AsTime().UTC()
	}
	if from := pf.GetFrom(); from != nil && from.IsValid() && !from.AsTime().IsZero() {
		f.From = from.AsTime().UTC()
	} else {
		f.From = f.To.AddDate(0, 0, -tl.DefaultWindowDays)
	}
	return f
}

// -----------------------------------------------------------------------------
// Mesh claims
// -----------------------------------------------------------------------------

// meshClaims extracts the BFF-stamped mesh-trust claims from the incoming gRPC
// metadata (chora-gcid / chora-tenant-id / x-mesh-user-roles / chora-role-summary),
// reusing the canonical servicemesh unmarshaller. Returns an empty (non-nil)
// claims set when no metadata is present.
func meshClaims(ctx context.Context) *servicemesh.MeshClaims {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return &servicemesh.MeshClaims{}
	}
	h := http.Header{}
	for _, key := range []string{
		servicemesh.HeaderGCID, servicemesh.HeaderTenantID,
		servicemesh.HeaderUserRoles, servicemesh.HeaderRoleSummary,
	} {
		if v := md.Get(key); len(v) > 0 && v[0] != "" {
			h.Set(key, v[0])
		}
	}
	c, err := servicemesh.UnmarshalFromHeaders(h)
	if err != nil || c == nil {
		return &servicemesh.MeshClaims{}
	}
	return c
}

// hasOperatorRole is the case-insensitive platform_operator gate (ADR-205 D1).
func hasOperatorRole(c *servicemesh.MeshClaims) bool {
	if c == nil {
		return false
	}
	for _, r := range c.Roles {
		if strings.EqualFold(strings.TrimSpace(r), roleOperator) {
			return true
		}
	}
	return false
}

// singleFranchisee returns the lone franchisee tenant_id for a single-select
// MASTER export (len==1, which runs under that franchisee's RLS GUC) so the job
// row's franchisee_tenant_id column is stamped. A span-all (0) or multi (2+)
// select returns "" — a multi select carries its set in Filters instead (the
// job runs span-all under the nil-uuid TenantID). tenantIDsInView holds exactly
// the single franchisee for len==1, 2+ for a multi-select, and is empty for
// span-all — so len==1 uniquely identifies the single-franchisee case.
func singleFranchisee(rs resolvedScope) string {
	if len(rs.tenantIDsInView) == 1 {
		return rs.tenantIDsInView[0]
	}
	return ""
}

// sanitizeIDs trims each id + drops empties, returning nil for an all-empty
// input so the repo emits no predicate. The gateway already validated each
// element's UUID shape; this is a defensive cleanup (never a nil-vs-empty trap).
func sanitizeIDs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// looksLikeUUID is a cheap shape guard for the franchisee selector — rejects
// the "platform" sentinel + anything non-UUID before it reaches the RLS GUC.
func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Domain ⇄ proto mapping
// -----------------------------------------------------------------------------

func protoKindToDomain(k tenancyv1.TransactionKind) string {
	switch k {
	case tenancyv1.TransactionKind_TRANSACTION_KIND_PURCHASE:
		return tl.KindPurchase
	case tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_TOPUP:
		return tl.KindManaTopup
	case tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_SPEND_DAILY:
		return tl.KindManaSpendDaily
	default:
		return ""
	}
}

func domainKindToProto(k string) tenancyv1.TransactionKind {
	switch k {
	case tl.KindPurchase:
		return tenancyv1.TransactionKind_TRANSACTION_KIND_PURCHASE
	case tl.KindManaTopup:
		return tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_TOPUP
	case tl.KindManaSpendDaily:
		return tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_SPEND_DAILY
	default:
		return tenancyv1.TransactionKind_TRANSACTION_KIND_UNSPECIFIED
	}
}

func protoStatusToDomain(s tenancyv1.TransactionStatus) string {
	switch s {
	case tenancyv1.TransactionStatus_TRANSACTION_STATUS_CAPTURED:
		return tl.StatusCaptured
	case tenancyv1.TransactionStatus_TRANSACTION_STATUS_REFUNDED:
		return tl.StatusRefunded
	case tenancyv1.TransactionStatus_TRANSACTION_STATUS_FAILED:
		return tl.StatusFailed
	case tenancyv1.TransactionStatus_TRANSACTION_STATUS_EXPIRED:
		return tl.StatusExpired
	case tenancyv1.TransactionStatus_TRANSACTION_STATUS_POSTED:
		return tl.StatusPosted
	default:
		return ""
	}
}

func domainStatusToProto(st string) tenancyv1.TransactionStatus {
	switch st {
	case tl.StatusCaptured:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_CAPTURED
	case tl.StatusRefunded:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_REFUNDED
	case tl.StatusFailed:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_FAILED
	case tl.StatusExpired:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_EXPIRED
	case tl.StatusPosted:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_POSTED
	default:
		return tenancyv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED
	}
}

func toLedgerItemProto(r tl.LedgerRow) *tenancyv1.TransactionLedgerItem {
	return &tenancyv1.TransactionLedgerItem{
		LedgerId:     r.LedgerID,
		OccurredAt:   timestamppb.New(r.OccurredAt.UTC()),
		TenantId:     r.TenantID,
		LearnerGcid:  r.LearnerGCID,
		Kind:         domainKindToProto(r.Kind),
		SourceDomain: r.SourceDomain,
		SourceRefId:  r.SourceRefID,
		Label:        r.Label,
		Amount: &tenancyv1.TransactionAmount{
			Currency:    r.Currency,
			AmountMinor: r.AmountMinor,
			ManaUnits:   r.ManaUnits,
		},
		Status:       domainStatusToProto(r.Status),
		HasDetail:    r.HasDetail,
		MetadataJson: r.MetadataJSON,
	}
}

func toDetailItemProto(d tl.DetailRow) *tenancyv1.TransactionDetailItem {
	return &tenancyv1.TransactionDetailItem{
		DetailId:     d.DetailID,
		LedgerId:     d.LedgerID,
		TenantId:     d.TenantID,
		LearnerGcid:  d.LearnerGCID,
		OccurredAt:   timestamppb.New(d.OccurredAt.UTC()),
		ActionCode:   d.ActionCode,
		ManaUnits:    d.ManaUnits,
		Model:        d.Model,
		TraceId:      d.TraceID,
		MetadataJson: d.MetadataJSON,
	}
}

// scopeName maps the proto scope to the domain job scope string.
func scopeName(s tenancyv1.TransactionScope) string {
	switch s {
	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER:
		return "learner"
	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT:
		return "tenant"
	case tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER:
		return "master"
	default:
		return ""
	}
}

func protoFormatToDomain(f tenancyv1.ExportFormat) (string, error) {
	switch f {
	case tenancyv1.ExportFormat_EXPORT_FORMAT_CSV:
		return tl.FormatCSV, nil
	case tenancyv1.ExportFormat_EXPORT_FORMAT_JSON:
		return tl.FormatJSON, nil
	default:
		return "", status.Error(codes.InvalidArgument, "format must be CSV or JSON")
	}
}

func domainFormatToProto(f string) tenancyv1.ExportFormat {
	switch f {
	case tl.FormatCSV:
		return tenancyv1.ExportFormat_EXPORT_FORMAT_CSV
	case tl.FormatJSON:
		return tenancyv1.ExportFormat_EXPORT_FORMAT_JSON
	default:
		return tenancyv1.ExportFormat_EXPORT_FORMAT_UNSPECIFIED
	}
}

func domainExportStatusToProto(st string) tenancyv1.ExportStatus {
	switch st {
	case tl.ExportPending:
		return tenancyv1.ExportStatus_EXPORT_STATUS_PENDING
	case tl.ExportBuilding:
		return tenancyv1.ExportStatus_EXPORT_STATUS_BUILDING
	case tl.ExportReady:
		return tenancyv1.ExportStatus_EXPORT_STATUS_READY
	case tl.ExportFailed:
		return tenancyv1.ExportStatus_EXPORT_STATUS_FAILED
	default:
		return tenancyv1.ExportStatus_EXPORT_STATUS_UNSPECIFIED
	}
}

func toExportJobProto(j tl.ExportJob) *tenancyv1.TransactionExportJob {
	out := &tenancyv1.TransactionExportJob{
		JobId:       j.JobID,
		Status:      domainExportStatusToProto(j.Status),
		Format:      domainFormatToProto(j.Format),
		CreatedAt:   timestamppb.New(j.CreatedAt.UTC()),
		DownloadUrl: j.DownloadURL,
		Error:       j.Error,
	}
	if !j.ExpiresAt.IsZero() {
		out.ExpiresAt = timestamppb.New(j.ExpiresAt.UTC())
	}
	return out
}

func toSummaryProto(s tl.Summary) *tenancyv1.TransactionSummary {
	out := &tenancyv1.TransactionSummary{
		TotalCount:            s.TotalCount,
		AmountMinorByCurrency: s.AmountMinorByCurrency,
		TotalManaToppedUp:     s.TotalManaToppedUp,
		TotalManaSpent:        s.TotalManaSpent,
		CountByKind:           map[string]int64{},
		CountByStatus:         map[string]int64{},
		WindowFrom:            timestamppb.New(s.WindowFrom.UTC()),
		WindowTo:              timestamppb.New(s.WindowTo.UTC()),
	}
	// Re-key the count maps onto the proto enum NAMES (the contract specifies
	// the map key = the TransactionKind/TransactionStatus enum name).
	for k, v := range s.CountByKind {
		out.CountByKind[domainKindToProto(k).String()] = v
	}
	for st, v := range s.CountByStatus {
		out.CountByStatus[domainStatusToProto(st).String()] = v
	}
	return out
}
