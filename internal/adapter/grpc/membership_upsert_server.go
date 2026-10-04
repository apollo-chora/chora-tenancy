// membership_upsert_server.go — the UpsertMembership RPC (ADR-182).
//
// Idempotently grants membership roles to a GCID in a tenant. Two callers:
//
//	(a) chora-identity resolve auto-enrolment into the public chora-master
//	    tenant (ADR-182 D2) — tenant_slug="chora-master",
//	    roles=[learner, author];
//	(b) chora-identity admin add-member-by-email (L1 tenant lane), which
//	    previously wrote only the identity-side mirror table and never the
//	    authoritative chora_tenancy.members store.
//
// One members row exists per (tenant_id, gcid, role) — migration 0021
// relaxed the historical UNIQUE(tenant_id, gcid) so the multi-role
// aggregation list_memberships_by_gcid() always performed becomes real.
//
// Suspended / soft-deleted memberships are NOT resurrected: the repo
// returns ErrMembershipSuspended and the RPC fails FAILED_PRECONDITION so
// un-suspending stays an explicit admin decision.
//
// Hexagonal: ADAPTER. Persistence behind the MembershipUpsertRepo port
// (pg.MembershipWriteRepository in production).
package grpc

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"
)

// ErrTenantNotFound is returned by MembershipUpsertRepo implementations
// when the slug (or id) resolves to no live tenant.
var ErrTenantNotFound = errors.New("tenancy: tenant not found")

// ErrMembershipSuspended is returned by MembershipUpsertRepo
// implementations when the (tenant, gcid) membership exists but is
// suspended or soft-deleted — upsert must not resurrect it.
var ErrMembershipSuspended = errors.New("tenancy: membership suspended or deleted")

// ErrLastOwnerProtected is returned by MembershipUpsertRepo implementations
// when the removal would take away the tenant's LAST live `owner` row
// (S7-B1 guard D1). The organisation would be left with nobody who owns it,
// and ownership is not admin-grantable, so nothing could put it back. The
// caller hands over ownership first (S7a) or the platform operator assigns a
// new owner (S7c).
var ErrLastOwnerProtected = errors.New("tenancy: cannot remove the tenant's last owner")

// ErrOwnerRoleProtected is returned by MembershipUpsertRepo implementations
// when a role REPLACE would soft-delete a live `owner` row (S7-B1 guard D2).
// Unlike ErrLastOwnerProtected this fires whether or not another owner
// remains: `owner` is the one role no admin API can grant (see
// upsertableRoles), so a role edit that drops it is one-way.
var ErrOwnerRoleProtected = errors.New("tenancy: cannot strip the owner role")

// Reason codes carried on the gRPC status as an errdetails.ErrorInfo so a
// caller can tell the three FAILED_PRECONDITION refusals apart without
// parsing message text. Consumed by chora-identity's AdminMembershipUpserter.
const (
	// ErrorInfoDomain namespaces the reasons below (AIP-193).
	ErrorInfoDomain = "chora-tenancy"
	// ReasonLastOwnerProtected maps to ErrLastOwnerProtected.
	ReasonLastOwnerProtected = "LAST_OWNER_PROTECTED"
	// ReasonOwnerRoleProtected maps to ErrOwnerRoleProtected.
	ReasonOwnerRoleProtected = "OWNER_ROLE_PROTECTED"
	// ReasonMembershipSuspended maps to ErrMembershipSuspended. Named even
	// though it is the legacy default, so the client's mapping is total
	// rather than "everything else is suspended".
	ReasonMembershipSuspended = "MEMBERSHIP_SUSPENDED"
)

// upsertableRoles is the grantable role set. "owner" is bootstrap-only;
// PLATFORM_OPERATOR is JWT-only (ADR-165) and never a members row.
var upsertableRoles = map[string]bool{
	"learner":    true,
	"author":     true,
	"instructor": true,
	"admin":      true,
	"auditor":    true,
}

// refusal builds a FAILED_PRECONDITION status carrying an
// errdetails.ErrorInfo, so a client can branch on `reason` instead of parsing
// the message. Three distinct refusals now share this code (suspended, last
// owner, owner strip) and chora-identity renders each as a different HTTP 409.
//
// status.WithDetails can only fail on a detail that will not marshal or on an
// OK code, neither of which is reachable here; if it ever did, the bare status
// is still returned so the refusal itself is never lost, losing only its
// machine-readable label.
func refusal(reason, msg string) error {
	st := status.New(codes.FailedPrecondition, msg)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: ErrorInfoDomain,
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// MembershipUpsertRepo is the write port behind UpsertMembership.
type MembershipUpsertRepo interface {
	// ResolveTenantIDBySlug maps a live tenant slug to its tenant_id.
	// Returns ErrTenantNotFound when no live tenant carries the slug.
	ResolveTenantIDBySlug(ctx context.Context, slug string) (string, error)

	// UpsertRoles inserts one members row per missing (tenant, gcid, role)
	// and returns the FULL post-upsert role set for the (tenant, gcid)
	// pair plus whether any row was newly created. Returns
	// ErrMembershipSuspended when the membership exists suspended or
	// soft-deleted, ErrTenantNotFound when tenantID is not a live tenant.
	UpsertRoles(ctx context.Context, tenantID, gcid string, roles []string) (allRoles []string, created bool, err error)

	// ReplaceRoles is the REPLACE flavour of UpsertRoles (CHO-1809
	// follow-up): soft-deletes every existing live row for (tenant, gcid)
	// whose role is NOT in `roles`, then upserts the rows that ARE in
	// `roles` (same semantics as UpsertRoles for the kept set). Returns
	// the FULL post-replace role set and whether any row was newly
	// created. Sentinels are the same as UpsertRoles.
	ReplaceRoles(ctx context.Context, tenantID, gcid string, roles []string) (allRoles []string, created bool, err error)

	// RemoveAllRoles SOFT-DELETES every live role row for (tenant, gcid) —
	// the member-centric revoke (WS2b / CHO-1869). Returns the count of rows
	// removed (0 ⇒ no live membership, which the RPC maps to NOT_FOUND).
	// Returns ErrMembershipSuspended when the membership is suspended
	// (un-suspend first) and ErrTenantNotFound when tenantID is not a live
	// tenant. NEVER hard-deletes.
	RemoveAllRoles(ctx context.Context, tenantID, gcid string) (removed int, err error)
}

// NewMembershipServerWithWriter constructs the server with both the read
// (lister) and write (upserter) ports. NewMembershipServer remains the
// read-only constructor — UpsertMembership answers Unimplemented on it.
func NewMembershipServerWithWriter(repo MembershipLister, writer MembershipUpsertRepo) *MembershipServer {
	return &MembershipServer{repo: repo, writer: writer}
}

// UpsertMembership implements tenancyv1.TenancyServer.
func (s *MembershipServer) UpsertMembership(
	ctx context.Context,
	req *tenancyv1.UpsertMembershipRequest,
) (*tenancyv1.UpsertMembershipResponse, error) {
	ctx, span := membershipTracer().Start(ctx, "Tenancy.UpsertMembership")
	defer span.End()

	if s.writer == nil {
		return nil, status.Error(codes.Unimplemented,
			"UpsertMembership not wired on this server (read-only constructor)")
	}

	gcid := strings.TrimSpace(req.GetGcid())
	if gcid == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid required")
	}
	if _, err := uuid.Parse(gcid); err != nil {
		return nil, status.Error(codes.InvalidArgument, "gcid must be a UUID")
	}
	span.SetAttributes(attribute.String("chora.gcid", gcid))

	tenantID := strings.TrimSpace(req.GetTenantId())
	tenantSlug := strings.TrimSpace(req.GetTenantSlug())
	switch {
	case tenantID == "" && tenantSlug == "":
		return nil, status.Error(codes.InvalidArgument,
			"exactly one of tenant_id / tenant_slug required (got neither)")
	case tenantID != "" && tenantSlug != "":
		return nil, status.Error(codes.InvalidArgument,
			"exactly one of tenant_id / tenant_slug required (got both)")
	case tenantID != "":
		if _, err := uuid.Parse(tenantID); err != nil {
			return nil, status.Error(codes.InvalidArgument, "tenant_id must be a UUID")
		}
	}

	roles := make([]string, 0, len(req.GetRoles()))
	seen := map[string]bool{}
	for _, r := range req.GetRoles() {
		role := strings.ToLower(strings.TrimSpace(r))
		if role == "" || seen[role] {
			continue
		}
		if !upsertableRoles[role] {
			return nil, status.Errorf(codes.InvalidArgument,
				"role %q not grantable (learner|author|instructor|admin|auditor)", r)
		}
		seen[role] = true
		roles = append(roles, role)
	}
	if len(roles) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one role required")
	}

	if tenantID == "" {
		resolved, err := s.writer.ResolveTenantIDBySlug(ctx, tenantSlug)
		if err != nil {
			if errors.Is(err, ErrTenantNotFound) {
				return nil, status.Errorf(codes.NotFound, "no live tenant with slug %q", tenantSlug)
			}
			return nil, status.Errorf(codes.Internal, "slug resolution: %v", err)
		}
		tenantID = resolved
	}
	span.SetAttributes(attribute.String("chora.tenant_id", tenantID))

	var (
		allRoles []string
		created  bool
		err      error
	)
	if req.GetReplaceExisting() {
		allRoles, created, err = s.writer.ReplaceRoles(ctx, tenantID, gcid, roles)
	} else {
		allRoles, created, err = s.writer.UpsertRoles(ctx, tenantID, gcid, roles)
	}
	switch {
	case errors.Is(err, ErrMembershipSuspended):
		return nil, refusal(ReasonMembershipSuspended,
			"membership is suspended or deleted — upsert does not resurrect it")
	case errors.Is(err, ErrOwnerRoleProtected):
		return nil, refusal(ReasonOwnerRoleProtected,
			"this member owns the tenant; a role replace cannot strip ownership, so hand ownership over first")
	case errors.Is(err, ErrTenantNotFound):
		return nil, status.Errorf(codes.NotFound, "no live tenant %q", tenantID)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "membership upsert: %v", err)
	}

	span.SetAttributes(attribute.Bool("chora.membership.created", created))

	return &tenancyv1.UpsertMembershipResponse{
		Membership: &tenancyv1.TenantMembership{
			TenantId:   tenantID,
			TenantSlug: tenantSlug,
			Roles:      append([]string(nil), allRoles...),
			Surfaces:   RolesToSurfaces(allRoles),
			// IsDefault is a cross-membership property — callers needing
			// it re-list via ListMembershipsByGCID (the auto-enrol caller
			// does exactly that).
			IsDefault: false,
		},
		Created: created,
	}, nil
}

// RemoveMembership implements tenancyv1.TenancyServer — the member-centric
// soft-delete (WS2b / CHO-1869). Soft-deletes every live role row for
// (tenant, gcid); NEVER hard-deletes. Mirrors UpsertMembership's gcid +
// tenant_id/tenant_slug validation, then maps repo sentinels to gRPC codes:
// zero rows removed ⇒ NOT_FOUND, suspended ⇒ FAILED_PRECONDITION.
func (s *MembershipServer) RemoveMembership(
	ctx context.Context,
	req *tenancyv1.RemoveMembershipRequest,
) (*tenancyv1.RemoveMembershipResponse, error) {
	ctx, span := membershipTracer().Start(ctx, "Tenancy.RemoveMembership")
	defer span.End()

	if s.writer == nil {
		return nil, status.Error(codes.Unimplemented,
			"RemoveMembership not wired on this server (read-only constructor)")
	}

	gcid := strings.TrimSpace(req.GetGcid())
	if gcid == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid required")
	}
	if _, err := uuid.Parse(gcid); err != nil {
		return nil, status.Error(codes.InvalidArgument, "gcid must be a UUID")
	}
	span.SetAttributes(attribute.String("chora.gcid", gcid))

	tenantID := strings.TrimSpace(req.GetTenantId())
	tenantSlug := strings.TrimSpace(req.GetTenantSlug())
	switch {
	case tenantID == "" && tenantSlug == "":
		return nil, status.Error(codes.InvalidArgument,
			"exactly one of tenant_id / tenant_slug required (got neither)")
	case tenantID != "" && tenantSlug != "":
		return nil, status.Error(codes.InvalidArgument,
			"exactly one of tenant_id / tenant_slug required (got both)")
	case tenantID != "":
		if _, err := uuid.Parse(tenantID); err != nil {
			return nil, status.Error(codes.InvalidArgument, "tenant_id must be a UUID")
		}
	}

	if tenantID == "" {
		resolved, err := s.writer.ResolveTenantIDBySlug(ctx, tenantSlug)
		if err != nil {
			if errors.Is(err, ErrTenantNotFound) {
				return nil, status.Errorf(codes.NotFound, "no live tenant with slug %q", tenantSlug)
			}
			return nil, status.Errorf(codes.Internal, "slug resolution: %v", err)
		}
		tenantID = resolved
	}
	span.SetAttributes(attribute.String("chora.tenant_id", tenantID))

	removed, err := s.writer.RemoveAllRoles(ctx, tenantID, gcid)
	switch {
	case errors.Is(err, ErrMembershipSuspended):
		return nil, refusal(ReasonMembershipSuspended,
			"membership is suspended — un-suspend before removing it")
	case errors.Is(err, ErrLastOwnerProtected):
		return nil, refusal(ReasonLastOwnerProtected,
			"this member is the tenant's last owner; hand ownership over before removing them")
	case errors.Is(err, ErrTenantNotFound):
		return nil, status.Errorf(codes.NotFound, "no live tenant %q", tenantID)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "membership remove: %v", err)
	}
	if removed == 0 {
		return nil, status.Error(codes.NotFound, "no live membership for (gcid, tenant)")
	}
	span.SetAttributes(attribute.Int("chora.membership.removed_role_count", removed))

	return &tenancyv1.RemoveMembershipResponse{RemovedRoleCount: int32(removed)}, nil
}
