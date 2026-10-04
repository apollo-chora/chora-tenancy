// membership_server.go — gRPC adapter for the ListMembershipsByGCID RPC
// (Bucket 1, 2026-05-14 multi-tenant identity arch-correct).
//
// Per the user-stated invariant: one GCID, many tenants, many roles per
// tenant. This server is the sole sanctioned cross-domain read path between
// chora-identity (the caller) and chora-tenancy (the owner of the members
// table). chora-identity MUST NOT query chora_tenancy directly per
// ddd-enforcement HARD RULE.
//
// Trust model: mTLS via Cloud Service Mesh. The server does NOT do
// JWT validation — the caller (chora-identity) is mesh-trusted by SPIFFE.
//
// Side-effects: emits OTLP span with chora.gcid + chora.memberships.count
// attributes for the IMDA D2 transparency audit trail.
//
// Hexagonal: adapter depends on the domain port MembershipLister + chora-
// contracts gen stubs only. Pure validation + role→surfaces derivation +
// is_default flagging. No business logic.
package grpc

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"
)

const membershipTracerName = "chora-tenancy.membership"

// MembershipRow is the minimal data the gRPC adapter needs to render a
// TenantMembership response payload. Implemented by the pg adapter (over
// the members + tenants tables) and by an in-memory test stub.
//
// Surfaces[] is NOT carried here — it's derived from Roles[] on the gRPC
// boundary via RolesToSurfaces.
type MembershipRow struct {
	TenantID   string
	TenantSlug string
	// Roles is the FULL role set held by the user in this tenant. May be
	// empty in raw form; the adapter drops empty-role rows from the
	// response so downstream consumers don't see meaningless memberships.
	Roles []string
	// JoinedAt is used for default-tenant flagging. Falls back to
	// InvitedAt at the repo layer when joined_at is NULL (status=invited).
	JoinedAt time.Time
}

// MembershipLister is the minimal domain port the gRPC adapter consumes.
// Production impl is the pg adapter (members table inner-joined to tenants
// for slug lookup); tests inject a stub.
//
// Returning an empty slice (with err=nil) signals "no memberships yet" —
// NOT an error. Caller (chora-identity) surfaces empty as HTTP 403.
//
// includeSuspended widens the read over `members.suspended_at` and nothing
// else. It is false for every caller except the closure ownership pre-flight
// (migration 0038): mint must keep excluding suspended members, or a suspended
// member authenticates back into the tenant that suspended them.
type MembershipLister interface {
	ListByGCID(ctx context.Context, gcid string, includeSuspended bool) ([]MembershipRow, error)
}

// MembershipServer is the gRPC server adapter for ListMembershipsByGCID.
//
// The server embeds UnimplementedTenancyServer so callers can compose this
// with the other RPCs on the same service (e.g. GetTenant, CreateTenant)
// without forcing this file to own them. Each RPC lives in its own file
// under this package once wired.
type MembershipServer struct {
	tenancyv1.UnimplementedTenancyServer
	repo MembershipLister
	// writer backs the UpsertMembership RPC (ADR-182). Nil on the
	// read-only NewMembershipServer constructor — the RPC then answers
	// Unimplemented. Wire via NewMembershipServerWithWriter.
	writer MembershipUpsertRepo
	// creator backs the CreateSubTenant RPC (ADR-217 Phase 2.1). Nil →
	// the RPC answers Unimplemented. Wire via WithSubTenantCreator.
	creator SubTenantCreator
	// hierarchy backs the GetTenantHierarchy RPC (ADR-217 Phase 2.3b). Nil →
	// the RPC answers Unimplemented. Wire via WithHierarchyReader.
	hierarchy HierarchyReader
}

// NewMembershipServer constructs the server with the supplied lister port.
func NewMembershipServer(repo MembershipLister) *MembershipServer {
	return &MembershipServer{repo: repo}
}

func membershipTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(membershipTracerName)
}

// ListMembershipsByGCID implements tenancyv1.TenancyServer.
//
// Returns all tenant memberships for the supplied GCID, with surfaces[]
// derived from each membership's roles[] via RolesToSurfaces. Exactly one
// membership is flagged is_default — the most-recently-active (max
// JoinedAt). When the user has zero memberships, returns an empty list
// with empty default_tenant_id — NOT an error.
func (s *MembershipServer) ListMembershipsByGCID(
	ctx context.Context,
	req *tenancyv1.ListMembershipsByGCIDRequest,
) (*tenancyv1.ListMembershipsByGCIDResponse, error) {
	ctx, span := membershipTracer().Start(ctx, "Tenancy.ListMembershipsByGCID")
	defer span.End()

	gcid := strings.TrimSpace(req.GetGcid())
	span.SetAttributes(attribute.String("chora.gcid", gcid))

	if gcid == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid required")
	}

	// The flag is caller-driven and defaults to false on the wire (proto3
	// bool zero value), so an old client keeps the 0011 behaviour exactly.
	includeSuspended := req.GetIncludeSuspended()
	span.SetAttributes(attribute.Bool("chora.include_suspended", includeSuspended))

	rows, err := s.repo.ListByGCID(ctx, gcid, includeSuspended)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "membership lookup: %v", err)
	}

	// Filter rows with empty role sets — they don't belong in the response.
	filtered := rows[:0]
	for _, r := range rows {
		if len(r.Roles) == 0 {
			continue
		}
		filtered = append(filtered, r)
	}

	span.SetAttributes(attribute.Int("chora.memberships.count", len(filtered)))

	if len(filtered) == 0 {
		return &tenancyv1.ListMembershipsByGCIDResponse{}, nil
	}

	// Find the default — max JoinedAt. Stable on ties (first-wins by
	// repo ordering). Rows with zero JoinedAt are eligible as a last
	// resort (post-bring-up migration may leave JoinedAt unset).
	defaultIdx := 0
	for i := 1; i < len(filtered); i++ {
		if filtered[i].JoinedAt.After(filtered[defaultIdx].JoinedAt) {
			defaultIdx = i
		}
	}

	out := make([]*tenancyv1.TenantMembership, 0, len(filtered))
	for i, r := range filtered {
		out = append(out, &tenancyv1.TenantMembership{
			TenantId:   r.TenantID,
			TenantSlug: r.TenantSlug,
			Roles:      append([]string(nil), r.Roles...),
			Surfaces:   RolesToSurfaces(r.Roles),
			IsDefault:  i == defaultIdx,
		})
	}

	return &tenancyv1.ListMembershipsByGCIDResponse{
		Memberships:     out,
		DefaultTenantId: filtered[defaultIdx].TenantID,
	}, nil
}

// -----------------------------------------------------------------------------
// Role → Surfaces map (Bucket 1 derived field)
// -----------------------------------------------------------------------------
//
// Aligned with CLAUDE.md §2 CHORA Surfaces — every membership role maps to
// the surfaces it unlocks for the holder:
//
//   learner       → [aplus, cplus]      (creator/consumer modes + social)
//   author        → [aplus]             (atom authoring)
//   instructor    → [rplus]             (training admin / classroom)
//   tenant_admin  → [hplus]             (tenant config / billing / addons)
//   auditor       → [oplus]             (governance + observability)
//
// Aliases are accepted for the canonical members table enum:
//   - "admin" treated as "tenant_admin"
//   - "owner" treated as "tenant_admin"
//   - "content_manager" treated as "author"
//   - "trainer" treated as "instructor"
//   - "supervisor" treated as "instructor"
//   - "super_admin" treated as "tenant_admin"
//   - "platform_ops" treated as "auditor"
//
// Unknown roles are silently ignored (yields no surfaces) — this keeps the
// adapter forward-compatible with role additions deferred to M14+ without
// blocking the membership response.

// roleSurfaceMap is the canonical role→surfaces table.
// Internal — exposed only via RolesToSurfaces for stability.
var roleSurfaceMap = map[string][]string{
	"learner":         {"aplus", "cplus"},
	"author":          {"aplus"},
	"content_manager": {"aplus"},
	"instructor":      {"rplus"},
	"trainer":         {"rplus"},
	"supervisor":      {"rplus"},
	"tenant_admin":    {"hplus"},
	"admin":           {"hplus"},
	"owner":           {"hplus"},
	"super_admin":     {"hplus"},
	"platform_ops":    {"oplus"},
	"auditor":         {"oplus"},
}

// RolesToSurfaces returns the deduplicated union of surfaces for the input
// roles[]. Unknown roles are dropped. Order is stable but not deterministic
// against a fixed canonical ordering (tests should sort before comparing).
//
// Exported so the same map can be reused by the chora-tenancy v2 admin
// handler when it lands the per-membership surface preview UI.
func RolesToSurfaces(roles []string) []string {
	if len(roles) == 0 {
		return []string{}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(roles)*2)
	for _, r := range roles {
		surfaces, ok := roleSurfaceMap[strings.ToLower(strings.TrimSpace(r))]
		if !ok {
			continue
		}
		for _, s := range surfaces {
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
