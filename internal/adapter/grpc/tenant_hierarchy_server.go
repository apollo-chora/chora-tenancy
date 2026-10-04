// tenant_hierarchy_server.go — gRPC adapter for the Tenancy/GetTenantHierarchy
// RPC (ADR-217 Phase 2.3b, CHO-2010). Returns the DIRECT children of the
// caller's current tenant (Model β franchise hierarchy) with a per-child live
// member count, backing the H+ tenant-deletion parent-check + the operator
// managed-tenant picker (chora-web account-lifecycle.service.ts).
//
// Trust model: mTLS via Cloud Service Mesh; the caller (chora-gateway, on behalf
// of a platform_operator OR tenant_admin) is authorized by the Istio
// AuthorizationPolicy allow-list + the gateway role gate. No JWT/role check
// here — tenant_id is stamped by the BFF from the validated session. This read
// only ever exposes the DIRECT children of the SUPPLIED tenant, so a caller can
// never enumerate a tenant they do not already hold a session for.
package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"
)

// TenantHierarchy is the domain view HierarchyReader returns: the direct
// children of a tenant + whether the tenant is a parent (>= 1 child).
type TenantHierarchy struct {
	IsParent bool
	Children []ChildTenantSummary
}

// ChildTenantSummary is one DIRECT child of the queried tenant + its counts.
// AtomCount is always 0 for now — atoms live in chora_creation and cross-DB
// reads are forbidden (ddd-enforcement); a future event-fed projection will
// populate it.
type ChildTenantSummary struct {
	TenantID   string
	TenantName string
	UserCount  int64
	AtomCount  int64
}

// HierarchyReader is the narrow port GetTenantHierarchy depends on. Satisfied
// by the pg TenantHierarchyRepo, which reads the RLS-free tenants registry for
// the direct children then counts each child's live members under that child's
// own RLS scope (the members policy is tenant-pinned, not span-all).
type HierarchyReader interface {
	GetHierarchy(ctx context.Context, tenantID string) (TenantHierarchy, error)
}

// WithHierarchyReader wires the hierarchy read port (ADR-217 Phase 2.3b) and
// returns the server for fluent composition at the composition root
// (cmd/server/main.go).
func (s *MembershipServer) WithHierarchyReader(h HierarchyReader) *MembershipServer {
	s.hierarchy = h
	return s
}

// GetTenantHierarchy implements tenancyv1.TenancyServer. Returns the DIRECT
// children (parent_tenant_id = req.tenant_id) of the caller's current tenant,
// with per-child counts. tenant_id is required + UUID-shaped.
func (s *MembershipServer) GetTenantHierarchy(
	ctx context.Context,
	req *tenancyv1.GetTenantHierarchyRequest,
) (*tenancyv1.GetTenantHierarchyResponse, error) {
	if s.hierarchy == nil {
		return nil, status.Error(codes.Unimplemented, "tenant hierarchy read path not wired")
	}

	tenantID := strings.TrimSpace(req.GetTenantId())
	if tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id required")
	}
	if !looksLikeUUID(tenantID) {
		return nil, status.Error(codes.InvalidArgument, "tenant_id must be a UUID")
	}

	h, err := s.hierarchy.GetHierarchy(ctx, tenantID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "tenant hierarchy: %v", err)
	}

	children := make([]*tenancyv1.ChildTenantSummary, 0, len(h.Children))
	for _, c := range h.Children {
		children = append(children, &tenancyv1.ChildTenantSummary{
			TenantId:   c.TenantID,
			TenantName: c.TenantName,
			UserCount:  c.UserCount,
			AtomCount:  c.AtomCount,
		})
	}

	return &tenancyv1.GetTenantHierarchyResponse{
		IsParent: h.IsParent,
		Children: children,
	}, nil
}
