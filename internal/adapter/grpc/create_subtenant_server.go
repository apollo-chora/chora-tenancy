// create_subtenant_server.go — gRPC adapter for the Tenancy/CreateSubTenant RPC
// (ADR-217 Phase 2.1, CHO-2006). Implements the persisted operator create-path:
// an operator provisions a sub-tenant under an existing parent, writing
// parent_tenant_id + hosting_mode. Reuses the atomic bootstrap flow (tenant +
// owner-member + core entitlement + outbox event); the migration-0026
// parent-invariant trigger validates the parent resolves to the chora-master
// tree at persist time.
//
// Trust model: mTLS via Cloud Service Mesh; the caller (chora-gateway, on behalf
// of a platform_operator) is authorized by the Istio AuthorizationPolicy
// (CreateSubTenant is already allow-listed). No JWT validation here.
package grpc

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"
)

// SubTenantCreator is the narrow port CreateSubTenant depends on. Satisfied by
// *bootstrap.Service, which persists the atomic tenant + owner-member + core
// entitlement + outbox event, extended (ADR-217) with parent_tenant_id +
// hosting_mode and WITHOUT the one-tenant-per-GCID self-onboard block.
type SubTenantCreator interface {
	CreateSubTenant(ctx context.Context, in bootstrap.Input) (*bootstrap.Output, error)
}

// WithSubTenantCreator wires the sub-tenant create port and returns the server
// for fluent composition at the composition root (cmd/server/main.go).
func (s *MembershipServer) WithSubTenantCreator(c SubTenantCreator) *MembershipServer {
	s.creator = c
	return s
}

// CreateSubTenant implements tenancyv1.TenancyServer.
func (s *MembershipServer) CreateSubTenant(
	ctx context.Context,
	req *tenancyv1.CreateSubTenantRequest,
) (*tenancyv1.CreateSubTenantResponse, error) {
	if s.creator == nil {
		return nil, status.Error(codes.Unimplemented, "sub-tenant create path not wired")
	}

	parent := strings.TrimSpace(req.GetParentTenantId())
	name := strings.TrimSpace(req.GetDisplayName())
	owner := strings.TrimSpace(req.GetOwnerGcid())
	if parent == "" {
		return nil, status.Error(codes.InvalidArgument, "parent_tenant_id required")
	}
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "display_name required")
	}
	if owner == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_gcid required")
	}

	// Resolve the effective hosting mode HERE so the response reflects what is
	// persisted: UNSPECIFIED → FRANCHISE (a sub-tenant is a franchise by
	// default in Model β); an explicit mode is honoured.
	hostingMode := hostingModeToString(req.GetHostingMode())
	if hostingMode == "" {
		hostingMode = bootstrap.HostingModeFranchise
	}

	out, err := s.creator.CreateSubTenant(ctx, bootstrap.Input{
		Name:           name,
		OwnerGCID:      owner,
		ParentTenantID: parent,
		HostingMode:    hostingMode,
		AddOnCodes:     req.GetAddOnCodes(),
	})
	if err != nil {
		if errors.Is(err, bootstrap.ErrInvalidArgument) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		// An unrecognised add-on code is a caller error in add_on_codes, not a
		// parent-invariant problem. Without this arm it falls through to
		// FailedPrecondition below and reaches the operator as a 409 about the
		// PARENT, which is a true sentence about the wrong field. The message
		// carries the offending code so they can see which checkbox was wrong.
		if errors.Is(err, bootstrap.ErrUnknownAddOnCode) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		// Parent-invariant trigger rejection (foreign root / cycle), missing
		// parent (FK), or other persist failure — the whole bundle rolled back
		// (no partial tenant). FailedPrecondition signals a caller-fixable
		// parent problem without leaking internals.
		return nil, status.Errorf(codes.FailedPrecondition, "create sub-tenant: %v", err)
	}

	return &tenancyv1.CreateSubTenantResponse{
		Tenant: &tenancyv1.Tenant{
			TenantId:       out.TenantID,
			ParentTenantId: parent,
			DisplayName:    name,
			HostingMode:    stringToHostingMode(hostingMode),
			OwnerGcid:      owner,
			State:          tenancyv1.TenantState_TENANT_STATE_ACTIVE,
			CreatedAt:      timestamppb.New(out.CreatedAt),
		},
	}, nil
}

// hostingModeToString maps the proto HostingMode enum to the DB/domain string.
// UNSPECIFIED maps to "" so the caller can apply the sub-tenant default.
func hostingModeToString(m tenancyv1.HostingMode) string {
	switch m {
	case tenancyv1.HostingMode_HOSTING_MODE_PLATFORM_HOSTED:
		return bootstrap.HostingModePlatformHosted
	case tenancyv1.HostingMode_HOSTING_MODE_WHITE_LABEL:
		return bootstrap.HostingModeWhiteLabel
	case tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE:
		return bootstrap.HostingModeFranchise
	case tenancyv1.HostingMode_HOSTING_MODE_SELF_HOST:
		return bootstrap.HostingModeSelfHost
	default:
		return ""
	}
}

// stringToHostingMode is the inverse, for building the response Tenant.
func stringToHostingMode(s string) tenancyv1.HostingMode {
	switch s {
	case bootstrap.HostingModePlatformHosted:
		return tenancyv1.HostingMode_HOSTING_MODE_PLATFORM_HOSTED
	case bootstrap.HostingModeWhiteLabel:
		return tenancyv1.HostingMode_HOSTING_MODE_WHITE_LABEL
	case bootstrap.HostingModeFranchise:
		return tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE
	case bootstrap.HostingModeSelfHost:
		return tenancyv1.HostingMode_HOSTING_MODE_SELF_HOST
	default:
		return tenancyv1.HostingMode_HOSTING_MODE_UNSPECIFIED
	}
}
