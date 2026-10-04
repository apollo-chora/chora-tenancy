// bootstrap_handler.go — H+ Setup-Tenant Phase 1 HTTP handler
// (CHO-1628). Implements POST /v1/tenants/bootstrap.
//
// Caller's GCID is read from the `X-GCID` request header, which the
// chora-gateway BFF stamps after verifying the Chora session JWT. This
// matches the pre-existing header convention used by all other handlers
// in this package (see v1_handlers.go / addon_screens_handlers.go).
//
// ADR-182 D5 (CHO-1717 §4.5) — private tenant creation is
// PLATFORM_OPERATOR-only. The caller's session-JWT roles arrive on the
// mTLS-bound `x-mesh-user-roles` header (servicemesh.HeaderUserRoles),
// stamped by the chora-gateway BFF after jwt_auth.go validates the Chora
// session JWT (MeshClaims.Roles → phyllis call() MarshalToHeaders). A
// caller whose roles do not include `platform_operator` (case-insensitive
// — the canonical platform constant is PLATFORM_OPERATOR per ADR-165)
// gets 403 AUTH_PLATFORM_OPERATOR_REQUIRED. The Phase 5 "bootstrap-mode"
// carve-out (any authenticated zero-membership caller may bootstrap,
// CHO-1648/CHO-1653) is DEAD.
//
// Domain → HTTP status mapping:
//
//	(no platform_operator role) → 403 AUTH_PLATFORM_OPERATOR_REQUIRED
//	bootstrap.ErrInvalidArgument  → 400 Bad Request
//	bootstrap.ErrAlreadyMember    → 409 Conflict
//	(other errors)                → 500 Internal Server Error
//
// JSON body decoding + response writing reuse the v2 helpers
// (v2Decode / v2WriteJSON / v2WriteError) for envelope consistency.
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-common/auth/servicemesh"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

// rolePlatformOperator is the role required to create a private tenant
// (ADR-182 D5). Matched case-insensitively so both the snake_case JWT
// claim form (`platform_operator`) and the canonical ADR-165 constant
// (`PLATFORM_OPERATOR`) pass.
const rolePlatformOperator = "platform_operator"

// errCodePlatformOperatorRequired is the pinned wire error code for the
// 403 — the FE H+ Setup-Tenant wizard keys off it.
const errCodePlatformOperatorRequired = "AUTH_PLATFORM_OPERATOR_REQUIRED"

// callerIsPlatformOperator reports whether the gateway-propagated
// session-JWT roles header carries platform_operator. Exact per-entry
// match (after trim, case-insensitive) — prefix/superset role names do
// NOT pass.
func callerIsPlatformOperator(h http.Header) bool {
	raw := h.Get(servicemesh.HeaderUserRoles)
	if strings.TrimSpace(raw) == "" {
		return false
	}
	for _, part := range strings.Split(raw, ",") {
		if strings.EqualFold(strings.TrimSpace(part), rolePlatformOperator) {
			return true
		}
	}
	return false
}

// BootstrapService is the narrow port the handler depends on. The
// production wiring (`bootstrap.NewService(pg.NewBootstrapRepository(...))`)
// satisfies it; tests use a fake.
type BootstrapService interface {
	Bootstrap(ctx context.Context, in bootstrap.Input) (*bootstrap.Output, error)
}

// BootstrapHandler serves POST /v1/tenants/bootstrap.
type BootstrapHandler struct {
	svc BootstrapService
}

// NewBootstrapHandler constructs the handler. Panics on nil service so
// wiring bugs fail loud at boot, not request time.
func NewBootstrapHandler(svc BootstrapService) *BootstrapHandler {
	if svc == nil {
		panic("httpapi.NewBootstrapHandler: nil BootstrapService")
	}
	return &BootstrapHandler{svc: svc}
}

// bootstrapRequest matches the OpenAPI BootstrapTenantRequest schema in
// chora-contracts/openapi/tenancy-admin.yaml (v1.3.0). OwnerGCID is NOT
// part of the body — it comes from the X-GCID header.
type bootstrapRequest struct {
	Name string `json:"name"`
}

// bootstrapResponse matches the OpenAPI BootstrapTenantResponse schema.
type bootstrapResponse struct {
	TenantID      string `json:"tenant_id"`
	OwnerMemberID string `json:"owner_member_id"`
	EntitlementID string `json:"entitlement_id"`
	CreatedAt     string `json:"created_at"`
}

// ServeHTTP routes POST and rejects everything else with 405.
func (h *BootstrapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	// 1. Verified-JWT GCID from BFF-stamped header. The chora-gateway
	// aggregator stamps `gcid` (lowercase, no dash) as the canonical
	// header for chora-consumption + chora-tenancy backends (see
	// services/chora-gateway/internal/aggregator/phyllis/phyllis.go).
	// The v1/v2 handlers in this package use the same fallback chain
	// (X-GCID → gcid). Empty / whitespace → 401.
	gcid := strings.TrimSpace(firstNonEmpty(
		r.Header.Get("X-GCID"),
		r.Header.Get("gcid"),
	))
	if gcid == "" {
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller GCID required (X-GCID or gcid header — chora-gateway stamps after JWT verify)")
		return
	}

	// 2. ADR-182 D5 operator gate — AFTER the 401 authn check (an
	// unauthenticated caller is 401, not 403), BEFORE any body work.
	if !callerIsPlatformOperator(r.Header) {
		v2WriteError(w, http.StatusForbidden, errCodePlatformOperatorRequired,
			"private tenant creation requires the platform_operator role (ADR-182 D5)")
		return
	}

	// 3. Decode body.
	var req bootstrapRequest
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}

	// 4. Invoke the orchestrator.
	out, err := h.svc.Bootstrap(r.Context(), bootstrap.Input{
		Name:      req.Name,
		OwnerGCID: gcid,
	})
	if err != nil {
		switch {
		case errors.Is(err, bootstrap.ErrInvalidArgument):
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		case errors.Is(err, bootstrap.ErrAlreadyMember):
			v2WriteError(w, http.StatusConflict, "already_member",
				"caller already holds an active TenantMembership")
		default:
			v2WriteError(w, http.StatusInternalServerError, "internal_error",
				"bootstrap failed")
		}
		return
	}

	// 5. Success — 201 with identifiers.
	v2WriteJSON(w, http.StatusCreated, bootstrapResponse{
		TenantID:      out.TenantID,
		OwnerMemberID: out.OwnerMemberID,
		EntitlementID: out.EntitlementID,
		CreatedAt:     out.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
	})
}
