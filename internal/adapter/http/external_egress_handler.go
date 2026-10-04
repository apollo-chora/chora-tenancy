// external_egress_handler.go — the H+ tenant external web-egress admin surface
// (CHO-2148).
//
//	GET   /api/v1/admin/tenants/{tenantID}/external-egress
//	PATCH /api/v1/admin/tenants/{tenantID}/external-egress
//
// The Tenant Admin opts their tenant in/out of external web egress (Far Sight /
// Seeker grounded search) and tunes the daily grounded-call ceiling.
//
// AUTHZ — gated on the mesh header, fail-CLOSED, via callerHoldsHPlusAdminRole.
// Do NOT reach for rolesAllowed(X-Role) (deleted, CHO-2072: the gateway never
// stamps X-Role, so that gate fail-OPENED and silently unenforced twelve
// tenant-admin writes in production). platform_operator is NOT admitted — egress
// is TENANT DATA and the operator is non-tenant-scoped (CHO-2076).
//
// A successful PATCH writes the policy row AND the
// chora.tenancy.external_egress_policy.updated.v1 event in ONE transaction (see
// pg.ExternalEgressRepository). chora-observability projects that event into the
// read-copy the model-gateway consults fail-closed on every grounded call —
// cross-DB writes are forbidden, so the event is the only bridge.
package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

// externalEgressDTO is the wire shape for both GET and PATCH responses.
type externalEgressDTO struct {
	TenantID         string `json:"tenant_id"`
	EgressEnabled    bool   `json:"egress_enabled"`
	DailyCallCeiling int32  `json:"daily_call_ceiling"`

	// OptedIn distinguishes "never opted in" (no row — the ADR-220 D4
	// default-deny posture a franchise tenant starts in) from "explicitly
	// turned off". Both are egress_enabled=false, but H+ renders them
	// differently and conflating them would hide the default from the admin.
	OptedIn bool `json:"opted_in"`

	// MaxDailyCallCeiling lets H+ bound its input without hardcoding the
	// platform cap (no inline config on the FE).
	MaxDailyCallCeiling int32 `json:"max_daily_call_ceiling"`

	Version       int64  `json:"version"`
	UpdatedByGCID string `json:"updated_by_gcid,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

// externalEgressPatchReq — PATCH-merge semantics. A nil field is LEFT ALONE, not
// reset: omitting egress_enabled while retuning the ceiling must never silently
// turn egress off.
type externalEgressPatchReq struct {
	EgressEnabled    *bool  `json:"egress_enabled"`
	DailyCallCeiling *int32 `json:"daily_call_ceiling"`
}

func toExternalEgressDTO(p external_egress.Policy) externalEgressDTO {
	dto := externalEgressDTO{
		TenantID:            p.TenantID,
		EgressEnabled:       p.EgressEnabled,
		DailyCallCeiling:    p.DailyCallCeiling,
		OptedIn:             p.Persisted(),
		MaxDailyCallCeiling: external_egress.MaxDailyCallCeiling,
		Version:             p.Version,
		UpdatedByGCID:       p.UpdatedByGCID,
	}
	if !p.UpdatedAt.IsZero() {
		dto.UpdatedAt = p.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return dto
}

// handleExternalEgress dispatches the GET/PATCH external-egress admin surface.
func handleExternalEgress(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	// Fail LOUD when unwired. A stubbed "OFF" here would be indistinguishable
	// from a real policy denial — exactly the silent-fallback the engineering
	// standard forbids.
	if deps.EgressPolicies == nil {
		v2WriteError(w, http.StatusServiceUnavailable, "egress_policy_unwired",
			"external-egress policy repository is not wired")
		return
	}

	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden",
			"tenant admin role required to view or change external web egress")
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleExternalEgressGet(deps, tenantID, w, r)
	case http.MethodPatch:
		handleExternalEgressPatch(deps, tenantID, w, r)
	default:
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET or PATCH only")
	}
}

func handleExternalEgressGet(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	p, err := deps.EgressPolicies.Get(r.Context(), tenantID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusOK, toExternalEgressDTO(p))
}

func handleExternalEgressPatch(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	actor := callerGCID(r)
	if actor == "" {
		// An unattributed egress change is refused — the audit trail must always
		// be able to answer "who turned this on" (IMDA D1).
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"gcid missing from session — an egress change must name its actor")
		return
	}

	var req externalEgressPatchReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if req.EgressEnabled == nil && req.DailyCallCeiling == nil {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed",
			"at least one of egress_enabled or daily_call_ceiling is required")
		return
	}

	current, err := deps.EgressPolicies.Get(r.Context(), tenantID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	// PATCH-merge: an omitted field keeps its current value.
	enabled := current.EgressEnabled
	if req.EgressEnabled != nil {
		enabled = *req.EgressEnabled
	}
	ceiling := current.DailyCallCeiling
	if req.DailyCallCeiling != nil {
		ceiling = *req.DailyCallCeiling
	}

	prev := current // value copy — the before-state the event + audit carry
	next := current
	changed, err := next.Update(enabled, ceiling, actor, time.Now().UTC())
	if err != nil {
		switch {
		case errors.Is(err, external_egress.ErrCeilingOutOfRange):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		case errors.Is(err, external_egress.ErrEmptyActor):
			v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated", err.Error())
		case errors.Is(err, external_egress.ErrEmptyTenantID):
			v2WriteError(w, http.StatusBadRequest, "invalid_tenant", err.Error())
		default:
			v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}

	// An identical write is a no-op: nothing persisted, nothing published,
	// nothing audited. The trail records real decisions only.
	if !changed {
		v2WriteJSON(w, http.StatusOK, toExternalEgressDTO(current))
		return
	}

	if err := deps.EgressPolicies.Save(r.Context(), next, prev); err != nil {
		switch {
		case errors.Is(err, external_egress.ErrConcurrentUpdate):
			v2WriteError(w, http.StatusConflict, "conflict",
				"the egress policy changed concurrently — re-read and retry")
		default:
			v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}

	v2WriteJSON(w, http.StatusOK, toExternalEgressDTO(next))
}
