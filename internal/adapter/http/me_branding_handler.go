// me_branding_handler.go — Setup Wizard Phase A HTTP handler
// (CHO-1655).
//
// Implements PATCH /api/v1/tenants/me/branding.
//
// Resolves the "me" tenant from the `X-Tenant-Id` request header, which
// chora-gateway's phyllis aggregator stamps after Chora session JWT
// validation (see services/chora-gateway/internal/aggregator/phyllis/
// phyllis.go: `req.Header.Set("X-Tenant-Id", auth.TenantID)`). Empty
// header → 401. Empty body or invalid field → 400.
//
// Body shape is FLAT (no nested `branding` wrapper). PATCH semantics:
// only fields the caller includes get updated; unspecified fields stay
// at their previous values (merge happens BEFORE the domain replace).
//
// Persistence uses the context-aware `tenant.Repository` port
// (pg.TenantRepository in production). NOT the in-memory v2 admin
// `*tenant.Registry` — that one drops state on pod restart and is
// pending M14+ migration (see services/chora-tenancy/cmd/server/
// main.go:124-129).
//
// Domain → HTTP status mapping:
//   tenant.ErrInvalidArgument  → 400 Bad Request (bad hex / bad logo URL)
//   tenant lookup miss          → 404 Not Found
//   missing tenant header       → 401 Unauthenticated
package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// MeBrandingHandler serves PATCH /api/v1/tenants/me/branding.
type MeBrandingHandler struct {
	repo tenant.Repository
}

// NewMeBrandingHandler constructs the handler. Panics on nil repo so
// wiring bugs fail loud at boot, not request time.
func NewMeBrandingHandler(repo tenant.Repository) *MeBrandingHandler {
	if repo == nil {
		panic("httpapi.NewMeBrandingHandler: nil tenant.Repository")
	}
	return &MeBrandingHandler{repo: repo}
}

// brandingRequest is the FLAT wire shape. `omitempty` so the FE can
// PATCH partial updates (e.g. color only) without zeroing other fields
// at the domain layer — the merge below skips empty inputs.
type brandingRequest struct {
	PrimaryColorHex string `json:"primary_color_hex,omitempty"`
	LogoURL         string `json:"logo_url,omitempty"`
	CustomDomain    string `json:"custom_domain,omitempty"`
}

// brandingResponse mirrors `tenant.BrandingConfig` on the wire. Always
// emits every field (no omitempty) so the FE can render the persisted
// state without a follow-up GET.
type brandingResponse struct {
	PrimaryColorHex string `json:"primary_color_hex"`
	LogoURL         string `json:"logo_url"`
	CustomDomain    string `json:"custom_domain"`
}

// ServeHTTP routes PATCH and rejects everything else with 405.
func (h *MeBrandingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller tenant required (X-Tenant-Id header — chora-gateway stamps after JWT verify)")
		return
	}

	var req brandingRequest
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}

	ctx := r.Context()
	t, ok, err := h.repo.Get(ctx, tenantID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal_error",
			"tenant lookup failed: "+err.Error())
		return
	}
	if !ok {
		v2WriteError(w, http.StatusNotFound, "tenant_not_found", "tenant not found")
		return
	}

	// PATCH-merge: start from current branding, overlay non-empty fields.
	// `tenant.UpdateBranding` REPLACES `t.Branding` wholesale so the
	// merge here is what gives this endpoint PATCH semantics.
	merged := t.Branding
	if req.PrimaryColorHex != "" {
		merged.PrimaryColorHex = req.PrimaryColorHex
	}
	if req.LogoURL != "" {
		merged.LogoURL = req.LogoURL
	}
	if req.CustomDomain != "" {
		merged.CustomDomain = req.CustomDomain
	}

	if err := t.UpdateBranding(merged); err != nil {
		switch {
		case errors.Is(err, tenant.ErrInvalidArgument):
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		default:
			v2WriteError(w, http.StatusInternalServerError, "internal_error",
				"branding update failed: "+err.Error())
		}
		return
	}

	if err := h.repo.Save(ctx, t); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal_error",
			"tenant save failed: "+err.Error())
		return
	}

	v2WriteJSON(w, http.StatusOK, brandingResponse{
		PrimaryColorHex: t.Branding.PrimaryColorHex,
		LogoURL:         t.Branding.LogoURL,
		CustomDomain:    t.Branding.CustomDomain,
	})
}
