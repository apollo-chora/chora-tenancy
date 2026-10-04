// me_tenant_handler.go — Setup Wizard re-entry hydration GET endpoint
// (CHO-1692).
//
// Implements GET /api/v1/tenants/me — returns the calling tenant's
// branding + wizard_completed_at + identity-bearing fields the wizard
// uses to pre-fill its form signals on re-entry. Reads from the
// pg-backed tenant.Repository (same source the wizard's
// PATCH /me/branding + PATCH /me/finish-setup writes go through), so
// the response always reflects the latest persisted state.
//
// Distinct from the LEGACY GET /api/tenants/{id} (handlers.go::tenantDTO)
// which reads from an in-memory registry that DOES NOT reflect the
// pg-backed wizard writes and DOES NOT carry wizard_completed_at —
// shipped pre-CHO-1655, unsuitable for hydration.
//
// Resolves the "me" tenant from the `X-Tenant-Id` header (stamped by
// chora-gateway after Chora session JWT validation). Empty header → 401.
// Tenant lookup miss → 404 (the JWT carried a tenant_id but it's not in
// chora_tenancy — possibly a stale token).
//
// Wire shape mirrors `v1TenantDTO` for cross-handler consistency.
// `wizard_completed_at` is OMITTED (not emitted as `null`) when NULL,
// matching the activated_at / deleted_at convention so the FE's
// `wizard_completed_at !== null` banner check works without special
// "is field present" guards.
package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// MeTenantHandler serves GET /api/v1/tenants/me.
type MeTenantHandler struct {
	repo tenant.Repository
}

// NewMeTenantHandler constructs the handler. Panics on nil repo so
// wiring bugs fail loud at boot.
func NewMeTenantHandler(repo tenant.Repository) *MeTenantHandler {
	if repo == nil {
		panic("httpapi.NewMeTenantHandler: nil tenant.Repository")
	}
	return &MeTenantHandler{repo: repo}
}

// ServeHTTP routes GET and rejects everything else with 405.
func (h *MeTenantHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller tenant required (X-Tenant-Id header — chora-gateway stamps after JWT verify)")
		return
	}

	t, found, err := h.repo.Get(r.Context(), tenantID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	if !found {
		v2WriteError(w, http.StatusNotFound, "tenant_not_found",
			"tenant not found for the caller (stale JWT or tenant deleted)")
		return
	}

	out := map[string]interface{}{
		"id":               t.ID,
		"display_name":     t.DisplayName,
		"owner_gcid":       t.OwnerGCID,
		"slug":             t.Slug,
		"country":          t.Country,
		"default_currency": t.DefaultCurrency,
		"parent_tenant_id": t.ParentTenantID,
		"self_hosted":      t.SelfHosted,
		"status":           string(t.Status),
		"branding": map[string]interface{}{
			"primary_color_hex": t.Branding.PrimaryColorHex,
			"logo_url":          t.Branding.LogoURL,
			"custom_domain":     t.Branding.CustomDomain,
		},
		"created_at": t.CreatedAt.Format(time.RFC3339Nano),
		"updated_at": t.UpdatedAt.Format(time.RFC3339Nano),
	}
	if t.StripeCustomerID != "" {
		out["stripe_customer_id"] = t.StripeCustomerID
	}
	if t.ActivatedAt != nil {
		out["activated_at"] = t.ActivatedAt.Format(time.RFC3339Nano)
	}
	if t.WizardCompletedAt != nil {
		out["wizard_completed_at"] = t.WizardCompletedAt.Format(time.RFC3339Nano)
	}

	v2WriteJSON(w, http.StatusOK, out)
}
