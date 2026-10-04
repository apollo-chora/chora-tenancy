// me_addons_handler.go — Setup Wizard Phase B HTTP handler (CHO-1664).
//
// Implements POST /api/v1/tenants/me/addons.
//
// Reads the "me" tenant from the `X-Tenant-Id` header (stamped by
// chora-gateway's phyllis aggregator after Chora session JWT verify;
// see services/chora-gateway/internal/aggregator/phyllis/phyllis.go).
// Owner GCID is read from the `gcid` / `X-GCID` header. Both must be
// present for the wizard's "tenant declared intent" attribution.
//
// Semantics — idempotent-additive. For every code in `add_on_codes`,
// `SubscribePending` records a row in StatusPendingActivation. Codes
// already subscribed (in any status) are no-ops. Codes the tenant
// previously had but that are NOT in this request are LEFT UNCHANGED
// — deactivation lives in the dedicated add-on lifecycle flow, not
// the wizard. See chora-contracts/openapi/tenancy-admin.yaml
// `subscribeMyTenantAddons` for the wire contract.
//
// Validation is up-front: every code is checked against the catalogue
// BEFORE any SubscribePending is called, so a request with one bad code
// fails atomically (no partial subscriptions).
package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// MeAddOnsHandler serves POST + GET /api/v1/tenants/me/addons.
//
// `reg` (in-memory SubscriptionRegistry) is consulted ONLY for catalogue
// validation (HasAddOn) — the source of truth for which codes are
// known. `pgRepo` handles persistence (writes survive pod restart) and
// is the source of truth for the GET hydration read (CHO-1692). When
// pgRepo is nil (dev env without a pgx pool) the handler falls back to
// the in-memory subscription store for both write + read, preserving
// the pre-CHO-1692 behaviour for that lane.
type MeAddOnsHandler struct {
	reg    *addon.SubscriptionRegistry
	pgRepo *pg.SetupWizardAddonRepository
}

// NewMeAddOnsHandler constructs the handler. Panics on nil registry so
// wiring bugs fail loud at boot, not request time. pgRepo MAY be nil
// in dev-without-pgx environments.
func NewMeAddOnsHandler(reg *addon.SubscriptionRegistry, pgRepo *pg.SetupWizardAddonRepository) *MeAddOnsHandler {
	if reg == nil {
		panic("httpapi.NewMeAddOnsHandler: nil SubscriptionRegistry")
	}
	return &MeAddOnsHandler{reg: reg, pgRepo: pgRepo}
}

type meAddOnsRequest struct {
	AddOnCodes []string `json:"add_on_codes"`
}

type meAddOnSubscriptionDTO struct {
	SubscriptionID  string `json:"subscription_id"`
	AddOnCode       string `json:"add_on_code"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	NewlySubscribed bool   `json:"newly_subscribed"`
}

type meAddOnsResponse struct {
	Subscriptions []meAddOnSubscriptionDTO `json:"subscriptions"`
}

// ServeHTTP dispatches GET (CHO-1692 — list active selections for re-entry
// hydration) and POST (CHO-1664 — subscribe pending) on the same path;
// everything else returns 405.
func (h *MeAddOnsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	default:
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

// handleGet serves GET /api/v1/tenants/me/addons (CHO-1692).
// Returns the persisted add-on selections so the wizard's re-entry
// hydration can pre-fill step 2's selectedAddOns Set. Empty list is
// 200, not 404 — a fresh tenant with no selections is normal.
func (h *MeAddOnsHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller tenant required (X-Tenant-Id header — chora-gateway stamps after JWT verify)")
		return
	}
	subs := []meAddOnSubscriptionDTO{}
	if h.pgRepo != nil {
		rows, err := h.pgRepo.ListByTenant(r.Context(), tenantID)
		if err != nil {
			v2WriteError(w, http.StatusInternalServerError, "internal_error", err.Error())
			return
		}
		for _, row := range rows {
			subs = append(subs, meAddOnSubscriptionDTO{
				AddOnCode: row.AddOnCode,
				Status:    row.Status,
			})
		}
	} else {
		// Dev-without-pgx fallback — read from the in-memory registry so
		// same-session selections still hydrate. NOT durable across
		// restart (that's the whole reason the pg path exists).
		for _, sub := range h.reg.ListAllByTenant(tenantID) {
			subs = append(subs, meAddOnSubscriptionDTO{
				SubscriptionID: sub.ID,
				AddOnCode:      sub.AddOnCode,
				Status:         string(sub.Status),
				CreatedAt:      sub.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
			})
		}
	}
	v2WriteJSON(w, http.StatusOK, meAddOnsResponse{Subscriptions: subs})
}

// handlePost serves POST /api/v1/tenants/me/addons (CHO-1664). Per
// docs, idempotent-additive: re-clicking Next is a no-op.
func (h *MeAddOnsHandler) handlePost(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		v2WriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller tenant required (X-Tenant-Id header — chora-gateway stamps after JWT verify)")
		return
	}
	gcid := strings.TrimSpace(firstNonEmpty(r.Header.Get("X-GCID"), r.Header.Get("gcid")))

	var req meAddOnsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	// CHO-1692 — empty list IS valid: the wizard sends [] when the
	// user has unticked every previously-selected add-on. The
	// REPLACE-semantics pg path below interprets [] as "remove all
	// wizard-managed selections for this tenant" (admin-side
	// add_on_subscriptions are untouched).

	// Up-front catalogue validation — all-or-nothing per the OpenAPI
	// description. Without the pre-check we'd partially subscribe the
	// tenant before hitting a bad code; with it, an unknown code fails
	// the whole request atomically.
	var unknown []string
	for _, code := range req.AddOnCodes {
		if !h.reg.HasAddOn(code) {
			unknown = append(unknown, code)
		}
	}
	if len(unknown) > 0 {
		v2WriteError(w, http.StatusBadRequest, "unknown_addon_code",
			"unknown add-on code(s): "+strings.Join(unknown, ", "))
		return
	}

	// Track which (tenant, code) rows already existed so we can set
	// newly_subscribed correctly without an extra round-trip.
	existed := map[string]bool{}
	for _, code := range req.AddOnCodes {
		_, ok := h.reg.GetSubscription(tenantID, code)
		existed[code] = ok
	}

	subs := make([]meAddOnSubscriptionDTO, 0, len(req.AddOnCodes))
	for _, code := range req.AddOnCodes {
		sub, err := h.reg.SubscribePending(tenantID, code, gcid)
		if err != nil {
			v2WriteError(w, http.StatusInternalServerError, "subscribe_failed", err.Error())
			return
		}
		subs = append(subs, meAddOnSubscriptionDTO{
			SubscriptionID:  sub.ID,
			AddOnCode:       sub.AddOnCode,
			Status:          string(sub.Status),
			CreatedAt:       sub.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
			NewlySubscribed: !existed[code],
		})
	}

	// CHO-1692 — REPLACE semantics on the wizard-managed selections so
	// unticking an option actually removes it. Single tx: upsert each
	// code in the request + delete every row for this tenant whose code
	// is NOT in the request. Admin-side add_on_subscriptions are
	// untouched — they own their own activation/deactivation lifecycle.
	if h.pgRepo != nil {
		if err := h.pgRepo.ReplaceSet(r.Context(), tenantID, req.AddOnCodes); err != nil {
			v2WriteError(w, http.StatusInternalServerError, "subscribe_failed",
				"pg replace failed: "+err.Error())
			return
		}
	}

	v2WriteJSON(w, http.StatusOK, meAddOnsResponse{Subscriptions: subs})
}
