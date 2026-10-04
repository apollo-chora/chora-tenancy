// me_finish_setup_handler.go — Setup Wizard Phase C HTTP handler (CHO-1682).
//
// Implements PATCH /api/v1/tenants/me/finish-setup — the chora-tenancy
// half of the BFF aggregator's fan-out. Set-once semantics:
//
//   - First successful call flips `wizard_completed_at` from NULL to
//     `now()`, persists via tenant.Repository.Save, and returns
//     `newly_completed=true` in the response body.
//   - Subsequent calls observe the already-stamped timestamp via the
//     domain Tenant.FinishWizard's set-once invariant, return the
//     existing timestamp, and report `newly_completed=false` —
//     critically, do NOT touch the repository (no spurious UPDATE,
//     and downstream consumers of any future
//     `chora.tenancy.tenant_wizard_completed.v1` event never see
//     duplicates).
//
// Tenant identity is read from the `X-Tenant-Id` header stamped by
// chora-gateway's phyllis aggregator after JWT verify. Missing header
// returns 401; an unknown tenant returns 404. The BFF aggregator at
// `POST /api/v1/tenants/setup` is the canonical caller; FE callers
// SHOULD NOT call this endpoint directly.
//
// NOTE — outbox emission is deliberately deferred to a follow-up story
// to keep CHO-1682's BE+BFF scope bounded. The existing branding +
// addons handlers in this service follow the same "persist-only,
// event-emission-follows" cadence. The `newly_completed` boolean is
// the FE-visible signal that gates the success toast; when the event
// path lands, `newly_completed` will also be the publisher's gate.
package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

// MeFinishSetupHandler serves PATCH /api/v1/tenants/me/finish-setup.
type MeFinishSetupHandler struct {
	repo tenant.Repository
}

// NewMeFinishSetupHandler constructs the handler. Panics on nil repo
// so wiring bugs fail loud at boot.
func NewMeFinishSetupHandler(repo tenant.Repository) *MeFinishSetupHandler {
	if repo == nil {
		panic("httpapi.NewMeFinishSetupHandler: nil tenant.Repository")
	}
	return &MeFinishSetupHandler{repo: repo}
}

type finishSetupResponse struct {
	TenantID          string `json:"tenant_id"`
	WizardCompletedAt string `json:"wizard_completed_at"`
	NewlyCompleted    bool   `json:"newly_completed"`
}

// ServeHTTP routes PATCH and rejects everything else with 405.
func (h *MeFinishSetupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	newly := t.FinishWizard(time.Now().UTC())
	if newly {
		// Only persist on actual transition. Re-finishes are pure no-ops
		// — no Save, no future event-republish.
		if err := h.repo.Save(ctx, t); err != nil {
			v2WriteError(w, http.StatusInternalServerError, "internal_error",
				"tenant save failed: "+err.Error())
			return
		}
	}

	v2WriteJSON(w, http.StatusOK, finishSetupResponse{
		TenantID:          t.ID,
		WizardCompletedAt: t.WizardCompletedAt.Format("2006-01-02T15:04:05Z07:00"),
		NewlyCompleted:    newly,
	})
}
