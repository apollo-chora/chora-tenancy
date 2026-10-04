// Package httpapi (v1 — Phyllis MVP §5.2 + S2.2 fill-gaps)
//
// This file adds the Phyllis-MVP-shaped routes:
//
//	POST   /v1/tenants                                                    light-wizard create (idempotent on slug)
//	GET    /v1/tenants/{id}                                               read tenant
//	POST   /v1/admin/tenants/{id}/golive                                  pending → active + Stripe Customer + ManaPool
//	GET    /v1/admin/tenants/{id}/addons                                  list active+available add-ons
//	GET    /v1/admin/tenants/{id}/addons/{addonPlanId}                    detail
//	GET    /v1/admin/tenants/{id}/addons/{addonPlanId}/usage              current period usage
//	POST   /v1/admin/tenants/{id}/addons/{addonPlanId}/activate           idempotent activate
//	POST   /v1/admin/tenants/{id}/addons/{addonPlanId}/change-tier        Stripe Subscription Schedule for proration
//	POST   /v1/admin/tenants/{id}/addons/{addonPlanId}/deactivate         reason + type-to-confirm + compliance-lock check
//	POST   /v1/admin/tenants/{id}/addons/{addonPlanId}/usage:record       record one usage snapshot (test + nightly job)
//	POST   /v1/admin/tenants/{id}/addons/{addonPlanId}/usage:flush        emit chora.tenancy.addon.usage_recorded.v1
//
// All event emission goes through deps.Events (Recorder in tests; Pub/Sub
// publisher in prod) using the shared envelope conventions.
//
// Per .claude/skills/secrets-and-env: STRIPE_API_URL is required and is
// pulled from env in main(); the stub fails closed without it.
package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	stripestub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/stripe"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// V1MountInto adds the Phyllis MVP §5.2 + addon lifecycle routes to the v2
// mux. Called from NewV2Server so a single handler chain covers all
// versions. Per ai-observability-cloud-trace skill every route is wrapped
// in v1OtelMiddleware so traceparent propagation + per-route span timing
// is captured for Cloud Trace.
func v1MountInto(mux *http.ServeMux, deps V2Deps) {
	mux.HandleFunc("/v1/tenants", v2Logging(v1OtelMiddleware("v1.tenants", handleV1Tenants(deps))))
	mux.HandleFunc("/v1/tenants/", v2Logging(v1OtelMiddleware("v1.tenants.sub", handleV1TenantSub(deps))))
	mux.HandleFunc("/v1/admin/tenants/", v2Logging(v1OtelMiddleware("v1.admin.tenants.sub", handleV1AdminTenantSub(deps))))
}

// v1OtelMiddleware is the per-route OTel-friendly span shim. Captures
// W3C traceparent propagation + structured request logs that Cloud
// Logging correlates with Cloud Trace.
//
// The actual OTel SDK wiring lands in M12 (per service skeleton README);
// this middleware reproduces the trace-context propagation discipline so
// the new routes are first-class observable from day-1.
func v1OtelMiddleware(spanName string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Propagate traceparent header (already echoed by v2Logging);
		// the named span context is captured for downstream OTel
		// integration (currently a no-op shim).
		_ = spanName
		next(w, r)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/tenants — light-wizard creator
// ---------------------------------------------------------------------------

type v1CreateTenantReq struct {
	DisplayName     string `json:"display_name"`
	OwnerGCID       string `json:"owner_gcid"`
	Slug            string `json:"slug"`
	Country         string `json:"country"`
	DefaultCurrency string `json:"default_currency"`
	SelfHosted      bool   `json:"self_hosted"`
}

func handleV1Tenants(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CHO-2077: root-tenant create (POST) + list-ALL (GET) are
		// platform_operator-only (mirrors bootstrap_handler.callerIsPlatformOperator).
		// Defense-in-depth — the tenancy allow-from-gateway mesh policy permits
		// this path; only the absent gateway route keeps it unreachable today.
		// Runs before body decode so a non-operator is 403 regardless of payload.
		if (r.Method == http.MethodPost || r.Method == http.MethodGet) &&
			!callerIsPlatformOperator(r.Header) {
			v2WriteError(w, http.StatusForbidden, errCodePlatformOperatorRequired,
				"root tenant management requires the platform_operator role")
			return
		}
		switch r.Method {
		case http.MethodPost:
			handleV1CreateTenant(deps, w, r)
		case http.MethodGet:
			handleV1ListTenants(deps, w, r)
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
	}
}

func handleV1CreateTenant(deps V2Deps, w http.ResponseWriter, r *http.Request) {
	var req v1CreateTenantReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	in := tenant.LightTenantInput{
		DisplayName:     req.DisplayName,
		OwnerGCID:       req.OwnerGCID,
		Slug:            req.Slug,
		Country:         req.Country,
		DefaultCurrency: req.DefaultCurrency,
		SelfHosted:      req.SelfHosted,
	}
	// FindBySlug check uses the lowercased+trimmed slug; we let
	// CreateRootLight handle the strict-validation gate first.
	preExisting, _ := deps.Tenants.FindBySlug(strings.TrimSpace(strings.ToLower(in.Slug)))
	t, err := deps.Tenants.CreateRootLight(in)
	if err != nil {
		switch {
		case errors.Is(err, tenant.ErrInvalidSlug):
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_slug", err.Error())
		case errors.Is(err, tenant.ErrInvalidCurrency),
			errors.Is(err, tenant.ErrInvalidCountry):
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_argument", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	// Idempotent retry: only emit event on the very first creation.
	if preExisting == nil {
		// X-Tenant-Id header is unused for tenant creation; envelope
		// requires SOME tenant context — we use the new tenant's ID so
		// the event lands in the right per-tenant trace.
		hdr := events.Header{
			TenantID:    t.ID,
			GCID:        firstNonEmpty(r.Header.Get("X-GCID"), t.OwnerGCID),
			Traceparent: r.Header.Get("traceparent"),
		}
		deps.Events.Publish("chora.tenancy.tenant.created.v1", hdr, map[string]interface{}{
			"tenant_id":        t.ID,
			"display_name":     t.DisplayName,
			"owner_gcid":       t.OwnerGCID,
			"slug":             t.Slug,
			"country":          t.Country,
			"default_currency": t.DefaultCurrency,
			"self_hosted":      t.SelfHosted,
		})
	}
	v2WriteJSON(w, http.StatusCreated, v1TenantDTO(t))
}

func handleV1ListTenants(deps V2Deps, w http.ResponseWriter, r *http.Request) {
	items := deps.Tenants.List()
	out := make([]map[string]interface{}, 0, len(items))
	for _, t := range items {
		out = append(out, v1TenantDTO(t))
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": out, "total": len(out)})
}

// ---------------------------------------------------------------------------
// GET /v1/tenants/{id}
// ---------------------------------------------------------------------------

func handleV1TenantSub(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) == 0 || parts[0] == "" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		tenantID := parts[0]
		if len(parts) == 1 {
			handleV1GetTenant(deps, tenantID, w, r)
			return
		}
		v2WriteError(w, http.StatusNotFound, "not_found", "not found")
	}
}

func handleV1GetTenant(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	t, ok := deps.Tenants.Get(tenantID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "tenant_not_found", "tenant not found")
		return
	}
	v2WriteJSON(w, http.StatusOK, v1TenantDTO(t))
}

// ---------------------------------------------------------------------------
// /v1/admin/tenants/{id}/...
// ---------------------------------------------------------------------------

func handleV1AdminTenantSub(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/admin/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) < 2 || parts[0] == "" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		tenantID := parts[0]
		switch parts[1] {
		case "golive":
			if len(parts) != 2 {
				v2WriteError(w, http.StatusNotFound, "not_found", "not found")
				return
			}
			handleV1Golive(deps, tenantID, w, r)
		case "addons":
			handleV1AdminAddons(deps, tenantID, parts[2:], w, r)
		default:
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
		}
	}
}

// handleV1Golive implements POST /v1/admin/tenants/{id}/golive.
//
// Side effects:
//   - Tenant.Status pending_activation → active.
//   - Stripe.CreateCustomer → stamps Tenant.StripeCustomerID.
//   - chora.tenancy.tenant.golive.v1 event with chora_imda_dimension=accountability.
//
// Idempotent: a second call on an already-active tenant returns 200 with
// the cached customer ID (NOT 409).
func handleV1Golive(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	t, ok := deps.Tenants.Get(tenantID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "tenant_not_found", "tenant not found")
		return
	}
	// Idempotent path: already-active tenant returns the cached state.
	if t.Status == tenant.StatusActive {
		v2WriteJSON(w, http.StatusOK, v1TenantDTO(t))
		return
	}
	if t.Status == tenant.StatusClosed || t.Status == tenant.StatusSuspended {
		v2WriteError(w, http.StatusConflict, "invalid_status_for_golive",
			"tenant status does not permit go-live")
		return
	}
	// Default-currency / country pulled from the Tenant; Stripe Customer
	// idempotency keyed on tenant_id by the stub.
	res, err := deps.Stripe.CreateCustomer(r.Context(), stripeCustomerInputFrom(t))
	if err != nil {
		v2WriteError(w, http.StatusBadGateway, "stripe_error", err.Error())
		return
	}
	if err := t.GoLive(res.CustomerID); err != nil {
		v2WriteError(w, http.StatusBadRequest, "golive_failed", err.Error())
		return
	}
	deps.Tenants.Save(t)

	// Emit chora.tenancy.tenant.golive.v1 with IMDA D1 accountability tag.
	hdr := events.Header{
		TenantID:    t.ID,
		GCID:        firstNonEmpty(r.Header.Get("X-GCID"), t.OwnerGCID),
		Traceparent: r.Header.Get("traceparent"),
	}
	deps.Events.Publish("chora.tenancy.tenant.golive.v1", hdr, map[string]interface{}{
		"tenant_id":              t.ID,
		"stripe_customer_id":     res.CustomerID,
		"activated_at":           t.ActivatedAt.Format(time.RFC3339Nano),
		"chora_imda_dimension":   "accountability",
		"imda_lifecycle_stage":   "post_deploy",
	})
	v2WriteJSON(w, http.StatusOK, v1TenantDTO(t))
}

// stripeCustomerInputFrom is the projection from the Tenant aggregate to
// the Stripe CreateCustomerInput. Display name + country + currency travel
// from the light-wizard form; Email is left empty until Identity Platform
// federates Phyllis's Google IdP profile.
func stripeCustomerInputFrom(t *tenant.Tenant) stripestub.CreateCustomerInput {
	return stripestub.CreateCustomerInput{
		TenantID:    t.ID,
		DisplayName: t.DisplayName,
		Country:     t.Country,
		Currency:    t.DefaultCurrency,
		Email:       "",
	}
}

// ---------------------------------------------------------------------------
// /v1/admin/tenants/{id}/addons[/{addonPlanId}[/...]]
// ---------------------------------------------------------------------------

func handleV1AdminAddons(deps V2Deps, tenantID string, rest []string, w http.ResponseWriter, r *http.Request) {
	if len(rest) == 0 {
		// list active subscriptions for the tenant.
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		handleAdminListAddons(deps, tenantID, w, r)
		return
	}
	planID := rest[0]
	if len(rest) == 1 {
		switch r.Method {
		case http.MethodGet:
			handleAdminAddonDetail(deps, tenantID, planID, w, r)
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
		return
	}
	// /addons/{planID}/{leaf}
	leaf := rest[1]
	switch leaf {
	case "activate":
		handleV1ActivateAddon(deps, tenantID, planID, w, r)
	case "deactivate":
		handleV1DeactivateAddon(deps, tenantID, planID, w, r)
	case "change-tier":
		handleV1ChangeTier(deps, tenantID, planID, w, r)
	case "usage":
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		handleAdminAddonUsage(deps, tenantID, planID, w, r)
	case "usage:record":
		handleV1RecordUsage(deps, tenantID, planID, w, r)
	case "usage:flush":
		handleV1FlushUsage(deps, tenantID, planID, w, r)
	default:
		v2WriteError(w, http.StatusNotFound, "not_found", "not found")
	}
}

// handleV1ActivateAddon — POST /v1/admin/tenants/{id}/addons/{planID}/activate
//
// Idempotent: a second call returns the same subscription_id.
func handleV1ActivateAddon(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	gcid := r.Header.Get("X-GCID")
	sub, err := deps.Subscriptions.Subscribe(tenantID, a.Code, gcid)
	if err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	deps.Events.Publish("chora.tenancy.addon.activated.v1", v2Header(r), map[string]interface{}{
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"addon_code":    a.Code,
		"activated_at":  sub.ActivatedAt.Format(time.RFC3339Nano),
	})
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"subscription_id": sub.ID,
		"tenant_id":       tenantID,
		"addon_plan_id":   a.ID,
		"addon_code":      a.Code,
		"status":          string(sub.Status),
		"current_tier":    sub.CurrentTier,
		"activated_at":    sub.ActivatedAt.Format(time.RFC3339Nano),
	})
}

// handleV1DeactivateAddon — POST /v1/admin/tenants/{id}/addons/{planID}/deactivate
//
// Compliance-lock policy: governance_dashboard + any add-on with
// `ComplianceLocked=true` rejects deactivation with 423 Locked unless the
// caller's role includes super_admin.
func handleV1DeactivateAddon(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req struct {
		Reason      string  `json:"reason"`
		ReasonText  string  `json:"reason_text"`
		EffectiveAt *string `json:"effective_at"`
	}
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	// Compliance-lock policy: only super_admin may override (CHO-2072 —
	// fail-closed mesh header; super_admin + operator only, NOT the general
	// tenant-admin set, so a tenant_admin cannot override a compliance lock).
	if a.ComplianceLocked && !callerHoldsSuperAdminRole(r.Header) {
		v2WriteError(w, http.StatusLocked, "compliance_locked",
			"add-on is compliance-locked; super_admin role required")
		return
	}
	var effAt *time.Time
	if req.EffectiveAt != nil && *req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, *req.EffectiveAt)
		if err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_effective_at", err.Error())
			return
		}
		effAt = &t
	}
	gcid := r.Header.Get("X-GCID")
	dreq := addon.DeactivationRequest{
		Reason:          addon.DeactivationReason(req.Reason),
		ReasonText:      req.ReasonText,
		EffectiveAt:     effAt,
		RequestedByGCID: gcid,
	}
	sub, status, ev, err := deps.Subscriptions.Deactivate(tenantID, a.Code, dreq)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrAddOnNotFound), errors.Is(err, addon.ErrSubscriptionNotFound):
			v2WriteError(w, http.StatusNotFound, "not_found", err.Error())
		case errors.Is(err, addon.ErrAlreadyDeactivated):
			v2WriteError(w, http.StatusConflict, "already_deactivated", err.Error())
		case errors.Is(err, addon.ErrComplianceLocked):
			v2WriteError(w, http.StatusLocked, "compliance_locked", err.Error())
		case errors.Is(err, addon.ErrCannotUnsubscribeBase):
			v2WriteError(w, http.StatusBadRequest, "cannot_unsubscribe_base", err.Error())
		case errors.Is(err, addon.ErrReasonTextRequired), errors.Is(err, addon.ErrInvalidArgument):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	requestedAt := time.Now().UTC()
	if ev != nil && !ev.OccurredAt.IsZero() {
		requestedAt = ev.OccurredAt
	}
	if status == "DEACTIVATION_SCHEDULED" {
		deps.Events.Publish("chora.tenancy.addon.deactivation_requested.v1", v2Header(r), map[string]interface{}{
			"tenant_id":         tenantID,
			"addon_plan_id":     a.ID,
			"addon_code":        a.Code,
			"reason":            req.Reason,
			"reason_text":       req.ReasonText,
			"requested_by_gcid": gcid,
			"effective_at":      v2NullableTime(effAt),
			"requested_at":      requestedAt.Format(time.RFC3339Nano),
		})
	} else {
		deps.Events.Publish("chora.tenancy.addon.deactivated.v1", v2Header(r), map[string]interface{}{
			"tenant_id":           tenantID,
			"addon_plan_id":       a.ID,
			"addon_code":          a.Code,
			"reason":              req.Reason,
			"reason_text":         req.ReasonText,
			"deactivated_by_gcid": gcid,
			"deactivated_at":      requestedAt.Format(time.RFC3339Nano),
		})
	}
	v2WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"addon_code":    a.Code,
		"status":        status,
		"requested_at":  requestedAt.Format(time.RFC3339Nano),
		"effective_at":  v2NullableTime(effAt),
		"reason":        req.Reason,
		"sub_status":    string(sub.Status),
	})
}

// handleV1ChangeTier — POST /v1/admin/tenants/{id}/addons/{planID}/change-tier
//
// Body: { target_tier_code, target_plan_id (alias for tier), proration_mode,
// effective_at?, expected_version? }
//
// Emits chora.tenancy.addon.upgraded.v1 OR chora.tenancy.addon.downgraded.v1
// based on price delta sign.
func handleV1ChangeTier(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req changeTierReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	target := req.TargetTierCode
	if target == "" {
		target = req.TargetPlanID
	}
	if target == "" {
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", "target_plan_id or target_tier_code required")
		return
	}
	prorationMode := req.ProrationMode
	if prorationMode == "" {
		prorationMode = string(addon.ProrationCreate)
	}
	var effAt *time.Time
	if req.EffectiveAt != nil && *req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, *req.EffectiveAt)
		if err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_effective_at", err.Error())
			return
		}
		effAt = &t
	}
	gcid := r.Header.Get("X-GCID")
	sub, delta, kind, err := deps.Subscriptions.ChangeTier(tenantID, a.Code, target, prorationMode, effAt, gcid, req.ExpectedVersion)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrSubscriptionNotFound):
			v2WriteError(w, http.StatusNotFound, "subscription_not_found", err.Error())
		case errors.Is(err, addon.ErrIneligibleForChange):
			v2WriteError(w, http.StatusConflict, "tier_change_ineligible", err.Error())
		case errors.Is(err, addon.ErrConcurrencyConflict):
			v2WriteError(w, http.StatusConflict, "concurrency_conflict", err.Error())
		case errors.Is(err, addon.ErrTierFamilyMismatch),
			errors.Is(err, addon.ErrInvalidProration),
			errors.Is(err, addon.ErrInvalidTier):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		case errors.Is(err, addon.ErrAddOnNotFound):
			v2WriteError(w, http.StatusNotFound, "addon_not_found", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	evs := deps.Subscriptions.ListEvents(tenantID, a.Code)
	var fromTier string
	for _, ev := range evs {
		if ev.Kind == kind {
			fromTier = ev.FromTier
			break
		}
	}
	topic := "chora.tenancy.addon.upgraded.v1"
	if kind == addon.EventDowngraded {
		topic = "chora.tenancy.addon.downgraded.v1"
	}
	effectiveAt := time.Now().UTC()
	if effAt != nil {
		effectiveAt = *effAt
	}
	// Stripe Subscription Schedule placeholder ID (real wiring M14).
	scheduleID := ""
	if effAt != nil && effAt.After(time.Now().UTC()) {
		_, err := deps.Stripe.ProrateChange(r.Context(),
			0, sub.MonthlyPriceCentsSnapshot, prorationMode, true)
		if err == nil {
			scheduleID = "sched_mock_" + a.Code
		}
	}
	deps.Events.Publish(topic, v2Header(r), map[string]interface{}{
		"tenant_id":           tenantID,
		"addon_plan_id":       a.ID,
		"addon_code":          a.Code,
		"from_tier":           fromTier,
		"to_tier":             sub.CurrentTier,
		"effective_at":        effectiveAt.Format(time.RFC3339Nano),
		"billing_delta_cents": delta,
		"requested_by_gcid":   gcid,
		"schedule_id":         scheduleID,
	})
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"subscription_id":     sub.ID,
		"billing_delta_cents": delta,
		"effective_at":        effectiveAt.Format(time.RFC3339Nano),
		"from_tier":           fromTier,
		"to_tier":             sub.CurrentTier,
		"schedule_id":         scheduleID,
	})
}

// handleV1RecordUsage records a single usage snapshot for the bucket
// `today`. Used by tests + the nightly Cloud Run Job's stage-write step.
func handleV1RecordUsage(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req struct {
		BucketDate    string `json:"bucket_date"`
		SeatsUsed     int64  `json:"seats_used"`
		SeatsUsedPeak int64  `json:"seats_used_peak"`
		QuotaConsumed int64  `json:"quota_consumed"`
		APICalls      int64  `json:"api_calls"`
		ErrorCount    int64  `json:"error_count"`
	}
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	bucket := time.Now().UTC().Truncate(24 * time.Hour)
	if req.BucketDate != "" {
		if t, err := time.Parse("2006-01-02", req.BucketDate); err == nil {
			bucket = t
		}
	}
	if req.SeatsUsedPeak < req.SeatsUsed {
		req.SeatsUsedPeak = req.SeatsUsed
	}
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID:       tenantID,
		AddOnID:        a.ID,
		BucketDate:     bucket,
		SeatsUsed:      req.SeatsUsed,
		SeatsUsedPeak:  req.SeatsUsedPeak,
		QuotaConsumed:  req.QuotaConsumed,
		APICalls:       req.APICalls,
		ErrorCount:     req.ErrorCount,
		LastRecordedAt: time.Now().UTC(),
	})
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"bucket_date":   bucket.Format("2006-01-02"),
	})
}

// handleV1FlushUsage emits chora.tenancy.addon.usage_recorded.v1 for every
// recorded usage snapshot in the active 30-day window. The nightly Cloud
// Run Job calls this per addon-tenant pair to flush the stage table to
// the rollup table + emit the audit event.
func handleV1FlushUsage(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	now := time.Now().UTC()
	from := now.Add(-30 * 24 * time.Hour)
	snaps := deps.Subscriptions.ListUsage(tenantID, a.ID, from, now.Add(24*time.Hour))
	emitted := 0
	for _, snap := range snaps {
		deps.Events.Publish("chora.tenancy.addon.usage_recorded.v1", v2Header(r), map[string]interface{}{
			"tenant_id":       tenantID,
			"addon_plan_id":   a.ID,
			"addon_code":      a.Code,
			"bucket_date":     snap.BucketDate.Format("2006-01-02"),
			"seats_used":      snap.SeatsUsed,
			"seats_used_peak": snap.SeatsUsedPeak,
			"quota_consumed":  snap.QuotaConsumed,
			"api_calls":       snap.APICalls,
			"error_count":     snap.ErrorCount,
		})
		emitted++
	}
	v2WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"flushed_count": emitted,
		"period_from":   from.Format(time.RFC3339Nano),
		"period_to":     now.Format(time.RFC3339Nano),
	})
}

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

func v1TenantDTO(t *tenant.Tenant) map[string]interface{} {
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
	// wizard_completed_at — set-once by the Setup Wizard step 4 Apply
	// (CHO-1682). Exposed on the tenant doc so the wizard's re-entry
	// hydration (CHO-1692) can show a "wizard already completed" banner
	// without a separate round-trip. NULL on fresh tenants — omitted
	// rather than emitted as `null` for consistency with the rest of
	// this DTO's optional fields.
	if t.WizardCompletedAt != nil {
		out["wizard_completed_at"] = t.WizardCompletedAt.Format(time.RFC3339Nano)
	}
	if t.DeletedAt != nil {
		out["deleted_at"] = t.DeletedAt.Format(time.RFC3339Nano)
	}
	return out
}

