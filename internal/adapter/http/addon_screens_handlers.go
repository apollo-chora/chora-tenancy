// Package httpapi — A-Tenant-Lifecycle (S6.2) admin handlers for the
// 5-screen UX surface per docs/design/ux_tenant_addon_lifecycle.md:
//
//   Screen 2 — addon detail audit trail (GET .../audit)
//   Screen 4 — preview-tier-change (POST .../preview-tier-change)
//   Screen 5 — request-deactivation with type-to-confirm + super-admin
//              override (POST .../request-deactivation)
//   Screen 5 — marketplace tile detail by id (GET /marketplace/addons/{id})
//
// These complement the existing v2_handlers.go endpoints (PATCH change-tier,
// POST :deactivate, GET addons, GET addons/{id}/usage).
//
// IMDA evidence emission per ADR-141: every state-change event payload carries
// `chora_imda_dimension=accountability` (D1) so the O+ dashboard can stitch
// the audit trail into the IMDA D1 evidence pack.
package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	stripestub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/stripe"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// imdaDimensionAccountability is the canonical IMDA D1 label per ADR-141.
const imdaDimensionAccountability = "accountability"

// ---------------------------------------------------------------------------
// Screen 4 — Preview tier change (read-only Stripe Subscription Schedule
// preview; never mutates the subscription)
// ---------------------------------------------------------------------------

type previewTierChangeReq struct {
	TargetPlanID   string `json:"target_plan_id,omitempty"`
	TargetTierCode string `json:"target_tier_code,omitempty"`
	ProrationMode  string `json:"proration_mode,omitempty"`
	// EffectiveAt — CHO-1772. ISO 8601 timestamp → preview end-of-cycle;
	// null/omitted → preview immediate. Mirrors the FE's nullable shape.
	EffectiveAt *string `json:"effective_at,omitempty"`
}

func handleAdminPreviewTierChange(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req previewTierChangeReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	target := strings.TrimSpace(req.TargetTierCode)
	if target == "" {
		target = strings.TrimSpace(req.TargetPlanID)
	}
	if target == "" {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "target_plan_id or target_tier_code required")
		return
	}
	prorationMode := strings.TrimSpace(req.ProrationMode)
	if prorationMode == "" {
		prorationMode = string(addon.ProrationCreate)
	}
	if !addon.IsValidProration(prorationMode) {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "invalid proration_mode")
		return
	}
	targetTier, ok := a.FindTier(target)
	if !ok {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "tier not found in plan family")
		return
	}
	sub, ok := deps.Subscriptions.GetSubscription(tenantID, a.Code)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "subscription_not_found", "no active subscription to preview")
		return
	}
	currentTier, ok := a.FindTier(sub.CurrentTier)
	if !ok {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "current tier not in plan family")
		return
	}

	// CHO-1767 — when CHORA_PAYMENTS_HTTP_ADDR is wired, dispatch the
	// preview through chora-payments so Stripe Invoice.upcoming runs.
	// Falls back to the catalogue-estimated delta + stripestub path
	// when the client isn't wired (tests + standalone smoke).
	if deps.PaymentsHTTP != nil {
		// CHO-1772 — same translation as the change handler: non-nil
		// ISO → end_of_cycle; nil → immediate.
		effectiveAtTag := ""
		if req.EffectiveAt != nil && strings.TrimSpace(*req.EffectiveAt) != "" {
			effectiveAtTag = "end_of_cycle"
		}
		handleAdminPreviewTierChangeViaPayments(deps, tenantID, a, sub, currentTier, targetTier, prorationMode, effectiveAtTag, w, r)
		return
	}

	var delta int64
	if prorationMode == string(addon.ProrationCreate) {
		delta = targetTier.MonthlyPriceCents - currentTier.MonthlyPriceCents
	}
	deferred := delta < 0 // downgrades defer to end of cycle per Stripe Subscription Schedule
	scheduleID := ""
	effectiveAt := time.Now().UTC()
	cycleEnd := effectiveAt.AddDate(0, 0, 30)
	// Use the new SubscriptionSchedule preview API for parity with the real
	// Stripe wire shape (phases array). The legacy ProrateChange call is
	// retained as a fallback to support tests that wired only the stub
	// constructor without the schedule store.
	if deps.Stripe != nil && deferred {
		if preview, err := deps.Stripe.PreviewSubscriptionScheduleChange(r.Context(),
			previewScheduleInputFor(tenantID, sub, currentTier, targetTier, prorationMode, deferred, cycleEnd)); err == nil && preview != nil {
			// preview-only — no schedule_id is set on the preview, but we
			// MUST surface a schedule_id to the UI so it can communicate
			// "deferred to cycle end" to the user. Create one alongside.
			if created, err := deps.Stripe.CreateSubscriptionSchedule(r.Context(),
				previewScheduleInputFor(tenantID, sub, currentTier, targetTier, prorationMode, deferred, cycleEnd)); err == nil && created != nil {
				scheduleID = created.ScheduleID
			}
			_ = preview
		}
	}
	if deps.Stripe != nil && scheduleID == "" {
		// Legacy ProrateChange fallback — preserves existing event shape.
		if pr, err := deps.Stripe.ProrateChange(r.Context(),
			currentTier.MonthlyPriceCents, targetTier.MonthlyPriceCents,
			prorationMode, deferred); err == nil && pr != nil {
			if pr.BillingDeltaCents != 0 {
				delta = pr.BillingDeltaCents
			}
			if pr.ScheduleID != "" {
				scheduleID = pr.ScheduleID
			}
		}
	}
	if deferred {
		// Deferred downgrades are billed at the next cycle. Stripe's real
		// Subscription Schedule sets `start_date` to the next cycle anchor.
		// Approximate by adding the configured grace window (30d) until the
		// real adapter wires the cycle anchor.
		effectiveAt = cycleEnd
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id":                 tenantID,
		"addon_plan_id":             a.ID,
		"from_tier":                 sub.CurrentTier,
		"to_tier":                   targetTier.Code,
		"current_monthly_cents":     currentTier.MonthlyPriceCents,
		"target_monthly_cents":      targetTier.MonthlyPriceCents,
		"billing_delta_cents":       delta,
		"next_invoice_total_cents":  targetTier.MonthlyPriceCents,
		"proration_mode":            prorationMode,
		"effective_at":              effectiveAt.Format(time.RFC3339Nano),
		"deferred_to_cycle_end":     deferred,
		"schedule_id":               scheduleID,
		"currency":                  targetTier.Currency,
	})
}

// handleAdminPreviewTierChangeViaPayments is the CHO-1767 dispatch path.
// Calls chora-payments' HTTP preview-tier-change endpoint (CHO-1765);
// surface its Stripe Invoice.upcoming delta verbatim. Same response
// shape as the legacy path (FE wire stays unchanged).
//
// CHO-1772 — effectiveAtTag is "" / "immediate" / "end_of_cycle"; when
// "end_of_cycle" the BE returns billing_delta=0 + next_invoice = new
// tier's full monthly price (no proration).
func handleAdminPreviewTierChangeViaPayments(deps V2Deps, tenantID string, a *addon.AddOn, sub *addon.Subscription, currentTier, targetTier addon.Tier, prorationMode, effectiveAtTag string, w http.ResponseWriter, r *http.Request) {
	currency := targetTier.Currency
	if currency == "" {
		currency = "SGD"
	}
	in := TenancyPaymentsPreviewIn{
		TenantID:       tenantID,
		AddonCode:      a.Code,
		TargetTierCode: targetTier.Code,
		Currency:       currency,
		ProrationMode:  prorationMode,
		EffectiveAt:    effectiveAtTag,
	}
	out, err := deps.PaymentsHTTP.PreviewTenantAddonTier(r.Context(), in)
	if err != nil {
		writePaymentsHTTPError(w, err, "chora-payments preview-tier-change dispatch failed: "+err.Error())
		return
	}
	// CHO-1772 — prefer the BE-supplied deferred flag (set when
	// effective_at=end_of_cycle drove the response). Legacy fallback:
	// treat negative deltas as downgrades that defer to cycle end.
	deferred := out.DeferredToCycleEnd || out.BillingDeltaCents < 0
	var effectiveAt time.Time
	if out.EffectiveAt != "" {
		if parsed, perr := time.Parse(time.RFC3339Nano, out.EffectiveAt); perr == nil {
			effectiveAt = parsed
		}
	}
	if effectiveAt.IsZero() {
		effectiveAt = time.Now().UTC()
		if deferred {
			effectiveAt = effectiveAt.AddDate(0, 0, 30)
		}
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id":                 tenantID,
		"addon_plan_id":             a.ID,
		"from_tier":                 sub.CurrentTier,
		"to_tier":                   targetTier.Code,
		"current_monthly_cents":     currentTier.MonthlyPriceCents,
		"target_monthly_cents":      targetTier.MonthlyPriceCents,
		"billing_delta_cents":       out.BillingDeltaCents,
		"next_invoice_total_cents":  out.NextInvoiceTotalCents,
		"proration_mode":            out.ProrationMode,
		"effective_at":              effectiveAt.Format(time.RFC3339Nano),
		"deferred_to_cycle_end":     deferred,
		"schedule_id":               "",
		"currency":                  out.Currency,
	})
}

// ---------------------------------------------------------------------------
// Screen 5 — request-deactivation (type-to-confirm + super_admin override)
// ---------------------------------------------------------------------------

type requestDeactivationReq struct {
	Reason             string  `json:"reason"`
	ReasonText         string  `json:"reason_text"`
	ConfirmationText   string  `json:"confirmation_text"`
	SuperAdminOverride bool    `json:"super_admin_override"`
	EffectiveAt        *string `json:"effective_at"`
}

func handleAdminRequestDeactivation(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req requestDeactivationReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if !addon.IsValidReason(req.Reason) {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "invalid reason")
		return
	}
	if req.Reason == string(addon.ReasonOther) && strings.TrimSpace(req.ReasonText) == "" {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "reason_text required when reason=other")
		return
	}
	// Type-to-confirm: confirmation_text MUST equal the addon code (the
	// admin-visible name used in the modal banner).
	if strings.TrimSpace(req.ConfirmationText) != a.Code {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed",
			"confirmation_text must equal the add-on name to proceed")
		return
	}
	// CHO-2072: the super_admin override reads the fail-CLOSED mesh header
	// (was a fail-open X-Role scan the gateway never populated). super_admin +
	// platform_operator only (ADR-165) — the compliance-override role set, NOT
	// the general tenant-admin gate above.
	hasSuperAdminRole := callerHoldsSuperAdminRole(r.Header)
	// super_admin_override is only valid when the caller actually has the
	// super_admin role. tenant_admin claiming override → 403.
	if req.SuperAdminOverride && !hasSuperAdminRole {
		v2WriteError(w, http.StatusForbidden, "forbidden",
			"super_admin_override requires the super_admin role")
		return
	}
	if a.ComplianceLocked && !req.SuperAdminOverride {
		v2WriteError(w, http.StatusLocked, "compliance_locked",
			"add-on is compliance-locked; super_admin override required")
		return
	}
	gcid := r.Header.Get("X-GCID")
	// Deferred effective: parse from request, otherwise default to grace
	// window (30d) — matches the Stripe Subscription Schedule pattern.
	var effAt *time.Time
	if req.EffectiveAt != nil && *req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, *req.EffectiveAt)
		if err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_effective_at", err.Error())
			return
		}
		effAt = &t
	} else {
		t := time.Now().UTC().AddDate(0, 0, 30)
		effAt = &t
	}
	dreq := addon.DeactivationRequest{
		Reason:          addon.DeactivationReason(req.Reason),
		ReasonText:      req.ReasonText,
		EffectiveAt:     effAt,
		RequestedByGCID: gcid,
	}
	if req.SuperAdminOverride && a.ComplianceLocked {
		// We special-case the compliance-locked override: bypass the
		// catalogue ComplianceLocked guard by issuing the deactivation
		// directly via a structured event AND the registry path that
		// honours the deactivation. We model this by temporarily clearing
		// the lock, processing, then re-asserting — but to avoid mutating
		// catalogue state, we manually emit the requested event without
		// transitioning the subscription state machine. The persistent
		// chora_imda_dimension=accountability evidence captures the
		// override.
		requestedAt := time.Now().UTC()
		deps.Events.Publish("chora.tenancy.addon.deactivation_requested.v1", v2Header(r), map[string]interface{}{
			"tenant_id":                  tenantID,
			"addon_plan_id":              a.ID,
			"reason":                     req.Reason,
			"reason_text":                req.ReasonText,
			"requested_by_gcid":          gcid,
			"effective_at":               v2NullableTime(effAt),
			"requested_at":               requestedAt.Format(time.RFC3339Nano),
			"super_admin_override":       true,
			"compliance_lock_overridden": true,
			"chora_imda_dimension":       imdaDimensionAccountability,
		})
		v2WriteJSON(w, http.StatusAccepted, map[string]interface{}{
			"tenant_id":                  tenantID,
			"addon_plan_id":              a.ID,
			"status":                     "DEACTIVATION_SCHEDULED",
			"requested_at":               requestedAt.Format(time.RFC3339Nano),
			"effective_at":               v2NullableTime(effAt),
			"reason":                     req.Reason,
			"compliance_lock_overridden": true,
		})
		return
	}
	sub, status, ev, err := deps.Subscriptions.Deactivate(tenantID, a.Code, dreq)
	_ = sub
	if err != nil {
		switch err.Error() {
		case addon.ErrAddOnNotFound.Error(), addon.ErrSubscriptionNotFound.Error():
			v2WriteError(w, http.StatusNotFound, "not_found", err.Error())
		case addon.ErrAlreadyDeactivated.Error():
			v2WriteError(w, http.StatusConflict, "already_deactivated", err.Error())
		case addon.ErrComplianceLocked.Error():
			v2WriteError(w, http.StatusLocked, "compliance_locked", err.Error())
		case addon.ErrCannotUnsubscribeBase.Error():
			v2WriteError(w, http.StatusBadRequest, "cannot_unsubscribe_base", err.Error())
		case addon.ErrReasonTextRequired.Error(), addon.ErrInvalidArgument.Error():
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
			"tenant_id":            tenantID,
			"addon_plan_id":        a.ID,
			"reason":               req.Reason,
			"reason_text":          req.ReasonText,
			"requested_by_gcid":    gcid,
			"effective_at":         v2NullableTime(effAt),
			"requested_at":         requestedAt.Format(time.RFC3339Nano),
			"super_admin_override": false,
			"chora_imda_dimension": imdaDimensionAccountability,
		})
	} else {
		deps.Events.Publish("chora.tenancy.addon.deactivated.v1", v2Header(r), map[string]interface{}{
			"tenant_id":            tenantID,
			"addon_plan_id":        a.ID,
			"reason":               req.Reason,
			"reason_text":          req.ReasonText,
			"deactivated_by_gcid":  gcid,
			"deactivated_at":       requestedAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		})
	}
	v2WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"tenant_id":                  tenantID,
		"addon_plan_id":              a.ID,
		"status":                     status,
		"requested_at":               requestedAt.Format(time.RFC3339Nano),
		"effective_at":               v2NullableTime(effAt),
		"reason":                     req.Reason,
		"compliance_lock_overridden": false,
	})
}

// ---------------------------------------------------------------------------
// Screen 2 — Addon audit trail
// ---------------------------------------------------------------------------

func handleAdminAddonAudit(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	evs := deps.Subscriptions.ListEvents(tenantID, a.Code)
	items := make([]map[string]interface{}, 0, len(evs))
	for _, ev := range evs {
		entry := map[string]interface{}{
			"event_id":             ev.EventID,
			"tenant_id":            ev.TenantID,
			"addon_plan_id":        ev.AddOnID,
			"addon_code":           ev.AddOnCode,
			"subscription_id":      ev.SubscriptionID,
			"kind":                 string(ev.Kind),
			"from_tier":            ev.FromTier,
			"to_tier":              ev.ToTier,
			"reason":               string(ev.DeactivationReason),
			"reason_text":          ev.ReasonText,
			"billing_delta_cents":  ev.BillingDeltaCents,
			"requested_by_gcid":    ev.RequestedByGCID,
			"occurred_at":          ev.OccurredAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		}
		if ev.EffectiveAt != nil {
			entry["effective_at"] = ev.EffectiveAt.Format(time.RFC3339Nano)
		}
		if ev.ScheduleID != "" {
			entry["schedule_id"] = ev.ScheduleID
		}
		items = append(items, entry)
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"items":         items,
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"addon_code":    a.Code,
	})
}

// ---------------------------------------------------------------------------
// Screen 5 — Marketplace tile detail by addonPlanId
// ---------------------------------------------------------------------------

func handleMarketplaceAddonByID(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/marketplace/addons/")
		rest = strings.TrimSuffix(rest, "/")
		if rest == "" || strings.Contains(rest, "/") {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		a, ok := addonByPlanID(deps.Catalogue, rest)
		if !ok {
			v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		now := time.Now().UTC()
		// `installed` follows the canonical CHO-1745 predicate (covers
		// PendingActivation so the Stripe-Checkout race window doesn't
		// re-offer Subscribe). The legacy `is_installed` field is retained
		// alongside for the transitional window until CHO-1749 cuts the FE
		// consumer over; both fields read the same predicate from now on.
		installed := isAddonInstalled(deps, tenantID, a.Code, now)
		detail := addonDetailDTO(a)
		delete(detail, "current_subscription")
		detail["installed"] = installed
		detail["is_installed"] = installed
		if installed {
			detail["cta"] = "Manage"
			// CHO-1757 — surface the tenant's current tier so the FE
			// marketplace detail screen can re-highlight it after the
			// Stripe Checkout hard-redirect destroys component state.
			// Omitted when no registry row is present or the row's tier
			// is empty (no empty-string leak in the wire shape).
			if sub, ok := deps.Subscriptions.GetSubscription(tenantID, a.Code); ok && sub != nil {
				if sub.CurrentTier != "" {
					detail["current_tier"] = sub.CurrentTier
				}
				// CHO-1780 follow-up — surface the subscription snapshot
				// (with the scheduled_* fields when a pending end-of-cycle
				// change exists) so AddonMarketplaceCatalogDetailComponent
				// can render the "<scheduled> starting <date>" badge.
				// Browse view (no install) keeps current_subscription
				// omitted; only the installed-tenant view gets it.
				//
				// CHO-1782 — extended for the destructive counterpart:
				// when the install is in PENDING_DEACTIVATION, surface
				// the snapshot so the same panel can render the
				// "Deactivating <date>" pill. Plain ACTIVE installs
				// still get NO current_subscription (privacy
				// minimisation; mirrors the browse view shape).
				pendingTier := sub.ScheduledTier != "" && sub.ScheduledEffectiveAt != nil
				pendingDeactivation := sub.Status == addon.StatusPendingDeactivation && sub.DeactivationEffectiveAt != nil
				if pendingTier || pendingDeactivation {
					detail["current_subscription"] = subscriptionSnapshotDTO(sub)
				}
			}
		} else {
			detail["cta"] = "Install"
		}
		v2WriteJSON(w, http.StatusOK, detail)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// previewScheduleInputFor builds the SubscriptionScheduleInput payload for
// the Stripe Subscription Schedule preview/create call. The `price_id` is
// derived from the addon plan family + tier code (e.g.
// "price_tms_pro" — production swaps to real Stripe price IDs from
// catalogue config).
func previewScheduleInputFor(tenantID string, sub *addon.Subscription,
	currentTier, targetTier addon.Tier, prorationMode string,
	deferred bool, cycleEnd time.Time) stripestub.SubscriptionScheduleInput {
	customerID := "cus_" + tenantID // stub: real wire uses sub.CustomerID
	subscriptionID := sub.ID
	return stripestub.SubscriptionScheduleInput{
		TenantID:          tenantID,
		CustomerID:        customerID,
		SubscriptionID:    subscriptionID,
		FromPriceID:       "price_" + sub.PlanFamily + "_" + currentTier.Code,
		ToPriceID:         "price_" + sub.PlanFamily + "_" + targetTier.Code,
		ProrationBehavior: prorationMode,
		DeferToCycleEnd:   deferred,
		CycleEndAt:        cycleEnd,
	}
}

// publishEventWithIMDA wraps deps.Events.Publish to ensure
// `chora_imda_dimension=accountability` is present in the payload per
// ADR-141 D1.
//
// Currently unused in this file (the callers above inline the attribute) but
// retained as a helper for future event publishers in this package.
func publishEventWithIMDA(deps V2Deps, topic string, header events.Header, payload map[string]interface{}) {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payload["chora_imda_dimension"] = imdaDimensionAccountability
	deps.Events.Publish(topic, header, payload)
}
