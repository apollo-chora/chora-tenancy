// Package httpapi_test — A-Tenant-Lifecycle (S6.2) tests for the 5-screen
// admin UX surface per docs/design/ux_tenant_addon_lifecycle.md:
//
//	Screen 1 — list active + marketplace [pre-existing]
//	Screen 2 — addon detail + audit-trail (new)
//	Screen 3 — usage analytics [pre-existing]
//	Screen 4 — preview-tier-change + change-tier (new preview endpoint)
//	Screen 5 — deactivate with type-to-confirm + super_admin override (new)
//
// Plus the marketplace tile detail by id and a non-installed CTA flag.
package httpapi_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// ---------------------------------------------------------------------------
// Screen 4 — Preview tier change (NEW)
// ---------------------------------------------------------------------------

func TestPreviewTierChange_UpgradeReturnsPositiveDelta(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	delta, _ := body["billing_delta_cents"].(float64)
	if delta <= 0 {
		t.Fatalf("expected positive proration delta on upgrade, got %v", delta)
	}
	if got, _ := body["from_tier"].(string); got != "starter" {
		t.Fatalf("expected from_tier=starter, got %q", got)
	}
	if got, _ := body["to_tier"].(string); got != "pro" {
		t.Fatalf("expected to_tier=pro, got %q", got)
	}
	if _, ok := body["next_invoice_total_cents"]; !ok {
		t.Fatalf("expected next_invoice_total_cents in preview")
	}
}

func TestPreviewTierChange_NoProrationModeReturnsZeroDelta(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "none",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if delta, _ := body["billing_delta_cents"].(float64); delta != 0 {
		t.Fatalf("expected zero delta with proration=none, got %v", delta)
	}
}

func TestPreviewTierChange_DowngradeProducesNegativeDeltaAndScheduleID(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	// Seed at pro tier (upgrade first).
	seedActiveSubscription(t, deps, "tms")
	if _, _, _, err := deps.Subscriptions.ChangeTier(adminTenantID, "tms",
		"pro", "create_prorations", nil, adminGCID, 0); err != nil {
		t.Fatalf("seed pro: %v", err)
	}
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "starter",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if delta, _ := body["billing_delta_cents"].(float64); delta >= 0 {
		t.Fatalf("expected negative delta on downgrade, got %v", delta)
	}
	// Downgrade preview should declare a Stripe Subscription Schedule for
	// end-of-cycle deferral (Phyllis MVP says downgrades wait until cycle end).
	sched, _ := body["schedule_id"].(string)
	if sched == "" {
		t.Fatalf("expected schedule_id populated for deferred downgrade preview")
	}
	if !strings.HasPrefix(sched, "sched_") {
		t.Fatalf("expected schedule_id mock prefix sched_, got %q", sched)
	}
	if effective, _ := body["effective_at"].(string); effective == "" {
		t.Fatalf("expected effective_at on downgrade")
	}
}

func TestPreviewTierChange_404OnUnknownAddon(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/unknown_addon/preview-tier-change",
		"tenant_admin", map[string]interface{}{"target_tier_code": "pro"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestPreviewTierChange_422OnUnknownTier(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{"target_tier_code": "fantasy_tier"})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestPreviewTierChange_ForbiddenForReadOnlyRole(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"learner", map[string]interface{}{"target_tier_code": "pro"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestPreviewTierChange_DoesNotMutateSubscription(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	priorSub, _ := deps.Subscriptions.GetSubscription(adminTenantID, "tms")
	priorVer := priorSub.Version
	priorTier := priorSub.CurrentTier
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	post, _ := deps.Subscriptions.GetSubscription(adminTenantID, "tms")
	if post.Version != priorVer {
		t.Fatalf("preview must NOT bump version; was %d, got %d", priorVer, post.Version)
	}
	if post.CurrentTier != priorTier {
		t.Fatalf("preview must NOT mutate tier; was %s, got %s", priorTier, post.CurrentTier)
	}
	if got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1"); len(got) != 0 {
		t.Fatalf("preview must NOT emit upgraded event; got %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// Screen 5 — request-deactivation with type-to-confirm + super_admin override
// ---------------------------------------------------------------------------

func TestRequestDeactivation_TypeToConfirmRequired(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":            "cost",
			"confirmation_text": "wrong-name",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on confirmation mismatch, got %d", rec.Code)
	}
}

func TestRequestDeactivation_HappyPathEmitsRequestedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":            "cost",
			"confirmation_text": "tms",
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "DEACTIVATION_SCHEDULED" {
		t.Fatalf("expected DEACTIVATION_SCHEDULED, got %q", got)
	}
	if _, ok := body["effective_at"].(string); !ok {
		t.Fatalf("expected effective_at populated (default grace cycle end)")
	}
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_requested.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 deactivation_requested event, got %d", len(got))
	}
	if r, _ := got[0].Payload["reason"].(string); r != "cost" {
		t.Fatalf("expected reason=cost in payload, got %q", r)
	}
}

func TestRequestDeactivation_ComplianceLocked423WithoutOverride(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	if _, ok := deps.Catalogue.Get("governance_dashboard"); !ok {
		t.Fatalf("governance_dashboard seed missing")
	}
	seedActiveSubscription(t, deps, "governance_dashboard")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":            "compliance",
			"confirmation_text": "governance_dashboard",
		})
	if rec.Code != http.StatusLocked {
		t.Fatalf("expected 423, got %d", rec.Code)
	}
}

func TestRequestDeactivation_SuperAdminOverrideAllowsComplianceLocked(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "governance_dashboard")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"super_admin", map[string]interface{}{
			"reason":               "compliance",
			"confirmation_text":    "governance_dashboard",
			"super_admin_override": true,
			"reason_text":          "Decommissioning regulated programme; auditor approved 2026-05-09",
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 with super_admin override, got %d (body=%v)", rec.Code, body)
	}
	if locked, _ := body["compliance_lock_overridden"].(bool); !locked {
		t.Fatalf("expected compliance_lock_overridden=true, got %v", body["compliance_lock_overridden"])
	}
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_requested.v1")
	if len(got) != 1 {
		t.Fatalf("expected deactivation_requested event with override, got %d", len(got))
	}
	override, _ := got[0].Payload["super_admin_override"].(bool)
	if !override {
		t.Fatalf("expected super_admin_override=true in event payload, got %v", got[0].Payload["super_admin_override"])
	}
}

func TestRequestDeactivation_SuperAdminOverrideRequiresExplicitFlag(t *testing.T) {
	// super_admin role alone is not enough — the explicit
	// super_admin_override boolean MUST be set.
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "governance_dashboard")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"super_admin", map[string]interface{}{
			"reason":            "compliance",
			"confirmation_text": "governance_dashboard",
		})
	if rec.Code != http.StatusLocked {
		t.Fatalf("super_admin without explicit override should still be 423, got %d", rec.Code)
	}
}

func TestRequestDeactivation_OverrideWithoutSuperAdminRoleForbidden(t *testing.T) {
	// tenant_admin sending super_admin_override=true is rejected.
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "governance_dashboard")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":               "compliance",
			"confirmation_text":    "governance_dashboard",
			"super_admin_override": true,
		})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for tenant_admin claiming override, got %d", rec.Code)
	}
}

func TestRequestDeactivation_RequiresValidReason(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":            "fantasy_reason",
			"confirmation_text": "tms",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on bogus reason, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Screen 2 — addon detail audit-trail
// ---------------------------------------------------------------------------

func TestGetAddonAudit_ReturnsLifecycleEvents(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	if _, _, _, err := deps.Subscriptions.ChangeTier(adminTenantID, "tms",
		"pro", "create_prorations", nil, adminGCID, 0); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/audit",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	items, _ := body["items"].([]interface{})
	if len(items) < 2 {
		t.Fatalf("expected ≥2 audit events (activated + upgraded), got %d", len(items))
	}
	found := map[string]bool{}
	for _, raw := range items {
		entry, _ := raw.(map[string]interface{})
		kind, _ := entry["kind"].(string)
		found[kind] = true
		// Each entry must carry the IMDA D1 (accountability) attribute.
		dim, _ := entry["chora_imda_dimension"].(string)
		if dim != "accountability" {
			t.Fatalf("expected chora_imda_dimension=accountability per ADR-141; got %q", dim)
		}
	}
	if !found["activated"] || !found["upgraded"] {
		t.Fatalf("expected activated+upgraded kinds; got %v", found)
	}
}

func TestGetAddonAudit_404OnUnknownAddon(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/no-such/audit",
		"tenant_admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Screen 5 — Marketplace tile detail by id
// ---------------------------------------------------------------------------

func TestGetMarketplaceAddonByID_ReturnsDetail(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["code"].(string); got != "tms" {
		t.Fatalf("expected code=tms, got %q", got)
	}
	tiers, _ := body["pricing_tiers"].([]interface{})
	if len(tiers) < 3 {
		t.Fatalf("expected ≥3 tiers, got %d", len(tiers))
	}
	// CTA depends on whether the tenant has the addon installed.
	if cta, _ := body["cta"].(string); cta != "Install" {
		t.Fatalf("expected cta=Install (no active subscription), got %q", cta)
	}
}

func TestGetMarketplaceAddonByID_CTAManageWhenInstalled(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	a, _ := deps.Catalogue.Get("tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if cta, _ := body["cta"].(string); cta != "Manage" {
		t.Fatalf("expected cta=Manage when installed, got %q", cta)
	}
}

func TestGetMarketplaceAddonByID_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/01970000-aaaa-bbbb-cccc-000000000000",
		"tenant_admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// IMDA evidence emission on every state change
// ---------------------------------------------------------------------------

func TestChangeTier_EmitsIMDAAccountabilityAttribute(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1")
	if len(got) != 1 {
		t.Fatalf("expected upgraded event, got %d", len(got))
	}
	dim, _ := got[0].Payload["chora_imda_dimension"].(string)
	if dim != "accountability" {
		t.Fatalf("expected chora_imda_dimension=accountability per ADR-141; got %q", dim)
	}
}

// ---------------------------------------------------------------------------
// Backwards-compat: legacy :deactivate route still works
// ---------------------------------------------------------------------------

func TestDeactivateAction_StillWorksAfterRequestDeactivationAdded(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339)
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "cost",
			"effective_at": future,
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("legacy :deactivate route broken; got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// PreviewTierChange + ChangeTier coverage of pseudonymise helper carryover
// ---------------------------------------------------------------------------

func TestPreviewTierChange_AssertsTopConsumersUnchanged(t *testing.T) {
	t.Parallel()
	// Sanity: preview must not record any usage snapshots either.
	srv, deps := newAdminServer(t)
	sub := seedActiveSubscription(t, deps, "tms")
	priorSnaps := deps.Subscriptions.ListUsage(adminTenantID, sub.AddOnID,
		time.Now().UTC().Add(-365*24*time.Hour),
		time.Now().UTC().Add(365*24*time.Hour))
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{"target_tier_code": "pro"})
	postSnaps := deps.Subscriptions.ListUsage(adminTenantID, sub.AddOnID,
		time.Now().UTC().Add(-365*24*time.Hour),
		time.Now().UTC().Add(365*24*time.Hour))
	if len(priorSnaps) != len(postSnaps) {
		t.Fatalf("preview should not record usage; before=%d after=%d", len(priorSnaps), len(postSnaps))
	}
}

// Sanity: the addon detail returned through the marketplace endpoint contains
// `is_installed` flag — used by the H+ UI to decide CTA.
func TestGetMarketplaceAddonByID_IncludesIsInstalledFlag(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if got, _ := body["is_installed"].(bool); got {
		t.Fatalf("expected is_installed=false, got true")
	}
	seedActiveSubscription(t, deps, "tms")
	_, body = adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if got, _ := body["is_installed"].(bool); !got {
		t.Fatalf("expected is_installed=true after subscribe; got false")
	}
}

// ---------------------------------------------------------------------------
// CHO-1757 — marketplace detail surfaces `current_tier` so the FE radio
// picker can re-highlight the actual tier after returning from Stripe.
// ---------------------------------------------------------------------------

// When the tenant holds the addon, the response carries `current_tier` set
// to the live SubscriptionRegistry row's `CurrentTier`.
func TestGetMarketplaceAddonByID_IncludesCurrentTier_WhenInstalled(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	seedActiveSubscription(t, deps, "tms")
	if _, _, _, err := deps.Subscriptions.ChangeTier(adminTenantID, "tms",
		"pro", "none", nil, adminGCID, 0); err != nil {
		t.Fatalf("seed pro: %v", err)
	}
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if got, _ := body["current_tier"].(string); got != "pro" {
		t.Fatalf("expected current_tier=pro, got %q", got)
	}
}

// When the tenant does not hold the addon, the response omits `current_tier`
// entirely (no empty-string leak).
func TestGetMarketplaceAddonByID_OmitsCurrentTier_WhenNotInstalled(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if _, present := body["current_tier"]; present {
		t.Fatalf("expected current_tier omitted when not installed, got %v", body["current_tier"])
	}
}

// CHO-1782 — when the tenant has an active install AND a pending end-of-cycle
// deactivation, the marketplace detail surfaces current_subscription with the
// deactivation_effective_at + deactivation_reason so the FE catalog detail
// page can render the destructive "Deactivating <date>" pill.
func TestGetMarketplaceAddonByID_SurfacesDeactivationFieldsWhenInstalledAndPending(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	seedActiveSubscription(t, deps, "tms")
	// Relative, never a hardcoded calendar date. The registry schedules only
	// when EffectiveAt is AFTER now, so a fixed date silently converts this
	// test from "schedules" to "acts immediately" the day the clock passes it,
	// which is exactly how this went red on main without a code change.
	effective := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	wantEffectiveDay := effective.Format("2006-01-02")
	if _, _, _, err := deps.Subscriptions.Deactivate(adminTenantID, "tms",
		addon.DeactivationRequest{
			Reason:          addon.ReasonCost,
			EffectiveAt:     &effective,
			RequestedByGCID: adminGCID,
		}); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present when installed + pending deactivation (body=%v)", body)
	}
	got, _ := snap["deactivation_effective_at"].(string)
	if got == "" {
		t.Fatalf("deactivation_effective_at missing from marketplace snapshot: %v", snap)
	}
	if !strings.HasPrefix(got, wantEffectiveDay) {
		t.Errorf("deactivation_effective_at = %q, want %s... ISO", got, wantEffectiveDay)
	}
	if reason, _ := snap["deactivation_reason"].(string); reason != "cost" {
		t.Errorf("deactivation_reason = %v, want cost", snap["deactivation_reason"])
	}
}

// Defensive: when nothing is scheduled and the install is just plain ACTIVE,
// the marketplace handler must not leak current_subscription (the browse view
// privacy guarantee — only installed + scheduled or installed + pending
// deactivation gets the snapshot).
func TestGetMarketplaceAddonByID_OmitsCurrentSubscription_WhenInstalledAndActive(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	a, _ := deps.Catalogue.Get("tms")
	seedActiveSubscription(t, deps, "tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)
	if _, present := body["current_subscription"]; present {
		t.Errorf("current_subscription MUST be omitted when installed + ACTIVE (no schedule, no pending deactivation); got: %v", body["current_subscription"])
	}
}

// AddonRefCheck — verify addon variable to satisfy import ref (avoid unused).
func TestAddonExportedTypesSurface(t *testing.T) {
	t.Parallel()
	if string(addon.ReasonCost) != "cost" {
		t.Fatalf("ReasonCost constant changed unexpectedly")
	}
}

// ---------------------------------------------------------------------------
// CHO-1747 (CHO-1745 Sub 2) — system + installed fields on marketplace DTO
//
// The shared AddOnDetail OpenAPI schema (CHO-1746 / chora-contracts) now
// requires both fields on every marketplace response item. `system` reflects
// the always-on baseline plans (`base` today; future `compliance_core`,
// `audit_baseline`) and lets the FE suppress Subscribe / Activate Free /
// Deactivate affordances. `installed` is the denormalised view of whether
// the calling tenant currently holds a live subscription, so marketplace
// tiles can render an `Installed` badge without an extra round-trip.
//
// The contract field name is `installed` (not the legacy `is_installed`
// shipped earlier on the detail endpoint). The list endpoint is the new
// surface this cycle; the detail endpoint adds `installed` alongside the
// legacy `is_installed` for a transitional window until Sub 4 updates the
// FE consumer.
// ---------------------------------------------------------------------------

// findMarketplaceItem locates an items[] entry by its `code` field.
func findMarketplaceItem(t *testing.T, items []interface{}, code string) map[string]interface{} {
	t.Helper()
	for _, raw := range items {
		entry, _ := raw.(map[string]interface{})
		if c, _ := entry["code"].(string); c == code {
			return entry
		}
	}
	t.Fatalf("marketplace items did not contain code=%q", code)
	return nil
}

// TestListMarketplaceAddons_IncludesSystemFlag — the always-on `base` plan
// surfaces system=true so the FE can suppress its Subscribe affordances;
// every other catalogue entry surfaces system=false.
func TestListMarketplaceAddons_IncludesSystemFlag(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons", "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	base := findMarketplaceItem(t, items, "base")
	if got, present := base["system"].(bool); !present || !got {
		t.Fatalf("base.system: expected true, got %v (present=%v)", got, present)
	}
	tms := findMarketplaceItem(t, items, "tms")
	if got, present := tms["system"].(bool); !present || got {
		t.Fatalf("tms.system: expected false, got %v (present=%v)", got, present)
	}
}

// TestListMarketplaceAddons_IncludesInstalledFlagForActiveSub — a tenant
// holding an ACTIVE subscription surfaces installed=true on the marketplace
// list (no detail round-trip needed). Unrelated catalogue entries surface
// installed=false.
func TestListMarketplaceAddons_IncludesInstalledFlagForActiveSub(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons", "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	tms := findMarketplaceItem(t, items, "tms")
	if got, present := tms["installed"].(bool); !present || !got {
		t.Fatalf("tms.installed: expected true after Subscribe, got %v (present=%v)", got, present)
	}
	cms := findMarketplaceItem(t, items, "cms")
	if got, present := cms["installed"].(bool); !present || got {
		t.Fatalf("cms.installed: expected false (no sub), got %v (present=%v)", got, present)
	}
}

// TestListMarketplaceAddons_InstalledTrueOnPendingActivation — closes the
// Subscribe-via-Stripe-Checkout race window. A tenant whose subscription
// is StatusPendingActivation (post-checkout-start, pre-webhook-payment)
// already counts as installed so the marketplace tile renders the
// Installed badge instead of re-offering Subscribe.
func TestListMarketplaceAddons_InstalledTrueOnPendingActivation(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	if _, err := deps.Subscriptions.SubscribePending(adminTenantID, "familiar", adminGCID); err != nil {
		t.Fatalf("SubscribePending: %v", err)
	}
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons", "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	familiar := findMarketplaceItem(t, items, "familiar")
	if got, present := familiar["installed"].(bool); !present || !got {
		t.Fatalf("familiar.installed: expected true while PendingActivation, got %v (present=%v)", got, present)
	}
}

// TestGetMarketplaceAddonByID_IncludesSystemFlag — the per-plan detail
// endpoint exposes the same system flag the list endpoint does, so the
// detail page can render the `Included for all tenants` badge + suppress
// Subscribe / Activate Free.
func TestGetMarketplaceAddonByID_IncludesSystemFlag(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	base, _ := deps.Catalogue.Get("base")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+base.ID,
		"tenant_admin", nil)
	if got, present := body["system"].(bool); !present || !got {
		t.Fatalf("base detail system: expected true, got %v (present=%v)", got, present)
	}
	tms, _ := deps.Catalogue.Get("tms")
	_, body = adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+tms.ID,
		"tenant_admin", nil)
	if got, present := body["system"].(bool); !present || got {
		t.Fatalf("tms detail system: expected false, got %v (present=%v)", got, present)
	}
}

// TestGetMarketplaceAddonByID_InstalledFieldMatchesContract — the detail
// endpoint exposes the contract-canonical `installed` field name (Sub 1).
// The legacy `is_installed` field remains for the transitional window
// until Sub 4 cuts the FE consumer over.
func TestGetMarketplaceAddonByID_InstalledFieldMatchesContract(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	tms, _ := deps.Catalogue.Get("tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+tms.ID,
		"tenant_admin", nil)
	if got, present := body["installed"].(bool); !present || got {
		t.Fatalf("tms detail installed: expected false (no sub), got %v (present=%v)", got, present)
	}
	seedActiveSubscription(t, deps, "tms")
	_, body = adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+tms.ID,
		"tenant_admin", nil)
	if got, present := body["installed"].(bool); !present || !got {
		t.Fatalf("tms detail installed: expected true after Subscribe, got %v (present=%v)", got, present)
	}
}
