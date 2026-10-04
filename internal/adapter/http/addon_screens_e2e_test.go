// Package httpapi_test — A-Tenant-Lifecycle (S6.2) end-to-end tests that
// walk the 8-stage value stream from docs/design/ux_tenant_addon_lifecycle.md:
//
//	Discover → Evaluate → Purchase → Operate → Monitor → Upgrade → Audit
//
// Verifies the cross-screen flow integrity: events emitted on purchase are
// surfaced in the audit screen; tier changes update the dashboard list;
// deactivation transitions the tile to deactivating state; etc.
package httpapi_test

import (
	"net/http"
	"testing"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// TestE2E_DiscoverEvaluatePurchaseUpgradeAudit walks the full value stream
// and asserts each step persists the expected state and emits the expected
// events.
func TestE2E_DiscoverEvaluatePurchaseUpgradeAudit(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	// Discover — list marketplace.
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons", "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Discover: expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) == 0 {
		t.Fatalf("Discover: expected ≥1 marketplace item")
	}
	// Evaluate — pick TMS, fetch detail by id.
	tmsAddon, _ := deps.Catalogue.Get("tms")
	rec, body = adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+tmsAddon.ID, "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Evaluate: expected 200, got %d", rec.Code)
	}
	if cta, _ := body["cta"].(string); cta != "Install" {
		t.Fatalf("Evaluate: expected Install CTA, got %q", cta)
	}
	// Purchase — subscribe via the registry (emulates Stripe checkout success).
	if _, err := deps.Subscriptions.Subscribe(adminTenantID, "tms", adminGCID); err != nil {
		t.Fatalf("Purchase: subscribe %v", err)
	}
	// Marketplace tile now shows Manage.
	_, body = adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+tmsAddon.ID, "tenant_admin", nil)
	if cta, _ := body["cta"].(string); cta != "Manage" {
		t.Fatalf("after Purchase: expected Manage CTA, got %q", cta)
	}
	// Monitor — record some usage and read it back.
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID:   adminTenantID,
		AddOnID:    tmsAddon.ID,
		BucketDate: time.Now().UTC().Truncate(24 * time.Hour),
		SeatsUsed:  10, SeatsUsedPeak: 12, QuotaConsumed: 150,
	})
	rec, body = adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/"+tmsAddon.ID+"/usage",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Monitor: expected 200, got %d", rec.Code)
	}
	// Preview Upgrade
	rec, body = adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("Preview: expected 200, got %d", rec.Code)
	}
	delta, _ := body["billing_delta_cents"].(float64)
	if delta <= 0 {
		t.Fatalf("Preview: expected positive delta, got %v", delta)
	}
	// Confirm Upgrade — change-tier alias.
	rec, _ = adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/change-tier",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("Upgrade: expected 200, got %d", rec.Code)
	}
	// Audit — should now contain activated + upgraded.
	rec, body = adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/audit",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Audit: expected 200, got %d", rec.Code)
	}
	auditItems, _ := body["items"].([]interface{})
	if len(auditItems) < 2 {
		t.Fatalf("Audit: expected ≥2 events, got %d", len(auditItems))
	}
	// All audit entries carry IMDA D1 attribution.
	for _, raw := range auditItems {
		entry, _ := raw.(map[string]interface{})
		dim, _ := entry["chora_imda_dimension"].(string)
		if dim != "accountability" {
			t.Fatalf("Audit: expected chora_imda_dimension=accountability, got %q", dim)
		}
	}
	// Verify upgraded event was emitted.
	if got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1"); len(got) != 1 {
		t.Fatalf("expected 1 upgraded event, got %d", len(got))
	}
}

// TestE2E_RequestDeactivation_FlowsCompliancePathThenSuperAdminOverride
// walks the deactivation sub-flow for a compliance-locked add-on:
//  1. tenant_admin attempts → 423.
//  2. super_admin without override flag → 423.
//  3. super_admin with override + correct confirmation_text → 202 +
//     chora.tenancy.addon.deactivation_requested.v1 with super_admin_override=true.
func TestE2E_RequestDeactivation_CompliancePath(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	if _, err := deps.Subscriptions.Subscribe(adminTenantID, "governance_dashboard", adminGCID); err != nil {
		t.Fatalf("seed governance_dashboard: %v", err)
	}
	// 1. tenant_admin → 423.
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"tenant_admin", map[string]interface{}{
			"reason":            "compliance",
			"confirmation_text": "governance_dashboard",
		})
	if rec.Code != http.StatusLocked {
		t.Fatalf("step 1: tenant_admin should be 423, got %d", rec.Code)
	}
	// 2. super_admin without override flag → 423.
	rec, _ = adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"super_admin", map[string]interface{}{
			"reason":            "compliance",
			"confirmation_text": "governance_dashboard",
		})
	if rec.Code != http.StatusLocked {
		t.Fatalf("step 2: super_admin no-override should be 423, got %d", rec.Code)
	}
	// 3. super_admin with override + correct confirmation_text → 202.
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard/request-deactivation",
		"super_admin", map[string]interface{}{
			"reason":               "compliance",
			"confirmation_text":    "governance_dashboard",
			"super_admin_override": true,
			"reason_text":          "decommissioning regulated programme; auditor approved",
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("step 3: super_admin override should be 202, got %d (body=%v)", rec.Code, body)
	}
	if locked, _ := body["compliance_lock_overridden"].(bool); !locked {
		t.Fatalf("expected compliance_lock_overridden=true, got %v", body["compliance_lock_overridden"])
	}
	// Event emitted with super_admin_override flag.
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_requested.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 deactivation_requested event, got %d", len(got))
	}
	if override, _ := got[0].Payload["super_admin_override"].(bool); !override {
		t.Fatalf("event payload missing super_admin_override=true")
	}
	if dim, _ := got[0].Payload["chora_imda_dimension"].(string); dim != "accountability" {
		t.Fatalf("event payload missing chora_imda_dimension=accountability, got %q", dim)
	}
}
