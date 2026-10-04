// admin_authz_gates_test.go — CHO-2072: every chora-tenancy H+ admin WRITE
// endpoint must authorize fail-CLOSED on the gateway-propagated
// x-mesh-user-roles header, NOT the legacy fail-open X-Role. Before CHO-2072
// these gates called rolesAllowed(X-Role), which the gateway never stamps and
// which returned true on an empty header — so a non-admin could perform every
// tenant-admin write. These black-box tests pin the gate at each endpoint:
// non-admin / no-role → 403; an hplus-admin role → NOT 403 (gate passed).
package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// doMeshReq issues method+path with the given x-mesh-user-roles value
// ("" = header absent) and a JSON body. It sets NO X-Role — the gate must
// depend only on the mesh header.
func doMeshReq(t *testing.T, srv http.Handler, method, path, meshRoles string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", adminTenantID)
	req.Header.Set("X-GCID", adminGCID)
	if meshRoles != "" {
		req.Header.Set("x-mesh-user-roles", meshRoles)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

// adminWriteEndpoints is the full set of H+ admin WRITE gates migrated in
// CHO-2072. The gate is the first statement in every handler, so a minimal
// body is enough to reach (and be blocked by) it.
func adminWriteEndpoints(tid string) []struct {
	name, method, path string
	body               interface{}
} {
	base := "/api/v1/admin/tenants/" + tid
	return []struct {
		name, method, path string
		body               interface{}
	}{
		{"change-tier (PATCH)", http.MethodPatch, base + "/addons/tms", map[string]interface{}{"target_tier_code": "pro"}},
		{"change-tier (POST leaf)", http.MethodPost, base + "/addons/tms/change-tier", map[string]interface{}{"target_tier_code": "pro"}},
		{"activate", http.MethodPost, base + "/addons/tms:activate", map[string]interface{}{}},
		{"deactivate", http.MethodPost, base + "/addons/tms:deactivate", map[string]interface{}{"reason": "cost", "reason_text": "x", "confirmation_text": "tms"}},
		{"cancel-deactivation", http.MethodPost, base + "/addons/tms:cancel-deactivation", map[string]interface{}{}},
		{"preview-tier-change", http.MethodPost, base + "/addons/tms/preview-tier-change", map[string]interface{}{"target_tier_code": "pro"}},
		{"request-deactivation", http.MethodPost, base + "/addons/tms/request-deactivation", map[string]interface{}{"reason": "cost", "confirmation_text": "tms"}},
		{"mana-pool create", http.MethodPost, base + "/mana-pool", map[string]interface{}{}},
		{"mana-pool auto-renew", http.MethodPost, base + "/mana-pool:auto-renew", map[string]interface{}{"monthly_topup_units": 10}},
		{"mana-pool allocations", http.MethodPost, base + "/mana-pool/allocations", map[string]interface{}{"units": 1, "reason": "x"}},
	}
}

// TestAdminWriteGates_DenyNonAdmin — a non-admin (learner/instructor) or a
// caller with NO mesh role must get 403 at every write gate (fail-closed).
func TestAdminWriteGates_DenyNonAdmin(t *testing.T) {
	for _, ep := range adminWriteEndpoints(adminTenantID) {
		// billing_admin: not an hplus surface role (legacy rolesAllowed named it
		// but never enforced, being fail-open). platform_operator (CHO-2076):
		// a NON-tenant-scoped role — its ONLY authorized cross-tenant capability
		// is the audited chora-payments READ; it must NOT reach a specific
		// tenant's entitlement/mana/compliance data. Both → 403 here.
		for _, roles := range []string{"", "learner", "instructor", "learner,author", "billing_admin", "platform_operator"} {
			t.Run(ep.name+"/roles="+roles, func(t *testing.T) {
				srv, deps := newAdminServer(t)
				seedActiveSubscription(t, deps, "tms")
				rec := doMeshReq(t, srv, ep.method, ep.path, roles, ep.body)
				if rec.Code != http.StatusForbidden {
					t.Fatalf("%s roles=%q: expected 403, got %d (body=%s)",
						ep.name, roles, rec.Code, rec.Body.String())
				}
			})
		}
	}
}

// TestSuperAdminOverrideGate_ComplianceLock — the STRICTER compliance-override
// gate (CHO-2072 Cat-B). Deactivating a ComplianceLocked add-on
// (governance_dashboard) with super_admin_override requires super_admin (or
// operator) — a general tenant_admin must NOT be able to override, and the
// compliance lock itself must now actually fire (it was fail-open pre-CHO-2072).
func TestSuperAdminOverrideGate_ComplianceLock(t *testing.T) {
	path := "/api/v1/admin/tenants/" + adminTenantID +
		"/addons/governance_dashboard/request-deactivation"
	override := func(v bool) map[string]interface{} {
		return map[string]interface{}{
			"reason": "cost", "confirmation_text": "governance_dashboard",
			"super_admin_override": v,
		}
	}

	// tenant_admin claiming override → 403 (passes the general gate, fails the
	// stricter super_admin gate). platform_operator → 403 too (CHO-2076): the
	// compliance lock is tenant data; a non-tenant-scoped operator cannot
	// override it.
	for _, role := range []string{"tenant_admin", "platform_operator"} {
		t.Run(role+" override denied", func(t *testing.T) {
			srv, _ := newAdminServer(t)
			rec := doMeshReq(t, srv, http.MethodPost, path, role, override(true))
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s override: expected 403, got %d (body=%s)", role, rec.Code, rec.Body.String())
			}
		})
	}

	// super_admin override → NOT 403 (the stricter tenant super_admin gate passed).
	for _, role := range []string{"super_admin", "SUPER_ADMIN"} {
		t.Run(role+" override allowed", func(t *testing.T) {
			srv, _ := newAdminServer(t)
			rec := doMeshReq(t, srv, http.MethodPost, path, role, override(true))
			if rec.Code == http.StatusForbidden {
				t.Fatalf("%s override: wrongly denied (403; body=%s)", role, rec.Body.String())
			}
		})
	}

	// tenant_admin WITHOUT override on a compliance-locked add-on → 423: the
	// compliance lock now actually fires (fail-closed), where the fail-open
	// legacy gate let it slip.
	t.Run("compliance lock fires without override", func(t *testing.T) {
		srv, _ := newAdminServer(t)
		rec := doMeshReq(t, srv, http.MethodPost, path, "tenant_admin", override(false))
		if rec.Code != http.StatusLocked {
			t.Fatalf("expected 423 compliance-locked, got %d (body=%s)", rec.Code, rec.Body.String())
		}
	})
}

// TestAdminWriteGates_AllowAdmin — every hplus-admin role + the uppercase
// contract form passes the gate (status is NOT 403; body-level validation is
// orthogonal to authorization). platform_operator is deliberately absent
// (CHO-2076 — operator is denied tenant data).
func TestAdminWriteGates_AllowAdmin(t *testing.T) {
	for _, ep := range adminWriteEndpoints(adminTenantID) {
		for _, roles := range []string{"tenant_admin", "admin", "owner", "super_admin", "TENANT_ADMIN"} {
			t.Run(ep.name+"/roles="+roles, func(t *testing.T) {
				srv, deps := newAdminServer(t)
				seedActiveSubscription(t, deps, "tms")
				rec := doMeshReq(t, srv, ep.method, ep.path, roles, ep.body)
				if rec.Code == http.StatusForbidden {
					t.Fatalf("%s roles=%q: gate wrongly denied an admin (403; body=%s)",
						ep.name, roles, rec.Body.String())
				}
			})
		}
	}
}
