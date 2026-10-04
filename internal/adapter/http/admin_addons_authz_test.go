// admin_addons_authz_test.go — CHO-1801 §4 BE gap: the H+ add-on
// entitlements list (`GET /api/v1/admin/tenants/{tenantId}/addons`, the FE
// `/h/addons` dashboard reached via the gateway `me`-alias ListAdminMyAddons)
// MUST be admin-gated, mirroring its siblings `/admin/tenant-members`
// (chora-identity callerHoldsAdminRole) and `/admin/tenants/me/invoices`.
//
// The gate reads the gateway-propagated `x-mesh-user-roles` mesh header
// (servicemesh.HeaderUserRoles) fail-CLOSED — NOT the legacy `X-Role` header
// adminDo/doV1 set (the gateway never stamps X-Role, so a gate on it fail-OPENS
// in prod). These tests therefore stamp the mesh header directly.
package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// meshRolesHeaderAddons is the wire-contract pin for the gateway-propagated
// Bucket-4 role header the H+ add-on admin gate reads.
const meshRolesHeaderAddons = "x-mesh-user-roles"

// doAddonsList issues GET /api/v1/admin/tenants/{id}/addons with the given
// x-mesh-user-roles value ("" = header absent). It deliberately does NOT set
// X-Role — the gate must not depend on it.
func doAddonsList(t *testing.T, srv http.Handler, meshRoles string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/tenants/"+adminTenantID+"/addons", nil)
	req.Header.Set("X-Tenant-Id", adminTenantID)
	req.Header.Set("X-GCID", adminGCID)
	if meshRoles != "" {
		req.Header.Set(meshRolesHeaderAddons, meshRoles)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

// TestListAddons_DeniesNonAdmin — a caller with NO mesh role (fail-closed) or a
// non-H+ role (learner / instructor) must get 403. This is the CHO-1801 §4
// broken-access-control gap: before the gate the list 200'd to any tenant
// member, leaking the tenant's paid entitlements via an admin-namespaced route.
func TestListAddons_DeniesNonAdmin(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	// Seed a subscription so an ungated handler would return 200 with items —
	// making the 403 unambiguously the gate's doing, not an empty-tenant 200.
	seedActiveSubscription(t, deps, "tms")

	cases := []struct {
		name  string
		roles string
	}{
		{"no mesh role at all (fail-closed)", ""},
		{"learner (A+/C+ member, not H+)", "learner"},
		{"instructor (R+ role, not H+)", "instructor"},
		{"multi non-admin roles", "learner,instructor,author"},
		// platform_operator (CHO-2076): a NON-tenant-scoped role — its only
		// authorized cross-tenant capability is the audited chora-payments READ;
		// it must NOT read a specific tenant's add-on entitlements.
		{"platform_operator (non-tenant-scoped)", "platform_operator"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doAddonsList(t, srv, tc.roles)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("roles=%q: expected 403, got %d (body=%s)",
					tc.roles, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestListAddons_AllowsAdminRoles — the H+ tenant-admin roles (RolesToSurfaces
// → hplus) reach the list. Case-insensitive: the uppercase contract form must
// also pass. platform_operator is deliberately absent (CHO-2076 — it is
// non-tenant-scoped and denied tenant entitlement data).
func TestListAddons_AllowsAdminRoles(t *testing.T) {
	t.Parallel()
	roles := []string{
		"tenant_admin",
		"admin",
		"owner",
		"super_admin",
		"TENANT_ADMIN",         // uppercase contract form (ADR-141)
		"learner,tenant_admin", // admin among other roles
	}
	for _, role := range roles {
		role := role
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			srv, deps := newAdminServer(t)
			seedActiveSubscription(t, deps, "tms")
			rec := doAddonsList(t, srv, role)
			if rec.Code != http.StatusOK {
				t.Fatalf("roles=%q: expected 200, got %d (body=%s)",
					role, rec.Code, rec.Body.String())
			}
		})
	}
}
