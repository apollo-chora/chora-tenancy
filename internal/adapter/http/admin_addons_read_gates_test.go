// admin_addons_read_gates_test.go — CHO-2073: the add-on admin READ endpoints
// detail (GET /addons/{id}) and usage (GET /addons/{id}/usage) leaked a
// tenant's entitlement + usage data to any authenticated member (no gate),
// same class as the CHO-1801 list gap. They now share the fail-closed
// callerHoldsHPlusAdminRole gate. (activate is a write, gated under CHO-2072.)
package httpapi_test

import (
	"net/http"
	"testing"
)

func adminReadEndpoints(tid string) []struct{ name, path string } {
	base := "/api/v1/admin/tenants/" + tid
	return []struct{ name, path string }{
		{"addon detail", base + "/addons/tms"},
		{"addon usage", base + "/addons/tms/usage"},
	}
}

// TestAdminReadGates_DenyNonAdmin — detail + usage reads must 403 for a
// non-admin / no-role caller (fail-closed).
func TestAdminReadGates_DenyNonAdmin(t *testing.T) {
	for _, ep := range adminReadEndpoints(adminTenantID) {
		// platform_operator (CHO-2076): non-tenant-scoped; denied tenant data.
		for _, roles := range []string{"", "learner", "instructor", "platform_operator"} {
			t.Run(ep.name+"/roles="+roles, func(t *testing.T) {
				srv, deps := newAdminServer(t)
				seedActiveSubscription(t, deps, "tms")
				rec := doMeshReq(t, srv, http.MethodGet, ep.path, roles, nil)
				if rec.Code != http.StatusForbidden {
					t.Fatalf("%s roles=%q: expected 403, got %d (body=%s)",
						ep.name, roles, rec.Code, rec.Body.String())
				}
			})
		}
	}
}

// TestAdminReadGates_AllowAdmin — the hplus-admin roles + operator reach the
// reads (status is NOT 403).
func TestAdminReadGates_AllowAdmin(t *testing.T) {
	for _, ep := range adminReadEndpoints(adminTenantID) {
		for _, roles := range []string{"tenant_admin", "owner", "super_admin", "TENANT_ADMIN"} {
			t.Run(ep.name+"/roles="+roles, func(t *testing.T) {
				srv, deps := newAdminServer(t)
				seedActiveSubscription(t, deps, "tms")
				rec := doMeshReq(t, srv, http.MethodGet, ep.path, roles, nil)
				if rec.Code == http.StatusForbidden {
					t.Fatalf("%s roles=%q: gate wrongly denied an admin (403; body=%s)",
						ep.name, roles, rec.Body.String())
				}
			})
		}
	}
}
