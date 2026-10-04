// root_tenant_authz_test.go — CHO-2077: the bare root-tenant HTTP endpoints
// POST /v1/tenants, POST /v2/tenants, POST /api/tenants (create) and their GET
// (list-ALL-tenants, a cross-tenant read) had NO role gate — only /v1 (none)
// and /v2//api (X-Tenant-Id presence). They are safe today ONLY because the
// gateway route table doesn't forward there, while the tenancy
// allow-from-gateway mesh policy DOES allow the paths — a routing accident, not
// a control. Root-tenant management is a platform_operator action (mirrors
// bootstrap_handler + the operator-gated sub-tenant path), so gate them
// operator-only. Real external create paths (/api/v1/tenants/bootstrap,
// sub-tenants) are already operator-gated and untouched.
package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type rootTenantEP struct {
	name string
	srv  http.Handler
	path string
}

func rootTenantEndpoints(t *testing.T) []rootTenantEP {
	t.Helper()
	apiSrv := newServer()
	v2Srv, _ := newAdminServer(t)
	return []rootTenantEP{
		{"/api/tenants", apiSrv, "/api/tenants"},
		{"/v1/tenants", v2Srv, "/v1/tenants"},
		{"/v2/tenants", v2Srv, "/v2/tenants"},
	}
}

// rootTenantReq builds a create (POST) or list (GET) request. X-Tenant-Id is
// set so the /v2 + /api tenant-required middleware passes THROUGH to the
// operator gate — it is the gate, not the middleware, that is under test.
func rootTenantReq(method, path, meshRole string) *http.Request {
	body := `{"display_name":"Acme","owner_gcid":"01970000-0000-7000-8000-0000000000cc",` +
		`"slug":"acme-op","country":"SG","default_currency":"SGD"}`
	var r *http.Request
	if method == http.MethodGet {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("X-Tenant-Id", "01970000-0000-7000-8000-000000000001")
	r.Header.Set("X-GCID", "01970000-0000-7000-8000-0000000000cc")
	if meshRole != "" {
		r.Header.Set("x-mesh-user-roles", meshRole)
	}
	return r
}

// TestRootTenant_DeniesNonOperator — create (POST) AND list-all (GET) must 403
// for any non-operator identity (no role / tenant_admin / admin / learner).
func TestRootTenant_DeniesNonOperator(t *testing.T) {
	for _, ep := range rootTenantEndpoints(t) {
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			for _, role := range []string{"", "tenant_admin", "admin", "learner"} {
				t.Run(ep.name+"/"+method+"/roles="+role, func(t *testing.T) {
					rec := httptest.NewRecorder()
					ep.srv.ServeHTTP(rec, rootTenantReq(method, ep.path, role))
					if rec.Code != http.StatusForbidden {
						t.Fatalf("%s %s roles=%q: expected 403, got %d (body=%s)",
							method, ep.name, role, rec.Code, rec.Body.String())
					}
				})
			}
		}
	}
}

// TestRootTenant_AllowsOperator — platform_operator passes the gate (NOT 403);
// body/paging validity is orthogonal to authorization.
func TestRootTenant_AllowsOperator(t *testing.T) {
	for _, ep := range rootTenantEndpoints(t) {
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			t.Run(ep.name+"/"+method, func(t *testing.T) {
				rec := httptest.NewRecorder()
				ep.srv.ServeHTTP(rec, rootTenantReq(method, ep.path, "platform_operator"))
				if rec.Code == http.StatusForbidden {
					t.Fatalf("%s %s operator: wrongly denied (403; body=%s)",
						method, ep.name, rec.Body.String())
				}
			})
		}
	}
}
