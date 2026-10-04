// RED-phase specs for the H+ external-egress admin endpoint (CHO-2148).
//
// The gate is the load-bearing part. Per [[reusable_tenancy_authz_mesh_roles_not_xrole_failopen]]
// the legacy rolesAllowed(X-Role) gate FAIL-OPENED in production (the gateway
// never stamps X-Role), so twelve tenant-admin writes were silently unenforced.
// This endpoint turns external web egress on for a whole tenant — it must
// authorize on the mesh header, fail-CLOSED, and it must NOT admit
// platform_operator (CHO-2076: the operator is non-tenant-scoped).
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/servicemesh"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

const (
	eeTenant = "11111111-1111-7111-8111-111111111111"
	eeActor  = "00000000-0000-7000-8000-000000001999"
)

// --- fake repo -------------------------------------------------------------

type fakeEgressRepo struct {
	policy    external_egress.Policy
	saved     *external_egress.Policy
	savedPrev *external_egress.Policy
	saveErr   error
	getErr    error
	saveCalls int
}

func (f *fakeEgressRepo) Get(_ context.Context, tenantID string) (external_egress.Policy, error) {
	if f.getErr != nil {
		return external_egress.Policy{}, f.getErr
	}
	if f.policy.TenantID == "" {
		return external_egress.DefaultPolicy(tenantID), nil
	}
	return f.policy, nil
}

func (f *fakeEgressRepo) Save(_ context.Context, next external_egress.Policy, prev external_egress.Policy) error {
	f.saveCalls++
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = &next
	f.savedPrev = &prev
	f.policy = next
	return nil
}

func egressReq(method, body string, roles string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/api/v1/admin/tenants/"+eeTenant+"/external-egress", nil)
	} else {
		r = httptest.NewRequest(method, "/api/v1/admin/tenants/"+eeTenant+"/external-egress", bytes.NewBufferString(body))
	}
	r.Header.Set("X-Tenant-Id", eeTenant)
	r.Header.Set("X-GCID", eeActor)
	if roles != "" {
		r.Header.Set(servicemesh.HeaderUserRoles, roles)
	}
	return r
}

func decodeEgress(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode body %q: %v", w.Body.String(), err)
	}
	return m
}

// --- authz (fail-CLOSED) ---------------------------------------------------

func TestExternalEgress_NoRoleHeader_403_FailsClosed(t *testing.T) {
	t.Parallel()
	deps := V2Deps{EgressPolicies: &fakeEgressRepo{}}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w, egressReq(http.MethodGet, "", ""))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — an absent mesh-roles header MUST deny "+
			"(the legacy rolesAllowed gate fail-OPENED here and left 12 writes unenforced)", w.Code)
	}
}

func TestExternalEgress_NonAdminRole_403(t *testing.T) {
	t.Parallel()
	deps := V2Deps{EgressPolicies: &fakeEgressRepo{}}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w, egressReq(http.MethodGet, "", "learner"))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a learner", w.Code)
	}
}

// CHO-2076: platform_operator is NON-tenant-scoped. Its only sanctioned
// cross-tenant capability is the audited chora-payments read. It must NOT be
// able to flip a specific tenant's egress entitlement.
func TestExternalEgress_PlatformOperator_403_NotTenantScoped(t *testing.T) {
	t.Parallel()
	deps := V2Deps{EgressPolicies: &fakeEgressRepo{}}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"egress_enabled":true}`, "platform_operator"))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — platform_operator must NOT reach tenant "+
			"entitlement data (CHO-2076 owner ruling)", w.Code)
	}
}

// --- GET -------------------------------------------------------------------

func TestExternalEgress_GET_NeverOptedIn_ReturnsFailClosedDefault(t *testing.T) {
	t.Parallel()
	deps := V2Deps{EgressPolicies: &fakeEgressRepo{}}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w, egressReq(http.MethodGet, "", "tenant_admin"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	body := decodeEgress(t, w)
	if body["egress_enabled"] != false {
		t.Errorf("egress_enabled = %v, want false (default-deny, ADR-220 D4)", body["egress_enabled"])
	}
	if body["opted_in"] != false {
		t.Errorf("opted_in = %v, want false — H+ must distinguish 'never opted in' "+
			"from 'explicitly turned off'", body["opted_in"])
	}
}

// --- PATCH -----------------------------------------------------------------

func TestExternalEgress_PATCH_OptIn_PersistsAndReturns200(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressRepo{}
	deps := V2Deps{EgressPolicies: repo}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"egress_enabled":true,"daily_call_ceiling":25}`, "tenant_admin"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if repo.saved == nil {
		t.Fatal("Save was not called")
	}
	if !repo.saved.EgressEnabled {
		t.Error("persisted EgressEnabled = false, want true")
	}
	if repo.saved.DailyCallCeiling != 25 {
		t.Errorf("persisted DailyCallCeiling = %d, want 25", repo.saved.DailyCallCeiling)
	}
	if repo.saved.UpdatedByGCID != eeActor {
		t.Errorf("persisted actor = %q, want %q — every egress change names its actor",
			repo.saved.UpdatedByGCID, eeActor)
	}
	body := decodeEgress(t, w)
	if body["opted_in"] != true {
		t.Errorf("opted_in = %v, want true after a successful opt-in", body["opted_in"])
	}
}

// A PATCH is a merge: an omitted field must not be silently reset to its zero
// value. Omitting egress_enabled while retuning the ceiling must NOT turn egress
// off.
func TestExternalEgress_PATCH_OmittedFieldIsNotReset(t *testing.T) {
	t.Parallel()
	seeded := external_egress.DefaultPolicy(eeTenant)
	if _, err := seeded.Update(true, 50, eeActor, time.Now().UTC()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo := &fakeEgressRepo{policy: seeded}
	deps := V2Deps{EgressPolicies: repo}
	w := httptest.NewRecorder()

	// Only the ceiling is sent. egress_enabled must stay TRUE.
	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"daily_call_ceiling":10}`, "tenant_admin"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if repo.saved == nil {
		t.Fatal("Save was not called")
	}
	if !repo.saved.EgressEnabled {
		t.Fatal("omitting egress_enabled silently turned egress OFF — a PATCH must merge, not reset")
	}
	if repo.saved.DailyCallCeiling != 10 {
		t.Errorf("DailyCallCeiling = %d, want 10", repo.saved.DailyCallCeiling)
	}
}

func TestExternalEgress_PATCH_IdenticalWrite_NoSave_NoEvent(t *testing.T) {
	t.Parallel()
	seeded := external_egress.DefaultPolicy(eeTenant)
	if _, err := seeded.Update(true, 50, eeActor, time.Now().UTC()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo := &fakeEgressRepo{policy: seeded}
	deps := V2Deps{EgressPolicies: repo}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"egress_enabled":true,"daily_call_ceiling":50}`, "tenant_admin"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if repo.saveCalls != 0 {
		t.Errorf("Save called %d times on an identical write, want 0 — "+
			"a no-op must emit no event and no audit row", repo.saveCalls)
	}
}

func TestExternalEgress_PATCH_CeilingAboveMax_422(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressRepo{}
	deps := V2Deps{EgressPolicies: repo}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"egress_enabled":true,"daily_call_ceiling":99999}`, "tenant_admin"))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 — the ceiling caps a PRICED egress call", w.Code)
	}
	if repo.saveCalls != 0 {
		t.Error("a rejected ceiling must persist nothing and publish nothing")
	}
}

func TestExternalEgress_PATCH_ConcurrentUpdate_409(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressRepo{saveErr: external_egress.ErrConcurrentUpdate}
	deps := V2Deps{EgressPolicies: repo}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w,
		egressReq(http.MethodPatch, `{"egress_enabled":true}`, "tenant_admin"))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — a lost update could silently re-enable "+
			"egress a tenant just turned off", w.Code)
	}
}

func TestExternalEgress_MethodNotAllowed_405(t *testing.T) {
	t.Parallel()
	deps := V2Deps{EgressPolicies: &fakeEgressRepo{}}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w, egressReq(http.MethodDelete, "", "tenant_admin"))

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

// Unwired repo must fail LOUD (503), never pretend the tenant is simply OFF —
// a silent default here would look identical to a real denial.
func TestExternalEgress_Unwired_503_FailsLoud(t *testing.T) {
	t.Parallel()
	deps := V2Deps{}
	w := httptest.NewRecorder()

	handleExternalEgress(deps, eeTenant, w, egressReq(http.MethodGet, "", "tenant_admin"))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when the repo is unwired — a stubbed 'OFF' "+
			"would be indistinguishable from a real policy denial", w.Code)
	}
}
