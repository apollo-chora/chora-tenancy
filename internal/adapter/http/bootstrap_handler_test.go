// bootstrap_handler_test.go — RED-phase tests for the H+ Setup-Tenant
// HTTP handler (CHO-1628 Phase 1).
//
// Exercises the request/response wiring with a fake service so the
// handler concerns (JSON decode, GCID header extraction, domain →
// HTTP error mapping) are isolated from the orchestrator + adapter
// layers (which have their own tests under internal/domain/bootstrap
// and internal/adapter/pg).
//
// ADR-182 D5 (CHO-1717 §4.5): private tenant creation is PLATFORM_OPERATOR
// only. The handler reads the session-JWT roles the chora-gateway BFF
// propagates via the mTLS-bound `x-mesh-user-roles` header (stamped from
// the validated Chora session JWT by jwt_auth.go → phyllis call()) and
// 403s with AUTH_PLATFORM_OPERATOR_REQUIRED for any non-operator caller.
// The old "Phase 5 bootstrap-mode" any-authenticated-caller flow is dead.
package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

// fakeBootstrapService records calls and returns canned values for the
// HTTP-layer tests.
type fakeBootstrapService struct {
	out       *bootstrap.Output
	err       error
	calls     int
	lastInput bootstrap.Input
}

func (s *fakeBootstrapService) Bootstrap(_ context.Context, in bootstrap.Input) (*bootstrap.Output, error) {
	s.calls++
	s.lastInput = in
	return s.out, s.err
}

// meshRolesHeader is the wire contract pin for the gateway-propagated
// session-JWT roles header (servicemesh.HeaderUserRoles). Kept as a literal
// here so a silent rename in the lib breaks this suite loudly.
const meshRolesHeader = "x-mesh-user-roles"

func TestBootstrapHandler_happyPath_returns201AndIdentifiers(t *testing.T) {
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID:      "01935f12-0000-7000-8000-000000000001",
			OwnerMemberID: "01935f12-0000-7000-8000-000000000002",
			EntitlementID: "01935f12-0000-7000-8000-000000000003",
			CreatedAt:     time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	body := strings.NewReader(`{"name":"Htet Aung Dev Tenant"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
	if svc.calls != 1 {
		t.Errorf("service should be invoked exactly once, got %d", svc.calls)
	}
	if svc.lastInput.Name != "Htet Aung Dev Tenant" {
		t.Errorf("forwarded name: got %q", svc.lastInput.Name)
	}
	if svc.lastInput.OwnerGCID != "01935f12-0000-7000-8000-0000000000ff" {
		t.Errorf("OwnerGCID should come from X-GCID header, got %q", svc.lastInput.OwnerGCID)
	}

	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["tenant_id"] != svc.out.TenantID {
		t.Errorf("response tenant_id mismatch: %v", got["tenant_id"])
	}
	if got["owner_member_id"] != svc.out.OwnerMemberID {
		t.Errorf("response owner_member_id mismatch: %v", got["owner_member_id"])
	}
	if got["entitlement_id"] != svc.out.EntitlementID {
		t.Errorf("response entitlement_id mismatch: %v", got["entitlement_id"])
	}
	if got["created_at"] == "" || got["created_at"] == nil {
		t.Errorf("response created_at missing: %v", got["created_at"])
	}
}

func TestBootstrapHandler_lowercaseGcidHeader_isAccepted(t *testing.T) {
	// CHO-1632 Phase 3 — chora-gateway aggregator stamps `gcid`
	// (lowercase, no dash) per the canonical house pattern.
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID:      "01935f12-0000-7000-8000-000000000001",
			OwnerMemberID: "01935f12-0000-7000-8000-000000000002",
			EntitlementID: "01935f12-0000-7000-8000-000000000003",
			CreatedAt:     time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Lowercase GCID Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("gcid", "01935f12-0000-7000-8000-0000000000ee")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
	if svc.lastInput.OwnerGCID != "01935f12-0000-7000-8000-0000000000ee" {
		t.Errorf("OwnerGCID should come from `gcid` header, got %q", svc.lastInput.OwnerGCID)
	}
}

func TestBootstrapHandler_xGcidPreferredOverLowercase(t *testing.T) {
	// When BOTH headers are present, X-GCID wins (firstNonEmpty order).
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID: "01935f12-0000-7000-8000-000000000001",
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Pref Test"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-000000000aaa")
	req.Header.Set("gcid", "01935f12-0000-7000-8000-000000000bbb")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if svc.lastInput.OwnerGCID != "01935f12-0000-7000-8000-000000000aaa" {
		t.Errorf("X-GCID should be preferred when both present, got %q", svc.lastInput.OwnerGCID)
	}
}

func TestBootstrapHandler_missingGCIDHeader_returns401(t *testing.T) {
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	body := strings.NewReader(`{"name":"Anon Tenant"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap", body)
	req.Header.Set("Content-Type", "application/json")
	// X-GCID intentionally absent

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusUnauthorized)
	}
	if svc.calls != 0 {
		t.Errorf("service must NOT be invoked when GCID missing; got %d", svc.calls)
	}
}

func TestBootstrapHandler_whitespaceGCIDHeader_returns401(t *testing.T) {
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "   ")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestBootstrapHandler_invalidJSON_returns400(t *testing.T) {
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name": [not json`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusBadRequest)
	}
	if svc.calls != 0 {
		t.Errorf("service must NOT be invoked on bad JSON; got %d", svc.calls)
	}
}

func TestBootstrapHandler_serviceErrAlreadyMember_returns409(t *testing.T) {
	svc := &fakeBootstrapService{err: bootstrap.ErrAlreadyMember}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Already In"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusConflict, w.Body.String())
	}
}

func TestBootstrapHandler_serviceErrInvalidArgument_returns400(t *testing.T) {
	svc := &fakeBootstrapService{err: bootstrap.ErrInvalidArgument}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusBadRequest)
	}
}

func TestBootstrapHandler_serviceGenericError_returns500(t *testing.T) {
	svc := &fakeBootstrapService{err: errors.New("db kaput")}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Will Blow Up"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusInternalServerError)
	}
}

func TestBootstrapHandler_wrongMethod_returns405(t *testing.T) {
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/bootstrap", nil)
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

// -----------------------------------------------------------------------------
// ADR-182 D5 — PLATFORM_OPERATOR-only gate (CHO-1717 §4.5)
// -----------------------------------------------------------------------------

// decodeErrorEnvelope pulls error.code out of the v2 error envelope
// {"error":{"code":..., "message":...}}.
func decodeErrorEnvelope(t *testing.T, body string) string {
	t.Helper()
	var got struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode error envelope: %v (body=%s)", err, body)
	}
	return got.Error.Code
}

func TestBootstrapHandler_nonOperatorRoles_returns403WithCode(t *testing.T) {
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Sneaky Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "learner,tenant_admin,owner")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusForbidden, w.Body.String())
	}
	if code := decodeErrorEnvelope(t, w.Body.String()); code != "AUTH_PLATFORM_OPERATOR_REQUIRED" {
		t.Errorf("error.code = %q, want AUTH_PLATFORM_OPERATOR_REQUIRED", code)
	}
	if svc.calls != 0 {
		t.Errorf("service must NOT be invoked for non-operator caller; got %d", svc.calls)
	}
}

func TestBootstrapHandler_missingRolesHeader_returns403(t *testing.T) {
	// The old Phase 5 bootstrap-mode flow (any authenticated zero-membership
	// caller may bootstrap) is DEAD per ADR-182 D5 — an authenticated caller
	// with no propagated roles is forbidden.
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Phase 5 Ghost"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	// x-mesh-user-roles intentionally absent

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusForbidden, w.Body.String())
	}
	if code := decodeErrorEnvelope(t, w.Body.String()); code != "AUTH_PLATFORM_OPERATOR_REQUIRED" {
		t.Errorf("error.code = %q, want AUTH_PLATFORM_OPERATOR_REQUIRED", code)
	}
	if svc.calls != 0 {
		t.Errorf("service must NOT be invoked without roles; got %d", svc.calls)
	}
}

func TestBootstrapHandler_canonicalUppercaseOperatorRole_isAccepted(t *testing.T) {
	// The canonical role constant elsewhere in the platform is
	// PLATFORM_OPERATOR (ADR-165) — the gate is case-insensitive.
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID:  "01935f12-0000-7000-8000-000000000001",
			CreatedAt: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Canonical Caps Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "PLATFORM_OPERATOR")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
	if svc.calls != 1 {
		t.Errorf("service should be invoked exactly once, got %d", svc.calls)
	}
}

func TestBootstrapHandler_mixedCaseOperatorRole_isAccepted(t *testing.T) {
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID:  "01935f12-0000-7000-8000-000000000001",
			CreatedAt: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Mixed Case Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "Platform_Operator")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
}

func TestBootstrapHandler_operatorAmongMultipleRoles_isAccepted(t *testing.T) {
	// Comma-separated multi-role header with stray whitespace — the
	// operator entry anywhere in the list passes the gate.
	svc := &fakeBootstrapService{
		out: &bootstrap.Output{
			TenantID:  "01935f12-0000-7000-8000-000000000001",
			CreatedAt: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		},
	}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Multi Role Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, " learner , platform_operator ,instructor")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
}

func TestBootstrapHandler_operatorPrefixRole_isRejected(t *testing.T) {
	// Exact-match guard: a role that merely STARTS with platform_operator
	// (e.g. a hypothetical platform_operator_readonly) must NOT pass.
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Prefix Spoof Tenant"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GCID", "01935f12-0000-7000-8000-0000000000ff")
	req.Header.Set(meshRolesHeader, "platform_operator_readonly")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusForbidden)
	}
	if svc.calls != 0 {
		t.Errorf("service must NOT be invoked; got %d", svc.calls)
	}
}

func TestBootstrapHandler_missingGCID_401TakesPrecedenceOver403(t *testing.T) {
	// Unauthenticated (no GCID) beats unauthorized (no operator role):
	// the caller gets 401, not 403, even though roles are also absent.
	svc := &fakeBootstrapService{}
	h := httpapi.NewBootstrapHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Anon"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(meshRolesHeader, "platform_operator") // roles but no GCID

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Roles present but unauthenticated → still 401 (GCID gate first).
	req2 := httptest.NewRequest(http.MethodPost, "/v1/tenants/bootstrap",
		strings.NewReader(`{"name":"Anon2"}`))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("roles-without-gcid: got %d want %d", w.Code, http.StatusUnauthorized)
	}
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("nothing-at-all: got %d want %d", w2.Code, http.StatusUnauthorized)
	}
}

func TestNewBootstrapHandler_nilService_panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewBootstrapHandler(nil) should panic — service is mandatory")
		}
	}()
	httpapi.NewBootstrapHandler(nil)
}
