// me_branding_handler_test.go — RED-phase tests for the Setup Wizard
// Phase A PATCH /api/v1/tenants/me/branding handler (CHO-1655).
//
// Strict TDD per .claude/rules/development-execution.md: this test file
// is written BEFORE me_branding_handler.go exists. Compile must fail
// with "undefined: httpapi.NewMeBrandingHandler" etc. — that's the
// observed RED phase. Implementation lands next and turns these GREEN.
//
// Exercises the request/response wiring with a fake `tenant.Repository`
// so handler concerns (header extraction, body decode, domain → HTTP
// error mapping, PATCH-merge semantics) stay isolated from the pg
// adapter and the rest of the chora-tenancy HTTP surface.
package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// fakeBrandingTenantRepo is a minimal `tenant.Repository` fake. Only
// Get + Save are exercised by the /me/branding handler; FindBySlug
// returns "not found" since it's irrelevant here.
type fakeBrandingTenantRepo struct {
	mu           sync.Mutex
	tenants      map[string]*tenant.Tenant
	getErr       error
	saveErr      error
	savedTenants []*tenant.Tenant
}

func newFakeBrandingTenantRepo() *fakeBrandingTenantRepo {
	return &fakeBrandingTenantRepo{tenants: map[string]*tenant.Tenant{}}
}

func (r *fakeBrandingTenantRepo) put(t *tenant.Tenant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants[t.ID] = t
}

func (r *fakeBrandingTenantRepo) Get(_ context.Context, id string) (*tenant.Tenant, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, false, r.getErr
	}
	t, ok := r.tenants[id]
	return t, ok, nil
}

func (r *fakeBrandingTenantRepo) Save(_ context.Context, t *tenant.Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.tenants[t.ID] = t
	r.savedTenants = append(r.savedTenants, t)
	return nil
}

func (r *fakeBrandingTenantRepo) FindBySlug(_ context.Context, _ string) (*tenant.Tenant, bool, error) {
	return nil, false, nil
}

// seedTenantForBranding returns a basic tenant with the given ID and
// existing branding so tests can exercise both initial-set and overlay
// flows.
func seedTenantForBranding(t *testing.T, id string, branding tenant.BrandingConfig) *tenant.Tenant {
	t.Helper()
	tt, err := tenant.NewTenant("Acme", "owner-gcid", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	tt.ID = id
	if branding.PrimaryColorHex != "" || branding.LogoURL != "" || branding.CustomDomain != "" {
		if err := tt.UpdateBranding(branding); err != nil {
			t.Fatalf("UpdateBranding seed: %v", err)
		}
	}
	return tt
}

func doBrandingPatch(t *testing.T, h http.Handler, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPatch, "/api/v1/tenants/me/branding",
		strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// ---------------------------------------------------------------------------
// Happy path — PATCH writes branding through to the repo
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_HappyPath_PersistsAndReturns200(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	const tid = "01970000-0000-7000-8000-000000000001"
	repo.put(seedTenantForBranding(t, tid, tenant.BrandingConfig{}))

	h := httpapi.NewMeBrandingHandler(repo)
	body := `{"primary_color_hex":"#FF5500","logo_url":"https://cdn.example.com/logo.png"}`
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": tid}, body)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		PrimaryColorHex string `json:"primary_color_hex"`
		LogoURL         string `json:"logo_url"`
		CustomDomain    string `json:"custom_domain"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body decode: %v; body = %s", err, w.Body.String())
	}
	if resp.PrimaryColorHex != "#FF5500" {
		t.Errorf("primary_color_hex = %q, want #FF5500", resp.PrimaryColorHex)
	}
	if resp.LogoURL != "https://cdn.example.com/logo.png" {
		t.Errorf("logo_url = %q, want the URL we PATCHed", resp.LogoURL)
	}
	if len(repo.savedTenants) != 1 {
		t.Fatalf("repo.Save called %d times, want 1", len(repo.savedTenants))
	}
	if repo.savedTenants[0].ID != tid {
		t.Errorf("saved tenant id = %q, want %q", repo.savedTenants[0].ID, tid)
	}
}

// ---------------------------------------------------------------------------
// PATCH semantics — partial body only overlays the included fields
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_PartialBody_PreservesUnmentionedFields(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	const tid = "01970000-0000-7000-8000-000000000002"
	repo.put(seedTenantForBranding(t, tid, tenant.BrandingConfig{
		PrimaryColorHex: "#000000",
		LogoURL:         "https://existing.example.com/old.png",
	}))

	h := httpapi.NewMeBrandingHandler(repo)
	// Body sends ONLY primary_color — logo_url should survive untouched.
	body := `{"primary_color_hex":"#123456"}`
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": tid}, body)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		PrimaryColorHex string `json:"primary_color_hex"`
		LogoURL         string `json:"logo_url"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.PrimaryColorHex != "#123456" {
		t.Errorf("primary_color_hex not overlaid; got %q", resp.PrimaryColorHex)
	}
	if resp.LogoURL != "https://existing.example.com/old.png" {
		t.Errorf("logo_url should be preserved; got %q", resp.LogoURL)
	}
}

// ---------------------------------------------------------------------------
// Missing tenant header — 401
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_MissingTenantHeader_401(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	h := httpapi.NewMeBrandingHandler(repo)
	w := doBrandingPatch(t, h, nil /*no headers*/, `{"primary_color_hex":"#FF0000"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "gateway_unauthenticated") {
		t.Errorf("expected gateway_unauthenticated error code; body = %s", w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Tenant not found — 404
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_TenantNotFound_404(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo() // empty
	h := httpapi.NewMeBrandingHandler(repo)
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": "missing-tid"},
		`{"primary_color_hex":"#FF0000"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Invalid hex color — 400 from domain layer
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_InvalidHexColor_400(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	const tid = "01970000-0000-7000-8000-000000000003"
	repo.put(seedTenantForBranding(t, tid, tenant.BrandingConfig{}))

	h := httpapi.NewMeBrandingHandler(repo)
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": tid},
		`{"primary_color_hex":"red"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_argument") {
		t.Errorf("expected invalid_argument code; body = %s", w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Invalid logo URL — 400 from domain layer
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_NonHttpLogoURL_400(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	const tid = "01970000-0000-7000-8000-000000000004"
	repo.put(seedTenantForBranding(t, tid, tenant.BrandingConfig{}))

	h := httpapi.NewMeBrandingHandler(repo)
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": tid},
		`{"logo_url":"javascript:alert(1)"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Wrong method — 405
// ---------------------------------------------------------------------------

func TestMeBranding_GET_405(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	h := httpapi.NewMeBrandingHandler(repo)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me/branding", nil)
	r.Header.Set("X-Tenant-Id", "any")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Repo save failure — 500 (defensive — repo is expected to be reliable)
// ---------------------------------------------------------------------------

func TestMeBranding_PATCH_RepoSaveFailure_500(t *testing.T) {
	t.Parallel()
	repo := newFakeBrandingTenantRepo()
	const tid = "01970000-0000-7000-8000-000000000005"
	repo.put(seedTenantForBranding(t, tid, tenant.BrandingConfig{}))
	repo.saveErr = errors.New("simulated pg failure")

	h := httpapi.NewMeBrandingHandler(repo)
	w := doBrandingPatch(t, h, map[string]string{"X-Tenant-Id": tid},
		`{"primary_color_hex":"#ABCDEF"}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Nil repo construction — panics with a clear message
// ---------------------------------------------------------------------------

func TestNewMeBrandingHandler_nilRepo_panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on nil repo")
		}
	}()
	httpapi.NewMeBrandingHandler(nil)
}
