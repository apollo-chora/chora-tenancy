// me_tenant_handler_test.go — unit tests for GET /api/v1/tenants/me
// (CHO-1692 wizard re-entry hydration). Verifies the X-Tenant-Id gate,
// the tenant-lookup failure modes and the identity/branding projection
// with and without the optional Stripe / activation / wizard fields.
package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

// fakeTenantRepo records lookups and returns scripted results.
type fakeTenantRepo struct {
	byID map[string]*tenant.Tenant
	err  error
}

func newFakeTenantRepo() *fakeTenantRepo {
	return &fakeTenantRepo{byID: map[string]*tenant.Tenant{}}
}

func (f *fakeTenantRepo) Save(context.Context, *tenant.Tenant) error { return nil }
func (f *fakeTenantRepo) Get(_ context.Context, id string) (*tenant.Tenant, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	t, ok := f.byID[id]
	return t, ok, nil
}
func (f *fakeTenantRepo) FindBySlug(context.Context, string) (*tenant.Tenant, bool, error) {
	return nil, false, nil
}

func newMeTenantServer(t *testing.T, repo tenant.Repository) http.Handler {
	t.Helper()
	h := httpapi.NewMeTenantHandler(repo)
	mux := http.NewServeMux()
	mux.Handle("/api/v1/tenants/me", h)
	return mux
}

func fullTenant() *tenant.Tenant {
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	act := now.Add(-24 * time.Hour)
	wiz := now.Add(-12 * time.Hour)
	return &tenant.Tenant{
		ID:                "t-1",
		DisplayName:       "Phyllis Coaching",
		OwnerGCID:         "g-owner",
		Slug:              "phyllis-coaching",
		Country:           "SG",
		DefaultCurrency:   "SGD",
		ParentTenantID:    "00000000-0000-7000-8000-000000000001",
		SelfHosted:        false,
		Status:            tenant.StatusActive,
		StripeCustomerID:  "cus_123",
		Branding:          tenant.BrandingConfig{PrimaryColorHex: "#ff00aa", LogoURL: "https://x/logo.png", CustomDomain: "coaching.example"},
		ActivatedAt:       &act,
		WizardCompletedAt: &wiz,
		CreatedAt:         now.Add(-48 * time.Hour),
		UpdatedAt:         now,
	}
}

func TestMeTenantHandler_HappyPath_FullProjection(t *testing.T) {
	t.Parallel()
	repo := newFakeTenantRepo()
	repo.byID["t-1"] = fullTenant()
	srv := newMeTenantServer(t, repo)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me", nil)
	req.Header.Set("X-Tenant-Id", "t-1")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		`"id":"t-1"`, `"display_name":"Phyllis Coaching"`, `"owner_gcid":"g-owner"`,
		`"slug":"phyllis-coaching"`, `"country":"SG"`, `"default_currency":"SGD"`,
		`"parent_tenant_id":"00000000-0000-7000-8000-000000000001"`,
		`"status":"active"`, `"stripe_customer_id":"cus_123"`,
		`"primary_color_hex":"#ff00aa"`, `"logo_url":"https://x/logo.png"`,
		`"custom_domain":"coaching.example"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}
	// Optional timestamps present.
	for _, key := range []string{`"activated_at"`, `"wizard_completed_at"`} {
		if !strings.Contains(body, key) {
			t.Fatalf("response missing %q: %s", key, body)
		}
	}
}

func TestMeTenantHandler_MinimalTenant_OmitsOptionals(t *testing.T) {
	t.Parallel()
	repo := newFakeTenantRepo()
	now := time.Now().UTC()
	repo.byID["t-min"] = &tenant.Tenant{
		ID: "t-min", DisplayName: "Min", OwnerGCID: "g", Slug: "min",
		Branding:  tenant.BrandingConfig{},
		CreatedAt: now, UpdatedAt: now,
	}
	srv := newMeTenantServer(t, repo)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me", nil)
	req.Header.Set("X-Tenant-Id", "t-min")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	for _, key := range []string{`"stripe_customer_id"`, `"activated_at"`, `"wizard_completed_at"`} {
		if strings.Contains(rec.Body.String(), key) {
			t.Fatalf("expected %q omitted, body=%s", key, rec.Body.String())
		}
	}
}

func TestMeTenantHandler_Errors(t *testing.T) {
	t.Parallel()
	repo := newFakeTenantRepo()
	srv := newMeTenantServer(t, repo)
	// Non-GET → 405.
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/tenants/me", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	// Missing tenant header → 401.
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	// Unknown tenant → 404.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me", nil)
	req.Header.Set("X-Tenant-Id", "missing-tenant")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	// Repo error → 500.
	repo2 := newFakeTenantRepo()
	repo2.err = errors.New("db down")
	srv2 := newMeTenantServer(t, repo2)
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me", nil)
	req2.Header.Set("X-Tenant-Id", "t-1")
	rec2 := httptest.NewRecorder()
	srv2.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec2.Code)
	}
}

func TestNewMeTenantHandler_NilRepoPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil repo")
		}
	}()
	httpapi.NewMeTenantHandler(nil)
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
