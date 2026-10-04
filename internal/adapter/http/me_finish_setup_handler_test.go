// me_finish_setup_handler_test.go — RED-phase tests for the Setup Wizard
// Phase C PATCH /api/v1/tenants/me/finish-setup handler (CHO-1682).
//
// Strict TDD per .claude/rules/development-execution.md: this file is
// written BEFORE me_finish_setup_handler.go exists. Compile must fail
// with "undefined: httpapi.NewMeFinishSetupHandler" — that's the
// observed RED.
//
// Mirrors the me_branding handler test shape: fake `tenant.Repository`
// + Save/Get assertions. Set-once invariant verified via two
// back-to-back calls — the second returns 200 + `newly_completed=false`
// + unchanged timestamp.
package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// fakeFinishTenantRepo is a minimal `tenant.Repository` fake. Only Get +
// Save are exercised by the /me/finish-setup handler.
type fakeFinishTenantRepo struct {
	mu       sync.Mutex
	tenants  map[string]*tenant.Tenant
	saveErr  error
	getErr   error
	saveCalls int
}

func newFakeFinishTenantRepo() *fakeFinishTenantRepo {
	return &fakeFinishTenantRepo{tenants: map[string]*tenant.Tenant{}}
}

func (r *fakeFinishTenantRepo) put(t *tenant.Tenant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants[t.ID] = t
}

func (r *fakeFinishTenantRepo) Get(_ context.Context, id string) (*tenant.Tenant, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, false, r.getErr
	}
	t, ok := r.tenants[id]
	if !ok {
		return nil, false, nil
	}
	clone := *t
	return &clone, true, nil
}

func (r *fakeFinishTenantRepo) Save(_ context.Context, t *tenant.Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saveCalls++
	clone := *t
	r.tenants[t.ID] = &clone
	return nil
}

func (r *fakeFinishTenantRepo) FindBySlug(_ context.Context, _ string) (*tenant.Tenant, bool, error) {
	return nil, false, nil
}

func seedTenantForFinish(t *testing.T, id string) *tenant.Tenant {
	t.Helper()
	tt, err := tenant.NewTenant("Acme", "owner-gcid", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	tt.ID = id
	return tt
}

func doFinishPatch(t *testing.T, h http.Handler, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPatch, "/api/v1/tenants/me/finish-setup", strings.NewReader(""))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// ---------------------------------------------------------------------------
// Happy path — first call stamps + newly_completed=true
// ---------------------------------------------------------------------------

func TestMeFinishSetup_PATCH_HappyPath_StampsAndReturns200(t *testing.T) {
	t.Parallel()
	repo := newFakeFinishTenantRepo()
	tenantID := "01970000-0000-7000-8000-aaaaaaaaaaaa"
	repo.put(seedTenantForFinish(t, tenantID))
	h := httpapi.NewMeFinishSetupHandler(repo)

	w := doFinishPatch(t, h, map[string]string{"X-Tenant-Id": tenantID, "gcid": "g"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		TenantID          string `json:"tenant_id"`
		WizardCompletedAt string `json:"wizard_completed_at"`
		NewlyCompleted    bool   `json:"newly_completed"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v; body=%s", err, w.Body.String())
	}
	if resp.TenantID != tenantID {
		t.Errorf("tenant_id = %q; want %q", resp.TenantID, tenantID)
	}
	if !resp.NewlyCompleted {
		t.Error("newly_completed should be true on first call")
	}
	if resp.WizardCompletedAt == "" {
		t.Error("wizard_completed_at should be stamped")
	}
	// Save was called.
	if repo.saveCalls != 1 {
		t.Errorf("Save calls = %d; want 1", repo.saveCalls)
	}
}

// ---------------------------------------------------------------------------
// Idempotent re-finish — newly_completed=false, NO new Save, same timestamp
// ---------------------------------------------------------------------------

func TestMeFinishSetup_PATCH_IdempotentReFinish_NewlyCompletedFalse(t *testing.T) {
	t.Parallel()
	repo := newFakeFinishTenantRepo()
	tenantID := "01970000-0000-7000-8000-bbbbbbbbbbbb"
	tt := seedTenantForFinish(t, tenantID)
	// Pre-stamp the wizard as already complete.
	stamped := time.Now().UTC().Add(-1 * time.Hour)
	tt.WizardCompletedAt = &stamped
	repo.put(tt)
	h := httpapi.NewMeFinishSetupHandler(repo)

	w := doFinishPatch(t, h, map[string]string{"X-Tenant-Id": tenantID, "gcid": "g"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		WizardCompletedAt string `json:"wizard_completed_at"`
		NewlyCompleted    bool   `json:"newly_completed"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NewlyCompleted {
		t.Error("newly_completed should be FALSE on re-finish (set-once)")
	}
	if !strings.Contains(resp.WizardCompletedAt, "T") {
		t.Errorf("wizard_completed_at not RFC3339-shaped: %q", resp.WizardCompletedAt)
	}
	// Save MUST NOT be re-called — re-finish is a true no-op so the
	// outbox publisher upstream of Save can't accidentally re-emit.
	if repo.saveCalls != 0 {
		t.Errorf("Save calls = %d; want 0 on idempotent re-finish", repo.saveCalls)
	}
}

// ---------------------------------------------------------------------------
// 401 — missing X-Tenant-Id
// ---------------------------------------------------------------------------

func TestMeFinishSetup_PATCH_MissingTenantHeader_401(t *testing.T) {
	t.Parallel()
	repo := newFakeFinishTenantRepo()
	h := httpapi.NewMeFinishSetupHandler(repo)
	w := doFinishPatch(t, h, map[string]string{"gcid": "g"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401", w.Code)
	}
}

// ---------------------------------------------------------------------------
// 404 — tenant not found
// ---------------------------------------------------------------------------

func TestMeFinishSetup_PATCH_TenantNotFound_404(t *testing.T) {
	t.Parallel()
	repo := newFakeFinishTenantRepo()
	h := httpapi.NewMeFinishSetupHandler(repo)
	w := doFinishPatch(t, h, map[string]string{"X-Tenant-Id": "missing-tenant", "gcid": "g"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// 405 — method gate
// ---------------------------------------------------------------------------

func TestMeFinishSetup_GET_405(t *testing.T) {
	t.Parallel()
	repo := newFakeFinishTenantRepo()
	h := httpapi.NewMeFinishSetupHandler(repo)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me/finish-setup", nil)
	r.Header.Set("X-Tenant-Id", "any")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want 405", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Constructor safety
// ---------------------------------------------------------------------------

func TestNewMeFinishSetupHandler_NilRepo_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil repo")
		}
	}()
	_ = httpapi.NewMeFinishSetupHandler(nil)
}
