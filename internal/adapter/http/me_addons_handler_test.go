// me_addons_handler_test.go — RED-phase tests for the Setup Wizard
// Phase B (CHO-1664) POST /api/v1/tenants/me/addons handler.
//
// Strict TDD per .claude/rules/development-execution.md: this file is
// written BEFORE me_addons_handler.go exists. Compile must fail with
// "undefined: httpapi.NewMeAddOnsHandler" — that's the observed RED.
//
// The handler is a thin shell over the existing
// `addon.SubscriptionRegistry` — it reads X-Tenant-Id (gateway-stamped
// after JWT verify), decodes `{add_on_codes: [...]}`, validates every
// code against the catalogue UP FRONT (so a partial failure can never
// half-subscribe a tenant), then calls `SubscribePending` per code and
// returns the resulting set. Idempotent-additive per OpenAPI
// description.
package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
)

// doAddonsPost is a one-shot helper for POST /api/v1/tenants/me/addons.
func doAddonsPost(t *testing.T, h http.Handler, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/me/addons", strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// newAddonsHandler wires a real seed catalogue + registry so we exercise
// the actual SubscribePending domain path (the only one that creates
// pending_activation rows). Tests assert on the on-disk-shaped JSON
// response and on the registry's resulting state.
func newAddonsHandler(t *testing.T) (http.Handler, *addon.SubscriptionRegistry) {
	t.Helper()
	cat := addon.NewSeedCatalogue()
	reg := addon.NewSubscriptionRegistry(cat)
	// pgRepo deliberately nil — in-memory fallback path is exercised
	// here; the pg-backed re-entry hydration write is covered by
	// integration tests under internal/adapter/pg/.
	h := httpapi.NewMeAddOnsHandler(reg, nil)
	return h, reg
}

// ---------------------------------------------------------------------------
// Happy path — 3 net-new codes create 3 pending_activation rows
// ---------------------------------------------------------------------------

func TestMeAddOns_POST_HappyPath_CreatesPendingActivationRows(t *testing.T) {
	t.Parallel()
	h, reg := newAddonsHandler(t)
	w := doAddonsPost(t, h, map[string]string{
		"X-Tenant-Id": "tenant-acme",
		"gcid":        "owner-1",
	}, `{"add_on_codes":["tms","cms","marketplace"]}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Subscriptions []struct {
			SubscriptionID  string `json:"subscription_id"`
			AddOnCode       string `json:"add_on_code"`
			Status          string `json:"status"`
			NewlySubscribed bool   `json:"newly_subscribed"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if got := len(resp.Subscriptions); got != 3 {
		t.Fatalf("subscriptions length = %d; want 3; body=%s", got, w.Body.String())
	}
	for _, s := range resp.Subscriptions {
		if s.Status != "pending_activation" {
			t.Errorf("subscription %s status = %q; want pending_activation", s.AddOnCode, s.Status)
		}
		if !s.NewlySubscribed {
			t.Errorf("subscription %s newly_subscribed = false; want true (it's a fresh row)", s.AddOnCode)
		}
		if s.SubscriptionID == "" {
			t.Errorf("subscription %s missing subscription_id", s.AddOnCode)
		}
	}
	// Registry actually got the rows.
	if active := reg.ListAllByTenant("tenant-acme"); len(active) != 3 {
		t.Errorf("registry has %d subs; want 3", len(active))
	}
}

// ---------------------------------------------------------------------------
// Idempotent — second call with same codes returns same IDs, newly_subscribed=false
// ---------------------------------------------------------------------------

func TestMeAddOns_POST_Idempotent_ReturnsExistingRowsAsNotNewlySubscribed(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	first := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":["tms"]}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first POST: status = %d; body=%s", first.Code, first.Body.String())
	}
	second := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":["tms"]}`)
	if second.Code != http.StatusOK {
		t.Fatalf("second POST: status = %d; body=%s", second.Code, second.Body.String())
	}
	var resp struct {
		Subscriptions []struct {
			AddOnCode       string `json:"add_on_code"`
			NewlySubscribed bool   `json:"newly_subscribed"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Subscriptions) != 1 {
		t.Fatalf("expected 1 sub on second call, got %d", len(resp.Subscriptions))
	}
	if resp.Subscriptions[0].NewlySubscribed {
		t.Errorf("second call newly_subscribed = true; want false (row already existed)")
	}
}

// ---------------------------------------------------------------------------
// Mix new + existing — newly_subscribed reflects per-code state
// ---------------------------------------------------------------------------

func TestMeAddOns_POST_MixedNewAndExisting_CorrectNewlySubscribedFlags(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	// Seed one.
	_ = doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":["tms"]}`)
	// Re-call with one repeat + one new.
	w := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":["tms","cms"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Subscriptions []struct {
			AddOnCode       string `json:"add_on_code"`
			NewlySubscribed bool   `json:"newly_subscribed"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]bool{}
	for _, s := range resp.Subscriptions {
		got[s.AddOnCode] = s.NewlySubscribed
	}
	if v, ok := got["tms"]; !ok || v {
		t.Errorf("tms.newly_subscribed = %v (present=%v); want false present=true", v, ok)
	}
	if v, ok := got["cms"]; !ok || !v {
		t.Errorf("cms.newly_subscribed = %v (present=%v); want true present=true", v, ok)
	}
}

// ---------------------------------------------------------------------------
// Unknown code — 400 with the offending subset listed
// ---------------------------------------------------------------------------

func TestMeAddOns_POST_UnknownCode_400_NamesUnknownSubset(t *testing.T) {
	t.Parallel()
	h, reg := newAddonsHandler(t)
	w := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":["tms","definitely_not_a_code","cms"]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400; body=%s", w.Code, w.Body.String())
	}
	// Critically — when validation fails, NO subscriptions are created
	// (all-or-nothing per the OpenAPI description).
	if subs := reg.ListAllByTenant("tenant-acme"); len(subs) != 0 {
		t.Errorf("partial failure leaked %d subscriptions; want 0", len(subs))
	}
	// And the error body names the unknown code(s) so the FE can tell the user.
	if !strings.Contains(w.Body.String(), "definitely_not_a_code") {
		t.Errorf("error body does not name the unknown code: %s", w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Missing X-Tenant-Id — 401 (gateway didn't stamp the header)
// ---------------------------------------------------------------------------

func TestMeAddOns_POST_MissingTenantHeader_401(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	w := doAddonsPost(t, h, map[string]string{"gcid": "owner-1"}, `{"add_on_codes":["tms"]}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401; body=%s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Empty body / empty list / malformed JSON — 400
// ---------------------------------------------------------------------------

// CHO-1692 — empty list is no longer 400; REPLACE semantics interpret
// it as "remove all wizard-managed selections for this tenant". The
// handler returns 200 with subscriptions:[] so the FE can render
// "nothing selected".
func TestMeAddOns_POST_EmptyAddOnCodes_200_DeletesAll(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	w := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":[]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (empty = remove all); body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"subscriptions":[]`) {
		t.Errorf("body = %s; want subscriptions:[]", w.Body.String())
	}
}

func TestMeAddOns_POST_MalformedJSON_400(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	w := doAddonsPost(t, h, map[string]string{"X-Tenant-Id": "tenant-acme", "gcid": "owner-1"},
		`{"add_on_codes":`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400; body=%s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Method gate — only POST
// ---------------------------------------------------------------------------

// CHO-1692 — GET is now a valid method on this path (re-entry
// hydration). Returns 200 with `{"subscriptions":[]}` for a fresh
// tenant; the legacy 405 expectation has been replaced.
func TestMeAddOns_GET_FreshTenant_Returns200EmptyArray(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me/addons", nil)
	r.Header.Set("X-Tenant-Id", "tenant-acme")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"subscriptions":[]`) {
		t.Errorf("body = %s; want subscriptions:[] for fresh tenant", w.Body.String())
	}
}

// PATCH (and other non-GET/POST verbs) remain 405.
func TestMeAddOns_PATCH_405(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	r := httptest.NewRequest(http.MethodPatch, "/api/v1/tenants/me/addons", nil)
	r.Header.Set("X-Tenant-Id", "tenant-acme")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want 405; body=%s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Constructor safety
// ---------------------------------------------------------------------------

func TestNewMeAddOnsHandler_nilRegistry_panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil registry")
		}
	}()
	_ = httpapi.NewMeAddOnsHandler(nil, nil)
}

func TestMeAddOns_GET_MissingTenantHeader_401(t *testing.T) {
	t.Parallel()
	h, _ := newAddonsHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me/addons", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401; body=%s", w.Code, w.Body.String())
	}
}

func TestMeAddOns_GET_RegistryFallback_WithRows(t *testing.T) {
	t.Parallel()
	h, reg := newAddonsHandler(t)
	// Subscribe two add-ons so the in-memory registry has rows to hydrate.
	if _, err := reg.Subscribe("tenant-acme", "tms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := reg.Subscribe("tenant-acme", "cms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/me/addons", nil)
	r.Header.Set("X-Tenant-Id", "tenant-acme")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, `"add_on_code":"tms"`) || !strings.Contains(body, `"add_on_code":"cms"`) {
		t.Fatalf("expected both subscriptions in body: %s", body)
	}
}
