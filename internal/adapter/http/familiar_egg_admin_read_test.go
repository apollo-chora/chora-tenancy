package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// D6. The H+ pod-catalogue editor saves through the FULL-entry upsert, so it
// must first READ the full entry. Neither existing read serves that:
//   - the catalog LIST filters on the availability window, so a SKU outside its
//     dates is invisible to the admin who needs to edit it;
//   - /api/familiar-eggs/{sku} only serves /odds.
// Without this route the editor would have to reconstruct an entry it never
// read, and every field it did not know about would be wiped on save.
func adminCreate(t *testing.T, mux http.Handler, body string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", strings.NewReader(body))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("seed create status %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminCatalogGet_ReturnsEveryFieldTheUpsertConsumes(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	adminCreate(t, mux, `{"sku":"egg.roundtrip.v1","display_name":"Round Trip","description":"desc",`+
		`"price_cents":700,"currency":"SGD","breed_distribution":{"owl":60,"penguin":40},`+
		`"is_trial":false,"soft_expiry_days":30,"hard_expiry_days":60}`)

	req := httptest.NewRequest(http.MethodGet, "/api/admin/familiar-eggs/catalog/egg.roundtrip.v1", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("got %d body=%s", w.Code, w.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Every field adminCatalogReq consumes must come back, or a read-modify-write
	// round-trip silently drops it.
	for _, k := range []string{
		"sku", "display_name", "description", "price_cents", "currency",
		"suggested_focal_atom_id", "breed_distribution", "purchasable",
		"is_trial", "soft_expiry_days", "hard_expiry_days",
	} {
		if _, ok := got[k]; !ok {
			t.Errorf("field %q missing — a save would wipe it", k)
		}
	}
	if got["display_name"] != "Round Trip" {
		t.Errorf("display_name = %v", got["display_name"])
	}
	dist, ok := got["breed_distribution"].(map[string]any)
	if !ok || dist["penguin"] == nil {
		t.Errorf("breed_distribution did not survive: %v", got["breed_distribution"])
	}
}

func TestAdminCatalogGet_ForbiddenWithoutRole(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/familiar-eggs/catalog/egg.x.v1", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("got %d, want 403 — the admin read is role-gated like its siblings", w.Code)
	}
}

func TestAdminCatalogGet_UnknownSKUIsNotFound(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/familiar-eggs/catalog/egg.nope.v1", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("got %d, want 404 — never fabricate an entry the editor would then save", w.Code)
	}
}
