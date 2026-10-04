// familiar_egg_handlers_test.go — HTTP-level tests for the Iter G.3
// Familiar Egg endpoints (ADR-149).
package httpapi_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareggstripe "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiareggstripe"
	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	familiareggpub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pubsub"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

// fakeEggCheckoutClient is the test double for the chora-payments delegate
// (ADR-164). It records the last input + call count and returns a fixed
// EggCheckoutOutput. Set err to force the checkout delegate to fail.
type fakeEggCheckoutClient struct {
	lastInput httpapi.EggCheckoutInput
	calls     int
	err       error
}

func (f *fakeEggCheckoutClient) CreateFamiliarEggCheckoutSession(_ context.Context, in httpapi.EggCheckoutInput) (httpapi.EggCheckoutOutput, error) {
	f.calls++
	f.lastInput = in
	if f.err != nil {
		return httpapi.EggCheckoutOutput{}, f.err
	}
	return httpapi.EggCheckoutOutput{
		PurchaseID:        "pur_test",
		StripeSessionID:   "cs_test",
		StripeCheckoutURL: "https://stripe.test/checkout/cs_test",
		State:             "checkout_started",
	}, nil
}

const (
	testTenantID      = "01970000-0000-7000-8000-000000000010"
	testGCID          = "01970000-0000-7000-8000-000000000020"
	testWebhookSecret = "whsec_test_familiar_egg_g3"
)

func newTestMux(t *testing.T) (http.Handler, *httpapi.InMemoryPurchaseStore, *httpapi.InMemoryCatalogStore, *events.Recorder, *familiareggstripe.StubClient, *fakeEggCheckoutClient) {
	t.Helper()
	purchases := httpapi.NewInMemoryPurchaseStore()
	catalog := httpapi.NewInMemoryCatalogStore()
	stripe := familiareggstripe.NewStubClient()
	rec := events.NewRecorder()
	payments := &fakeEggCheckoutClient{}
	deps := httpapi.FamiliarEggDeps{
		Purchases:     purchases,
		Catalog:       catalog,
		Stripe:        stripe,
		Publisher:     rec,
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		Payments:      payments,
		WebhookSecret: testWebhookSecret,
		SuccessURL:    "https://example.test/ok",
		CancelURL:     "https://example.test/cancel",
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
		SignatureSkew: 5 * time.Minute,
	}
	return httpapi.NewFamiliarEggMux(deps), purchases, catalog, rec, stripe, payments
}

// -----------------------------------------------------------------------------
// POST /api/familiar-eggs/checkout
// -----------------------------------------------------------------------------

func TestCheckout_HappyPath(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, fake := newTestMux(t)

	body := strings.NewReader(`{"egg_sku":"egg.standard.v1","return_url":"https://app.test/post-checkout"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		PurchaseID        string `json:"purchase_id"`
		StripeCheckoutURL string `json:"stripe_checkout_url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Response mirrors the chora-payments delegate result (ADR-164).
	if resp.PurchaseID != "pur_test" {
		t.Errorf("purchase_id: got %q want pur_test", resp.PurchaseID)
	}
	if resp.StripeCheckoutURL != "https://stripe.test/checkout/cs_test" {
		t.Errorf("stripe_checkout_url: got %q", resp.StripeCheckoutURL)
	}
	// chora-payments delegate called exactly once with the mapped input.
	if fake.calls != 1 {
		t.Fatalf("payments calls: got %d want 1", fake.calls)
	}
	if fake.lastInput.EggSKU != "egg.standard.v1" {
		t.Errorf("egg_sku: got %q", fake.lastInput.EggSKU)
	}
	if fake.lastInput.TenantID != testTenantID {
		t.Errorf("tenant_id: got %q", fake.lastInput.TenantID)
	}
	if fake.lastInput.LearnerGCID != testGCID {
		t.Errorf("learner_gcid: got %q", fake.lastInput.LearnerGCID)
	}
	// return_url overrides the env SuccessURL.
	if fake.lastInput.SuccessURL != "https://app.test/post-checkout" {
		t.Errorf("success_url override: got %q", fake.lastInput.SuccessURL)
	}
}

func TestCheckout_MissingHeaders(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	// no X-Tenant-Id
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.standard.v1"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 missing tenant; got %d", w.Code)
	}
	// only X-Tenant-Id, no X-GCID
	req2 := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.standard.v1"}`))
	req2.Header.Set("X-Tenant-Id", testTenantID)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 missing gcid; got %d", w2.Code)
	}
}

func TestCheckout_MissingSKU(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422 missing sku; got %d", w.Code)
	}
}

func TestCheckout_SKUNotFound(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.unknown.v1"}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404; got %d", w.Code)
	}
}

func TestCheckout_NonPurchasableSKU(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	// egg.trial.v1 has Purchasable=false in the seed inmem catalog.
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.trial.v1"}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403; got %d body=%s", w.Code, w.Body.String())
	}
}

// CHO-2035: a purchasable SKU whose breed_distribution cannot hatch (empty /
// malformed) must be rejected at CHECKOUT with an explicit 4xx — never allowed
// to create a purchase that provisions an egg that then 500s at hatch (egg SKU
// can't roll a species), leaving a permanently-stuck egg on a roster slot. The
// admin upsert path enforces distribution via CatalogEntry.Validate, but a
// legacy / DB-direct row bypasses it; the checkout re-check catches those.
func TestCheckout_RejectsUnhatchableDistribution(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, payments := newTestMux(t)

	// Inject a legacy-shaped row directly (bypassing CatalogEntry.Validate):
	// purchasable + available, but an EMPTY breed_distribution — unhatchable.
	if err := catalog.Upsert(context.Background(), &familiareag.CatalogEntry{
		SKU:               "egg.legacy-broken.v1",
		DisplayName:       "Legacy Broken",
		PriceCents:        999,
		Currency:          "SGD",
		BreedDistribution: familiareag.BreedDistribution{}, // empty ⇒ cannot hatch
		Purchasable:       true,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	}); err != nil {
		t.Fatalf("seed malformed SKU: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.legacy-broken.v1"}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for unhatchable SKU; got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "sku_invalid_distribution") {
		t.Errorf("expected explicit sku_invalid_distribution code; got %s", w.Body.String())
	}
	// Fail loud BEFORE money moves — no chora-payments checkout session created.
	if payments.calls != 0 {
		t.Errorf("payments delegate must NOT be called for an unhatchable SKU; calls=%d", payments.calls)
	}
}

// -----------------------------------------------------------------------------
// GET /api/familiar-eggs/catalog
// -----------------------------------------------------------------------------

func TestCatalogList_ReturnsAllPlatformSKUs(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/catalog", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 3 {
		t.Errorf("expected 3 seeded platform SKUs; got %d", resp.Total)
	}
}

func TestCatalogList_MissingTenant(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/catalog", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400; got %d", w.Code)
	}
}

// -----------------------------------------------------------------------------
// GET /api/familiar-eggs/{sku}/odds
// -----------------------------------------------------------------------------

func TestOdds_ReturnsSortedList(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/egg.standard.v1/odds", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		SKU                   string  `json:"sku"`
		Odds                  []any   `json:"odds"`
		TotalWeight           float64 `json:"total_weight"`
		DistributionUpdatedAt string  `json:"distribution_updated_at"`
		IMDADimension         string  `json:"chora_imda_dimension"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SKU != "egg.standard.v1" {
		t.Errorf("sku: got %q", resp.SKU)
	}
	if len(resp.Odds) != 5 {
		t.Errorf("expected 5 odds rows; got %d", len(resp.Odds))
	}
	if resp.TotalWeight < 99.99 || resp.TotalWeight > 100.01 {
		t.Errorf("total_weight: got %v", resp.TotalWeight)
	}
	if resp.IMDADimension != "transparency" {
		t.Errorf("imda dimension tag missing")
	}
}

func TestOdds_UnknownSKU(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/egg.unknown.v1/odds", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404; got %d", w.Code)
	}
}

// -----------------------------------------------------------------------------
// POST /api/admin/familiar-eggs/catalog
// -----------------------------------------------------------------------------

func TestAdminCatalog_RejectsNonAdmin(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.test.v1","display_name":"Test","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 missing role; got %d", w.Code)
	}
}

// CHO-2075: the familiar-egg admin catalog gate must authorize on the
// gateway-stamped, non-spoofable x-mesh-user-roles mesh header — exactly like
// every other H+ admin write gate after CHO-2072 (admin_authz.go) — NOT the
// legacy X-Chora-Role / X-Role header (which the gateway only stamps on the H+
// tx-history routes, and which a client could otherwise supply). A legitimate
// admin whose request carries x-mesh-user-roles (as the gateway always stamps
// on every proxied route) passes.
func TestAdminCatalog_AuthorizesOnMeshUserRoles(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.mesh.v1","display_name":"Mesh","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for mesh-stamped tenant_admin; got %d body=%s", w.Code, w.Body.String())
	}
	if _, ok, _ := catalog.GetBySKU(context.Background(), testTenantID, "egg.mesh.v1"); !ok {
		t.Error("SKU was not persisted — the mesh-stamped admin should have been authorized")
	}
}

// CHO-2075 (security): the gate must NOT trust the client-suppliable
// X-Chora-Role / X-Role header. A request carrying only the legacy header and
// no gateway-stamped x-mesh-user-roles must be denied fail-closed.
func TestAdminCatalog_RejectsLegacyChoraRoleHeader(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.spoof.v1","display_name":"Spoof","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-Chora-Role", "super_admin") // legacy header ONLY — must not authorize
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for legacy X-Chora-Role only (no mesh header); got %d body=%s", w.Code, w.Body.String())
	}
	if _, ok, _ := catalog.GetBySKU(context.Background(), testTenantID, "egg.spoof.v1"); ok {
		t.Error("SKU was persisted via a legacy-header request — the gate trusted a non-mesh header")
	}
}

func TestAdminCatalog_RejectsBreedDistributionNot100(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.bad.v1","display_name":"Bad","price_cents":500,"currency":"SGD","breed_distribution":{"owl":40,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400; got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "sum to") {
		t.Errorf("expected sum error msg; got %s", w.Body.String())
	}
}

func TestAdminCatalog_RejectsUnknownSpecies(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.bad2.v1","display_name":"Bad2","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"unicorn":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400; got %d", w.Code)
	}
}

func TestAdminCatalog_HappyPath(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.skillflow.v1","display_name":"SkillFlow Egg","description":"branded","price_cents":1499,"currency":"SGD","breed_distribution":{"owl":30,"fox":30,"penguin":20,"dragon":15,"phoenix":5},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", w.Code, w.Body.String())
	}
	// Verify via store fetch.
	got, ok, err := catalog.GetBySKU(context.Background(), testTenantID, "egg.skillflow.v1")
	if err != nil || !ok || got == nil {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if got.PriceCents != 1499 {
		t.Errorf("price: got %d", got.PriceCents)
	}
}

func TestAdminCatalog_TrialMustBeFree(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.bogus-trial.v1","display_name":"Bogus Trial","price_cents":500,"currency":"SGD","is_trial":true,"breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 trial-not-free; got %d", w.Code)
	}
}

func TestAdminCatalog_SoftDelete(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	// Create.
	body := strings.NewReader(`{"sku":"egg.tempo.v1","display_name":"Tempo","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create status: %d", w.Code)
	}
	// Delete.
	req2 := httptest.NewRequest(http.MethodDelete, "/api/admin/familiar-eggs/catalog/egg.tempo.v1", nil)
	req2.Header.Set("X-Tenant-Id", testTenantID)
	req2.Header.Set("x-mesh-user-roles", "tenant_admin")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("delete status: %d", w2.Code)
	}
	_, ok, _ := catalog.GetBySKU(context.Background(), testTenantID, "egg.tempo.v1")
	if ok {
		t.Errorf("expected SKU gone after soft-delete")
	}
}

// -----------------------------------------------------------------------------
// POST /webhooks/stripe — signature + happy path + failure path + idempotency
// -----------------------------------------------------------------------------

func TestWebhook_RejectsMissingSignature(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := bytes.NewReader([]byte(`{"id":"evt_x","type":"checkout.session.completed"}`))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", body)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401; got %d", w.Code)
	}
}

func TestWebhook_RejectsBadSignature(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_x","type":"checkout.session.completed"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "t=1747137600,v1=deadbeef")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401; got %d", w.Code)
	}
}

func TestWebhook_CheckoutCompletedHappyPath(t *testing.T) {
	t.Parallel()
	mux, purchases, _, rec, _, _ := newTestMux(t)
	// First create a purchase via checkout so the session row exists.
	seedPurchase(t, purchases)
	sessID := purchases.AllRows()[0].StripeSessionID

	body := []byte(fmt.Sprintf(`{
        "id":"evt_paid_1",
        "type":"checkout.session.completed",
        "data":{"object":{"id":%q,"payment_intent":"pi_x","amount_total":999,"currency":"sgd"}}
    }`, sessID))
	postWebhook(t, mux, body, 200)

	row, ok, _ := purchases.GetByStripeSessionID(context.Background(), sessID)
	if !ok || row.State != "paid" {
		t.Errorf("expected paid; got %+v", row)
	}
	if len(rec.RecordedByTopic(familiareggpub.TopicPaymentSucceeded)) != 1 {
		t.Errorf("payment_succeeded event missing")
	}
}

func TestWebhook_ChargeFailedTransitionsToPaymentFailed(t *testing.T) {
	t.Parallel()
	mux, purchases, _, rec, _, _ := newTestMux(t)
	seedPurchase(t, purchases)
	sessID := purchases.AllRows()[0].StripeSessionID

	body := []byte(fmt.Sprintf(`{
        "id":"evt_fail_1",
        "type":"checkout.session.async_payment_failed",
        "data":{"object":{"id":%q,"payment_intent":"pi_x"}}
    }`, sessID))
	postWebhook(t, mux, body, 200)
	row, _, _ := purchases.GetByStripeSessionID(context.Background(), sessID)
	if row.State != "payment_failed" {
		t.Errorf("expected payment_failed; got %q", row.State)
	}
	if len(rec.RecordedByTopic(familiareggpub.TopicPaymentFailed)) != 1 {
		t.Errorf("payment_failed event missing")
	}
}

func TestWebhook_DuplicateEventIsIdempotent(t *testing.T) {
	t.Parallel()
	mux, purchases, _, rec, _, _ := newTestMux(t)
	seedPurchase(t, purchases)
	sessID := purchases.AllRows()[0].StripeSessionID

	body := []byte(fmt.Sprintf(`{
        "id":"evt_dup_1",
        "type":"checkout.session.completed",
        "data":{"object":{"id":%q,"payment_intent":"pi_x","amount_total":999,"currency":"sgd"}}
    }`, sessID))
	postWebhook(t, mux, body, 200)
	// Redeliver same event.
	w := postWebhook(t, mux, body, 200)
	var resp struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != "duplicate" {
		t.Errorf("expected duplicate; got %q body=%s", resp.Status, w.Body.String())
	}
	// Only one event emitted despite 2 webhook calls.
	if got := len(rec.RecordedByTopic(familiareggpub.TopicPaymentSucceeded)); got != 1 {
		t.Errorf("expected 1 paid event despite duplicate webhook; got %d", got)
	}
}

func TestWebhook_OrphanSessionReturns202(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{
        "id":"evt_orphan_1",
        "type":"checkout.session.completed",
        "data":{"object":{"id":"cs_unknown","payment_intent":"pi_x","amount_total":999,"currency":"sgd"}}
    }`)
	w := postWebhook(t, mux, body, http.StatusAccepted)
	if !strings.Contains(w.Body.String(), "orphan_session") {
		t.Errorf("expected orphan_session; got %s", w.Body.String())
	}
}

func TestWebhook_IgnoredEventTypeReturns202(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_other","type":"invoice.payment_succeeded","data":{"object":{}}}`)
	postWebhook(t, mux, body, http.StatusAccepted)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// seedPurchase inserts a checkout_started Purchase directly into the local
// store so the (legacy) webhook handler has a row to transition. Checkout no
// longer persists locally (ADR-164 moved that to chora-payments), so the
// webhook tests seed via the PurchaseStore port instead of POSTing /checkout.
// Returns the seeded StripeSessionID.
func seedPurchase(t *testing.T, purchases httpapi.PurchaseStore) string {
	t.Helper()
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "pur_seed_1",
		TenantID:          testTenantID,
		PurchaserGCID:     testGCID,
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "sgd",
		StripeSessionID:   "cs_seed_1",
		StripeCheckoutURL: "https://stripe.test/seed",
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
		Now:               time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
	})
	if err := purchases.Insert(context.Background(), p); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	return p.StripeSessionID
}

func postWebhook(t *testing.T, mux http.Handler, body []byte, wantStatus int) *httptest.ResponseRecorder {
	t.Helper()
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != wantStatus {
		t.Errorf("webhook status: got %d want %d body=%s", w.Code, wantStatus, w.Body.String())
	}
	return w
}

// stripeSignatureHeader builds a t=...,v1=... Stripe-Signature header
// matching the test fixture clock.
func stripeSignatureHeader(payload []byte, secret string, now time.Time) string {
	ts := now.Unix()
	signed := fmt.Sprintf("%d.%s", ts, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signed))
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}
