// familiar_egg_handlers_coverage_test.go — additional Iter G.3 HTTP
// coverage cases for the 85% domain gate.
package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareggstripe "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiareggstripe"
	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

func TestCheckout_RejectsWrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/checkout", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestCheckout_InvalidBody(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`not json`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestCheckout_StripeError(t *testing.T) {
	t.Parallel()
	// The chora-payments delegate (ADR-164) returns an error — the handler
	// surfaces it as 502 payments_checkout_failed. (Local Stripe + local
	// persistence were removed from the checkout path.)
	payments := &fakeEggCheckoutClient{err: errors.New("payments down")}
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     httpapi.NewInMemoryPurchaseStore(),
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		Payments:      payments,
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Now() },
	})
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.standard.v1"}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "payments_checkout_failed") {
		t.Errorf("expected payments_checkout_failed code; got %s", w.Body.String())
	}
}

func TestCheckout_CatalogLookupError(t *testing.T) {
	t.Parallel()
	purchases := httpapi.NewInMemoryPurchaseStore()
	catalog := &failingCatalog{getErr: errors.New("db down")}
	stripe := familiareggstripe.NewStubClient()
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases: purchases, Catalog: catalog, Stripe: stripe, Publisher: events.NewRecorder(),
		WebhookIdem: httpapi.NewInMemoryWebhookIdemStore(), WebhookSecret: testWebhookSecret,
		Now: func() time.Time { return time.Now() },
	})
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", strings.NewReader(`{"egg_sku":"egg.x.v1"}`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d", w.Code)
	}
}

func TestCheckout_CrossTenantSKURejected(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	otherTenant := "01970000-0000-7000-8000-deadbeef0001"
	now := time.Now().UTC()
	_ = catalog.Upsert(context.Background(), &familiareag.CatalogEntry{
		SKU: "egg.other.v1", TenantID: otherTenant,
		DisplayName: "Other", PriceCents: 100, Currency: "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 50, "fox": 50},
		Purchasable:       true,
		SoftExpiryDays:    30, HardExpiryDays: 60, CreatedAt: now, UpdatedAt: now,
	})
	body := strings.NewReader(`{"egg_sku":"egg.other.v1"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	// 404, not 403: see TestOdds_CrossTenantForbidden. The catalog lookup is
	// tenant-scoped, so another tenant's SKU does not resolve at all and
	// checkout never reaches the cross-tenant branch (which stays as
	// defence-in-depth).
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for another tenant's SKU; got %d body=%s", w.Code, w.Body.String())
	}
}

func TestCheckout_OutsideAvailabilityWindow(t *testing.T) {
	t.Parallel()
	mux, _, catalog, _, _, _ := newTestMux(t)
	now := time.Now().UTC()
	past := now.Add(-2 * 24 * time.Hour)
	yesterday := now.Add(-24 * time.Hour)
	_ = catalog.Upsert(context.Background(), &familiareag.CatalogEntry{
		SKU: "egg.season.v1", DisplayName: "Seasonal", PriceCents: 100, Currency: "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 50, "fox": 50},
		Purchasable: true, SoftExpiryDays: 30, HardExpiryDays: 60,
		AvailableFrom: &past, AvailableUntil: &yesterday, CreatedAt: now, UpdatedAt: now,
	})
	body := strings.NewReader(`{"egg_sku":"egg.season.v1"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/checkout", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("X-GCID", testGCID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 unavailable; got %d", w.Code)
	}
}

func TestCatalogList_WrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/catalog", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestOdds_MissingTenant(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/egg.standard.v1/odds", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestOdds_WrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/familiar-eggs/egg.standard.v1/odds", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestEggBySKU_UnknownSubpath(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/egg.standard.v1/unknown", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("got %d", w.Code)
	}
}

func TestEggBySKU_BadPathReturns404(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/familiar-eggs/onlysku", nil)
	req.Header.Set("X-Tenant-Id", testTenantID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalog_WrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/familiar-eggs/catalog", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalog_InvalidBody(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", strings.NewReader(`not json`))
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalog_MissingTenant(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", strings.NewReader(`{}`))
	req.Header.Set("x-mesh-user-roles", "tenant_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalog_SuperAdminRoleAllowed(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := strings.NewReader(`{"sku":"egg.super.v1","display_name":"Super","price_cents":500,"currency":"SGD","breed_distribution":{"owl":50,"fox":50},"soft_expiry_days":30,"hard_expiry_days":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/familiar-eggs/catalog", body)
	req.Header.Set("X-Tenant-Id", testTenantID)
	req.Header.Set("x-mesh-user-roles", "super_admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got %d body=%s", w.Code, w.Body.String())
	}
}

func TestAdminCatalogDelete_WrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	// GET is a supported verb on this path now (the admin single-entry read the
	// H+ editor round-trips through), so probe the gate with a verb that is not.
	req := httptest.NewRequest(http.MethodPut, "/api/admin/familiar-eggs/catalog/egg.x.v1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalogDelete_MissingSKU(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/admin/familiar-eggs/catalog/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("got %d", w.Code)
	}
}

func TestAdminCatalogDelete_ForbiddenWithoutRole(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/admin/familiar-eggs/catalog/egg.x.v1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("got %d", w.Code)
	}
}

func TestWebhook_WrongMethod(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	req := httptest.NewRequest(http.MethodGet, "/webhooks/stripe", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got %d", w.Code)
	}
}

func TestWebhook_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`not json`)
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestWebhook_CheckoutCompletedMissingSession(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_miss","type":"checkout.session.completed","data":{"object":{"amount_total":999,"currency":"sgd"}}}`)
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d", w.Code)
	}
}

func TestWebhook_ChargeRefundedOrphan(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_re_orphan","type":"charge.refunded","data":{"object":{"id":"ch_x","amount_refunded":999,"refunds":{"data":[{"id":"re_x"}]}}}}`)
	postWebhook(t, mux, body, http.StatusAccepted)
}

func TestWebhook_ChargeRefundedWithMetadata(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_re_meta","type":"charge.refunded","data":{"object":{"id":"ch_x","amount_refunded":999,"metadata":{"chora_purchase_id":"p-x"},"refunds":{"data":[{"id":"re_x"}]}}}}`)
	w := postWebhook(t, mux, body, http.StatusAccepted)
	if !strings.Contains(w.Body.String(), "refund_ack") {
		t.Errorf("expected refund_ack; got %s", w.Body.String())
	}
}

func TestWebhook_ChargeFailedUncorrelated(t *testing.T) {
	t.Parallel()
	mux, _, _, _, _, _ := newTestMux(t)
	body := []byte(`{"id":"evt_chfail","type":"charge.failed","data":{"object":{"id":"ch_x","failure_code":"card_declined"}}}`)
	w := postWebhook(t, mux, body, http.StatusAccepted)
	if !strings.Contains(w.Body.String(), "uncorrelated_failure") {
		t.Errorf("expected uncorrelated_failure; got %s", w.Body.String())
	}
}

// -----------------------------------------------------------------------------
// failing-port helpers
// -----------------------------------------------------------------------------

type failingPurchaseStore struct{}

func (*failingPurchaseStore) Insert(context.Context, *familiareag.Purchase) error {
	return errors.New("boom")
}
func (*failingPurchaseStore) UpdateState(context.Context, *familiareag.Purchase) error {
	return errors.New("boom")
}
func (*failingPurchaseStore) GetByStripeSessionID(context.Context, string) (*familiareag.Purchase, bool, error) {
	return nil, false, errors.New("boom")
}

type failingCatalog struct {
	getErr error
}

func (f *failingCatalog) GetBySKU(_ context.Context, _, _ string) (*familiareag.CatalogEntry, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	return nil, false, nil
}
func (*failingCatalog) ListForTenant(context.Context, string, time.Time) ([]*familiareag.CatalogEntry, error) {
	return nil, errors.New("boom")
}
func (*failingCatalog) Upsert(context.Context, *familiareag.CatalogEntry) error {
	return errors.New("boom")
}
func (*failingCatalog) SoftDelete(context.Context, string, string) error {
	return errors.New("boom")
}
