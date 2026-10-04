// familiar_egg_handlers_coverage3_test.go — error-path coverage for the
// webhook handlers + InMemory helpers.
package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	familiareggstripe "github.com/apollo-chora/chora-tenancy/internal/adapter/familiareggstripe"
	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

// partialFailingPurchaseStore looks up successfully but fails on UpdateState.
type partialFailingPurchaseStore struct {
	base *httpapi.InMemoryPurchaseStore
}

func (p *partialFailingPurchaseStore) Insert(ctx context.Context, pp *familiareag.Purchase) error {
	return p.base.Insert(ctx, pp)
}
func (p *partialFailingPurchaseStore) UpdateState(_ context.Context, _ *familiareag.Purchase) error {
	return errors.New("update boom")
}
func (p *partialFailingPurchaseStore) GetByStripeSessionID(ctx context.Context, sid string) (*familiareag.Purchase, bool, error) {
	return p.base.GetByStripeSessionID(ctx, sid)
}

func TestWebhook_CheckoutCompletedUpdateError(t *testing.T) {
	t.Parallel()
	purchases := &partialFailingPurchaseStore{base: httpapi.NewInMemoryPurchaseStore()}
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     purchases,
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	seedPurchase(t, purchases)
	sessID := purchases.base.AllRows()[0].StripeSessionID
	body := []byte(fmt.Sprintf(`{"id":"evt_x","type":"checkout.session.completed","data":{"object":{"id":%q,"payment_intent":"pi","amount_total":999,"currency":"sgd"}}}`, sessID))
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d body=%s", w.Code, w.Body.String())
	}
}

func TestWebhook_AsyncFailedUpdateError(t *testing.T) {
	t.Parallel()
	purchases := &partialFailingPurchaseStore{base: httpapi.NewInMemoryPurchaseStore()}
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     purchases,
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	seedPurchase(t, purchases)
	sessID := purchases.base.AllRows()[0].StripeSessionID
	body := []byte(fmt.Sprintf(`{"id":"evt_y","type":"checkout.session.async_payment_failed","data":{"object":{"id":%q}}}`, sessID))
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d", w.Code)
	}
}

// Webhook handlers when GetByStripeSessionID returns a lookup error.
func TestWebhook_CheckoutCompletedLookupError(t *testing.T) {
	t.Parallel()
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     &failingPurchaseStore{},
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	body := []byte(`{"id":"evt_lookup_err","type":"checkout.session.completed","data":{"object":{"id":"cs_x","payment_intent":"pi","amount_total":999,"currency":"sgd"}}}`)
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d", w.Code)
	}
}

func TestWebhook_AsyncFailedLookupError(t *testing.T) {
	t.Parallel()
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     &failingPurchaseStore{},
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   httpapi.NewInMemoryWebhookIdemStore(),
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	body := []byte(`{"id":"evt_async_lookup_err","type":"checkout.session.async_payment_failed","data":{"object":{"id":"cs_x"}}}`)
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d", w.Code)
	}
}

// Idem store error path.
type failingIdem struct{}

func (failingIdem) MarkSeen(_ context.Context, _ string, _ time.Time) (bool, error) {
	return false, errors.New("redis down")
}

func TestWebhook_IdemStoreError(t *testing.T) {
	t.Parallel()
	mux := httpapi.NewFamiliarEggMux(httpapi.FamiliarEggDeps{
		Purchases:     httpapi.NewInMemoryPurchaseStore(),
		Catalog:       httpapi.NewInMemoryCatalogStore(),
		Stripe:        familiareggstripe.NewStubClient(),
		Publisher:     events.NewRecorder(),
		WebhookIdem:   failingIdem{},
		WebhookSecret: testWebhookSecret,
		Now:           func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	body := []byte(`{"id":"evt_idem","type":"checkout.session.completed","data":{"object":{"id":"cs_x"}}}`)
	sig := stripeSignatureHeader(body, testWebhookSecret, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("got %d", w.Code)
	}
}

// InMemoryPurchaseStore error coverage.
func TestInMemoryPurchaseStore_NilPurchase(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryPurchaseStore()
	if err := s.Insert(context.Background(), nil); err == nil {
		t.Errorf("expected nil error")
	}
	if err := s.UpdateState(context.Background(), nil); err == nil {
		t.Errorf("expected nil error")
	}
}

func TestInMemoryPurchaseStore_DuplicateSession(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryPurchaseStore()
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-1", TenantID: "t-1", PurchaserGCID: "g-1", EggSKU: "egg.standard.v1",
		AmountCents: 999, Currency: "SGD",
		StripeSessionID: "cs_dup", StripeCheckoutURL: "https://x",
		Now: time.Now().UTC(), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	if err := s.Insert(context.Background(), p); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := s.Insert(context.Background(), p); err == nil {
		t.Errorf("expected duplicate error")
	}
}

func TestInMemoryPurchaseStore_UpdateMissing(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryPurchaseStore()
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-not", TenantID: "t", PurchaserGCID: "g", EggSKU: "x",
		AmountCents: 1, Currency: "SGD",
		StripeSessionID: "cs_x", StripeCheckoutURL: "https://x",
		Now: time.Now().UTC(), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	if err := s.UpdateState(context.Background(), p); err == nil {
		t.Errorf("expected not-found error")
	}
}

// InMemoryWebhookIdemStore double-MarkSeen.
func TestInMemoryWebhookIdemStore_DuplicateReturnsTrue(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryWebhookIdemStore()
	seen, err := s.MarkSeen(context.Background(), "evt_x", time.Now())
	if err != nil || seen {
		t.Fatalf("first: seen=%v err=%v", seen, err)
	}
	seen, err = s.MarkSeen(context.Background(), "evt_x", time.Now())
	if err != nil || !seen {
		t.Fatalf("second: seen=%v err=%v", seen, err)
	}
}

// InMemoryCatalogStore error paths.
func TestInMemoryCatalogStore_NilUpsert(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryCatalogStore()
	if err := s.Upsert(context.Background(), nil); err == nil {
		t.Errorf("expected nil error")
	}
}

func TestInMemoryCatalogStore_SoftDeleteMissing(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryCatalogStore()
	if err := s.SoftDelete(context.Background(), testTenantID, "egg.nope.v1"); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

func TestInMemoryCatalogStore_ListExcludesCrossTenant(t *testing.T) {
	t.Parallel()
	s := httpapi.NewInMemoryCatalogStore()
	_ = s.Upsert(context.Background(), &familiareag.CatalogEntry{
		SKU: "egg.t1.v1", TenantID: "t-1",
		DisplayName: "T1", PriceCents: 100, Currency: "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 100},
		Purchasable:       true, SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	list, err := s.ListForTenant(context.Background(), "t-2", time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Should NOT contain egg.t1.v1 (cross-tenant) but should contain seeded platform SKUs.
	for _, e := range list {
		if e.SKU == "egg.t1.v1" {
			t.Errorf("cross-tenant SKU leaked")
		}
	}
}
