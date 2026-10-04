// familiar_egg_coverage_test.go — bumps coverage on the domain
// validation + transition edge cases.
package familiar_egg_test

import (
	"testing"
	"time"

	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

func TestBreedDistribution_Total(t *testing.T) {
	d := familiareag.BreedDistribution{"owl": 50, "fox": 50}
	if got := d.Total(); got != 100.0 {
		t.Errorf("Total: got %v, want 100", got)
	}
	if got := (familiareag.BreedDistribution{}).Total(); got != 0 {
		t.Errorf("empty Total: got %v", got)
	}
}

func TestCatalogEntry_Validate_Passes(t *testing.T) {
	e := &familiareag.CatalogEntry{
		SKU:               "egg.x.v1",
		DisplayName:       "X",
		PriceCents:        500,
		Currency:          "SGD",
		BreedDistribution: familiareag.BreedDistribution{"owl": 50, "fox": 50},
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate happy path: %v", err)
	}
}

func TestCatalogEntry_Validate_Errors(t *testing.T) {
	good := familiareag.BreedDistribution{"owl": 50, "fox": 50}
	cases := []struct {
		name string
		e    *familiareag.CatalogEntry
	}{
		{"missing sku", &familiareag.CatalogEntry{DisplayName: "x", Currency: "SGD", BreedDistribution: good, SoftExpiryDays: 30, HardExpiryDays: 60}},
		{"missing display", &familiareag.CatalogEntry{SKU: "s", Currency: "SGD", BreedDistribution: good, SoftExpiryDays: 30, HardExpiryDays: 60}},
		{"bad currency", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SG", BreedDistribution: good, SoftExpiryDays: 30, HardExpiryDays: 60}},
		{"negative price", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SGD", PriceCents: -1, BreedDistribution: good, SoftExpiryDays: 30, HardExpiryDays: 60}},
		{"trial not free", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SGD", PriceCents: 5, IsTrial: true, BreedDistribution: good, SoftExpiryDays: 30, HardExpiryDays: 60}},
		{"soft zero", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SGD", BreedDistribution: good, SoftExpiryDays: 0, HardExpiryDays: 60}},
		{"hard < soft", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SGD", BreedDistribution: good, SoftExpiryDays: 60, HardExpiryDays: 30}},
		{"bad dist", &familiareag.CatalogEntry{SKU: "s", DisplayName: "x", Currency: "SGD", BreedDistribution: familiareag.BreedDistribution{"owl": 30}, SoftExpiryDays: 30, HardExpiryDays: 60}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.e.Validate(); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

func TestCatalogEntry_AvailableAt_DeletedRowsAreUnavailable(t *testing.T) {
	now := time.Now().UTC()
	e := &familiareag.CatalogEntry{DeletedAt: &now}
	if e.AvailableAt(now.Add(time.Hour)) {
		t.Errorf("deleted SKU should be unavailable")
	}
}

func TestPurchase_AllTerminalReapplyIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	// terminal expired: MarkPaid/MarkPaymentFailed/MarkRefunded must be no-ops.
	p := samplePurchaseCov(now)
	_ = p.MarkPaid(now.Add(time.Minute), "pi", "ch", 999)
	_ = p.MarkExpired(now.Add(72*time.Hour), 999)
	if err := p.MarkPaid(now.Add(96*time.Hour), "", "", 0); err != nil {
		t.Errorf("MarkPaid on expired should be no-op; got %v", err)
	}
	if err := p.MarkRefunded(now.Add(96*time.Hour), "", 0, "", false); err != nil {
		t.Errorf("MarkRefunded on expired should be no-op; got %v", err)
	}
	if err := p.MarkExpired(now.Add(96*time.Hour), 0); err != nil {
		t.Errorf("MarkExpired idempotent should be no-op; got %v", err)
	}
}

func TestPurchase_MarkPaymentFailed_Idempotent(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchaseCov(now)
	_ = p.MarkPaymentFailed(now, "card_declined", "")
	if err := p.MarkPaymentFailed(now.Add(time.Minute), "card_declined", ""); err != nil {
		t.Errorf("idem MarkPaymentFailed; got %v", err)
	}
}

func TestPurchase_MarkRefundedFromCheckoutFails(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchaseCov(now)
	if err := p.MarkRefunded(now, "re_x", 999, "fraud", false); err == nil {
		t.Errorf("expected error refunding from checkout_started")
	}
}

func TestPurchase_MarkExpiredFromRefundedFails(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchaseCov(now)
	_ = p.MarkPaid(now.Add(time.Minute), "", "", 999)
	_ = p.MarkRefunded(now.Add(time.Hour), "", 999, "fraud", false)
	if err := p.MarkExpired(now.Add(72*time.Hour), 999); err == nil {
		t.Errorf("expected error expiring refunded purchase")
	}
}

func TestPurchase_MarkPaymentFailedFromPaidFails(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchaseCov(now)
	_ = p.MarkPaid(now.Add(time.Minute), "", "", 999)
	if err := p.MarkPaymentFailed(now.Add(time.Hour), "", ""); err == nil {
		t.Errorf("expected error transitioning paid -> payment_failed")
	}
}

func samplePurchaseCov(now time.Time) *familiareag.Purchase {
	return familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "01970000-0000-7000-8000-000000000001",
		TenantID:          "tenant-1",
		PurchaserGCID:     "gcid-1",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_x",
		StripeCheckoutURL: "https://stripe.example/cs_test_x",
		Now:               now,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
}
