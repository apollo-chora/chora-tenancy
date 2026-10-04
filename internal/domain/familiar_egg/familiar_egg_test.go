// familiar_egg_test.go — RED-phase tests for the Iter G.3 Familiar Egg
// purchase + catalog domain (ADR-149).
package familiar_egg_test

import (
	"testing"
	"time"

	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

func TestBreedDistribution_ValidSumIsExactly100(t *testing.T) {
	// 5 hero species only (CHO-2032) — each has art, a persona, and a species
	// Path in chora-consumption. penguin is the 5th hero.
	d := familiareag.BreedDistribution{
		"owl":     25.0,
		"fox":     25.0,
		"penguin": 20.0,
		"dragon":  20.0,
		"phoenix": 10.0,
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestBreedDistribution_RejectsUnknownSpecies(t *testing.T) {
	d := familiareag.BreedDistribution{
		"owl":     50.0,
		"unicorn": 50.0, // not a canonical hero species
	}
	if err := d.Validate(); err == nil {
		t.Fatal("expected validation error for unknown species")
	}
}

func TestBreedDistribution_RejectsRetiredGachaBreeds(t *testing.T) {
	// The legacy 8-breed gacha (cat/turtle/wolf/raven) is retired: those
	// breeds have no species Path or art in the Grimoire layer, so an egg
	// that could roll one would hatch a familiar with an empty Grimoire.
	for _, retired := range []string{"cat", "turtle", "wolf", "raven"} {
		d := familiareag.BreedDistribution{"owl": 50.0, retired: 50.0}
		if err := d.Validate(); err == nil {
			t.Errorf("retired breed %q must be rejected by BreedDistribution.Validate", retired)
		}
	}
}

func TestBreedDistribution_RejectsNegativeWeight(t *testing.T) {
	d := familiareag.BreedDistribution{
		"owl": 100.0,
		"fox": -5.0,
	}
	if err := d.Validate(); err == nil {
		t.Fatal("expected validation error for negative weight")
	}
}

func TestBreedDistribution_RejectsSumNot100(t *testing.T) {
	cases := []struct {
		name string
		d    familiareag.BreedDistribution
	}{
		{"sum 99", familiareag.BreedDistribution{"owl": 50.0, "fox": 49.0}},
		{"sum 101", familiareag.BreedDistribution{"owl": 50.0, "fox": 51.0}},
		{"sum 0", familiareag.BreedDistribution{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.d.Validate(); err == nil {
				t.Fatalf("expected validation error; got nil")
			}
		})
	}
}

func TestBreedDistribution_AllowsToleranceOf0_01(t *testing.T) {
	// floating-point tolerance — 33.33 * 3 == 99.99
	d := familiareag.BreedDistribution{
		"owl":     33.33,
		"fox":     33.33,
		"penguin": 33.34,
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("expected to allow 99.99..100.01 tolerance; got %v", err)
	}
}

func TestBreedDistribution_OddsAsSortedListSortedDescByProbability(t *testing.T) {
	d := familiareag.BreedDistribution{
		"owl":     30.0,
		"fox":     20.0,
		"phoenix": 20.0,
		"dragon":  15.0,
		"penguin": 15.0,
	}
	odds := d.Odds()
	if len(odds) != 5 {
		t.Fatalf("odds len: got %d want 5", len(odds))
	}
	prev := 100.0
	for i, o := range odds {
		if o.Probability > prev {
			t.Errorf("odds not sorted desc at idx %d: %v > prev %v", i, o.Probability, prev)
		}
		prev = o.Probability
		if o.Rarity == "" {
			t.Errorf("rarity missing for %s", o.Species)
		}
	}
	// Ties broken alphabetically — dragon before penguin at the 15.0 tier.
	if odds[3].Species != "dragon" || odds[4].Species != "penguin" {
		t.Errorf("tie-break order: got %s,%s want dragon,penguin", odds[3].Species, odds[4].Species)
	}
}

func TestBreedDistribution_RarityBuckets(t *testing.T) {
	// Per ADR-149: common=>=20%, uncommon=10-20%, rare=5-10%, legendary=<5%.
	cases := map[float64]string{
		25.0: "common",
		15.0: "uncommon",
		7.0:  "rare",
		3.0:  "legendary",
	}
	for pct, want := range cases {
		got := familiareag.RarityForProbability(pct)
		if got != want {
			t.Errorf("RarityForProbability(%v) = %q; want %q", pct, got, want)
		}
	}
}

func TestPurchase_NewStartsInCheckoutStarted(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "00000000-0000-0000-0000-000000000001",
		TenantID:          "tenant-1",
		PurchaserGCID:     "gcid-1",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_1",
		StripeCheckoutURL: "https://stripe.example/cs_test_1",
		Now:               now,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
	if p.State != familiareag.StateCheckoutStarted {
		t.Errorf("initial state: got %q want %q", p.State, familiareag.StateCheckoutStarted)
	}
	if !p.SoftExpiryAt.Equal(now.AddDate(0, 0, 30)) {
		t.Errorf("soft expiry: got %v", p.SoftExpiryAt)
	}
	if !p.HardExpiryAt.Equal(now.AddDate(0, 0, 60)) {
		t.Errorf("hard expiry: got %v", p.HardExpiryAt)
	}
}

func TestPurchase_MarkPaidTransition(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchase(now)
	paidAt := now.Add(2 * time.Minute)
	if err := p.MarkPaid(paidAt, "pi_x", "ch_x", 999); err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}
	if p.State != familiareag.StatePaid {
		t.Errorf("state: got %q want paid", p.State)
	}
	if p.PaidAt == nil || !p.PaidAt.Equal(paidAt) {
		t.Errorf("paid_at not set correctly")
	}
	if p.AmountCentsPaid != 999 {
		t.Errorf("amount_paid: got %d", p.AmountCentsPaid)
	}

	// Idempotency: re-applying MarkPaid is a no-op.
	if err := p.MarkPaid(paidAt.Add(time.Hour), "pi_x", "ch_x", 999); err != nil {
		t.Fatalf("re-MarkPaid: %v", err)
	}
	if !p.PaidAt.Equal(paidAt) {
		t.Errorf("idempotency: paid_at changed on re-apply")
	}
}

func TestPurchase_MarkPaidFromPaymentFailedReturnsError(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchase(now)
	if err := p.MarkPaymentFailed(now, "card_declined", "Card was declined"); err != nil {
		t.Fatalf("MarkPaymentFailed: %v", err)
	}
	if err := p.MarkPaid(now.Add(time.Hour), "", "", 0); err == nil {
		t.Errorf("expected error transitioning payment_failed -> paid")
	}
}

func TestPurchase_MarkRefundedFromPaid(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchase(now)
	_ = p.MarkPaid(now.Add(time.Minute), "pi_x", "ch_x", 999)
	refundAt := now.Add(3 * time.Hour)
	if err := p.MarkRefunded(refundAt, "re_x", 999, "customer_request", false); err != nil {
		t.Fatalf("MarkRefunded: %v", err)
	}
	if p.State != familiareag.StateRefunded {
		t.Errorf("state: got %q want refunded", p.State)
	}
	if p.AmountCentsRefunded != 999 {
		t.Errorf("amount_refunded: got %d", p.AmountCentsRefunded)
	}
	if p.RefundReason != "customer_request" {
		t.Errorf("reason: got %q", p.RefundReason)
	}
}

func TestPurchase_MarkExpiredFromPaid(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := samplePurchase(now)
	_ = p.MarkPaid(now.Add(time.Minute), "pi_x", "ch_x", 999)
	expiry := now.AddDate(0, 0, 61)
	if err := p.MarkExpired(expiry, 999); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	if p.State != familiareag.StateExpired {
		t.Errorf("state: got %q want expired", p.State)
	}
	if !p.RefundCreditOnly {
		t.Errorf("expired path must credit_only=true")
	}
	if p.RefundReason != "hard_expiry_unhatched" {
		t.Errorf("reason: got %q", p.RefundReason)
	}
}

func TestCatalogEntry_AvailableNow_RespectsWindow(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	e := &familiareag.CatalogEntry{
		AvailableFrom:  &from,
		AvailableUntil: &until,
	}
	if !e.AvailableAt(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("should be available mid-window")
	}
	if e.AvailableAt(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("should NOT be available before window")
	}
	if e.AvailableAt(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("should NOT be available after window")
	}
}

func TestCatalogEntry_AvailableNow_NilWindowAlwaysAvailable(t *testing.T) {
	e := &familiareag.CatalogEntry{}
	if !e.AvailableAt(time.Now()) {
		t.Errorf("nil window should be always available")
	}
}

func samplePurchase(now time.Time) *familiareag.Purchase {
	return familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "00000000-0000-0000-0000-000000000001",
		TenantID:          "tenant-1",
		PurchaserGCID:     "gcid-1",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_test_1",
		StripeCheckoutURL: "https://stripe.example/cs_test_1",
		Now:               now,
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
}
