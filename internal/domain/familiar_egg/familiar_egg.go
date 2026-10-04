// Package familiar_egg is the Iter G.3 domain for ADR-149 Familiar Egg
// Stripe-driven purchases + per-tenant egg catalog.
//
// Two aggregates:
//
//   - Purchase: a single Stripe-tracked egg purchase, 1:1 with a Stripe
//     Checkout session. Lifecycle states match the
//     familiar_egg_purchases.state column CHECK constraint:
//
//       checkout_started -> paid -> provisioned
//                        -> payment_failed
//                        -> refunded
//                        -> expired
//
//   - CatalogEntry: a per-tenant or platform-global egg SKU. Carries the
//     IMDA D2 transparency-required breed_distribution probability table.
//
// All state transitions are pure functions of (current state + new event),
// guarded for forward-only motion. Re-applying the same terminal event is
// idempotent (matches Stripe webhook redelivery semantics).
package familiar_egg

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// -----------------------------------------------------------------------------
// Breed distribution
// -----------------------------------------------------------------------------

// canonicalSpecies enumerates the 5 hero species allowed in
// breed_distribution JSONB keys (CHO-2032). Mirrors growth.CanonicalSpecies
// in chora-consumption (the roll authority) + the species_paths seed + the
// familiar_instances.species CHECK. The legacy gacha breeds
// (cat/turtle/wolf/raven) are retired — they have no species Path or art, so
// an egg that could roll one would hatch a familiar with a dead Grimoire.
// Kept in sync with the SQL trigger in 0007_familiar_egg_purchases.sql (see
// migration 0033).
var canonicalSpecies = map[string]struct{}{
	"owl": {}, "fox": {}, "dragon": {}, "phoenix": {}, "penguin": {},
}

// distributionTolerance is the absolute float tolerance for the
// breed_distribution sum to be considered 100.0. The SQL trigger uses
// NUMERIC(6,2) so we allow 0.01 of float-noise wiggle.
const distributionTolerance = 0.01

// BreedDistribution maps a species name to its weight (0..100). Total
// must sum to 100.0 within distributionTolerance.
type BreedDistribution map[string]float64

// Validate checks the distribution against the canonical species enum,
// non-negative weights, and a 100.0 sum within tolerance.
func (d BreedDistribution) Validate() error {
	if len(d) == 0 {
		return fmt.Errorf("breed_distribution empty")
	}
	var total float64
	for k, v := range d {
		if _, ok := canonicalSpecies[k]; !ok {
			return fmt.Errorf("breed_distribution: unknown species %q (heroes: owl|fox|dragon|phoenix|penguin)", k)
		}
		if v < 0 {
			return fmt.Errorf("breed_distribution: negative weight for %q: %v", k, v)
		}
		total += v
	}
	if math.Abs(total-100.0) > distributionTolerance {
		return fmt.Errorf("breed_distribution: weights sum to %.4f, want 100.0 (tolerance %.2f)", total, distributionTolerance)
	}
	return nil
}

// Total returns the sum of all species weights.
func (d BreedDistribution) Total() float64 {
	var t float64
	for _, v := range d {
		t += v
	}
	return t
}

// Odd is one row of the IMDA D2 transparency odds table.
type Odd struct {
	Species     string  `json:"species"`
	Probability float64 `json:"probability"`
	Rarity      string  `json:"rarity"`
}

// Odds returns the distribution as a stable-sorted Odd list (probability
// DESC, ties broken alphabetically). Powers GET /odds.
func (d BreedDistribution) Odds() []Odd {
	out := make([]Odd, 0, len(d))
	for k, v := range d {
		out = append(out, Odd{Species: k, Probability: v, Rarity: RarityForProbability(v)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Probability != out[j].Probability {
			return out[i].Probability > out[j].Probability
		}
		return out[i].Species < out[j].Species
	})
	return out
}

// RarityForProbability maps a percentage weight to the player-facing
// rarity bucket per ADR-149 §Breed lootbox.
func RarityForProbability(pct float64) string {
	switch {
	case pct >= 20.0:
		return "common"
	case pct >= 10.0:
		return "uncommon"
	case pct >= 5.0:
		return "rare"
	default:
		return "legendary"
	}
}

// -----------------------------------------------------------------------------
// Catalog entry
// -----------------------------------------------------------------------------

// CatalogEntry mirrors one row of familiar_egg_catalog.
//
// TenantID is empty string when this is a platform-global SKU
// (familiar_egg_catalog.tenant_id IS NULL).
type CatalogEntry struct {
	SKU                       string
	TenantID                  string // "" = platform-global
	DisplayName               string
	Description               string
	PriceCents                int64
	Currency                  string
	SuggestedFocalAtomID      string
	BreedDistribution         BreedDistribution
	BreedDistributionUpdated  time.Time
	Purchasable               bool
	IsTrial                   bool
	SoftExpiryDays            int
	HardExpiryDays            int
	AvailableFrom             *time.Time
	AvailableUntil            *time.Time
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	DeletedAt                 *time.Time
}

// AvailableAt reports whether the SKU is available at `at`. Nil
// available_from / available_until == always available.
func (e *CatalogEntry) AvailableAt(at time.Time) bool {
	if e.DeletedAt != nil {
		return false
	}
	if e.AvailableFrom != nil && at.Before(*e.AvailableFrom) {
		return false
	}
	if e.AvailableUntil != nil && at.After(*e.AvailableUntil) {
		return false
	}
	return true
}

// Validate runs CatalogEntry invariants. The matching SQL CHECK constraints
// + the breed_distribution trigger enforce these at the DB layer too, but
// the app validates for friendlier 400 errors per the Iter G.3 spec.
func (e *CatalogEntry) Validate() error {
	if e.SKU == "" {
		return fmt.Errorf("sku required")
	}
	if e.DisplayName == "" {
		return fmt.Errorf("display_name required")
	}
	if len(e.Currency) != 3 {
		return fmt.Errorf("currency must be 3-letter ISO 4217 code (got %q)", e.Currency)
	}
	if e.PriceCents < 0 {
		return fmt.Errorf("price_cents must be >= 0")
	}
	if e.IsTrial && e.PriceCents != 0 {
		return fmt.Errorf("trial eggs must be free (price_cents=0)")
	}
	if e.SoftExpiryDays <= 0 {
		return fmt.Errorf("soft_expiry_days must be > 0")
	}
	if e.HardExpiryDays < e.SoftExpiryDays {
		return fmt.Errorf("hard_expiry_days must be >= soft_expiry_days")
	}
	if err := e.BreedDistribution.Validate(); err != nil {
		return err
	}
	return nil
}

// -----------------------------------------------------------------------------
// Purchase aggregate
// -----------------------------------------------------------------------------

// State enumerates the familiar_egg_purchases.state values.
type State string

const (
	StateCheckoutStarted State = "checkout_started"
	StatePaid            State = "paid"
	StatePaymentFailed   State = "payment_failed"
	StateProvisioned     State = "provisioned"
	StateRefunded        State = "refunded"
	StateExpired         State = "expired"
)

// RefundReason enumerates familiar_egg_purchases.refund_reason values.
type RefundReason string

const (
	RefundReasonHardExpiryUnhatched RefundReason = "hard_expiry_unhatched"
	RefundReasonSupportInitiated    RefundReason = "support_initiated"
	RefundReasonTenantPolicy        RefundReason = "tenant_policy"
	RefundReasonCustomerRequest     RefundReason = "customer_request"
	RefundReasonDuplicateCharge     RefundReason = "duplicate_charge"
	RefundReasonFraud               RefundReason = "fraud"
)

// Purchase mirrors familiar_egg_purchases.
type Purchase struct {
	PurchaseID            string
	TenantID              string
	PurchaserGCID         string
	EggSKU                string
	SuggestedFocalAtomID  string
	State                 State
	AmountCents           int64
	AmountCentsPaid       int64
	AmountCentsRefunded   int64
	Currency              string
	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	StripeRefundID        string
	StripeCheckoutURL     string
	StripeFailureCode     string
	StripeFailureMessage  string
	RefundReason          string
	RefundCreditOnly      bool
	CheckoutStartedAt     time.Time
	PaidAt                *time.Time
	FailedAt              *time.Time
	ProvisionedAt         *time.Time
	RefundedAt            *time.Time
	ExpiredAt             *time.Time
	SoftExpiryAt          time.Time
	HardExpiryAt          time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// NewPurchaseInput is the constructor input for NewPurchase.
type NewPurchaseInput struct {
	PurchaseID           string
	TenantID             string
	PurchaserGCID        string
	EggSKU               string
	SuggestedFocalAtomID string
	AmountCents          int64
	Currency             string
	StripeSessionID      string
	StripeCheckoutURL    string
	SoftExpiryDays       int
	HardExpiryDays       int
	Now                  time.Time
}

// NewPurchase constructs a Purchase in StateCheckoutStarted.
func NewPurchase(in NewPurchaseInput) *Purchase {
	now := in.Now.UTC()
	return &Purchase{
		PurchaseID:        in.PurchaseID,
		TenantID:          in.TenantID,
		PurchaserGCID:     in.PurchaserGCID,
		EggSKU:            in.EggSKU,
		SuggestedFocalAtomID: in.SuggestedFocalAtomID,
		State:             StateCheckoutStarted,
		AmountCents:       in.AmountCents,
		Currency:          in.Currency,
		StripeSessionID:   in.StripeSessionID,
		StripeCheckoutURL: in.StripeCheckoutURL,
		CheckoutStartedAt: now,
		SoftExpiryAt:      now.AddDate(0, 0, in.SoftExpiryDays),
		HardExpiryAt:      now.AddDate(0, 0, in.HardExpiryDays),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

// MarkPaid transitions to StatePaid. Idempotent: re-applying when already
// paid is a no-op (matches Stripe webhook redelivery semantics).
//
// Forward-only: payment_failed/refunded/expired states cannot be promoted
// back to paid.
func (p *Purchase) MarkPaid(at time.Time, paymentIntentID, chargeID string, amountPaid int64) error {
	at = at.UTC()
	switch p.State {
	case StatePaid, StateProvisioned, StateRefunded, StateExpired:
		// idempotent / terminal — no-op without error so Stripe redeliveries
		// + late-arriving webhooks stay safe.
		return nil
	case StatePaymentFailed:
		return fmt.Errorf("familiar_egg: cannot transition %s -> paid", p.State)
	}
	p.State = StatePaid
	pt := at
	p.PaidAt = &pt
	p.StripePaymentIntentID = paymentIntentID
	p.StripeChargeID = chargeID
	p.AmountCentsPaid = amountPaid
	p.UpdatedAt = at
	return nil
}

// MarkPaymentFailed transitions to StatePaymentFailed.
//
// Forward-only: paid/refunded/expired cannot drop back to payment_failed.
func (p *Purchase) MarkPaymentFailed(at time.Time, code, message string) error {
	at = at.UTC()
	switch p.State {
	case StatePaid, StateProvisioned, StateRefunded, StateExpired:
		return fmt.Errorf("familiar_egg: cannot transition %s -> payment_failed", p.State)
	case StatePaymentFailed:
		return nil
	}
	p.State = StatePaymentFailed
	ft := at
	p.FailedAt = &ft
	p.StripeFailureCode = code
	p.StripeFailureMessage = message
	p.UpdatedAt = at
	return nil
}

// MarkRefunded transitions to StateRefunded.
func (p *Purchase) MarkRefunded(at time.Time, refundID string, amountRefunded int64, reason string, creditOnly bool) error {
	at = at.UTC()
	switch p.State {
	case StateRefunded, StateExpired:
		return nil
	case StateCheckoutStarted, StatePaymentFailed:
		return fmt.Errorf("familiar_egg: cannot transition %s -> refunded (must be paid first)", p.State)
	}
	p.State = StateRefunded
	rt := at
	p.RefundedAt = &rt
	p.StripeRefundID = refundID
	p.AmountCentsRefunded = amountRefunded
	p.RefundReason = reason
	p.RefundCreditOnly = creditOnly
	p.UpdatedAt = at
	return nil
}

// MarkExpired transitions to StateExpired with a mana-credit refund for
// the full purchase amount. Used by the hard-expiry sweeper.
func (p *Purchase) MarkExpired(at time.Time, amountRefundedAsCredit int64) error {
	at = at.UTC()
	switch p.State {
	case StateExpired:
		return nil
	case StateCheckoutStarted, StatePaymentFailed, StateRefunded:
		return fmt.Errorf("familiar_egg: cannot transition %s -> expired", p.State)
	}
	p.State = StateExpired
	et := at
	p.ExpiredAt = &et
	p.AmountCentsRefunded = amountRefundedAsCredit
	p.RefundReason = string(RefundReasonHardExpiryUnhatched)
	p.RefundCreditOnly = true
	p.UpdatedAt = at
	return nil
}
