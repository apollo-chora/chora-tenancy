// Package addon is the pure-domain core of the Composable Add-On Plans
// sub-domain inside chora-tenancy.
package addon

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"
)

// Errors
var (
	ErrInvalidArgument       = errors.New("invalid argument")
	ErrAddOnNotFound         = errors.New("add-on not found")
	ErrSubscriptionNotFound  = errors.New("subscription not found")
	ErrCannotUnsubscribeBase = errors.New("cannot unsubscribe always-on base add-on")
	ErrAlreadyDeactivated    = errors.New("subscription already deactivated")
	ErrInvalidTier           = errors.New("invalid tier")
	ErrTierFamilyMismatch    = errors.New("target plan tier family does not match current")
	ErrInvalidProration      = errors.New("invalid proration mode")
	ErrIneligibleForChange   = errors.New("subscription not eligible for tier change")
	ErrComplianceLocked      = errors.New("add-on is compliance-locked")
	ErrReasonTextRequired    = errors.New("reason_text required when reason=other")
	ErrConcurrencyConflict   = errors.New("concurrent modification — retry")
	// CHO-1776 — dunning-state mutator invariants.
	ErrNotActiveForPastDue  = errors.New("cannot mark past_due: subscription not active")
	ErrNotPastDueForRecover = errors.New("cannot mark recovered: subscription not past_due")
)

// NewUUIDv7 returns a freshly generated UUIDv7 string.
func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Tier is a pricing rung within a plan family.
type Tier struct {
	Code              string
	DisplayName       string
	MonthlyPriceCents int64
	IncludedUnits     *int64
	OverageUnitCents  *int64
	Currency          string
}

// Validate enforces required fields on a Tier.
func (t Tier) Validate() error {
	if strings.TrimSpace(t.Code) == "" {
		return fmt.Errorf("%w: tier code required", ErrInvalidTier)
	}
	if t.MonthlyPriceCents < 0 {
		return fmt.Errorf("%w: monthly_price_cents must be >= 0", ErrInvalidTier)
	}
	if len(t.Currency) != 3 {
		return fmt.Errorf("%w: currency must be 3-letter ISO 4217", ErrInvalidTier)
	}
	return nil
}

// Entitlement is one feature unit included in an add-on plan.
type Entitlement struct {
	FeatureCode string
	Label       string
	Limit       *int64
	LimitUnit   string
}

// Integration is an external integration the add-on exposes.
type Integration struct {
	Type   string
	Label  string
	Status string
}

// Compliance carries the regulatory claims for an add-on.
type Compliance struct {
	GDPR                 bool
	PDPA                 bool
	CCPA                 bool
	IMDA                 bool
	DataResidencyRegions []string
}

// AddOn is a catalogue entry — a feature flag bundle with tier rungs.
type AddOn struct {
	ID                string
	Code              string
	DisplayName       string
	Category          string
	Description       string
	PlanFamily        string
	Tiers             []Tier
	Entitlements      []Entitlement
	Integrations      []Integration
	Compliance        Compliance
	MonthlyPriceCents int64
	AlwaysOn          bool
	ComplianceLocked  bool
	CreatedAt         time.Time
}

// FindTier returns the Tier with code or false.
func (a *AddOn) FindTier(code string) (Tier, bool) {
	for _, t := range a.Tiers {
		if t.Code == code {
			return t, true
		}
	}
	return Tier{}, false
}

// Catalogue is a registry of AddOn catalogue entries keyed by code.
type Catalogue struct {
	mu sync.RWMutex
	by map[string]*AddOn
}

// NewCatalogue returns an empty catalogue.
func NewCatalogue() *Catalogue {
	return &Catalogue{by: make(map[string]*AddOn)}
}

// Register stores a in the catalogue.
func (c *Catalogue) Register(a *AddOn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.by[a.Code] = a
}

// Get returns an AddOn by code.
func (c *Catalogue) Get(code string) (*AddOn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.by[code]
	return a, ok
}

// List returns all catalogue entries sorted by code.
func (c *Catalogue) List() []*AddOn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*AddOn, 0, len(c.by))
	for _, a := range c.by {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// NewSeedCatalogue returns an in-memory catalogue with the 12 seeded add-ons
// aligned to the Phyllis MVP scope (audit-platform-fillgaps.md §9.6 + the
// catalog refresh PR notes 2026-05-09).
//
// Removed (deferred to post-MVP catalog refresh):
//   - singpass-federation (premature seed; gov-grade integration is M15+)
//
// Added (Phyllis MVP scope per phyllis-mvp-2026-05-08.md §5.2):
//   - ai_assist            — Tier 4 guardrail-screened AI Assist generation
//   - daily_dose           — Notifications subscriber for the daily dose loop
//   - kg_hexagonal         — Per-User Knowledge Graph fog (per ADR-143)
//   - course_application   — Stripe Checkout for paid course applications
//   - governance_dashboard — O+ Observability+ baseline (compliance-locked)
func NewSeedCatalogue() *Catalogue {
	cat := NewCatalogue()
	now := time.Now().UTC()
	seed := []*AddOn{
		{ID: NewUUIDv7(), Code: "base", DisplayName: "Chora Base", Category: "Core",
			MonthlyPriceCents: 0, AlwaysOn: true, PlanFamily: "base",
			Tiers: []Tier{{Code: "default", DisplayName: "Default", Currency: "SGD"}},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "tms", DisplayName: "Training Management Suite", Category: "Delivery",
			MonthlyPriceCents: 4900, PlanFamily: "tms",
			Tiers: []Tier{
				{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 4900, Currency: "SGD"},
				{Code: "pro", DisplayName: "Pro", MonthlyPriceCents: 9900, Currency: "SGD"},
				{Code: "enterprise", DisplayName: "Enterprise", MonthlyPriceCents: 19900, Currency: "SGD"},
			},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "cms", DisplayName: "Content Management Suite", Category: "Creation",
			MonthlyPriceCents: 2900, PlanFamily: "cms",
			Tiers:     []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 2900, Currency: "SGD"}},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "pvp-arena", DisplayName: "PvP Arena", Category: "Engagement",
			MonthlyPriceCents: 1900, PlanFamily: "pvp-arena",
			Tiers:     []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 1900, Currency: "SGD"}},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "familiar", DisplayName: "Familiar Companion", Category: "Engagement",
			MonthlyPriceCents: 990, PlanFamily: "familiar",
			Tiers:     []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 990, Currency: "SGD"}},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "campus-ops", DisplayName: "Campus Operations", Category: "Delivery",
			MonthlyPriceCents: 9900, PlanFamily: "campus-ops",
			Tiers:     []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 9900, Currency: "SGD"}},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "marketplace", DisplayName: "Marketplace Add-Ons", Category: "Engagement",
			MonthlyPriceCents: 1490, PlanFamily: "marketplace",
			Tiers:     []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 1490, Currency: "SGD"}},
			CreatedAt: now},

		// --- Phyllis MVP additions (2026-05-09 catalog refresh) ----------
		{ID: NewUUIDv7(), Code: "ai_assist", DisplayName: "AI Assist (Tier-4 Guardrail)", Category: "Creation",
			MonthlyPriceCents: 1490, PlanFamily: "ai_assist",
			Description: "Guardrail-screened AI Assist generation (Cloud Model Armor + Cloud DLP + regex baseline + output validators).",
			Tiers: []Tier{
				{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 1490, Currency: "SGD"},
				{Code: "pro", DisplayName: "Pro", MonthlyPriceCents: 4900, Currency: "SGD"},
			},
			CreatedAt: now},
		{ID: NewUUIDv7(), Code: "daily_dose", DisplayName: "Familiar Daily Dose", Category: "Engagement",
			MonthlyPriceCents: 590, PlanFamily: "daily_dose",
			Description: "Daily 5-atom dose composed deterministically from TopicRetention; Notifications subscriber drives in-app banner.",
			Tiers:       []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 590, Currency: "SGD"}},
			CreatedAt:   now},
		{ID: NewUUIDv7(), Code: "kg_hexagonal", DisplayName: "Hexagonal Knowledge Graph", Category: "Consumption",
			MonthlyPriceCents: 990, PlanFamily: "kg_hexagonal",
			Description: "Per-user hexagonal Knowledge Graph fog with 1 focal + 6 LLM-generated neighbours (per ADR-143).",
			Tiers:       []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 990, Currency: "SGD"}},
			CreatedAt:   now},
		{ID: NewUUIDv7(), Code: "course_application", DisplayName: "Course Application + Checkout", Category: "Delivery",
			MonthlyPriceCents: 2490, PlanFamily: "course_application",
			Description: "Stripe Checkout for paid course applications + interview + tax invoice + confirmation flow.",
			Tiers:       []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 2490, Currency: "SGD"}},
			CreatedAt:   now},
		{ID: NewUUIDv7(), Code: "governance_dashboard", DisplayName: "Governance Dashboard (O+)", Category: "Governance",
			MonthlyPriceCents: 4900, PlanFamily: "governance_dashboard",
			Description:      "O+ Observability+ baseline IMDA evidence dashboard (D1 accountability / D2 transparency / D3 safety / D4 fairness per ADR-141).",
			Tiers:            []Tier{{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 4900, Currency: "SGD"}},
			Compliance:       Compliance{IMDA: true, PDPA: true, GDPR: true, CCPA: true, DataResidencyRegions: []string{"SG", "US", "EU"}},
			ComplianceLocked: true, // cannot deactivate without super_admin override
			CreatedAt:        now},
		{ID: NewUUIDv7(), Code: "cplus_social", DisplayName: "C+ Content Sharing", Category: "Engagement",
			MonthlyPriceCents: 0, PlanFamily: "cplus_social", AlwaysOn: true,
			Description: "C+ social feed, profiles, leaderboards, duels, connections. Entitled to the public chora-master tenant by default (ADR-182 D5).",
			Tiers:       []Tier{{Code: "default", DisplayName: "Default", Currency: "SGD"}},
			CreatedAt:   now},
	}
	for _, a := range seed {
		cat.Register(a)
	}
	return cat
}

// Status is the lifecycle state of a Subscription.
type Status string

const (
	StatusPendingActivation   Status = "pending_activation"
	StatusActive              Status = "active"
	StatusGracePeriod         Status = "grace_period"
	StatusSuspended           Status = "suspended"
	StatusPendingDeactivation Status = "pending_deactivation"
	StatusDeactivated         Status = "deactivated"
)

// DeactivationReason is the structured audit reason enum.
type DeactivationReason string

const (
	ReasonNoLongerNeeded DeactivationReason = "no_longer_needed"
	ReasonCost           DeactivationReason = "cost"
	ReasonConsolidation  DeactivationReason = "consolidation"
	ReasonMigration      DeactivationReason = "migration"
	ReasonCompliance     DeactivationReason = "compliance"
	ReasonOther          DeactivationReason = "other"
)

// IsValidReason reports whether the given string is a known reason.
func IsValidReason(r string) bool {
	switch DeactivationReason(r) {
	case ReasonNoLongerNeeded, ReasonCost, ReasonConsolidation,
		ReasonMigration, ReasonCompliance, ReasonOther:
		return true
	}
	return false
}

// ProrationMode is a Stripe-style proration toggle.
type ProrationMode string

const (
	ProrationCreate        ProrationMode = "create_prorations"
	ProrationNone          ProrationMode = "none"
	ProrationAlwaysInvoice ProrationMode = "always_invoice"
)

// IsValidProration reports whether mode is in the allowed set.
// CHO-1786 — always_invoice surfaces the "Immediately" radio's intent
// (bill the prorated delta today via a separate Stripe invoice) instead
// of deferring to the next upcoming invoice. chora-payments accepts the
// value; chora-tenancy must too — otherwise the proxy at
// addon_screens_handlers.go::72 422s before forwarding.
func IsValidProration(mode string) bool {
	return mode == string(ProrationCreate) ||
		mode == string(ProrationNone) ||
		mode == string(ProrationAlwaysInvoice)
}

// DeactivationRequest is the structured deactivation payload.
type DeactivationRequest struct {
	Reason          DeactivationReason
	ReasonText      string
	EffectiveAt     *time.Time
	RequestedByGCID string
}

// Validate checks invariants on the request.
func (d DeactivationRequest) Validate() error {
	if !IsValidReason(string(d.Reason)) {
		return fmt.Errorf("%w: reason %q invalid", ErrInvalidArgument, d.Reason)
	}
	if d.Reason == ReasonOther && strings.TrimSpace(d.ReasonText) == "" {
		return ErrReasonTextRequired
	}
	return nil
}

// Subscription is the per-tenant per-add-on subscription aggregate.
type Subscription struct {
	ID                        string
	TenantID                  string
	AddOnCode                 string
	AddOnID                   string
	PlanFamily                string
	CurrentTier               string
	MonthlyPriceCentsSnapshot int64
	OwnerGCID                 string
	Status                    Status
	Version                   int64
	ActivatedAt               time.Time
	CancelledAt               *time.Time
	GraceEndsAt               *time.Time
	DeactivatedAt             *time.Time
	DeactivationReason        DeactivationReason
	DeactivationReasonText    string
	DeactivationRequestedBy   string
	DeactivationRequestedAt   *time.Time
	DeactivationEffectiveAt   *time.Time
	UpdatedAt                 time.Time
	// CHO-1776 — Stripe dunning flag. Driven out-of-band by
	// chora.payments.tenant_addon_purchase.subscription_payment_failed.v1
	// (sets true) + .payment_recovered.v1 (clears false). Independent of
	// Status — a row stays ACTIVE during dunning so existing access
	// continues, only the FE banner changes.
	PastDue bool
	// CHO-1772 — pending end-of-cycle tier change. Set when the
	// admin picks the end-of-cycle option on /h/addons/.../change-tier
	// AND chora-payments creates the Stripe SubscriptionSchedule.
	// CurrentTier stays at the paid-for tier until the schedule
	// releases; the FE renders "<ScheduledTier> starting <date>" on
	// the tile when both fields are populated.
	ScheduledTier        string
	ScheduledEffectiveAt *time.Time
	// CHO-1784 — when the next billing cycle anchor lands. Populated by
	// Subscribe as ActivatedAt + 30d (first-cycle approximation matching
	// Stripe's default monthly cadence). Surfaced via the snapshot DTO so
	// the FE deactivate modal can use it as effective_at on the
	// end_of_cycle path. A follow-up will refresh this from a chora-
	// payments event so it tracks Stripe's actual current_period_end
	// across renewals (the +30d approximation drifts after the first
	// cycle if the anchor rotates).
	NextRenewalAt *time.Time
}

// IsActive returns whether the subscription is "active" for entitlement
// decisions at the given time.
func (s *Subscription) IsActive(at time.Time) bool {
	switch s.Status {
	case StatusActive, StatusPendingDeactivation:
		return true
	case StatusGracePeriod:
		if s.GraceEndsAt == nil {
			return true
		}
		return at.Before(*s.GraceEndsAt)
	default:
		return false
	}
}

// LifecycleEventKind mirrors the addon_lifecycle_event_kind enum.
type LifecycleEventKind string

const (
	EventActivated             LifecycleEventKind = "activated"
	EventUpgraded              LifecycleEventKind = "upgraded"
	EventDowngraded            LifecycleEventKind = "downgraded"
	EventDeactivationRequested LifecycleEventKind = "deactivation_requested"
	EventDeactivated           LifecycleEventKind = "deactivated"
)

// LifecycleEvent is an append-only audit record for the dashboard sub-tab.
type LifecycleEvent struct {
	EventID            string
	TenantID           string
	AddOnID            string
	AddOnCode          string
	SubscriptionID     string
	Kind               LifecycleEventKind
	FromTier           string
	ToTier             string
	DeactivationReason DeactivationReason
	ReasonText         string
	BillingDeltaCents  int64
	ScheduleID         string
	RequestedByGCID    string
	EffectiveAt        *time.Time
	OccurredAt         time.Time
	EventEnvelopeID    string
	Traceparent        string
}

// UsageSnapshot is a daily usage rollup matching addon_usage_daily.
type UsageSnapshot struct {
	TenantID       string
	AddOnID        string
	BucketDate     time.Time
	SeatsUsed      int64
	SeatsUsedPeak  int64
	QuotaConsumed  int64
	APICalls       int64
	ErrorCount     int64
	LastRecordedAt time.Time
}

const graceWindow = 30 * 24 * time.Hour

// SubscriptionRegistry is the in-memory subscription store.
type SubscriptionRegistry struct {
	mu       sync.Mutex
	cat      *Catalogue
	byKey    map[string]*Subscription
	byTenant map[string][]*Subscription
	clock    func() time.Time
	events   []*LifecycleEvent
	usage    map[string]*UsageSnapshot
}

// NewSubscriptionRegistry returns an empty registry bound to a Catalogue.
func NewSubscriptionRegistry(cat *Catalogue) *SubscriptionRegistry {
	return &SubscriptionRegistry{
		cat:      cat,
		byKey:    make(map[string]*Subscription),
		byTenant: make(map[string][]*Subscription),
		clock:    func() time.Time { return time.Now().UTC() },
		usage:    make(map[string]*UsageSnapshot),
	}
}

// SetClock overrides the clock for testing.
// HasAddOn reports whether the catalogue the registry was built with
// knows the given code. Read-only — adapters use this to validate
// payloads up-front without triggering side-effects on the registry
// (e.g. the wizard's POST /api/v1/tenants/me/addons handler validates
// every code BEFORE calling SubscribePending so that a bad code fails
// the whole request atomically rather than half-subscribing).
func (r *SubscriptionRegistry) HasAddOn(code string) bool {
	_, ok := r.cat.Get(code)
	return ok
}

func (r *SubscriptionRegistry) SetClock(c func() time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock = c
}

// SubscribePending records "declared intent" — the Setup Wizard step 2
// (CHO-1664) entry point. Creates a subscription row in
// `StatusPendingActivation` for net-new (tenant, addOnCode) pairs and
// is a no-op for any pair that already has a subscription in any
// status. Critically it NEVER demotes an existing Active row to
// Pending and NEVER reactivates a Deactivated row — the wizard's job
// is to surface intent, not to transition lifecycle state.
//
// No lifecycle event is recorded for pending creation: the lifecycle
// log captures activation / deactivation transitions, and "created
// in pending state" is a creation, not a transition. The first
// LifecycleEvent for a pending row lands when Subscribe is called
// later (which promotes pending → active and emits EventActivated).
func (r *SubscriptionRegistry) SubscribePending(tenantID, addOnCode, ownerGCID string) (*Subscription, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(addOnCode) == "" {
		return nil, fmt.Errorf("%w: addon_code required", ErrInvalidArgument)
	}
	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return nil, ErrAddOnNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	key := tenantID + "|" + addOnCode
	if existing, ok := r.byKey[key]; ok {
		return existing, nil
	}
	defaultTier := ""
	defaultPrice := a.MonthlyPriceCents
	if len(a.Tiers) > 0 {
		if tier, ok := a.FindTier("starter"); ok {
			defaultTier = tier.Code
			defaultPrice = tier.MonthlyPriceCents
		} else if tier, ok := a.FindTier("default"); ok {
			defaultTier = tier.Code
			defaultPrice = tier.MonthlyPriceCents
		} else {
			defaultTier = a.Tiers[0].Code
			defaultPrice = a.Tiers[0].MonthlyPriceCents
		}
	}
	sub := &Subscription{
		ID:                        NewUUIDv7(),
		TenantID:                  tenantID,
		AddOnCode:                 addOnCode,
		AddOnID:                   a.ID,
		PlanFamily:                a.PlanFamily,
		CurrentTier:               defaultTier,
		MonthlyPriceCentsSnapshot: defaultPrice,
		OwnerGCID:                 ownerGCID,
		Status:                    StatusPendingActivation,
		// ActivatedAt left at zero value — set later when Subscribe
		// promotes this row to Active. UpdatedAt tracks creation.
		UpdatedAt: now,
		Version:   1,
	}
	r.byKey[key] = sub
	r.byTenant[tenantID] = append(r.byTenant[tenantID], sub)
	return sub, nil
}

// Subscribe creates or reactivates a subscription. Idempotent.
func (r *SubscriptionRegistry) Subscribe(tenantID, addOnCode, ownerGCID string) (*Subscription, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(addOnCode) == "" {
		return nil, fmt.Errorf("%w: addon_code required", ErrInvalidArgument)
	}
	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return nil, ErrAddOnNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	key := tenantID + "|" + addOnCode
	if existing, ok := r.byKey[key]; ok {
		switch existing.Status {
		case StatusActive:
			return existing, nil
		case StatusGracePeriod, StatusDeactivated, StatusPendingDeactivation:
			existing.Status = StatusActive
			existing.CancelledAt = nil
			existing.GraceEndsAt = nil
			existing.DeactivatedAt = nil
			existing.DeactivationReason = ""
			existing.DeactivationReasonText = ""
			existing.DeactivationRequestedAt = nil
			existing.DeactivationEffectiveAt = nil
			existing.UpdatedAt = now
			existing.Version++
			r.recordEventLocked(existing, &LifecycleEvent{
				Kind:            EventActivated,
				ToTier:          existing.CurrentTier,
				RequestedByGCID: ownerGCID,
				OccurredAt:      now,
			})
			return existing, nil
		}
	}
	defaultTier := ""
	defaultPrice := a.MonthlyPriceCents
	if len(a.Tiers) > 0 {
		if tier, ok := a.FindTier("starter"); ok {
			defaultTier = tier.Code
			defaultPrice = tier.MonthlyPriceCents
		} else if tier, ok := a.FindTier("default"); ok {
			defaultTier = tier.Code
			defaultPrice = tier.MonthlyPriceCents
		} else {
			defaultTier = a.Tiers[0].Code
			defaultPrice = a.Tiers[0].MonthlyPriceCents
		}
	}
	// CHO-1784 — first-cycle approximation of the Stripe anchor. Matches
	// Stripe's default monthly cadence; follow-up extends the
	// payment_captured event so this gets refreshed with the real
	// current_period_end on activation.
	renewal := now.Add(30 * 24 * time.Hour)
	sub := &Subscription{
		ID:                        NewUUIDv7(),
		TenantID:                  tenantID,
		AddOnCode:                 addOnCode,
		AddOnID:                   a.ID,
		PlanFamily:                a.PlanFamily,
		CurrentTier:               defaultTier,
		MonthlyPriceCentsSnapshot: defaultPrice,
		OwnerGCID:                 ownerGCID,
		Status:                    StatusActive,
		ActivatedAt:               now,
		NextRenewalAt:             &renewal,
		UpdatedAt:                 now,
		Version:                   1,
	}
	r.byKey[key] = sub
	r.byTenant[tenantID] = append(r.byTenant[tenantID], sub)
	r.recordEventLocked(sub, &LifecycleEvent{
		Kind:            EventActivated,
		ToTier:          sub.CurrentTier,
		RequestedByGCID: ownerGCID,
		OccurredAt:      now,
	})
	return sub, nil
}

// Unsubscribe is the legacy 30-day grace path.
func (r *SubscriptionRegistry) Unsubscribe(tenantID, addOnCode, requesterGCID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return ErrAddOnNotFound
	}
	if a.AlwaysOn {
		return ErrCannotUnsubscribeBase
	}
	key := tenantID + "|" + addOnCode
	sub, ok := r.byKey[key]
	if !ok {
		return ErrSubscriptionNotFound
	}
	now := r.clock()
	sub.Status = StatusGracePeriod
	cancelAt := now
	sub.CancelledAt = &cancelAt
	graceEnd := now.Add(graceWindow)
	sub.GraceEndsAt = &graceEnd
	sub.UpdatedAt = now
	sub.Version++
	return nil
}

// Deactivate is the BE-T-ADD-1 structured deactivation path.
func (r *SubscriptionRegistry) Deactivate(tenantID, addOnCode string, req DeactivationRequest) (*Subscription, string, *LifecycleEvent, error) {
	if err := req.Validate(); err != nil {
		return nil, "", nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return nil, "", nil, ErrAddOnNotFound
	}
	if a.AlwaysOn {
		return nil, "", nil, ErrCannotUnsubscribeBase
	}
	if a.ComplianceLocked {
		return nil, "", nil, ErrComplianceLocked
	}
	key := tenantID + "|" + addOnCode
	sub, ok := r.byKey[key]
	if !ok {
		return nil, "", nil, ErrSubscriptionNotFound
	}
	if sub.Status == StatusDeactivated {
		return nil, "", nil, ErrAlreadyDeactivated
	}
	now := r.clock()
	sub.DeactivationReason = req.Reason
	sub.DeactivationReasonText = req.ReasonText
	sub.DeactivationRequestedBy = req.RequestedByGCID
	requestedAt := now
	sub.DeactivationRequestedAt = &requestedAt
	sub.UpdatedAt = now
	sub.Version++

	respStatus := "DEACTIVATED"
	var ev *LifecycleEvent
	if req.EffectiveAt != nil && req.EffectiveAt.After(now) {
		sub.Status = StatusPendingDeactivation
		sub.DeactivationEffectiveAt = req.EffectiveAt
		respStatus = "DEACTIVATION_SCHEDULED"
		eff := *req.EffectiveAt
		ev = r.recordEventLocked(sub, &LifecycleEvent{
			Kind:               EventDeactivationRequested,
			DeactivationReason: req.Reason,
			ReasonText:         req.ReasonText,
			RequestedByGCID:    req.RequestedByGCID,
			EffectiveAt:        &eff,
			OccurredAt:         now,
		})
	} else {
		sub.Status = StatusDeactivated
		sub.DeactivatedAt = &now
		ev = r.recordEventLocked(sub, &LifecycleEvent{
			Kind:               EventDeactivated,
			DeactivationReason: req.Reason,
			ReasonText:         req.ReasonText,
			RequestedByGCID:    req.RequestedByGCID,
			OccurredAt:         now,
		})
	}
	return sub, respStatus, ev, nil
}

// ChangeTier upgrades or downgrades the subscription tier.
func (r *SubscriptionRegistry) ChangeTier(tenantID, addOnCode, targetTierCode, prorationMode string, effectiveAt *time.Time, requesterGCID string, expectedVersion int64) (*Subscription, int64, LifecycleEventKind, error) {
	if !IsValidProration(prorationMode) {
		return nil, 0, "", fmt.Errorf("%w: proration_mode %q", ErrInvalidProration, prorationMode)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return nil, 0, "", ErrAddOnNotFound
	}
	key := tenantID + "|" + addOnCode
	sub, ok := r.byKey[key]
	if !ok {
		return nil, 0, "", ErrSubscriptionNotFound
	}
	if expectedVersion > 0 && sub.Version != expectedVersion {
		return nil, 0, "", ErrConcurrencyConflict
	}
	if sub.Status != StatusActive {
		return nil, 0, "", ErrIneligibleForChange
	}
	target, ok := a.FindTier(targetTierCode)
	if !ok {
		return nil, 0, "", fmt.Errorf("%w: tier %q not in plan family", ErrTierFamilyMismatch, targetTierCode)
	}
	now := r.clock()
	current, _ := a.FindTier(sub.CurrentTier)
	fromTier := sub.CurrentTier
	var delta int64
	if prorationMode == string(ProrationCreate) {
		delta = target.MonthlyPriceCents - current.MonthlyPriceCents
	}
	kind := EventUpgraded
	if target.MonthlyPriceCents < current.MonthlyPriceCents {
		kind = EventDowngraded
	}
	sub.CurrentTier = target.Code
	sub.MonthlyPriceCentsSnapshot = target.MonthlyPriceCents
	sub.UpdatedAt = now
	sub.Version++
	eff := now
	if effectiveAt != nil {
		eff = *effectiveAt
	}
	r.recordEventLocked(sub, &LifecycleEvent{
		Kind:              kind,
		FromTier:          fromTier,
		ToTier:            target.Code,
		BillingDeltaCents: delta,
		RequestedByGCID:   requesterGCID,
		EffectiveAt:       &eff,
		OccurredAt:        now,
	})
	return sub, delta, kind, nil
}

func (r *SubscriptionRegistry) recordEventLocked(sub *Subscription, ev *LifecycleEvent) *LifecycleEvent {
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = r.clock()
	}
	ev.EventID = NewUUIDv7()
	ev.TenantID = sub.TenantID
	ev.AddOnID = sub.AddOnID
	ev.AddOnCode = sub.AddOnCode
	ev.SubscriptionID = sub.ID
	r.events = append(r.events, ev)
	return ev
}

// ListEvents returns lifecycle events for a tenant + add-on, newest first.
func (r *SubscriptionRegistry) ListEvents(tenantID, addOnCode string) []*LifecycleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*LifecycleEvent, 0)
	for _, ev := range r.events {
		if ev.TenantID == tenantID && (addOnCode == "" || ev.AddOnCode == addOnCode) {
			out = append(out, ev)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OccurredAt.After(out[j].OccurredAt) })
	return out
}

// GetSubscription returns the subscription for tenant+addon.
func (r *SubscriptionRegistry) GetSubscription(tenantID, addOnCode string) (*Subscription, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	return sub, ok
}

// MarkPastDue flips the dunning flag on the (tenant, addon) row to true.
// Driven by the chora.payments.tenant_addon_purchase.subscription_payment_failed.v1
// subscriber. CHO-1776.
//
// Idempotent on already-past_due (no-op). Returns ErrSubscriptionNotFound
// if the row doesn't exist, and ErrNotActiveForPastDue if the row isn't
// currently ACTIVE — dunning only applies to active subscriptions
// (deactivated / grace_period rows are billed by Stripe via their own
// cycle and don't need a banner).
func (r *SubscriptionRegistry) MarkPastDue(tenantID, addOnCode string) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	if sub.PastDue {
		return sub, nil
	}
	if sub.Status != StatusActive {
		return nil, ErrNotActiveForPastDue
	}
	sub.PastDue = true
	sub.UpdatedAt = r.clock()
	return sub, nil
}

// MarkRecovered clears the dunning flag on the (tenant, addon) row.
// Driven by the chora.payments.tenant_addon_purchase.payment_recovered.v1
// subscriber after Stripe smart-retry succeeded. CHO-1776.
//
// Idempotent on already-not-past_due (no-op). Returns ErrSubscriptionNotFound
// if missing. Does NOT validate Status — a recovered-then-cancelled
// sequence can land out-of-order and we'd rather clear stale dunning
// state than refuse.
func (r *SubscriptionRegistry) MarkRecovered(tenantID, addOnCode string) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	if !sub.PastDue {
		return sub, nil
	}
	sub.PastDue = false
	sub.UpdatedAt = r.clock()
	return sub, nil
}

// SetScheduledTier records a pending end-of-cycle tier change on the
// (tenant, addon) row. Called by handleAdminChangeAddonTierViaPayments
// after chora-payments confirms the Stripe SubscriptionSchedule was
// created. Idempotent — calling with the same tier + effective_at is
// a no-op. CHO-1772.
func (r *SubscriptionRegistry) SetScheduledTier(tenantID, addOnCode, tier string, effectiveAt time.Time) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	utc := effectiveAt.UTC()
	sub.ScheduledTier = tier
	sub.ScheduledEffectiveAt = &utc
	sub.UpdatedAt = r.clock()
	return sub, nil
}

// PromoteScheduledTier flips CurrentTier ← ScheduledTier + clears the
// schedule fields + records an upgraded/downgraded lifecycle event.
// Called by the payments subscriber on
// chora.payments.tenant_addon_purchase.subscription_schedule_released.v1
// when Stripe activates the deferred tier change at cycle anchor.
//
// Idempotent: if no schedule is pending (duplicate Stripe webhook
// delivery, or schedule already promoted), returns the current sub +
// empty LifecycleEventKind (signalling no event recorded) + nil error.
//
// Fails-loud (ErrTierFamilyMismatch) when ScheduledTier no longer
// exists in the catalogue — the price snapshot would be wrong + the
// admin can't reason about it. CHO-1779.
func (r *SubscriptionRegistry) PromoteScheduledTier(tenantID, addOnCode string) (*Subscription, LifecycleEventKind, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, "", ErrSubscriptionNotFound
	}
	if sub.ScheduledTier == "" || sub.ScheduledEffectiveAt == nil {
		return sub, "", nil
	}

	a, ok := r.cat.Get(addOnCode)
	if !ok {
		return nil, "", ErrAddOnNotFound
	}
	target, ok := a.FindTier(sub.ScheduledTier)
	if !ok {
		return nil, "", fmt.Errorf("%w: scheduled tier %q not in plan family", ErrTierFamilyMismatch, sub.ScheduledTier)
	}

	now := r.clock()
	fromTier := sub.CurrentTier
	current, _ := a.FindTier(fromTier)
	kind := EventUpgraded
	if target.MonthlyPriceCents < current.MonthlyPriceCents {
		kind = EventDowngraded
	}
	effectiveAt := *sub.ScheduledEffectiveAt

	sub.CurrentTier = target.Code
	sub.MonthlyPriceCentsSnapshot = target.MonthlyPriceCents
	sub.ScheduledTier = ""
	sub.ScheduledEffectiveAt = nil
	sub.UpdatedAt = now
	sub.Version++

	r.recordEventLocked(sub, &LifecycleEvent{
		Kind:              kind,
		FromTier:          fromTier,
		ToTier:            target.Code,
		BillingDeltaCents: target.MonthlyPriceCents - current.MonthlyPriceCents,
		RequestedByGCID:   "", // Stripe-driven; no requester GCID
		EffectiveAt:       &effectiveAt,
		OccurredAt:        now,
	})
	return sub, kind, nil
}

// CancelScheduledDeactivation reverses a pending end-of-cycle deactivation
// queued by the FE "End of billing cycle" modal flow (CHO-1782). Flips
// Status PENDING_DEACTIVATION → ACTIVE, clears every deactivation_*
// field, records EventActivated. Distinct from AnchorDeactivation
// (CHO-1783) — that closes the row at the Stripe anchor; this is the
// opposite direction (admin clicked "(undo)" on the amber pill).
//
// Idempotent: if already ACTIVE (duplicate click, race), returns the
// current sub + nil event + nil error. Returns ErrAlreadyDeactivated
// if the row is already DEACTIVATED (can't undo a closed row; admin
// must re-subscribe). Returns ErrSubscriptionNotFound on miss. CHO-1785.
func (r *SubscriptionRegistry) CancelScheduledDeactivation(tenantID, addOnCode, requesterGCID string) (*Subscription, *LifecycleEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, nil, ErrSubscriptionNotFound
	}
	if sub.Status == StatusDeactivated {
		return nil, nil, ErrAlreadyDeactivated
	}
	if sub.Status == StatusActive {
		return sub, nil, nil
	}
	now := r.clock()
	sub.Status = StatusActive
	sub.DeactivationEffectiveAt = nil
	sub.DeactivationRequestedAt = nil
	sub.DeactivationRequestedBy = ""
	sub.DeactivationReason = ""
	sub.DeactivationReasonText = ""
	sub.UpdatedAt = now
	sub.Version++
	ev := r.recordEventLocked(sub, &LifecycleEvent{
		Kind:            EventActivated,
		ToTier:          sub.CurrentTier,
		RequestedByGCID: requesterGCID,
		OccurredAt:      now,
	})
	return sub, ev, nil
}

// AnchorDeactivation flips Status → DEACTIVATED at the Stripe cycle
// anchor. Called by the payments subscriber on
// chora.payments.tenant_addon_purchase.cancelled.v1 when Stripe fires
// `customer.subscription.deleted` after the FE end-of-cycle
// deactivation path's schedule expires.
//
// Distinct from Unsubscribe (legacy 30-day grace path): AnchorDeactivation
// is the destructive parallel of PromoteScheduledTier — Stripe has
// already declared the subscription cancelled, so the registry flips
// straight to DEACTIVATED without tacking on more grace. PENDING_DEACTIVATION
// rows reach here naturally; ACTIVE rows (Stripe Dashboard cancel without
// FE flow) also flip directly since Stripe is the source of truth.
//
// Idempotent: if already DEACTIVATED (duplicate webhook delivery), returns
// the current sub + nil event + nil error. Returns ErrSubscriptionNotFound
// on miss. CHO-1783.
func (r *SubscriptionRegistry) AnchorDeactivation(tenantID, addOnCode string) (*Subscription, *LifecycleEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, nil, ErrSubscriptionNotFound
	}
	if sub.Status == StatusDeactivated {
		return sub, nil, nil
	}
	now := r.clock()
	sub.Status = StatusDeactivated
	sub.DeactivatedAt = &now
	sub.DeactivationEffectiveAt = nil
	sub.GraceEndsAt = nil
	sub.UpdatedAt = now
	sub.Version++
	ev := r.recordEventLocked(sub, &LifecycleEvent{
		Kind:       EventDeactivated,
		OccurredAt: now,
	})
	return sub, ev, nil
}

// ClearScheduledTier removes a pending end-of-cycle tier change.
// Driven by the subscription_schedule.released subscriber after
// Stripe applies the new tier; the registry's CurrentTier is then
// the post-promotion value. CHO-1772.
func (r *SubscriptionRegistry) ClearScheduledTier(tenantID, addOnCode string) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.byKey[tenantID+"|"+addOnCode]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	sub.ScheduledTier = ""
	sub.ScheduledEffectiveAt = nil
	sub.UpdatedAt = r.clock()
	return sub, nil
}

// ListActiveByTenant returns subs that are still active.
// KnowsTenant reports whether the registry holds a subscription row for the
// tenant in ANY status. It is deliberately NOT the same question as "does the
// tenant have anything active": Deactivate and Unsubscribe flip Status and
// never remove the row, so a tenant whose add-ons have all been switched off
// stays known and reads as an empty active list.
//
// The composition root's entitlement store depends on exactly that difference.
// It falls through to the durable add_on_subscriptions table when the registry
// has never seen a tenant, which is how a tenant created after the boot
// hydrator ran is still readable. Were an emptied tenant to read as unknown,
// that fall-through would return the PG rows, which still say active, and
// would undo every runtime deactivation.
func (r *SubscriptionRegistry) KnowsTenant(tenantID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byTenant[tenantID]) > 0
}

// Evict forgets every subscription the registry holds for a tenant.
//
// It exists for ONE caller: the entitlement write-through's failure path (UX
// Track U, row E4). The rule that row enforces is that memory must never be
// ahead of the database. The registry cannot honour that by persisting first,
// because every mutation method takes the mutex, applies the state machine and
// returns the mutated subscription in one step, so there is no way to ask what
// the result WOULD be without already having applied it. Instead the call site
// mutates, persists, and on a persist FAILURE calls this.
//
// After eviction memory is not ahead, it is ABSENT: KnowsTenant reports false,
// which is what makes the entitlement read fall through to the durable
// add_on_subscriptions table. So a failed write degrades to reading the source
// of truth rather than serving a state PostgreSQL does not hold.
//
// ⚠ WHAT IT DOES NOT CLOSE. Between the in-memory mutation and the commit, a
// reader on the SAME POD can observe a state that is not yet durable. That
// window is one write long. Closing it needs the lock or the transaction to
// span both, which is the load-modify-save shape the read-side retirement
// points at, and is deliberately not done here.
//
// TENANT-WIDE on purpose. Dropping only the failed (tenant, code) pair would
// leave the registry authoritative for that tenant's other add-ons and stale
// for one, which is the half-truth this work exists to remove. The cost is a
// cache refill for that tenant on its next read.
//
// Idempotent, and safe for a tenant the registry has never seen: it is called
// on an error path where the tenant may already be gone, and a panic there
// would turn a recoverable write failure into a crash.
func (r *SubscriptionRegistry) Evict(tenantID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sub := range r.byTenant[tenantID] {
		delete(r.byKey, tenantID+"|"+sub.AddOnCode)
	}
	delete(r.byTenant, tenantID)
}

func (r *SubscriptionRegistry) ListActiveByTenant(tenantID string) []*Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	all := r.byTenant[tenantID]
	out := make([]*Subscription, 0, len(all))
	for _, s := range all {
		if s.IsActive(now) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// ListAllByTenant returns all subs.
func (r *SubscriptionRegistry) ListAllByTenant(tenantID string) []*Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.byTenant[tenantID]
	out := append([]*Subscription(nil), all...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// RecordUsage upserts a usage snapshot for the given bucket date.
func (r *SubscriptionRegistry) RecordUsage(snap *UsageSnapshot) {
	if snap == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := snap.TenantID + "|" + snap.AddOnID + "|" + snap.BucketDate.Format("2006-01-02")
	r.usage[key] = snap
}

// ListUsage returns usage snapshots for a tenant + addon within [from, to).
func (r *SubscriptionRegistry) ListUsage(tenantID, addOnID string, from, to time.Time) []*UsageSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*UsageSnapshot, 0)
	for _, snap := range r.usage {
		if snap.TenantID != tenantID || snap.AddOnID != addOnID {
			continue
		}
		if !snap.BucketDate.Before(from) && snap.BucketDate.Before(to) {
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketDate.Before(out[j].BucketDate) })
	return out
}

// PseudonymiseGCID returns a tenant+window-scoped pseudonym for a GCID.
func PseudonymiseGCID(tenantID, windowKey, gcid string) string {
	h := fnv.New128a()
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(windowKey))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(gcid))
	sum := h.Sum(nil)
	return "u_" + hex.EncodeToString(sum[:8])
}
