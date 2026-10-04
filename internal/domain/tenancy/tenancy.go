// Package tenancy is the pure-domain core of the Tenancy + Billing combined
// supporting domain.
//
// It owns the 5 aggregates called out in the M11+ Phase D brief:
//
//   - Tenant            — primary aggregate; display_name, branding_config,
//                         parent_tenant_id (franchise), self_hosted, status
//   - TenantEntitlement — tenant_id + add_on_id + status (composable)
//   - AddOn             — catalog of marketplace items (no tier hierarchy)
//   - Invoice           — period billing with line_items per AddOn
//   - PaymentMethod     — Stripe stub (real Stripe calls come later)
//
// Hard rules (per .claude/rules/ddd-enforcement.md + CLAUDE.md §1):
//
//   - Composable Add-On Plans — NOT tier pricing. A tenant can hold any
//     combination of entitlements; there is no plan hierarchy enforced at
//     the domain level. Each entitlement is a feature flag.
//   - UUIDv7 IDs (sortable + lexicographically equivalent to creation order).
//   - Soft delete on Tenant via DeletedAt + Status transition.
//   - Cross-domain references travel as UUIDs (no FK across DBs).
//   - Stripe is a stub here — `stripe_payment_method_id` and
//     `stripe_invoice_id` are pass-through fields; real Stripe API integration
//     comes in M12+ via an adapter package.
//
// This file contains the domain only — NO HTTP, NO persistence.
package tenancy

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

var (
	// ErrInvalidArgument signals a guard-clause failure inside a constructor.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrSelfParent is returned when a tenant is set as its own parent.
	ErrSelfParent = errors.New("tenant cannot be its own parent")

	// ErrEntitlementNotFound is returned when cancelling an unknown
	// (tenant_id, add_on_id) pair. Per the brief: cancel idempotent only
	// applies when the entitlement EXISTS — unknown still returns a 404.
	ErrEntitlementNotFound = errors.New("entitlement not found")

	// ErrInvalidPeriod is returned when an Invoice period_end is not strictly
	// after period_start.
	ErrInvalidPeriod = errors.New("invalid invoice period")
)

// -----------------------------------------------------------------------------
// UUIDv7 — minimal local generator (deps-free skeleton)
// -----------------------------------------------------------------------------

// NewUUIDv7 returns a freshly generated UUIDv7 string.
//
// We generate UUIDv7 ourselves to keep the skeleton dependency-free. The
// format follows RFC 9562 §5.7: unix_ts_ms (48 bits) || ver (4) || rand_a
// (12) || var (2) || rand_b (62).
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
		// rand.Read failure is exceedingly rare; fall back to time bits
		// so the service stays up under entropy starvation.
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	// Set version (7) on the high nibble of byte 6.
	b[6] = (b[6] & 0x0F) | 0x70
	// Set IETF variant on the high two bits of byte 8.
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// Tenant
// -----------------------------------------------------------------------------

// TenantStatus is the high-level lifecycle state of a Tenant.
type TenantStatus string

const (
	TenantStatusActive    TenantStatus = "active"
	TenantStatusSuspended TenantStatus = "suspended"
	TenantStatusClosed    TenantStatus = "closed"
)

// Tenant is the primary aggregate root for the Tenancy + Billing combined
// supporting domain.
//
// `BrandingConfig` is a free-form JSON-serialisable map for now; the H+ Hub+
// surface will lock its schema in M13. `ParentTenantID` enables the franchise
// model — a parent tenant can carry child tenants for multi-school rollups.
// `SelfHosted` flips the deployment posture: the H+ marketplace shows
// different add-on availability for self-hosted vs SaaS tenants.
type Tenant struct {
	ID             string
	DisplayName    string
	BrandingConfig map[string]interface{}
	ParentTenantID string
	SelfHosted     bool
	Status         TenantStatus
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      *time.Time
}

// NewTenant constructs a Tenant aggregate with default values populated.
//
// Validation guards rejected with ErrInvalidArgument:
//   - display_name trim-non-empty
//
// `parent_tenant_id` is accepted at construction time (caller's responsibility
// to verify external existence). For the safe self-reference guard, callers
// should use `tenant.SetParent` after construction.
func NewTenant(displayName string, selfHosted bool, parentTenantID string) (*Tenant, error) {
	if strings.TrimSpace(displayName) == "" {
		return nil, fmt.Errorf("%w: display_name required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &Tenant{
		ID:             NewUUIDv7(),
		DisplayName:    strings.TrimSpace(displayName),
		BrandingConfig: map[string]interface{}{},
		ParentTenantID: parentTenantID,
		SelfHosted:     selfHosted,
		Status:         TenantStatusActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// SetParent binds a parent tenant for franchise rollups, rejecting
// self-reference cycles.
func (t *Tenant) SetParent(parentTenantID string) error {
	if parentTenantID == t.ID {
		return ErrSelfParent
	}
	t.ParentTenantID = parentTenantID
	t.UpdatedAt = time.Now().UTC()
	return nil
}

// Suspend transitions the tenant to suspended status (reversible).
func (t *Tenant) Suspend() {
	if t.Status == TenantStatusClosed {
		return
	}
	t.Status = TenantStatusSuspended
	t.UpdatedAt = time.Now().UTC()
}

// Reactivate moves a suspended tenant back to active.
func (t *Tenant) Reactivate() {
	if t.Status == TenantStatusClosed {
		return
	}
	t.Status = TenantStatusActive
	t.UpdatedAt = time.Now().UTC()
}

// Close transitions the tenant to closed status and stamps DeletedAt.
// Idempotent: calling twice does not bump DeletedAt. Per
// .claude/rules/ddd-enforcement.md "soft delete + pseudonymise".
func (t *Tenant) Close() {
	if t.Status == TenantStatusClosed {
		return
	}
	now := time.Now().UTC()
	t.Status = TenantStatusClosed
	t.DeletedAt = &now
	t.UpdatedAt = now
}

// -----------------------------------------------------------------------------
// AddOn (catalog item)
// -----------------------------------------------------------------------------

// AddOn is a marketplace item — a feature flag bundle with a monthly price.
//
// HARD RULE: there is NO tier hierarchy. Two add-ons may overlap in
// included capabilities; the tenant pays for whichever they activate. The
// H+ Hub+ marketplace surface presents these as a flat catalog.
type AddOn struct {
	ID                   string
	Name                 string
	Description          string
	MonthlyPriceCents    int64
	IncludedCapabilities []string
	CreatedAt            time.Time
}

// NewAddOn constructs an AddOn catalog item.
func NewAddOn(name, description string, monthlyPriceCents int64, capabilities []string) (*AddOn, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if monthlyPriceCents < 0 {
		return nil, fmt.Errorf("%w: monthly_price_cents must be >= 0", ErrInvalidArgument)
	}
	caps := append([]string(nil), capabilities...)
	return &AddOn{
		ID:                   NewUUIDv7(),
		Name:                 strings.TrimSpace(name),
		Description:          description,
		MonthlyPriceCents:    monthlyPriceCents,
		IncludedCapabilities: caps,
		CreatedAt:            time.Now().UTC(),
	}, nil
}

// AddOnCatalog is the in-memory registry of available add-ons.
type AddOnCatalog struct {
	mu sync.RWMutex
	by map[string]*AddOn
}

// NewAddOnCatalog returns an empty catalog.
func NewAddOnCatalog() *AddOnCatalog { return &AddOnCatalog{by: make(map[string]*AddOn)} }

// Register adds an AddOn to the catalog (admin-only stub).
func (c *AddOnCatalog) Register(a *AddOn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.by[a.ID] = a
}

// Get returns an AddOn by ID and a bool ok flag.
func (c *AddOnCatalog) Get(id string) (*AddOn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.by[id]
	return a, ok
}

// List returns all add-ons sorted by ID (UUIDv7 ⇒ creation order).
func (c *AddOnCatalog) List() []*AddOn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*AddOn, 0, len(c.by))
	for _, a := range c.by {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// -----------------------------------------------------------------------------
// TenantEntitlement (composable, NOT tier pricing)
// -----------------------------------------------------------------------------

// EntitlementStatus is the lifecycle state of a TenantEntitlement.
type EntitlementStatus string

const (
	EntitlementStatusActive    EntitlementStatus = "active"
	EntitlementStatusCancelled EntitlementStatus = "cancelled"
)

// TenantEntitlement records a tenant's subscription to a single AddOn.
//
// Composability: a tenant can hold N entitlements simultaneously; there is
// NO tier hierarchy. The H+ Hub+ marketplace surface lets a tenant subscribe
// or cancel each independently.
//
// `MonthlyPriceCentsSnapshot` captures the price at activation; future price
// changes in the catalog do NOT retroactively change the snapshot, ensuring
// invoice generation is reproducible from the entitlement record alone.
type TenantEntitlement struct {
	ID       string
	TenantID string
	AddOnID  string
	// AddOnCode is the stable snake_case machine code of the subscribed
	// add-on (add_ons.code, migration 0021 — CHO-1717 §4.6 addon_code
	// bridge). Read-model projection JOINed from the add-on catalog by
	// the pg adapter; the FE FeatureFlagService gates features via
	// isEnabled('<code>'). Empty for legacy rows whose catalog entry
	// predates the code backfill (the wire DTO pin allows addon_code:"").
	AddOnCode                 string
	MonthlyPriceCentsSnapshot int64
	Status                    EntitlementStatus
	ActivatedAt               time.Time
	CancelledAt               *time.Time
	UpdatedAt                 time.Time
}

// NewTenantEntitlement constructs an active TenantEntitlement.
func NewTenantEntitlement(tenantID, addOnID string, monthlyPriceCents int64) (*TenantEntitlement, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(addOnID) == "" {
		return nil, fmt.Errorf("%w: addon_id required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &TenantEntitlement{
		ID:                        NewUUIDv7(),
		TenantID:                  tenantID,
		AddOnID:                   addOnID,
		MonthlyPriceCentsSnapshot: monthlyPriceCents,
		Status:                    EntitlementStatusActive,
		ActivatedAt:               now,
		UpdatedAt:                 now,
	}, nil
}

// Cancel marks the entitlement cancelled. Idempotent: a second call does NOT
// bump CancelledAt.
func (e *TenantEntitlement) Cancel() {
	if e.Status == EntitlementStatusCancelled {
		return
	}
	now := time.Now().UTC()
	e.Status = EntitlementStatusCancelled
	e.CancelledAt = &now
	e.UpdatedAt = now
}

// Reactivate moves a cancelled entitlement back to active and refreshes the
// price snapshot. Used when a tenant re-subscribes to an add-on.
func (e *TenantEntitlement) Reactivate(newPriceCents int64) {
	now := time.Now().UTC()
	e.Status = EntitlementStatusActive
	e.MonthlyPriceCentsSnapshot = newPriceCents
	e.CancelledAt = nil
	e.UpdatedAt = now
}

// EntitlementRegistry holds (tenant_id, add_on_id) → TenantEntitlement.
//
// The (tenant_id, add_on_id) pair is unique: a tenant cannot hold TWO
// entitlements for the same add-on. Re-subscribing reactivates the existing
// record (cancelled → active) rather than creating a duplicate.
type EntitlementRegistry struct {
	mu       sync.Mutex
	byKey    map[string]*TenantEntitlement // key = tenant_id|addon_id
	byTenant map[string][]*TenantEntitlement
}

// NewEntitlementRegistry returns an empty registry.
func NewEntitlementRegistry() *EntitlementRegistry {
	return &EntitlementRegistry{
		byKey:    make(map[string]*TenantEntitlement),
		byTenant: make(map[string][]*TenantEntitlement),
	}
}

// Subscribe creates a new active TenantEntitlement OR reactivates a
// previously-cancelled one. Idempotent for already-active entries (returns
// existing). Per the brief, in real impl this also emits a Stripe event;
// in the skeleton we only manage in-memory state.
func (r *EntitlementRegistry) Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*TenantEntitlement, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(addOnID) == "" {
		return nil, fmt.Errorf("%w: addon_id required", ErrInvalidArgument)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tenantID + "|" + addOnID
	if existing, ok := r.byKey[key]; ok {
		if existing.Status == EntitlementStatusActive {
			return existing, nil
		}
		// Reactivate cancelled entitlement.
		existing.Reactivate(monthlyPriceCents)
		return existing, nil
	}
	e, err := NewTenantEntitlement(tenantID, addOnID, monthlyPriceCents)
	if err != nil {
		return nil, err
	}
	r.byKey[key] = e
	r.byTenant[tenantID] = append(r.byTenant[tenantID], e)
	return e, nil
}

// Cancel marks the entitlement cancelled. Idempotent: cancelling an already
// cancelled entitlement returns the existing record (NOT 404). Cancelling an
// unknown (tenant_id, add_on_id) pair returns ErrEntitlementNotFound.
func (r *EntitlementRegistry) Cancel(tenantID, addOnID string) (*TenantEntitlement, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tenantID + "|" + addOnID
	e, ok := r.byKey[key]
	if !ok {
		return nil, ErrEntitlementNotFound
	}
	e.Cancel()
	return e, nil
}

// ListActiveByTenant returns all active entitlements for a tenant, sorted
// by ID (UUIDv7 ⇒ creation order).
func (r *EntitlementRegistry) ListActiveByTenant(tenantID string) []*TenantEntitlement {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.byTenant[tenantID]
	out := make([]*TenantEntitlement, 0, len(all))
	for _, e := range all {
		if e.Status == EntitlementStatusActive {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// ListAllByTenant returns all entitlements (both active and cancelled).
func (r *EntitlementRegistry) ListAllByTenant(tenantID string) []*TenantEntitlement {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.byTenant[tenantID]
	out := make([]*TenantEntitlement, 0, len(all))
	out = append(out, all...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

// -----------------------------------------------------------------------------
// Invoice
// -----------------------------------------------------------------------------

// InvoiceLineItem is a single charge on an Invoice. We keep the snapshot
// data here so an invoice is reproducible without re-reading the entitlement.
type InvoiceLineItem struct {
	AddOnID         string
	EntitlementID   string
	UnitPriceCents  int64
	Quantity        int
	LineTotalCents  int64
}

// Invoice is a billing record for a tenant's period.
//
// `StripeInvoiceID` is a pass-through stub here — in the real impl, the
// adapter layer creates a Stripe invoice via the Stripe API and writes the
// returned ID back here.
type Invoice struct {
	ID              string
	TenantID        string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	LineItems       []InvoiceLineItem
	TotalCents      int64
	StripeInvoiceID string // populated by Stripe adapter (stub here)
	CreatedAt       time.Time
}

// GenerateInvoice sums all active entitlement prices for the period.
//
// Per the brief: invoice line item sum = sum of all active entitlement
// prices. This is the skeleton — the real impl will pro-rate by partial
// month, apply tenant-level discounts, and call out to Stripe.
func GenerateInvoice(tenantID string, periodStart, periodEnd time.Time, activeEntitlements []*TenantEntitlement) (*Invoice, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if !periodEnd.After(periodStart) {
		return nil, fmt.Errorf("%w: period_end must be after period_start", ErrInvalidPeriod)
	}
	items := make([]InvoiceLineItem, 0, len(activeEntitlements))
	var total int64
	for _, e := range activeEntitlements {
		if e.Status != EntitlementStatusActive {
			continue
		}
		items = append(items, InvoiceLineItem{
			AddOnID:        e.AddOnID,
			EntitlementID:  e.ID,
			UnitPriceCents: e.MonthlyPriceCentsSnapshot,
			Quantity:       1,
			LineTotalCents: e.MonthlyPriceCentsSnapshot,
		})
		total += e.MonthlyPriceCentsSnapshot
	}
	return &Invoice{
		ID:          NewUUIDv7(),
		TenantID:    tenantID,
		PeriodStart: periodStart.UTC(),
		PeriodEnd:   periodEnd.UTC(),
		LineItems:   items,
		TotalCents:  total,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// PaymentMethod (Stripe stub)
// -----------------------------------------------------------------------------

// PaymentMethodStatus is the lifecycle of a tenant payment method.
type PaymentMethodStatus string

const (
	PaymentMethodStatusActive   PaymentMethodStatus = "active"
	PaymentMethodStatusDetached PaymentMethodStatus = "detached"
)

// PaymentMethod is a Stripe payment method handle for a tenant.
//
// `StripePaymentMethodID` is opaque to this domain — the real impl creates
// the Stripe object, gets back a `pm_xxx` id, and stores it here. The
// skeleton is a placeholder that does NOT call Stripe.
type PaymentMethod struct {
	ID                    string
	TenantID              string
	StripePaymentMethodID string
	Status                PaymentMethodStatus
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// NewPaymentMethod constructs a PaymentMethod (Stripe stub — no API call).
func NewPaymentMethod(tenantID, stripePaymentMethodID string) (*PaymentMethod, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(stripePaymentMethodID) == "" {
		return nil, fmt.Errorf("%w: stripe_payment_method_id required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &PaymentMethod{
		ID:                    NewUUIDv7(),
		TenantID:              tenantID,
		StripePaymentMethodID: stripePaymentMethodID,
		Status:                PaymentMethodStatusActive,
		CreatedAt:             now,
		UpdatedAt:             now,
	}, nil
}

// Detach marks the payment method detached (the Stripe equivalent of
// removing a card from a customer).
func (p *PaymentMethod) Detach() {
	if p.Status == PaymentMethodStatusDetached {
		return
	}
	p.Status = PaymentMethodStatusDetached
	p.UpdatedAt = time.Now().UTC()
}
