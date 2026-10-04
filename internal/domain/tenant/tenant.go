// Package tenant is the pure-domain core of the Tenant lifecycle sub-domain.
package tenant

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidArgument        = errors.New("invalid argument")
	ErrSelfParent             = errors.New("tenant cannot be its own parent")
	ErrCycleDetected          = errors.New("parent assignment would create a cycle")
	ErrParentNotFound         = errors.New("parent tenant not found")
	ErrTenantNotFound         = errors.New("tenant not found")
	ErrInvalidSlug            = errors.New("invalid tenant slug")
	ErrInvalidCurrency        = errors.New("invalid currency (3-letter ISO 4217)")
	ErrInvalidCountry         = errors.New("invalid country (ISO 3166-1 alpha-2)")
	ErrAlreadyActive          = errors.New("tenant already active")
	ErrInvalidStatusForGoLive = errors.New("tenant status does not permit go-live")
)

// ChoraMasterTenantID is the immutable tenant_id of the franchisor ROOT tenant
// `chora-master` (slug `chora-master`), seeded by migration 0026 (ADR-217
// Tenancy Model β). It is the single hierarchy root: every other tenant is a
// descendant via parent_tenant_id, and the chora-tenancy parent-invariant
// trigger `trg_tenants_parent_in_master_tree` rejects any parent that does not
// resolve into this tree. Self-onboarded tenants parent directly to it.
const ChoraMasterTenantID = "00000000-0000-7000-8000-000000000001"

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

type Status string

const (
	// StatusPendingActivation is the initial state of a freshly-created
	// light-wizard tenant. Only the GoLive transition (creates Stripe
	// Customer + allocates default ManaPool) flips it to Active.
	StatusPendingActivation Status = "pending_activation"
	StatusActive            Status = "active"
	StatusSuspended         Status = "suspended"
	StatusClosed            Status = "closed"
)

type BrandingConfig struct {
	PrimaryColorHex string
	LogoURL         string
	CustomDomain    string
}

type Tenant struct {
	ID               string
	DisplayName      string
	OwnerGCID        string
	ParentTenantID   string
	SelfHosted       bool
	Branding         BrandingConfig
	Status           Status
	SuspensionReason string
	ClosureReason    string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DeletedAt        *time.Time

	// Phyllis MVP §5.2 light-wizard fields:
	// - Slug      idempotency key for POST /v1/tenants
	// - Country   ISO 3166-1 alpha-2 (default "SG")
	// - Currency  ISO 4217 (default "SGD")
	Slug             string
	Country          string
	DefaultCurrency  string
	StripeCustomerID string
	ActivatedAt      *time.Time

	// WizardCompletedAt is stamped by Setup Wizard step 4 Apply (CHO-1682)
	// and is set-once — see Tenant.FinishWizard. NULL on every fresh
	// tenant; subsequent re-finish calls are no-ops.
	WizardCompletedAt *time.Time
}

// NowUTC returns the canonical "now" used by the domain — a thin alias
// so tests + the domain share one clock source.
func NowUTC() time.Time { return time.Now().UTC() }

// FinishWizard marks the Setup Wizard as complete with set-once
// semantics. Returns `newly = true` on the first call (and stamps
// WizardCompletedAt to `now`); subsequent calls are no-ops returning
// `newly = false` with the original timestamp preserved.
//
// The HTTP handler uses `newly` to decide whether to emit
// `chora.tenancy.tenant_wizard_completed.v1` on the outbox — so
// downstream consumers never see duplicate completion events.
func (t *Tenant) FinishWizard(now time.Time) bool {
	if t.WizardCompletedAt != nil {
		return false
	}
	stamped := now
	t.WizardCompletedAt = &stamped
	t.UpdatedAt = now
	return true
}

// LightTenantInput captures the Phyllis MVP §5.2 minimum form fields. All
// optional fields default to SG / SGD. Slug is required + validated as the
// idempotency key for POST /v1/tenants.
type LightTenantInput struct {
	DisplayName     string
	OwnerGCID       string
	Country         string
	DefaultCurrency string
	Slug            string
	SelfHosted      bool
}

// slugRE is the canonical tenant slug pattern: lowercase ASCII letters,
// digits, dashes, with no leading/trailing dash, length 3-64.
var slugRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{1,62}[a-z0-9])?$`)
var currencyRE = regexp.MustCompile(`^[A-Z]{3}$`)
var countryRE = regexp.MustCompile(`^[A-Z]{2}$`)

// NewLightTenant constructs a Tenant in the pending_activation state from
// the Phyllis MVP §5.2 light-wizard inputs. Country defaults to "SG";
// DefaultCurrency to "SGD"; SelfHosted to false.
func NewLightTenant(in LightTenantInput) (*Tenant, error) {
	in.DisplayName = strings.TrimSpace(in.DisplayName)
	in.OwnerGCID = strings.TrimSpace(in.OwnerGCID)
	in.Slug = strings.TrimSpace(in.Slug)
	in.Country = strings.ToUpper(strings.TrimSpace(in.Country))
	in.DefaultCurrency = strings.ToUpper(strings.TrimSpace(in.DefaultCurrency))

	if in.DisplayName == "" {
		return nil, fmt.Errorf("%w: display_name required", ErrInvalidArgument)
	}
	if in.OwnerGCID == "" {
		return nil, fmt.Errorf("%w: owner_gcid required", ErrInvalidArgument)
	}
	// Slug strict-mode: must already be lowercase ASCII; reject upper/space.
	if !slugRE.MatchString(in.Slug) {
		return nil, fmt.Errorf("%w: slug must match [a-z0-9][a-z0-9-]{1,62}[a-z0-9]", ErrInvalidSlug)
	}
	if in.Country == "" {
		in.Country = "SG"
	}
	if !countryRE.MatchString(in.Country) {
		return nil, fmt.Errorf("%w: country %q", ErrInvalidCountry, in.Country)
	}
	if in.DefaultCurrency == "" {
		in.DefaultCurrency = "SGD"
	}
	if !currencyRE.MatchString(in.DefaultCurrency) {
		return nil, fmt.Errorf("%w: currency %q", ErrInvalidCurrency, in.DefaultCurrency)
	}
	now := time.Now().UTC()
	return &Tenant{
		ID:              NewUUIDv7(),
		DisplayName:     in.DisplayName,
		OwnerGCID:       in.OwnerGCID,
		Slug:            in.Slug,
		Country:         in.Country,
		DefaultCurrency: in.DefaultCurrency,
		SelfHosted:      in.SelfHosted,
		Status:          StatusPendingActivation,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

// GoLive transitions the tenant from pending_activation → active and stamps
// the StripeCustomerID + ActivatedAt. Idempotency rule: a tenant already
// active rejects a second GoLive call (caller must use the cached result).
// Closed/suspended tenants reject the transition entirely.
func (t *Tenant) GoLive(stripeCustomerID string) error {
	stripeCustomerID = strings.TrimSpace(stripeCustomerID)
	if stripeCustomerID == "" {
		return fmt.Errorf("%w: stripe_customer_id required", ErrInvalidArgument)
	}
	switch t.Status {
	case StatusActive:
		return ErrAlreadyActive
	case StatusClosed, StatusSuspended:
		return fmt.Errorf("%w: status=%s", ErrInvalidStatusForGoLive, t.Status)
	}
	now := time.Now().UTC()
	t.Status = StatusActive
	t.StripeCustomerID = stripeCustomerID
	t.ActivatedAt = &now
	t.UpdatedAt = now
	return nil
}

func NewTenant(displayName, ownerGCID string, selfHosted bool) (*Tenant, error) {
	if strings.TrimSpace(displayName) == "" {
		return nil, fmt.Errorf("%w: display_name required", ErrInvalidArgument)
	}
	if strings.TrimSpace(ownerGCID) == "" {
		return nil, fmt.Errorf("%w: owner_gcid required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &Tenant{
		ID:          NewUUIDv7(),
		DisplayName: strings.TrimSpace(displayName),
		OwnerGCID:   strings.TrimSpace(ownerGCID),
		SelfHosted:  selfHosted,
		Status:      StatusActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (t *Tenant) UpdateDisplayName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: display_name cannot be empty", ErrInvalidArgument)
	}
	t.DisplayName = strings.TrimSpace(name)
	t.UpdatedAt = time.Now().UTC()
	return nil
}

var hexColorRE = regexp.MustCompile(`^#[0-9A-Fa-f]{6}$`)

func (t *Tenant) UpdateBranding(cfg BrandingConfig) error {
	if cfg.PrimaryColorHex != "" && !hexColorRE.MatchString(cfg.PrimaryColorHex) {
		return fmt.Errorf("%w: primary_color_hex must be #RRGGBB", ErrInvalidArgument)
	}
	if cfg.LogoURL != "" {
		u, err := url.Parse(cfg.LogoURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("%w: logo_url must be http(s)", ErrInvalidArgument)
		}
	}
	t.Branding = cfg
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (t *Tenant) Suspend(reason string) {
	if t.Status == StatusClosed {
		return
	}
	t.Status = StatusSuspended
	t.SuspensionReason = reason
	t.UpdatedAt = time.Now().UTC()
}

func (t *Tenant) Reactivate() {
	if t.Status == StatusClosed {
		return
	}
	t.Status = StatusActive
	t.SuspensionReason = ""
	t.UpdatedAt = time.Now().UTC()
}

func (t *Tenant) Close(reason string) {
	if t.Status == StatusClosed {
		return
	}
	now := time.Now().UTC()
	t.Status = StatusClosed
	t.DeletedAt = &now
	t.UpdatedAt = now
	t.ClosureReason = reason
}

type Registry struct {
	mu     sync.RWMutex
	by     map[string]*Tenant
	bySlug map[string]string // slug → tenant_id (only for slug-keyed light-wizard tenants)
}

func NewRegistry() *Registry {
	return &Registry{
		by:     make(map[string]*Tenant),
		bySlug: make(map[string]string),
	}
}

// CreateRootLight is the idempotent creator for Phyllis MVP §5.2: if a
// tenant with the supplied slug already exists, the existing tenant is
// returned (200-style idempotency). Otherwise a new pending_activation
// tenant is created and persisted. Validation runs before idempotency
// lookup so callers cannot get an existing tenant back via an invalid slug
// shape (rejection trumps idempotency).
func (r *Registry) CreateRootLight(in LightTenantInput) (*Tenant, error) {
	// Validate via constructor first so invalid slugs/currencies fail fast,
	// regardless of registry state.
	t, err := NewLightTenant(in)
	if err != nil {
		return nil, err
	}
	if existing, ok := r.FindBySlug(t.Slug); ok {
		return existing, nil
	}
	r.mu.Lock()
	r.by[t.ID] = t
	r.bySlug[t.Slug] = t.ID
	r.mu.Unlock()
	return t, nil
}

// FindBySlug returns the Tenant with the supplied slug or false. Slug match
// is case-insensitive (slugs are normalised lowercase at insert).
func (r *Registry) FindBySlug(slug string) (*Tenant, bool) {
	slug = strings.ToLower(strings.TrimSpace(slug))
	if slug == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.bySlug[slug]
	if !ok {
		return nil, false
	}
	t, ok := r.by[id]
	return t, ok
}

func (r *Registry) CreateRoot(displayName, ownerGCID string, selfHosted bool) (*Tenant, error) {
	t, err := NewTenant(displayName, ownerGCID, selfHosted)
	if err != nil {
		return nil, err
	}
	r.Save(t)
	return t, nil
}

func (r *Registry) CreateSubTenant(parentID, displayName, ownerGCID string, selfHosted bool) (*Tenant, error) {
	r.mu.RLock()
	_, ok := r.by[parentID]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrParentNotFound
	}
	t, err := NewTenant(displayName, ownerGCID, selfHosted)
	if err != nil {
		return nil, err
	}
	t.ParentTenantID = parentID
	r.Save(t)
	return t, nil
}

func (r *Registry) SetParent(tenantID, newParentID string) error {
	if tenantID == newParentID {
		return ErrSelfParent
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.by[tenantID]
	if !ok {
		return ErrTenantNotFound
	}
	if newParentID != "" {
		cursor := newParentID
		for cursor != "" {
			if cursor == tenantID {
				return ErrCycleDetected
			}
			parent, ok := r.by[cursor]
			if !ok {
				return ErrParentNotFound
			}
			cursor = parent.ParentTenantID
		}
	}
	t.ParentTenantID = newParentID
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *Registry) Save(t *Tenant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.by[t.ID] = t
	if t.Slug != "" {
		r.bySlug[t.Slug] = t.ID
	}
}

func (r *Registry) Get(id string) (*Tenant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.by[id]
	return t, ok
}

func (r *Registry) ListChildren(parentID string) []*Tenant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Tenant, 0)
	for _, t := range r.by {
		if t.ParentTenantID == parentID {
			out = append(out, t)
		}
	}
	return out
}

func (r *Registry) List() []*Tenant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Tenant, 0, len(r.by))
	for _, t := range r.by {
		if t.Status == StatusClosed {
			continue
		}
		out = append(out, t)
	}
	return out
}
