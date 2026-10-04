// Package tenant_test holds the RED-phase TDD specs for the Tenant lifecycle
// sub-domain (parent + sub-tenant + branding) inside the chora-tenancy service.
//
// Aligned with:
//   - Comic Ch3 P7 P1 — initial Super Admin creation on tenant create
//   - Sequel Act 4 — Franchise sub-tenant nesting under a parent tenant
//   - CLAUDE.md §1 — UUIDv7, soft delete + pseudonymise, no hard-delete
//
// HARD RULE — sub-tenant cycle prevention: A → B → A (and longer chains)
// MUST be rejected by the registry. The domain layer enforces this; the HTTP
// layer surfaces it as 400.
package tenant_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

// -----------------------------------------------------------------------------
// Tenant constructor
// -----------------------------------------------------------------------------

func TestNewTenant_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()
	tt, err := tenant.NewTenant("Acme Corp", "owner-gcid-1", false)
	if err != nil {
		t.Fatalf("NewTenant: unexpected error: %v", err)
	}
	if tt.ID == "" {
		t.Fatalf("expected non-empty tenant ID")
	}
	if tt.DisplayName != "Acme Corp" {
		t.Fatalf("display_name mismatch: got %q", tt.DisplayName)
	}
	if tt.OwnerGCID != "owner-gcid-1" {
		t.Fatalf("owner_gcid mismatch: got %q", tt.OwnerGCID)
	}
	if tt.Status != tenant.StatusActive {
		t.Fatalf("expected active status, got %q", tt.Status)
	}
	if tt.ParentTenantID != "" {
		t.Fatalf("expected empty parent_tenant_id by default")
	}
	if tt.SelfHosted != false {
		t.Fatalf("expected self_hosted=false")
	}
	if tt.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt set")
	}
}

func TestNewTenant_RejectsEmptyDisplayName(t *testing.T) {
	t.Parallel()
	if _, err := tenant.NewTenant("", "owner-1", false); err == nil {
		t.Fatalf("expected error for empty display_name")
	}
	if _, err := tenant.NewTenant("   ", "owner-1", false); err == nil {
		t.Fatalf("expected error for whitespace display_name")
	}
}

func TestNewTenant_RejectsEmptyOwnerGCID(t *testing.T) {
	t.Parallel()
	if _, err := tenant.NewTenant("Acme", "", false); err == nil {
		t.Fatalf("expected error for empty owner_gcid")
	}
}

// -----------------------------------------------------------------------------
// Branding (PATCH /tenants/{id})
// -----------------------------------------------------------------------------

func TestTenant_UpdateBranding_ApplyValidConfig(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	cfg := tenant.BrandingConfig{
		PrimaryColorHex: "#00B4FF",
		LogoURL:         "https://cdn.acme.com/logo.png",
		CustomDomain:    "learn.acme.com",
	}
	if err := tt.UpdateBranding(cfg); err != nil {
		t.Fatalf("UpdateBranding: unexpected error: %v", err)
	}
	if tt.Branding.PrimaryColorHex != "#00B4FF" {
		t.Fatalf("expected primary_color_hex applied")
	}
	if tt.Branding.CustomDomain != "learn.acme.com" {
		t.Fatalf("expected custom_domain applied")
	}
}

func TestTenant_UpdateBranding_RejectsInvalidColor(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	if err := tt.UpdateBranding(tenant.BrandingConfig{PrimaryColorHex: "not-a-color"}); err == nil {
		t.Fatalf("expected error for invalid hex color")
	}
}

func TestTenant_UpdateDisplayName_RejectsEmpty(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	if err := tt.UpdateDisplayName("   "); err == nil {
		t.Fatalf("expected error for whitespace display_name")
	}
	if err := tt.UpdateDisplayName("New Name"); err != nil {
		t.Fatalf("UpdateDisplayName valid: unexpected error: %v", err)
	}
	if tt.DisplayName != "New Name" {
		t.Fatalf("expected display_name updated")
	}
}

// -----------------------------------------------------------------------------
// Soft delete + status transitions
// -----------------------------------------------------------------------------

func TestTenant_Suspend_TransitionsStatusReversibly(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	tt.Suspend("non-payment")
	if tt.Status != tenant.StatusSuspended {
		t.Fatalf("expected suspended, got %q", tt.Status)
	}
	tt.Reactivate()
	if tt.Status != tenant.StatusActive {
		t.Fatalf("expected active after reactivate, got %q", tt.Status)
	}
}

func TestTenant_Close_StampsDeletedAt(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	tt.Close("voluntary")
	if tt.Status != tenant.StatusClosed {
		t.Fatalf("expected closed, got %q", tt.Status)
	}
	if tt.DeletedAt == nil {
		t.Fatalf("expected deleted_at set on close")
	}
}

// -----------------------------------------------------------------------------
// Sub-tenant registry — cycle prevention is the headline contract
// -----------------------------------------------------------------------------

func TestTenantRegistry_CreateSubTenant_RejectsMissingParent(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	_, err := reg.CreateSubTenant("does-not-exist", "Sub", "owner-2", false)
	if err == nil {
		t.Fatalf("expected error when parent does not exist")
	}
	if err != tenant.ErrParentNotFound {
		t.Fatalf("expected ErrParentNotFound, got %v", err)
	}
}

func TestTenantRegistry_CreateSubTenant_AllowsValidParent(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	parent, _ := reg.CreateRoot("Parent", "owner-1", false)
	sub, err := reg.CreateSubTenant(parent.ID, "Sub", "owner-2", false)
	if err != nil {
		t.Fatalf("CreateSubTenant: unexpected error: %v", err)
	}
	if sub.ParentTenantID != parent.ID {
		t.Fatalf("expected parent_tenant_id on sub-tenant")
	}
}

// Cycle prevention: A is parent of B; B cannot be parent of A.
func TestTenantRegistry_CreateSubTenant_RejectsDirectCycle(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	b, _ := reg.CreateSubTenant(a.ID, "B", "owner-b", false)
	if _, err := reg.CreateSubTenant(b.ID, "A2", "owner-c", false); err != nil {
		t.Fatalf("creating A2 under B should be fine: %v", err)
	}
	// Now try to make A's parent = B (would form a cycle B → A → B)
	if err := reg.SetParent(a.ID, b.ID); err == nil {
		t.Fatalf("expected cycle error setting A's parent to B")
	}
}

// Cycle prevention: 3-node ring A → B → C → A
func TestTenantRegistry_SetParent_RejectsTransitiveCycle(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	b, _ := reg.CreateSubTenant(a.ID, "B", "owner-b", false)
	c, _ := reg.CreateSubTenant(b.ID, "C", "owner-c", false)
	// A's parent = C would create cycle A → C → B → A
	if err := reg.SetParent(a.ID, c.ID); err == nil {
		t.Fatalf("expected transitive cycle error")
	}
}

// SetParent self-reference is rejected (Same tenant cannot be its own parent).
func TestTenantRegistry_SetParent_RejectsSelfReference(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	if err := reg.SetParent(a.ID, a.ID); err == nil {
		t.Fatalf("expected error for self-reference")
	}
	if err := reg.SetParent(a.ID, a.ID); err != tenant.ErrSelfParent {
		t.Fatalf("expected ErrSelfParent")
	}
}

// Get returns the tenant by ID; misses return false.
func TestTenantRegistry_Get(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	got, ok := reg.Get(a.ID)
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != a.ID {
		t.Fatalf("ID mismatch")
	}
	if _, ok := reg.Get("no-such-tenant"); ok {
		t.Fatalf("expected miss")
	}
}

// Save also lets external callers update mutable fields (branding patch flow).
func TestTenantRegistry_Save_UpdatesExisting(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("Old Name", "owner-a", false)
	a.UpdateDisplayName("New Name")
	reg.Save(a)
	got, _ := reg.Get(a.ID)
	if got.DisplayName != "New Name" {
		t.Fatalf("expected save to persist updated name")
	}
}

// ListChildren returns direct children only (not recursive).
func TestTenantRegistry_ListChildren(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	b, _ := reg.CreateSubTenant(a.ID, "B", "owner-b", false)
	c, _ := reg.CreateSubTenant(a.ID, "C", "owner-c", false)
	reg.CreateSubTenant(b.ID, "D", "owner-d", false) // grandchild
	children := reg.ListChildren(a.ID)
	if len(children) != 2 {
		t.Fatalf("expected 2 direct children, got %d", len(children))
	}
	got := []string{children[0].ID, children[1].ID}
	expectIDs := map[string]bool{b.ID: true, c.ID: true}
	for _, id := range got {
		if !expectIDs[id] {
			t.Fatalf("unexpected child: %s", id)
		}
	}
}

// List returns all non-closed tenants (excluding soft-deleted).
func TestTenantRegistry_List_ExcludesClosed(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	a, _ := reg.CreateRoot("A", "owner-a", false)
	reg.CreateRoot("B", "owner-b", false)
	a.Close("test")
	reg.Save(a)
	items := reg.List()
	if len(items) != 1 {
		t.Fatalf("expected 1 active tenant (A excluded), got %d", len(items))
	}
}

// Branding LogoURL must be valid http/https url if non-empty.
func TestTenant_UpdateBranding_RejectsNonHttpLogoURL(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	if err := tt.UpdateBranding(tenant.BrandingConfig{LogoURL: "javascript:alert(1)"}); err == nil {
		t.Fatalf("expected error for non-http logo URL")
	}
}

func TestTenant_UpdateBranding_AcceptsEmptyOptionalFields(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	if err := tt.UpdateBranding(tenant.BrandingConfig{}); err != nil {
		t.Fatalf("empty branding should be allowed: %v", err)
	}
}

// Reactivate after close is rejected (closed is terminal in this domain).
func TestTenant_Reactivate_NoOpAfterClose(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	tt.Close("voluntary")
	tt.Reactivate()
	if tt.Status != tenant.StatusClosed {
		t.Fatalf("Reactivate should not move closed→active; got %q", tt.Status)
	}
}

// Suspend after close is rejected (closed is terminal).
func TestTenant_Suspend_NoOpAfterClose(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewTenant("Acme", "owner-1", false)
	tt.Close("voluntary")
	tt.Suspend("post-close")
	if tt.Status != tenant.StatusClosed {
		t.Fatalf("Suspend should not move closed→suspended; got %q", tt.Status)
	}
}

// UUIDv7 generator coverage hardening: IDs are non-empty and unique.
func TestNewUUIDv7_EmittedByConstructor(t *testing.T) {
	t.Parallel()
	tt1, _ := tenant.NewTenant("A", "o1", false)
	tt2, _ := tenant.NewTenant("B", "o2", false)
	if tt1.ID == "" || tt2.ID == "" {
		t.Fatalf("expected non-empty IDs")
	}
	if tt1.ID == tt2.ID {
		t.Fatalf("expected distinct IDs")
	}
	if !strings.Contains(tt1.ID, "-") {
		t.Fatalf("expected dashed UUID format, got %q", tt1.ID)
	}
}

// =============================================================================
// Phyllis MVP §5.2 — light wizard fields + go-live transition
// =============================================================================

// NewLightTenant accepts the Phyllis-MVP minimum form fields:
// (display_name, owner_gcid, country, default_currency, slug). The tenant
// starts in pending_activation; only the /golive transition flips active.
func TestNewLightTenant_AssignsPendingActivationStatus(t *testing.T) {
	t.Parallel()
	tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName:     "MTM Singapore",
		OwnerGCID:       "owner-gcid-1",
		Country:         "SG",
		DefaultCurrency: "SGD",
		Slug:            "mtm-singapore",
	})
	if err != nil {
		t.Fatalf("NewLightTenant: unexpected error: %v", err)
	}
	if tt.Status != tenant.StatusPendingActivation {
		t.Fatalf("expected pending_activation, got %q", tt.Status)
	}
	if tt.Country != "SG" {
		t.Fatalf("expected country=SG, got %q", tt.Country)
	}
	if tt.DefaultCurrency != "SGD" {
		t.Fatalf("expected currency=SGD, got %q", tt.DefaultCurrency)
	}
	if tt.Slug != "mtm-singapore" {
		t.Fatalf("expected slug=mtm-singapore, got %q", tt.Slug)
	}
}

func TestNewLightTenant_DefaultsCountryAndCurrency(t *testing.T) {
	t.Parallel()
	tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "DefaultsCheck",
		OwnerGCID:   "owner-1",
		Slug:        "defaults",
	})
	if err != nil {
		t.Fatalf("NewLightTenant: unexpected error: %v", err)
	}
	if tt.Country != "SG" {
		t.Fatalf("expected default country=SG, got %q", tt.Country)
	}
	if tt.DefaultCurrency != "SGD" {
		t.Fatalf("expected default currency=SGD, got %q", tt.DefaultCurrency)
	}
}

func TestNewLightTenant_RejectsBadCurrency(t *testing.T) {
	t.Parallel()
	if _, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName:     "Acme",
		OwnerGCID:       "g1",
		DefaultCurrency: "ZZ",
		Slug:            "acme",
	}); err == nil {
		t.Fatalf("expected error for non-3-letter currency")
	}
}

func TestNewLightTenant_RejectsInvalidSlug(t *testing.T) {
	t.Parallel()
	bad := []string{"With Space", "UPPER", "with_underscore", "trailing-", "-leading", ""}
	for _, s := range bad {
		if _, err := tenant.NewLightTenant(tenant.LightTenantInput{
			DisplayName: "X", OwnerGCID: "g1", Slug: s,
		}); err == nil {
			t.Fatalf("expected error for slug %q", s)
		}
	}
}

func TestTenant_GoLive_TransitionsPendingToActive(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "mtm",
	})
	if err := tt.GoLive("cus_test_123"); err != nil {
		t.Fatalf("GoLive: %v", err)
	}
	if tt.Status != tenant.StatusActive {
		t.Fatalf("expected active after go-live, got %q", tt.Status)
	}
	if tt.StripeCustomerID != "cus_test_123" {
		t.Fatalf("expected stripe_customer_id stamped")
	}
	if tt.ActivatedAt == nil {
		t.Fatalf("expected activated_at stamped")
	}
}

func TestTenant_GoLive_RejectsAlreadyActive(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "mtm",
	})
	if err := tt.GoLive("cus_test_first"); err != nil {
		t.Fatalf("first GoLive: %v", err)
	}
	if err := tt.GoLive("cus_test_second"); err == nil {
		t.Fatalf("expected error on second GoLive")
	}
	if tt.StripeCustomerID != "cus_test_first" {
		t.Fatalf("stripe customer must remain idempotent on duplicate GoLive")
	}
}

func TestTenant_GoLive_RejectsClosedOrSuspended(t *testing.T) {
	t.Parallel()
	tt, _ := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "X", OwnerGCID: "g1", Slug: "x",
	})
	tt.Close("test")
	if err := tt.GoLive("cus_x"); err == nil {
		t.Fatalf("expected GoLive on closed tenant to error")
	}
}

// CreateRootBySlug is the idempotency primitive: creating a tenant with the
// same slug twice returns the existing tenant (not a duplicate).
func TestTenantRegistry_CreateRootBySlug_IsIdempotent(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	in := tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "mtm-1",
	}
	first, err := reg.CreateRootLight(in)
	if err != nil {
		t.Fatalf("first CreateRootLight: %v", err)
	}
	second, err := reg.CreateRootLight(in)
	if err != nil {
		t.Fatalf("second CreateRootLight: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected idempotent same-id, got %s vs %s", first.ID, second.ID)
	}
}

// FindBySlug returns the tenant or false; used by the HTTP layer's idempotent
// POST.
func TestTenantRegistry_FindBySlug(t *testing.T) {
	t.Parallel()
	reg := tenant.NewRegistry()
	created, _ := reg.CreateRootLight(tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "mtm-2",
	})
	got, ok := reg.FindBySlug("mtm-2")
	if !ok {
		t.Fatalf("expected hit on slug")
	}
	if got.ID != created.ID {
		t.Fatalf("expected same tenant ID")
	}
	if _, ok := reg.FindBySlug("does-not-exist"); ok {
		t.Fatalf("expected miss for unknown slug")
	}
}

// -----------------------------------------------------------------------------
// FinishWizard — set-once semantics for the Setup Wizard step 4 (CHO-1682)
// -----------------------------------------------------------------------------

// TestTenant_FinishWizard_SetOnceStampsTimestamp covers the first finish:
// WizardCompletedAt is NULL, FinishWizard stamps it, returns newly=true,
// and bumps UpdatedAt.
func TestTenant_FinishWizard_SetOnceStampsTimestamp(t *testing.T) {
	t.Parallel()
	tt, err := tenant.NewTenant("Wizard Co", "owner-gcid-w", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	if tt.WizardCompletedAt != nil {
		t.Fatalf("fresh tenant must have nil WizardCompletedAt; got %v", tt.WizardCompletedAt)
	}
	now := tenant.NowUTC()
	newly := tt.FinishWizard(now)
	if !newly {
		t.Fatalf("FinishWizard on a fresh tenant must return newly=true")
	}
	if tt.WizardCompletedAt == nil {
		t.Fatalf("FinishWizard must stamp WizardCompletedAt")
	}
	if !tt.WizardCompletedAt.Equal(now) {
		t.Fatalf("WizardCompletedAt = %v; want %v", *tt.WizardCompletedAt, now)
	}
	if !tt.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt must advance to %v on first finish; got %v", now, tt.UpdatedAt)
	}
}

// TestTenant_FinishWizard_IdempotentNoOp covers the second finish:
// FinishWizard returns newly=false and preserves the original timestamp.
// The aggregator relies on `newly` to decide whether to emit the
// `tenant_wizard_completed.v1` event — duplicate finishes must not
// republish.
func TestTenant_FinishWizard_IdempotentNoOp(t *testing.T) {
	t.Parallel()
	tt, err := tenant.NewTenant("Wizard Co Idempotent", "owner-gcid-w2", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	first := tenant.NowUTC()
	if newly := tt.FinishWizard(first); !newly {
		t.Fatalf("first FinishWizard must return newly=true")
	}
	firstStamp := *tt.WizardCompletedAt

	second := first.Add(5 * 60 * 1000 * 1000 * 1000) // +5 min
	newly := tt.FinishWizard(second)
	if newly {
		t.Fatalf("second FinishWizard must return newly=false")
	}
	if tt.WizardCompletedAt == nil || !tt.WizardCompletedAt.Equal(firstStamp) {
		t.Fatalf("WizardCompletedAt mutated on idempotent re-finish; got %v want %v",
			tt.WizardCompletedAt, firstStamp)
	}
}
