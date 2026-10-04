// Package tenancy_test holds the RED-phase TDD specs for the Tenancy +
// Billing combined domain skeleton. It exercises the public domain API
// exclusively — it MUST NOT touch HTTP, persistence, or observability concerns.
//
// Aggregates under test (per the M11+ Phase D brief):
//   - Tenant
//   - TenantEntitlement
//   - AddOn
//   - Invoice
//   - PaymentMethod
//
// HARD RULE — Composable Add-On Plans (NOT tier pricing). Per CLAUDE.md §1
// the commercial model is feature-flag-driven add-ons: a tenant can hold
// any combination of entitlements; there is NO tier hierarchy enforced at
// the domain level.
package tenancy_test

import (
	"testing"
	"time"

	domain "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	tenantB = "01970000-0000-7000-8000-000000000002"
	tenantC = "01970000-0000-7000-8000-000000000003"
)

// -----------------------------------------------------------------------------
// Tenant
// -----------------------------------------------------------------------------

func TestTenant_New_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()

	tt, err := domain.NewTenant("Acme School", false, "")
	if err != nil {
		t.Fatalf("NewTenant: unexpected error: %v", err)
	}
	if tt.ID == "" {
		t.Fatalf("NewTenant: expected non-empty ID")
	}
	if tt.DisplayName != "Acme School" {
		t.Fatalf("NewTenant: display_name mismatch")
	}
	if tt.Status != domain.TenantStatusActive {
		t.Fatalf("NewTenant: expected active status, got %q", tt.Status)
	}
	if tt.SelfHosted != false {
		t.Fatalf("NewTenant: expected self_hosted=false")
	}
	if tt.ParentTenantID != "" {
		t.Fatalf("NewTenant: expected empty parent_tenant_id by default")
	}
	if tt.CreatedAt.IsZero() {
		t.Fatalf("NewTenant: expected CreatedAt set")
	}
}

func TestTenant_New_RejectsEmptyDisplayName(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewTenant("", false, ""); err == nil {
		t.Fatalf("expected error for empty display_name")
	}
	if _, err := domain.NewTenant("   ", false, ""); err == nil {
		t.Fatalf("expected error for whitespace-only display_name")
	}
}

// Self-tenant cycle: parent_tenant_id cannot be self → 400. Skeleton enforces
// this in SetParent (the constructor takes parent_tenant_id but it is the
// caller's responsibility to verify; SetParent provides the safe path).
func TestTenant_SetParent_RejectsSelfReference(t *testing.T) {
	t.Parallel()
	tt, _ := domain.NewTenant("Acme", false, "")
	if err := tt.SetParent(tt.ID); err == nil {
		t.Fatalf("expected error setting parent to self, got nil")
	}
}

func TestTenant_SetParent_AllowsValidParent(t *testing.T) {
	t.Parallel()
	parent, _ := domain.NewTenant("Parent Co", false, "")
	child, _ := domain.NewTenant("Child Co", false, "")
	if err := child.SetParent(parent.ID); err != nil {
		t.Fatalf("SetParent: unexpected error: %v", err)
	}
	if child.ParentTenantID != parent.ID {
		t.Fatalf("SetParent: expected parent_tenant_id set, got %q", child.ParentTenantID)
	}
}

func TestTenant_SoftDelete_TransitionsStatus(t *testing.T) {
	t.Parallel()
	tt, _ := domain.NewTenant("Acme", false, "")
	tt.Suspend()
	if tt.Status != domain.TenantStatusSuspended {
		t.Fatalf("Suspend: expected suspended status, got %q", tt.Status)
	}
	tt.Close()
	if tt.Status != domain.TenantStatusClosed {
		t.Fatalf("Close: expected closed status, got %q", tt.Status)
	}
	if tt.DeletedAt == nil {
		t.Fatalf("Close: expected deleted_at set")
	}
	first := *tt.DeletedAt
	tt.Close() // idempotent
	if !tt.DeletedAt.Equal(first) {
		t.Fatalf("Close: should be idempotent")
	}
}

func TestTenant_New_AcceptsSelfHostedFlag(t *testing.T) {
	t.Parallel()
	tt, _ := domain.NewTenant("Acme", true, "")
	if !tt.SelfHosted {
		t.Fatalf("expected self_hosted=true")
	}
}

// -----------------------------------------------------------------------------
// AddOn
// -----------------------------------------------------------------------------

func TestAddOn_New_AssignsIDAndDefaults(t *testing.T) {
	t.Parallel()
	a, err := domain.NewAddOn("Reward Vault", "Per-tenant reward economy", 4900, []string{"rewards", "marketplace"})
	if err != nil {
		t.Fatalf("NewAddOn: unexpected error: %v", err)
	}
	if a.ID == "" {
		t.Fatalf("NewAddOn: expected non-empty ID")
	}
	if a.MonthlyPriceCents != 4900 {
		t.Fatalf("NewAddOn: price mismatch")
	}
	if len(a.IncludedCapabilities) != 2 {
		t.Fatalf("NewAddOn: expected 2 capabilities, got %d", len(a.IncludedCapabilities))
	}
}

func TestAddOn_New_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewAddOn("", "x", 100, nil); err == nil {
		t.Fatalf("expected error for empty name")
	}
}

func TestAddOn_New_RejectsNegativePrice(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewAddOn("Free Tier", "free", -1, nil); err == nil {
		t.Fatalf("expected error for negative price")
	}
}

func TestAddOn_New_AllowsZeroPrice(t *testing.T) {
	t.Parallel()
	a, err := domain.NewAddOn("Free Tier", "free baseline", 0, nil)
	if err != nil {
		t.Fatalf("NewAddOn(price=0): unexpected error: %v", err)
	}
	if a.MonthlyPriceCents != 0 {
		t.Fatalf("expected price=0")
	}
}

// -----------------------------------------------------------------------------
// TenantEntitlement (composable — NOT tier pricing)
// -----------------------------------------------------------------------------

func TestTenantEntitlement_New_DefaultsToActive(t *testing.T) {
	t.Parallel()
	e, err := domain.NewTenantEntitlement(tenantA, "addon-1", 4900)
	if err != nil {
		t.Fatalf("NewTenantEntitlement: unexpected error: %v", err)
	}
	if e.Status != domain.EntitlementStatusActive {
		t.Fatalf("expected active status, got %q", e.Status)
	}
	if e.MonthlyPriceCentsSnapshot != 4900 {
		t.Fatalf("expected price snapshot 4900, got %d", e.MonthlyPriceCentsSnapshot)
	}
	if e.ActivatedAt.IsZero() {
		t.Fatalf("expected activated_at set")
	}
}

func TestTenantEntitlement_Cancel_TransitionsStatus(t *testing.T) {
	t.Parallel()
	e, _ := domain.NewTenantEntitlement(tenantA, "addon-1", 100)
	e.Cancel()
	if e.Status != domain.EntitlementStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", e.Status)
	}
	if e.CancelledAt == nil {
		t.Fatalf("expected cancelled_at set")
	}
}

// Composability: a tenant can hold N entitlements; no tier hierarchy
// enforced. Per the brief — Composable Add-On Plans NOT tier pricing.
func TestTenantEntitlement_Composability_TenantCanHoldMany(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	for i, addonID := range []string{"addon-rewards", "addon-marketplace", "addon-bigquery", "addon-mcp", "addon-byoa"} {
		_, err := reg.Subscribe(tenantA, addonID, int64(100*(i+1)))
		if err != nil {
			t.Fatalf("Subscribe %d: unexpected error: %v", i, err)
		}
	}
	active := reg.ListActiveByTenant(tenantA)
	if len(active) != 5 {
		t.Fatalf("Composability: expected 5 active entitlements, got %d", len(active))
	}
}

// Cancel idempotent: cancelling already-cancelled returns existing (not 404).
// Per the brief — must return the existing entitlement, not error 404.
func TestEntitlementRegistry_Cancel_IsIdempotent(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	if _, err := reg.Subscribe(tenantA, "addon-1", 100); err != nil {
		t.Fatalf("Subscribe: unexpected error: %v", err)
	}
	first, err := reg.Cancel(tenantA, "addon-1")
	if err != nil {
		t.Fatalf("Cancel #1: unexpected error: %v", err)
	}
	if first.Status != domain.EntitlementStatusCancelled {
		t.Fatalf("Cancel: expected cancelled status, got %q", first.Status)
	}
	// Second cancel must return the SAME entitlement, not 404.
	second, err := reg.Cancel(tenantA, "addon-1")
	if err != nil {
		t.Fatalf("Cancel #2: expected idempotent success, got error: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("Cancel idempotent: expected same entitlement ID")
	}
	if second.Status != domain.EntitlementStatusCancelled {
		t.Fatalf("Cancel idempotent: status should remain cancelled")
	}
}

// Cancelling a non-existent entitlement IS a 404.
func TestEntitlementRegistry_Cancel_UnknownReturnsNotFound(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	if _, err := reg.Cancel(tenantA, "no-such-addon"); err != domain.ErrEntitlementNotFound {
		t.Fatalf("expected ErrEntitlementNotFound, got %v", err)
	}
}

// Re-subscribing to the same add-on after cancel should reactivate
// (composability — tenant can resume any add-on).
func TestEntitlementRegistry_Subscribe_ReactivatesCancelled(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	first, _ := reg.Subscribe(tenantA, "addon-1", 100)
	reg.Cancel(tenantA, "addon-1")
	second, err := reg.Subscribe(tenantA, "addon-1", 200)
	if err != nil {
		t.Fatalf("Re-subscribe: unexpected error: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("Re-subscribe: expected same entitlement ID (reactivation)")
	}
	if second.Status != domain.EntitlementStatusActive {
		t.Fatalf("Re-subscribe: expected active status")
	}
	if second.MonthlyPriceCentsSnapshot != 200 {
		t.Fatalf("Re-subscribe: expected price snapshot updated to 200, got %d", second.MonthlyPriceCentsSnapshot)
	}
}

// Subscribing twice while active is idempotent (returns existing).
func TestEntitlementRegistry_Subscribe_DuplicateIsIdempotent(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	first, _ := reg.Subscribe(tenantA, "addon-1", 100)
	second, err := reg.Subscribe(tenantA, "addon-1", 100)
	if err != nil {
		t.Fatalf("Duplicate Subscribe: unexpected error: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("Duplicate Subscribe: expected same entitlement ID")
	}
}

func TestEntitlementRegistry_List_FiltersOnlyActive(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	reg.Subscribe(tenantA, "addon-1", 100)
	reg.Subscribe(tenantA, "addon-2", 200)
	reg.Cancel(tenantA, "addon-1")
	active := reg.ListActiveByTenant(tenantA)
	if len(active) != 1 {
		t.Fatalf("expected 1 active entitlement, got %d", len(active))
	}
	if active[0].AddOnID != "addon-2" {
		t.Fatalf("expected active addon-2, got %q", active[0].AddOnID)
	}
}

func TestNewTenantEntitlement_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewTenantEntitlement("", "addon-1", 100); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

func TestNewTenantEntitlement_RejectsEmptyAddOn(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewTenantEntitlement(tenantA, "", 100); err == nil {
		t.Fatalf("expected error for empty addon_id")
	}
}

// -----------------------------------------------------------------------------
// Invoice
// -----------------------------------------------------------------------------

// Invoice line item sum: sums all active entitlement prices for the period.
func TestInvoice_Generate_SumsActiveEntitlements(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	reg.Subscribe(tenantB, "addon-rewards", 4900)
	reg.Subscribe(tenantB, "addon-marketplace", 2900)
	reg.Subscribe(tenantB, "addon-bigquery", 9900)
	reg.Cancel(tenantB, "addon-bigquery") // should NOT be billed

	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	inv, err := domain.GenerateInvoice(tenantB, periodStart, periodEnd, reg.ListActiveByTenant(tenantB))
	if err != nil {
		t.Fatalf("GenerateInvoice: unexpected error: %v", err)
	}
	if inv.TotalCents != 7800 {
		t.Fatalf("expected total 7800 (4900+2900), got %d", inv.TotalCents)
	}
	if len(inv.LineItems) != 2 {
		t.Fatalf("expected 2 line items, got %d", len(inv.LineItems))
	}
	if inv.PeriodStart != periodStart {
		t.Fatalf("invoice period_start mismatch")
	}
}

// Empty entitlement list → zero-total invoice with empty line items.
func TestInvoice_Generate_NoEntitlementsProducesZeroTotal(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	inv, err := domain.GenerateInvoice(tenantC, periodStart, periodEnd, nil)
	if err != nil {
		t.Fatalf("GenerateInvoice empty: unexpected error: %v", err)
	}
	if inv.TotalCents != 0 {
		t.Fatalf("expected zero total, got %d", inv.TotalCents)
	}
	if len(inv.LineItems) != 0 {
		t.Fatalf("expected 0 line items, got %d", len(inv.LineItems))
	}
}

func TestInvoice_Generate_RejectsInvertedPeriod(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.Add(-24 * time.Hour)
	if _, err := domain.GenerateInvoice(tenantA, periodStart, periodEnd, nil); err == nil {
		t.Fatalf("expected error for period_end before period_start")
	}
}

func TestInvoice_Generate_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if _, err := domain.GenerateInvoice("", periodStart, periodEnd, nil); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

// -----------------------------------------------------------------------------
// PaymentMethod (Stripe stub)
// -----------------------------------------------------------------------------

func TestPaymentMethod_New_DefaultsToActive(t *testing.T) {
	t.Parallel()
	pm, err := domain.NewPaymentMethod(tenantA, "pm_test_stub_1234")
	if err != nil {
		t.Fatalf("NewPaymentMethod: unexpected error: %v", err)
	}
	if pm.Status != domain.PaymentMethodStatusActive {
		t.Fatalf("expected active status, got %q", pm.Status)
	}
	if pm.StripePaymentMethodID != "pm_test_stub_1234" {
		t.Fatalf("expected stripe id passthrough")
	}
}

func TestPaymentMethod_New_RejectsEmptyStripeID(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewPaymentMethod(tenantA, ""); err == nil {
		t.Fatalf("expected error for empty stripe_payment_method_id")
	}
}

func TestPaymentMethod_New_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := domain.NewPaymentMethod("", "pm_x"); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

func TestPaymentMethod_Detach_TransitionsStatus(t *testing.T) {
	t.Parallel()
	pm, _ := domain.NewPaymentMethod(tenantA, "pm_x")
	pm.Detach()
	if pm.Status != domain.PaymentMethodStatusDetached {
		t.Fatalf("expected detached status, got %q", pm.Status)
	}
}

// -----------------------------------------------------------------------------
// AddOn catalog
// -----------------------------------------------------------------------------

func TestAddOnCatalog_RegisterAndList(t *testing.T) {
	t.Parallel()
	cat := domain.NewAddOnCatalog()
	a1, _ := domain.NewAddOn("Rewards", "x", 100, nil)
	a2, _ := domain.NewAddOn("Marketplace", "y", 200, nil)
	cat.Register(a1)
	cat.Register(a2)
	all := cat.List()
	if len(all) != 2 {
		t.Fatalf("expected 2 add-ons, got %d", len(all))
	}
}

func TestAddOnCatalog_GetByID(t *testing.T) {
	t.Parallel()
	cat := domain.NewAddOnCatalog()
	a, _ := domain.NewAddOn("Rewards", "x", 100, nil)
	cat.Register(a)
	got, ok := cat.Get(a.ID)
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != a.ID {
		t.Fatalf("ID mismatch")
	}
	if _, ok := cat.Get("nope"); ok {
		t.Fatalf("expected miss")
	}
}

// -----------------------------------------------------------------------------
// Coverage hardening: state transitions
// -----------------------------------------------------------------------------

func TestTenant_Suspend_ReversibleViaReactivate(t *testing.T) {
	t.Parallel()
	tt, _ := domain.NewTenant("Acme", false, "")
	tt.Suspend()
	if tt.Status != domain.TenantStatusSuspended {
		t.Fatalf("expected suspended, got %q", tt.Status)
	}
	tt.Reactivate()
	if tt.Status != domain.TenantStatusActive {
		t.Fatalf("expected active after reactivate, got %q", tt.Status)
	}
}

func TestTenant_Suspend_NoOpAfterClose(t *testing.T) {
	t.Parallel()
	tt, _ := domain.NewTenant("Acme", false, "")
	tt.Close()
	tt.Suspend() // closed tenants cannot be suspended
	if tt.Status != domain.TenantStatusClosed {
		t.Fatalf("expected status to remain closed, got %q", tt.Status)
	}
	tt.Reactivate() // closed tenants cannot be reactivated
	if tt.Status != domain.TenantStatusClosed {
		t.Fatalf("expected status to remain closed, got %q", tt.Status)
	}
}

func TestPaymentMethod_Detach_IsIdempotent(t *testing.T) {
	t.Parallel()
	pm, _ := domain.NewPaymentMethod(tenantA, "pm_x")
	pm.Detach()
	prev := pm.UpdatedAt
	pm.Detach() // second call should NOT bump updated_at
	if !pm.UpdatedAt.Equal(prev) {
		t.Fatalf("Detach should be idempotent")
	}
}

func TestEntitlementRegistry_ListAllByTenant_IncludesCancelled(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	reg.Subscribe(tenantA, "addon-1", 100)
	reg.Subscribe(tenantA, "addon-2", 200)
	reg.Cancel(tenantA, "addon-1")
	all := reg.ListAllByTenant(tenantA)
	if len(all) != 2 {
		t.Fatalf("expected 2 entitlements (active+cancelled), got %d", len(all))
	}
}

func TestEntitlementRegistry_Subscribe_RejectsEmpty(t *testing.T) {
	t.Parallel()
	reg := domain.NewEntitlementRegistry()
	if _, err := reg.Subscribe("", "a", 100); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
	if _, err := reg.Subscribe(tenantA, "", 100); err == nil {
		t.Fatalf("expected error for empty addon")
	}
}

// -----------------------------------------------------------------------------
// UUIDv7 generator coverage
// -----------------------------------------------------------------------------

func TestNewUUIDv7_NonEmpty(t *testing.T) {
	t.Parallel()
	id1 := domain.NewUUIDv7()
	id2 := domain.NewUUIDv7()
	if id1 == "" || id2 == "" {
		t.Fatalf("expected non-empty UUIDv7 strings")
	}
	if id1 == id2 {
		t.Fatalf("expected distinct UUIDv7 values")
	}
}
