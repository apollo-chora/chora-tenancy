// addon_entitlements_test.go: E1 (UX refactor wave W2), durable add-on
// entitlements written at tenant creation.
//
// What was wrong. Creating a tenant persists exactly ONE entitlement, `core`,
// inside the atomic bundle. The add-ons an operator picks are not written at
// all, and the `base` subscribe that runs after the transaction goes to
// `addon.SubscriptionRegistry`, which is maps plus a mutex with no repository:
// it dies with the pod. There is a boot hydrator that reloads the registry from
// `add_on_subscriptions`, and its own comment calls itself "a UX nicety, not a
// correctness requirement". So the H+ wizard's add-on step records intent, not
// features, and a restart is enough to lose it.
//
// The fix is to carry the chosen codes into the bundle so they are written as
// real `add_on_subscriptions` rows in the SAME transaction as the tenant, the
// owner member and `core`. Then the wizard's add-on step becomes a read-back of
// what was granted rather than a promise, and "a created org holds its
// entitlements after a restart" becomes true by construction rather than by
// the pod staying up.
//
// Deliberately strict on an unknown code: the whole creation fails. Half a
// tenant with some of its add-ons is worse than no tenant, because the operator
// cannot see which half they got, and the alternative (drop the unknown code
// silently) is how a customer ends up paying for a feature nobody granted.
package bootstrap_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

// entitlementCodes projects the bundle's entitlements to their codes, in order.
func entitlementCodes(b *bootstrap.Bundle) []string {
	out := make([]string, 0, 1+len(b.ExtraEntitlements))
	out = append(out, b.Entitlement.AddOnCode)
	for _, e := range b.ExtraEntitlements {
		out = append(out, e.AddOnCode)
	}
	return out
}

func TestNew_CarriesChosenAddOnsAsExtraEntitlements(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{
		Name:        "Northwind Academy",
		OwnerGCID:   sampleGCID,
		AddOnCodes:  []string{"tms", "cms"},
		HostingMode: "FRANCHISE",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := entitlementCodes(b)
	want := []string{"core", "tms", "cms"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("entitlement codes = %v, want %v (core first, then the chosen set in order)", got, want)
	}
	// Every extra must be scoped to the same tenant and carry its own id, or
	// the INSERT collides on the primary key and the transaction rolls back.
	seen := map[string]bool{b.Entitlement.ID: true}
	for _, e := range b.ExtraEntitlements {
		if e.TenantID != b.Tenant.ID {
			t.Errorf("extra entitlement %q scoped to %q, want the new tenant %q", e.AddOnCode, e.TenantID, b.Tenant.ID)
		}
		if e.ID == "" || seen[e.ID] {
			t.Errorf("extra entitlement %q has a missing or duplicate id %q", e.AddOnCode, e.ID)
		}
		seen[e.ID] = true
	}
}

func TestNew_AddOnCodesAreNormalisedAndDeduped(t *testing.T) {
	// The wire is a list an operator's checkboxes produced, so it can carry
	// whitespace, case drift and repeats. None of those should reach an INSERT.
	b, err := bootstrap.New(bootstrap.Input{
		Name:       "Northwind Academy",
		OwnerGCID:  sampleGCID,
		AddOnCodes: []string{" TMS ", "tms", "cms", "TMS"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := entitlementCodes(b), []string{"core", "tms", "cms"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("codes = %v, want %v", got, want)
	}
}

func TestNew_CoreIsNeverDuplicatedByAChosenCode(t *testing.T) {
	// `core` is already the mandatory entitlement. Asking for it again must not
	// produce a second row: the unique constraint would reject it and the whole
	// creation would fail on a request that was, in substance, fine.
	b, err := bootstrap.New(bootstrap.Input{
		Name:       "Northwind Academy",
		OwnerGCID:  sampleGCID,
		AddOnCodes: []string{"core", "tms"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := entitlementCodes(b), []string{"core", "tms"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("codes = %v, want %v", got, want)
	}
}

func TestNew_BaseIsTreatedAsCore(t *testing.T) {
	// `base` has no add_ons row: migration 0035's reconciler maps it onto
	// `core` (`OR (c.code = 'base' AND a.code = 'core')`). Passing it through
	// verbatim would resolve to nothing and roll the creation back.
	b, err := bootstrap.New(bootstrap.Input{
		Name:       "Northwind Academy",
		OwnerGCID:  sampleGCID,
		AddOnCodes: []string{"base"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := entitlementCodes(b), []string{"core"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("codes = %v, want %v (base is an alias of core, not a second row)", got, want)
	}
}

func TestNew_RejectsABlankAddOnCode(t *testing.T) {
	for _, codes := range [][]string{{""}, {"tms", "   "}} {
		if _, err := bootstrap.New(bootstrap.Input{
			Name:       "Northwind Academy",
			OwnerGCID:  sampleGCID,
			AddOnCodes: codes,
		}); !errors.Is(err, bootstrap.ErrInvalidArgument) {
			t.Errorf("New(add_on_codes=%q) err = %v, want ErrInvalidArgument", codes, err)
		}
	}
}

func TestNew_NoAddOnCodesIsUnchangedBehaviour(t *testing.T) {
	// The regression fence: every existing caller passes nothing, and must keep
	// getting exactly the core-only bundle it gets today.
	b, err := bootstrap.New(bootstrap.Input{Name: "Northwind Academy", OwnerGCID: sampleGCID})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(b.ExtraEntitlements) != 0 {
		t.Fatalf("ExtraEntitlements = %v, want none", b.ExtraEntitlements)
	}
	if b.Entitlement.AddOnCode != bootstrap.CoreAddOnCode {
		t.Fatalf("Entitlement.AddOnCode = %q, want %q", b.Entitlement.AddOnCode, bootstrap.CoreAddOnCode)
	}
}

func TestService_CreateSubTenant_PersistsTheChosenAddOns(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()

	if _, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:           "Northwind Academy",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		AddOnCodes:     []string{"tms", "familiar"},
	}); err != nil {
		t.Fatalf("CreateSubTenant: %v", err)
	}
	if repo.lastBundle == nil {
		t.Fatal("nothing persisted")
	}
	if got, want := entitlementCodes(repo.lastBundle), []string{"core", "tms", "familiar"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("persisted codes = %v, want %v", got, want)
	}
	// The durable rows are the point. The in-memory registry subscribe that
	// runs after the transaction is display state and must not be mistaken for
	// the grant: it is exactly what dies on a restart.
	if repo.lastBundle.ExtraEntitlements[0].TenantID != repo.lastBundle.Tenant.ID {
		t.Errorf("extras must be scoped to the tenant being created")
	}
}
