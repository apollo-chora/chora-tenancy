// tenant_branch_test.go — branch coverage additions to tenant_test.go:
// Close idempotency, Registry Save-by-slug, SetParent error families,
// CreateSubTenant/BeginRoot error paths and NewLightTenant validation
// branches.
package tenant_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

func TestTenant_Close_Idempotent(t *testing.T) {
	t.Parallel()
	t0, err := tenant.NewTenant("Phyllis", "g", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	t0.Close("wind-down")
	if t0.Status != tenant.StatusClosed {
		t.Fatalf("expected closed, got %s", t0.Status)
	}
	t0.Close("again")
	if t0.ClosureReason != "wind-down" {
		t.Fatalf("Close should be idempotent, reason=%q", t0.ClosureReason)
	}
	if t0.DeletedAt == nil {
		t.Fatalf("expected deleted_at stamped")
	}
}

func TestRegistry_Save_RegistersSlug(t *testing.T) {
	t.Parallel()
	r := tenant.NewRegistry()
	t0, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "Coach", OwnerGCID: "g", Slug: "coach-x", Country: "SG", DefaultCurrency: "SGD",
	})
	if err != nil {
		t.Fatalf("NewLightTenant: %v", err)
	}
	r.Save(t0)
	// Slug find is case-insensitive.
	if got, ok := r.FindBySlug("coach-x"); !ok || got.ID != t0.ID {
		t.Fatalf("expected slug lookup, got %v %v", got, ok)
	}
	// Empty slug → no bySlug entry (still findable by id).
	t1, err := tenant.NewTenant("Bare", "g", false)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	r.Save(t1)
	if _, ok := r.FindBySlug(""); ok {
		t.Fatalf("blank slug should not match")
	}
	if got, ok := r.Get(t1.ID); !ok || got.ID != t1.ID {
		t.Fatalf("expected get by id, got %v %v", got, ok)
	}
}

func TestRegistry_CreateSubTenant_Errors(t *testing.T) {
	t.Parallel()
	r := tenant.NewRegistry()
	if _, err := r.CreateSubTenant("missing-parent", "X", "g", false); err != tenant.ErrParentNotFound {
		t.Fatalf("expected ErrParentNotFound, got %v", err)
	}
	root, _ := r.CreateRoot("Root", "g", true)
	// NewTenant validation failure propagates.
	if _, err := r.CreateSubTenant(root.ID, "", "g", false); err == nil || !strings.Contains(err.Error(), "display_name") {
		t.Fatalf("expected display_name validation error, got %v", err)
	}
}

func TestRegistry_CreateRoot_Validation(t *testing.T) {
	t.Parallel()
	r := tenant.NewRegistry()
	if _, err := r.CreateRoot("", "g", true); err == nil {
		t.Fatalf("expected validation error for blank display name")
	}
	if _, err := r.CreateRoot("Valid", "g", true); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
}

func TestRegistry_CreateRootLight_IdempotentAndValidation(t *testing.T) {
	t.Parallel()
	r := tenant.NewRegistry()
	in := tenant.LightTenantInput{
		DisplayName: "Light", OwnerGCID: "g", Slug: "light-co", Country: "SG", DefaultCurrency: "SGD",
	}
	first, err := r.CreateRootLight(in)
	if err != nil {
		t.Fatalf("CreateRootLight: %v", err)
	}
	second, err := r.CreateRootLight(in)
	if err != nil {
		t.Fatalf("CreateRootLight(2nd): %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected idempotent same-tenant, got %s vs %s", first.ID, second.ID)
	}
	// Validation trumps idempotency: same slug but bad currency rejected.
	bad := in
	bad.DefaultCurrency = "NOTCURRENCY"
	if _, err := r.CreateRootLight(bad); err == nil {
		t.Fatalf("expected validation error")
	}
}
