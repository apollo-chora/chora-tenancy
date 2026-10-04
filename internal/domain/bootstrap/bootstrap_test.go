// bootstrap_test.go — RED phase (CHO-1628 Phase 1).
//
// Tests Input validation + Bundle assembly for the H+ Setup-Tenant
// bootstrap orchestration. Pure-domain tests — no I/O.
package bootstrap_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
)

const sampleGCID = "01935f12-0000-7000-8000-000000000001"

func TestNew_validInput_assemblesActiveTenantOwnerAndCoreEntitlement(t *testing.T) {
	in := bootstrap.Input{
		Name:      "Htet Aung Dev Tenant",
		OwnerGCID: sampleGCID,
	}

	b, err := bootstrap.New(in)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if b.Tenant.Name != in.Name {
		t.Errorf("Tenant.Name mismatch: got %q want %q", b.Tenant.Name, in.Name)
	}
	if b.Tenant.Status != bootstrap.TenantStatusActive {
		t.Errorf("Tenant.Status: want %q, got %q", bootstrap.TenantStatusActive, b.Tenant.Status)
	}
	if b.Tenant.OwnerGCID != sampleGCID {
		t.Errorf("Tenant.OwnerGCID mismatch")
	}
	if b.Tenant.Branding != "{}" {
		t.Errorf("Tenant.Branding default should be {}, got %q", b.Tenant.Branding)
	}
	if b.Tenant.SelfHosted {
		t.Errorf("Tenant.SelfHosted default should be false")
	}

	if b.OwnerMember.GCID != sampleGCID {
		t.Errorf("OwnerMember.GCID should match caller GCID")
	}
	if b.OwnerMember.TenantID != b.Tenant.ID {
		t.Errorf("OwnerMember.TenantID should match Tenant.ID")
	}
	if b.OwnerMember.Role != bootstrap.MemberRoleOwner {
		t.Errorf("OwnerMember.Role: want %q got %q", bootstrap.MemberRoleOwner, b.OwnerMember.Role)
	}
	if b.OwnerMember.JoinedAt.IsZero() {
		t.Errorf("OwnerMember.JoinedAt should be set")
	}

	if b.Entitlement.TenantID != b.Tenant.ID {
		t.Errorf("Entitlement.TenantID should match Tenant.ID")
	}
	if b.Entitlement.AddOnCode != bootstrap.CoreAddOnCode {
		t.Errorf("Entitlement.AddOnCode: want %q got %q", bootstrap.CoreAddOnCode, b.Entitlement.AddOnCode)
	}

	if b.CreatedAt.IsZero() {
		t.Errorf("CreatedAt should be set")
	}
}

func TestNew_trimsName(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{
		Name:      "   Trimmed Tenant   ",
		OwnerGCID: sampleGCID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Tenant.Name != "Trimmed Tenant" {
		t.Errorf("expected trimmed name, got %q", b.Tenant.Name)
	}
}

func TestNew_rejectsEmptyName(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "", OwnerGCID: sampleGCID})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestNew_rejectsWhitespaceOnlyName(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "   ", OwnerGCID: sampleGCID})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for whitespace-only name, got %v", err)
	}
}

func TestNew_rejectsTooShortName(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "ab", OwnerGCID: sampleGCID})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for 2-char name, got %v", err)
	}
}

func TestNew_acceptsNameAtMinBoundary(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "abc", OwnerGCID: sampleGCID})
	if err != nil {
		t.Errorf("expected success at min boundary (3 chars), got %v", err)
	}
}

func TestNew_rejectsTooLongName(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{
		Name:      strings.Repeat("a", 257),
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for 257-char name, got %v", err)
	}
}

func TestNew_acceptsNameAtMaxBoundary(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{
		Name:      strings.Repeat("a", 256),
		OwnerGCID: sampleGCID,
	})
	if err != nil {
		t.Errorf("expected success at max boundary (256 chars), got %v", err)
	}
}

func TestNew_rejectsEmptyGCID(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "Some Tenant", OwnerGCID: ""})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for empty GCID, got %v", err)
	}
}

func TestNew_rejectsWhitespaceOnlyGCID(t *testing.T) {
	_, err := bootstrap.New(bootstrap.Input{Name: "Some Tenant", OwnerGCID: "   "})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for whitespace-only GCID, got %v", err)
	}
}

func TestNew_generatesUUIDv7Ids(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{Name: "uuid test", OwnerGCID: sampleGCID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, c := range []struct {
		name string
		id   string
	}{
		{"Tenant.ID", b.Tenant.ID},
		{"OwnerMember.ID", b.OwnerMember.ID},
		{"Entitlement.ID", b.Entitlement.ID},
	} {
		if len(c.id) != 36 {
			t.Errorf("%s: expected 36-char UUID, got %q (len=%d)", c.name, c.id, len(c.id))
		}
		// UUIDv7 marker: 13th nibble must be '7'
		if len(c.id) >= 15 && c.id[14] != '7' {
			t.Errorf("%s: expected UUIDv7 (13th nibble '7'), got %q", c.name, c.id)
		}
	}
}

func TestNew_uniqueIdsAcrossBundle(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{Name: "unique ids", OwnerGCID: sampleGCID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	uniq := map[string]struct{}{
		b.Tenant.ID:      {},
		b.OwnerMember.ID: {},
		b.Entitlement.ID: {},
	}
	if len(uniq) != 3 {
		t.Errorf("expected 3 distinct UUIDs across bundle, got %d", len(uniq))
	}
}

func TestBundle_Output_mirrorsBundleIdentifiers(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{Name: "Output Mirror", OwnerGCID: sampleGCID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := b.Output()

	if out.TenantID != b.Tenant.ID {
		t.Errorf("Output.TenantID should mirror Bundle.Tenant.ID")
	}
	if out.OwnerMemberID != b.OwnerMember.ID {
		t.Errorf("Output.OwnerMemberID should mirror Bundle.OwnerMember.ID")
	}
	if out.EntitlementID != b.Entitlement.ID {
		t.Errorf("Output.EntitlementID should mirror Bundle.Entitlement.ID")
	}
	if !out.CreatedAt.Equal(b.CreatedAt) {
		t.Errorf("Output.CreatedAt should mirror Bundle.CreatedAt")
	}
}
