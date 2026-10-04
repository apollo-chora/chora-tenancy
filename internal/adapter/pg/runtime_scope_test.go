package pg

import "testing"

// Span-all (ADR-205/ADR-165) requires the read-side runner to accept the
// "platform" sentinel while the write-side runner keeps rejecting it.
func TestValidateScopeID_PlatformSentinel(t *testing.T) {
	const tenant = "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b"

	// validateScopeID: tenant UUID + the platform sentinel are both OK.
	if err := validateScopeID(tenant); err != nil {
		t.Fatalf("validateScopeID(uuid) = %v; want nil", err)
	}
	if err := validateScopeID(platformScope); err != nil {
		t.Fatalf("validateScopeID(%q) = %v; want nil", platformScope, err)
	}

	// validateTenantID: the write path MUST still reject "platform" (hex+dash
	// only) so a span-all sentinel can never leak into a write.
	if err := validateTenantID(platformScope); err == nil {
		t.Fatalf("validateTenantID(%q) = nil; want rejection", platformScope)
	}
	if err := validateTenantID(tenant); err != nil {
		t.Fatalf("validateTenantID(uuid) = %v; want nil", err)
	}

	// Defence-in-depth: an injection-shaped scope is rejected by both.
	for _, bad := range []string{"", "platform; DROP TABLE x", "tenant'--", "platformx"} {
		if err := validateScopeID(bad); err == nil {
			t.Errorf("validateScopeID(%q) = nil; want rejection", bad)
		}
	}
}
