// legacy_tenancy_validate_test.go — in-package (white-box) unit tests for
// the package-private validateTenantID guard used by RunInTenantTx (A7).
//
// validateTenantID is the defence-in-depth check that keeps a SET LOCAL
// chora.tenant_id statement from receiving anything other than a UUID-
// shaped identifier — Postgres parses SET LOCAL (not the prepared-stmt
// path), so a malformed tenant_id must be rejected before it reaches the
// wire. The pgxpool RunInTenantTx path itself needs a live pool and is
// covered by legacy_tenancy_integration_test.go; this file pins the
// validation branch that gates it.
package pg

import "testing"

func TestValidateTenantID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"valid uuid lowercase", "11111111-1111-7111-8111-111111111111", false},
		{"valid uuid uppercase hex", "ABCDEF12-1111-7111-8111-111111111111", false},
		{"empty", "", true},
		{"single quote injection", "11111111'; DROP TABLE tenants;--", true},
		{"semicolon", "11111111;11111111", true},
		{"whitespace", "1111 1111", true},
		{"non-hex letter g", "g1111111-1111-7111-8111-111111111111", true},
		{"underscore", "11111111_1111", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateTenantID(tc.id)
			if tc.wantErr && err == nil {
				t.Errorf("validateTenantID(%q) = nil, want error", tc.id)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateTenantID(%q) = %v, want nil", tc.id, err)
			}
		})
	}
}
