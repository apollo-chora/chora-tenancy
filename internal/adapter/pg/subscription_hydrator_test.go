// subscription_hydrator_test.go — CHO-1752 unit tests for the pure
// helpers in the hydrator. Live-pg coverage lives under
// subscription_hydrator_integration_test.go (//go:build integration).
package pg_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
)

func TestTranslateAddOnCode(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		// Legacy `core` migrates to the canonical CHO-1745 `base` code.
		"core": "base",
		// Already-canonical codes pass through unchanged.
		"base":     "base",
		"familiar": "familiar",
		"tms":      "tms",
		// Unknowns pass through — the registry's Subscribe will reject
		// codes the catalogue does not know, so the hydrator does not
		// have to gate them here.
		"completely_unknown_code": "completely_unknown_code",
		// Empty stays empty — defensive only; the SQL guarantees non-null.
		"": "",
	}
	for in, want := range cases {
		got := pg.TranslateAddOnCode(in)
		if got != want {
			t.Errorf("TranslateAddOnCode(%q) = %q; want %q", in, got, want)
		}
	}
}
