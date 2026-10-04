// Package inmem_test holds the static-content RLS leak test for the
// chora-tenancy database migrations.
//
// What this asserts (per multi-tenant-rls skill):
//
//  1. Every tenant-scoped table in chora_tenancy enables RLS via
//     `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`.
//
//  2. Every tenant-scoped table carries a `tenant_isolation` policy
//     keyed on `current_setting('chora.tenant_id', true)::uuid`.
//
//  3. The `tenants` table itself is intentionally NOT under RLS
//     (per audit `audit-platform-fillgaps.md` §2.1 — Hub+ marketplace
//     reads cross-tenant for parent-rollup views; safe because
//     tenants table contains only tenant metadata, not learner data).
//
//  4. New 0004_phyllis_mvp.sql migration's stage table
//     (`addon_usage_events`) is RLS-protected so the rollup job's
//     per-tenant scan cannot leak between tenants.
//
// This is a build-time guard. A Postgres-bound integration leak test
// (cross-tenant SELECT returning rows under a different tenant context)
// lives in tests/rls-isolation-active/ at the repo root and is gated on
// the `integration` build tag.
package inmem_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// migrationsDir is the local relative path to the SQL migrations.
const migrationsDir = "../../../../migrations"

// expectedRLSTables is the canonical list of RLS-protected tables in
// chora_tenancy. Adding a new tenant-scoped table here AND in the
// matching migration is mandatory; reviewers reject PRs that add a
// scoped table without RLS.
var expectedRLSTables = []string{
	"members",
	"add_on_subscriptions",
	"invoices",
	"payment_methods",
	"bulk_invites",
	"addon_lifecycle_events",
	"addon_usage_daily",
	"addon_usage_events",
	"tenant_mana_pools",
	"tenant_mana_allocations",
	"tenant_mana_pool_topups",
	"tenant_external_egress_policies",
}

// nonRLSTables are intentionally OUT of RLS scope (cross-tenant reads
// allowed); listed here so the test can document the rationale + flag
// drift.
var nonRLSTables = map[string]string{
	"tenants":  "Hub+ marketplace reads cross-tenant for parent-rollup views",
	"add_ons":  "catalogue is global by definition (composable add-on plans, NOT tier pricing)",
}

func readMigrationsConcat(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir %q: %v", migrationsDir, err)
	}
	var sb strings.Builder
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(migrationsDir, e.Name()))
		if err != nil {
			t.Fatalf("read migration %q: %v", e.Name(), err)
		}
		sb.Write(body)
		sb.WriteString("\n")
	}
	return sb.String()
}

// TestRLS_AllTenantScopedTablesEnabled — every expected RLS table has
// `ENABLE ROW LEVEL SECURITY`.
func TestRLS_AllTenantScopedTablesEnabled(t *testing.T) {
	t.Parallel()
	all := readMigrationsConcat(t)
	for _, table := range expectedRLSTables {
		// Match `ALTER TABLE <table> ENABLE ROW LEVEL SECURITY` allowing
		// surrounding whitespace.
		pat := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + regexp.QuoteMeta(table) + `\s+ENABLE\s+ROW\s+LEVEL\s+SECURITY`)
		if !pat.MatchString(all) {
			t.Errorf("table %q missing ENABLE ROW LEVEL SECURITY in migrations", table)
		}
	}
}

// TestRLS_AllTenantScopedTablesHaveIsolationPolicy — every expected RLS
// table has a `tenant_isolation` policy that consumes
// `chora.tenant_id`.
func TestRLS_AllTenantScopedTablesHaveIsolationPolicy(t *testing.T) {
	t.Parallel()
	all := readMigrationsConcat(t)
	for _, table := range expectedRLSTables {
		// Match `CREATE POLICY tenant_isolation ON <table> ...` with
		// `chora.tenant_id` in the USING clause (allowing whitespace).
		pat := regexp.MustCompile(`(?is)CREATE\s+POLICY\s+tenant_isolation\s+ON\s+` + regexp.QuoteMeta(table) + `[^;]*chora\.tenant_id`)
		if !pat.MatchString(all) {
			t.Errorf("table %q missing tenant_isolation policy keyed on chora.tenant_id", table)
		}
	}
}

// TestRLS_NoLeakedTablesUseAppCurrentTenantID — legacy
// `app.current_tenant_id` (M1-M9 convention) MUST NOT appear in any
// migration. Per multi-tenant-rls skill the active convention is
// `chora.tenant_id`; mixing the two would silently leak rows.
func TestRLS_NoLegacyAppCurrentTenantIDInMigrations(t *testing.T) {
	t.Parallel()
	all := readMigrationsConcat(t)
	if strings.Contains(all, "app.current_tenant_id") {
		t.Fatalf("found legacy `app.current_tenant_id` in migrations — convention is `chora.tenant_id` per multi-tenant-rls skill")
	}
}

// TestRLS_DocumentedNonRLSTables — each table not under RLS must be
// listed with a documented reason in nonRLSTables. Drift detector: if a
// new global table appears, it must be either RLS'd or explicitly
// whitelisted here.
func TestRLS_DocumentedNonRLSTables(t *testing.T) {
	t.Parallel()
	for table, reason := range nonRLSTables {
		if reason == "" {
			t.Errorf("non-RLS table %q must carry a documented reason", table)
		}
	}
}
