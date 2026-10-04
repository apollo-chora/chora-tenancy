package pg_test

// one_live_owner_migration_test.go: static guard for tenancy migration 0036,
// the database half of S7-B1 (UX refactor R21).
//
// The Go guards in membership_write_repository.go refuse to strip or empty
// ownership, and they lock the owner rows they assert on, so within one
// process they are sound. They cannot state the invariant itself. This
// migration does: at most one live `owner` row per tenant, enforced by a
// partial unique index, which holds against a hand-written UPDATE, a future
// service that forgets the guard, and a migration nobody reviewed carefully.
//
// The two halves are complementary, not redundant. The index says "never two
// owners"; the guards say "never zero, and never lose one by accident". Only
// the second can be expressed in application code, and only the first survives
// somebody bypassing that code.
//
// Consequence the ownership handover MUST honour (S7-B4, not built here): a
// partial unique INDEX cannot be deferred (Postgres defers CONSTRAINTs, and a
// unique constraint cannot carry a WHERE clause), so the transfer transaction
// has to soft-delete the outgoing owner row BEFORE inserting the incoming one.
// Insert-then-delete violates the index mid-transaction and fails.

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const (
	oneLiveOwnerUpFile   = "0036_one_live_owner_per_tenant.up.sql"
	oneLiveOwnerDownFile = "0036_one_live_owner_per_tenant.down.sql"
)

func readTenancyMigration(t *testing.T, name string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v (RED until it exists)", name, err)
	}
	return string(raw)
}

func TestMigration0036_OneLiveOwnerPerTenant(t *testing.T) {
	up := readTenancyMigration(t, oneLiveOwnerUpFile)

	t.Run("creates_a_partial_unique_index_on_tenant_id", func(t *testing.T) {
		re := regexp.MustCompile(`(?is)CREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+\S+\s+ON\s+members\s*\(\s*tenant_id\s*\)\s*WHERE`)
		if !re.MatchString(up) {
			t.Errorf("0036.up must CREATE UNIQUE INDEX IF NOT EXISTS ... ON members (tenant_id) WHERE ...; got:\n%s", up)
		}
	})

	t.Run("the_index_predicate_scopes_to_live_owner_rows", func(t *testing.T) {
		// Both halves matter. Without role='owner' the index would forbid a
		// tenant having more than ONE MEMBER. Without deleted_at IS NULL a
		// soft-deleted former owner would block the next one forever, which
		// would make the handover impossible.
		i := strings.Index(up, "WHERE")
		if i < 0 {
			t.Fatalf("no index predicate found:\n%s", up)
		}
		pred := up[i:]
		if !strings.Contains(pred, "role = 'owner'") {
			t.Errorf("the index predicate must scope to owner rows, or it caps every tenant at one member:\n%s", pred)
		}
		if !strings.Contains(pred, "deleted_at IS NULL") {
			t.Errorf("the index predicate must scope to LIVE rows, or a soft-deleted former owner blocks the "+
				"next one forever and the handover can never complete:\n%s", pred)
		}
	})

	t.Run("fails_loud_on_a_tenant_that_already_has_two_owners", func(t *testing.T) {
		// The index creation would fail on its own, with a message about a
		// duplicated key. A named RAISE says what the operator must do, which
		// is the difference between a migration that stops and a migration
		// that stops usefully.
		if !regexp.MustCompile(`(?i)RAISE\s+EXCEPTION`).MatchString(up) {
			t.Errorf("0036.up must RAISE EXCEPTION with a named reason when a tenant already holds two live "+
				"owner rows, rather than leaving the operator to decode an index-build error:\n%s", up)
		}
		if !strings.Contains(up, "HAVING count(*) > 1") {
			t.Errorf("0036.up must detect the offending tenants by grouping live owner rows:\n%s", up)
		}
	})

	t.Run("moves_no_data", func(t *testing.T) {
		// A migration that quietly deleted a "surplus" owner row would resolve
		// the conflict by destroying exactly the fact this package exists to
		// protect. It must refuse instead.
		for _, kw := range []string{"DELETE FROM members", "UPDATE members", "INSERT INTO members"} {
			if strings.Contains(strings.ToUpper(up), strings.ToUpper(kw)) {
				t.Errorf("0036.up must not write members, found %q; a conflict is refused, never resolved:\n%s", kw, up)
			}
		}
	})

	down := readTenancyMigration(t, oneLiveOwnerDownFile)
	t.Run("down_drops_only_the_index", func(t *testing.T) {
		if !regexp.MustCompile(`(?i)DROP\s+INDEX\s+IF\s+EXISTS`).MatchString(down) {
			t.Errorf("0036.down must DROP INDEX IF EXISTS:\n%s", down)
		}
		if strings.Contains(strings.ToUpper(down), "MEMBERS SET") || strings.Contains(strings.ToUpper(down), "DELETE FROM MEMBERS") {
			t.Errorf("0036.down must not touch member rows:\n%s", down)
		}
	})
}
