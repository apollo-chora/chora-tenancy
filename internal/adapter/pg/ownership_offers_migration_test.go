package pg_test

// ownership_offers_migration_test.go: static guard for tenancy migration 0037,
// the consent record behind the ownership handover (UX refactor E3, S7-B2).
//
// Every clause this asserts was verified against the live local Postgres 18
// mirror before it was written: each CHECK refuses the row it names, the
// partial-unique index refuses a second pending offer, a declined offer does
// NOT block a re-offer, the updated_at trigger fires, and down then re-up
// leaves the table present and empty. This test is the cheap lexical partner
// that keeps those clauses from being edited away, not a substitute for having
// run them.
//
// The one thing worth restating here rather than leaving in the SQL: this table
// inherits an ORDERING constraint from 0036. uq_members_one_live_owner_per_tenant
// is a partial unique index, and a partial unique index cannot be deferred, so
// the accept transaction must soft-delete the outgoing owner row BEFORE
// inserting the incoming one.

import (
	"strings"
	"testing"
)

const (
	ownershipOffersUpFile   = "0037_ownership_offers.up.sql"
	ownershipOffersDownFile = "0037_ownership_offers.down.sql"
)

func TestMigration0037_OwnershipOffers(t *testing.T) {
	up := readTenancyMigration(t, ownershipOffersUpFile)

	t.Run("one pending offer per tenant, terminal offers do not block", func(t *testing.T) {
		if !strings.Contains(up, "uq_ownership_offers_one_pending_per_tenant") {
			t.Fatal("the partial-unique index is gone; two racing offers would each try to write an owner row")
		}
		if !strings.Contains(up, "WHERE status = 'pending'") {
			t.Fatal("the index lost its WHERE status = 'pending' predicate. Without it a declined or expired offer blocks the tenant from ever handing over again, and the platform never hard-deletes these rows")
		}
	})

	t.Run("self-nomination is refused in the database", func(t *testing.T) {
		if !strings.Contains(up, "ck_ownership_offers_not_self") ||
			!strings.Contains(up, "CHECK (from_gcid <> to_gcid)") {
			t.Fatal("the self-nomination CHECK is gone; the owner could create a pending handover that can never change anything")
		}
	})

	t.Run("the operator override cannot be reasonless", func(t *testing.T) {
		if !strings.Contains(up, "ck_ownership_offers_operator_reason") {
			t.Fatal("the operator-reason CHECK is gone; an override nobody can explain later is not auditable")
		}
		if !strings.Contains(up, "btrim(coalesce(reason, ''))") {
			t.Fatal("the reason check no longer trims; whitespace would pass for an explanation")
		}
	})

	t.Run("an offer cannot be born expired", func(t *testing.T) {
		if !strings.Contains(up, "ck_ownership_offers_ttl_future") {
			t.Fatal("the TTL CHECK is gone; an offer with a past expiry is one nobody can accept and is indistinguishable from a clock bug")
		}
	})

	t.Run("only the owner or an operator can initiate", func(t *testing.T) {
		if !strings.Contains(up, "CREATE TYPE ownership_offer_initiator AS ENUM ('owner', 'operator')") {
			t.Fatal("the initiator enum changed. A tenant admin must never be able to start a handover: ownership is the one role that is not admin-grantable, and an admin nominating is self-promotion by another name")
		}
	})

	t.Run("RLS is enabled with the same tenant isolation as members", func(t *testing.T) {
		if !strings.Contains(up, "ALTER TABLE ownership_offers ENABLE ROW LEVEL SECURITY") {
			t.Fatal("RLS is not enabled; chora_tenancy_app_rw is NOBYPASSRLS but only for tables that have a policy")
		}
		if !strings.Contains(up, "tenant_id = current_setting('chora.tenant_id', true)::uuid") {
			t.Fatal("the tenant_isolation policy no longer matches the shape used by members and add_on_subscriptions")
		}
	})

	t.Run("no soft-delete column, because rows are never deleted", func(t *testing.T) {
		// Stricter than the platform rule, not a departure from it: every
		// outcome is a terminal status and the row remains as its record. The
		// pending_invites precedent is the same shape.
		// Scoped to the column list on purpose. An absence check over the
		// whole file is wrong by default: this migration's own prose explains
		// the members-side soft-delete ordering, and matching that prose made
		// the first version of this assertion fail against a correct schema.
		start := strings.Index(up, "CREATE TABLE IF NOT EXISTS ownership_offers")
		if start < 0 {
			t.Fatal("the CREATE TABLE statement is gone")
		}
		end := strings.Index(up[start:], "\n);")
		if end < 0 {
			t.Fatal("could not find the end of the CREATE TABLE statement")
		}
		if strings.Contains(up[start:start+end], "deleted_at") {
			t.Fatal("a deleted_at column appeared. If offers are now soft-deleted, the partial-unique index must also filter on it or a soft-deleted pending offer blocks the tenant forever")
		}
	})

	t.Run("the down migration exists and drops what the up created", func(t *testing.T) {
		down := readTenancyMigration(t, ownershipOffersDownFile)
		for _, want := range []string{
			"DROP TABLE IF EXISTS ownership_offers",
			"DROP TYPE IF EXISTS ownership_offer_status",
			"DROP TYPE IF EXISTS ownership_offer_initiator",
		} {
			if !strings.Contains(down, want) {
				t.Fatalf("down migration does not %q, so a re-up would fail on the leftover", want)
			}
		}
	})
}
