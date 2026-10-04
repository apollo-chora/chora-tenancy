-- =============================================================================
-- chora-tenancy : 0039_add_on_subscriptions_hold_registry_state.up.sql
--
-- UX refactor Track U, row E4 slice 1: make the durable table able to HOLD the
-- state the in-memory SubscriptionRegistry holds. Precondition for the
-- write-through, not the write-through itself.
--
-- PROBLEM
--   Every runtime entitlement mutation (the v2 handlers, the payments
--   subscriber activator, the wizard's SubscribePending) mutates the per-pod
--   in-memory *add_on.SubscriptionRegistry ONLY. The registry is hydrated
--   from PG at boot, so a pod restart re-reads the old row and resurrects an
--   add-on the admin deactivated. That is CHO-1811, which
--   `cmd/server/registry_entitlement_store.go` calls breakage 4/5 in its own
--   header.
--
--   Row E4 closes it by writing through. But the destination could not hold
--   the state, so a write-through against the pre-0039 table would have
--   persisted a SUBSET and called the divergence closed:
--
--     - `entitlement_status` carried TWO values, `active` and `cancelled`.
--       The domain has SIX. Five of them raised 22P02 on insert (proven by
--       the RED of `Integration_AddOnSubscriptionRoundTrip`), so a
--       write-through would have had to collapse `grace_period`,
--       `suspended`, `pending_activation`, `pending_deactivation` and
--       `deactivated` into the two that existed, destroying the distinction.
--
--     - SEVEN domain fields had no column at all: owner_gcid, version,
--       grace_ends_at, scheduled_tier, scheduled_effective_at,
--       next_renewal_at, deactivated_at.
--
-- ⚠⚠ THE ONE THAT BITES, AND IT BITES THE OTHER WAY.
--   `Subscription.IsActive` reads:
--       case StatusGracePeriod: if s.GraceEndsAt == nil { return true }
--   So a grace period persisted WITHOUT its end date reads back as active
--   FOREVER. A write-through against a table with no `grace_ends_at` would
--   not merely fail to fix CHO-1811, it would convert a time-bounded grace
--   period into a PERMANENT FREE ENTITLEMENT. CHO-1811 wrongly RESTORES an
--   entitlement an admin removed; this would wrongly GRANT one nobody paid
--   for, which is worse. That is why the schema is proven before any
--   write-through exists.
--
-- WHY THE ENUM VALUES AND THEIR USE CANNOT SHARE A TRANSACTION
--   PostgreSQL 18 ALLOWS `ALTER TYPE ... ADD VALUE` inside a transaction
--   block, but REFUSES to use the new value in that same transaction:
--       ERROR: unsafe use of new value "x" of enum type
--       HINT:  New enum values must be committed before they can be used.
--   Verified on the local mirror. (A first probe appeared to disprove this;
--   it was invalid, because it created the enum inside the same transaction,
--   which is precisely the documented exempt case. Re-probed against a
--   pre-existing type, the restriction holds.)
--
--   This migration therefore only ADDS the values and the columns and USES
--   neither. Nothing here casts to a new label, defaults to one, or
--   backfills with one. Any backfill belongs in a later migration, in a
--   later transaction.
--
-- ADDITIVE AND NULLABLE ON PURPOSE
--   Every existing row predates these columns and the running binary does
--   not write them yet. A NOT NULL or a CHECK over a column the running
--   binary never writes breaks production with zero code shipped, so all
--   seven are nullable with no default. NULL means "never set", which a
--   caller can tell apart from a written zero.
--
-- NO RLS CHANGE. `add_on_subscriptions` keeps its `tenant_isolation` policy
-- untouched; this migration adds no policy, drops none, and grants nothing
-- new (column privileges are inherited from the table grants app_rw and
-- app_ro already hold). The four sanctioned RLS-bypass surfaces are
-- UNCHANGED at four.
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Date    : 2026-09-03
--
-- IDEMPOTENCY
--   `ADD VALUE IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS` throughout, so
--   re-application under the migrations runner is a no-op.
--
-- HARD RULE: cross-database queries forbidden. Touches only chora_tenancy.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. The five domain statuses the enum could not express.
--
-- `active` already exists. `cancelled` also already exists and is NOT a domain
-- status: it predates the registry's vocabulary and is left in place because
-- dropping an enum value is not supported by PostgreSQL and existing rows may
-- carry it. The write-through slice must map the domain onto the six labels
-- below and never onto `cancelled`.
--
-- Order matters only for `enumsortorder`, which nothing in this codebase sorts
-- on; the labels are compared by value.
-- -----------------------------------------------------------------------------
ALTER TYPE entitlement_status ADD VALUE IF NOT EXISTS 'pending_activation';
ALTER TYPE entitlement_status ADD VALUE IF NOT EXISTS 'grace_period';
ALTER TYPE entitlement_status ADD VALUE IF NOT EXISTS 'suspended';
ALTER TYPE entitlement_status ADD VALUE IF NOT EXISTS 'pending_deactivation';
ALTER TYPE entitlement_status ADD VALUE IF NOT EXISTS 'deactivated';

-- -----------------------------------------------------------------------------
-- 2. The seven fields `add_on.Subscription` carries and the table did not.
-- -----------------------------------------------------------------------------

-- Who holds the subscription. The registry keys events and refusals on it.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS owner_gcid uuid;

-- Optimistic concurrency for ChangeTier, which takes an expectedVersion. With
-- no column the version resets to zero on every pod restart, so two admins
-- racing a tier change across a restart both see version 0 and both win.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS version bigint;

-- ⚠ The load-bearing one. See the header: without it a grace period reads back
-- as active forever.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS grace_ends_at timestamptz;

-- CHO-1772 end-of-cycle tier change. Both halves, or the FE renders
-- "<tier> starting <date>" with half the sentence missing.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS scheduled_tier varchar(64);
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS scheduled_effective_at timestamptz;

-- CHO-1784 next billing cycle anchor, surfaced to the deactivate modal as the
-- end_of_cycle effective_at.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS next_renewal_at timestamptz;

-- Distinct from `ended_at` and from `cancelled_at`, both of which already
-- exist and carry the legacy v1 vocabulary. The domain tracks the moment a
-- deactivation took effect separately from the moment it was requested
-- (`deactivation_requested_at`) and from when it becomes effective
-- (`deactivation_effective_at`), and all three are already distinct columns.
ALTER TABLE add_on_subscriptions
    ADD COLUMN IF NOT EXISTS deactivated_at timestamptz;

COMMENT ON COLUMN add_on_subscriptions.grace_ends_at IS
    'E4 slice 1. When the grace period ends. NOT optional for a row in '
    'grace_period status: Subscription.IsActive treats a nil end as active '
    'forever, so a grace period written without this grants a permanent '
    'entitlement. Nullable at the schema level only because the column is '
    'additive over rows that predate it.';

COMMENT ON COLUMN add_on_subscriptions.version IS
    'E4 slice 1. Optimistic concurrency counter for ChangeTier. Without it '
    'the version resets on every pod restart.';

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   -- Six domain labels present (plus the legacy `cancelled`).
--   SELECT e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
--    WHERE t.typname = 'entitlement_status' ORDER BY e.enumsortorder;
--
--   -- Seven columns present and NULLABLE.
--   SELECT column_name, is_nullable, column_default
--     FROM information_schema.columns
--    WHERE table_name = 'add_on_subscriptions'
--      AND column_name IN ('owner_gcid','version','grace_ends_at',
--                          'scheduled_tier','scheduled_effective_at',
--                          'next_renewal_at','deactivated_at')
--    ORDER BY column_name;
--
--   -- RLS unchanged: one policy, still tenant_isolation.
--   SELECT polname FROM pg_policy WHERE polrelid = 'add_on_subscriptions'::regclass;
-- =============================================================================
