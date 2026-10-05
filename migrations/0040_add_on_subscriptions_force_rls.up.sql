-- =============================================================================
-- chora-tenancy : 0040_add_on_subscriptions_force_rls.up.sql
--
-- UX refactor Track U, row E4: close the owner hole and the unguarded cast on
-- `add_on_subscriptions`, BEFORE the entitlement write-through is built on it.
--
-- Row E4 makes PostgreSQL the source of truth for entitlements. The
-- write-through must not be built on a table whose OWNER can silently bypass
-- the isolation that is supposed to protect it, so this lands first.
--
-- TWO DEFECTS, both measured live, both against the convention the rest of the
-- estate already follows (FORCE ROW LEVEL SECURITY is on 243 tables across 117
-- migration files, and policies cast NULLIF-guarded).
--
--   1. RLS was ENABLED but NOT FORCED (`relrowsecurity = t`,
--      `relforcerowsecurity = f`). PostgreSQL exempts a table's OWNER from its
--      own policies unless FORCE is set, and `chora_tenancy_migrate` owns this
--      table. An owner connection therefore read every tenant's entitlements
--      with no tenant context at all.
--
--   2. `tenant_isolation` cast `current_setting('chora.tenant_id', true)::uuid`
--      with NO NULLIF guard. An unset GUC yields NULL and casts fine, but a GUC
--      set to the EMPTY STRING reaches `''::uuid` and raises 22P02, so an
--      isolation miss surfaces as a 500 instead of as zero rows.
--
-- ⚠ THE SECOND DEFECT WAS INVISIBLE BEHIND THE FIRST, and the RED proved it.
--   The obvious guess is that an empty GUC raised 22P02 before this migration.
--   It did NOT: from an owner connection it returned the row, exactly like the
--   no-GUC and wrong-tenant arms, because with FORCE off the policy is never
--   evaluated and the cast never runs. With force=false an owner connection
--   cannot measure this table's isolation in ANY direction. The unguarded cast
--   only becomes observable once FORCE is on, which is why both halves ship
--   together.
--
-- NOT A LIVE BYPASS, verified rather than assumed. chora-tenancy connects as
-- `chora_tenancy_app_rw`: the deployment sets `CHORA_DB_DSN_SECRET_ID` and
-- `CHORA_OUTBOX_DSN` to the chora_tenancy app_rw DSN,
-- those are its only DSN envs, no manifest references a migrate DSN, and the
-- secret's DSN username reads `chora_tenancy_app_rw`. app_rw is NOBYPASSRLS
-- (migration 0013) and was already filtered by the policy. So the owner hole
-- was reachable by the migrations runner and by test suites, NOT by the running
-- service. This is hardening, not an incident.
--
-- NO NEW RLS SURFACE. This migration REMOVES an unintended bypass rather than
-- adding a sanctioned one. It grants nothing, adds no PERMISSIVE widening and
-- introduces no GUC. The four sanctioned RLS-bypass surfaces of
-- `.claude/rules/ddd-enforcement.md` rule 2 are UNCHANGED at FOUR.
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Date    : 2026-09-03
--
-- IDEMPOTENCY
--   FORCE is a no-op when already forced; the policy is dropped and recreated
--   by name, so re-application converges.
--
-- HARD RULE: cross-database queries forbidden. Touches only chora_tenancy.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Subject the OWNER to the policy.
--
-- Without this the migrate role, which owns the table, reads and writes every
-- tenant's rows regardless of `chora.tenant_id`. Note the consequence for
-- tests: an integration test running as the owner must now set the GUC to see
-- its own fixtures, which is correct. A test that could read without a tenant
-- context was not testing isolation.
-- -----------------------------------------------------------------------------
ALTER TABLE add_on_subscriptions FORCE ROW LEVEL SECURITY;

-- -----------------------------------------------------------------------------
-- 2. NULLIF-guard the cast.
--
-- Recreated rather than altered because a policy's USING expression cannot be
-- amended in place. Same name, same role set, same FOR ALL scope; only the
-- expression changes, from
--     tenant_id = (current_setting('chora.tenant_id', true))::uuid
-- to the NULLIF-guarded form, so an empty GUC yields NULL and the comparison
-- is simply false: zero rows, no error.
--
-- The DROP and CREATE are in ONE transaction so there is no window in which
-- the table carries no policy at all. Under FORCE, a missing policy denies
-- everything rather than allowing it, so the window would fail closed rather
-- than open; it is still not a window worth having.
-- -----------------------------------------------------------------------------
DROP POLICY IF EXISTS tenant_isolation ON add_on_subscriptions;

CREATE POLICY tenant_isolation ON add_on_subscriptions
    FOR ALL
    USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);

COMMENT ON TABLE add_on_subscriptions IS
    'Durable add-on subscriptions. RLS FORCED (0040) so the table-owning '
    'migrate role is subject to tenant_isolation like every other role, and '
    'the policy is NULLIF-guarded so an empty chora.tenant_id reads as zero '
    'rows rather than raising 22P02.';

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   -- Forced, and the policy carries a NULLIF.
--   SELECT relrowsecurity, relforcerowsecurity FROM pg_class
--    WHERE relname = 'add_on_subscriptions';
--   SELECT polname, pg_get_expr(polqual, polrelid)
--     FROM pg_policy WHERE polrelid = 'add_on_subscriptions'::regclass;
--
--   -- The owner now sees nothing without a tenant, and its own rows with one.
--   BEGIN; SELECT count(*) FROM add_on_subscriptions; ROLLBACK;   -- expect 0
--   BEGIN; SELECT set_config('chora.tenant_id', '<a tenant>', true);
--          SELECT count(*) FROM add_on_subscriptions; ROLLBACK;   -- expect > 0
-- =============================================================================
