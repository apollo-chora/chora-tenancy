-- =============================================================================
-- chora-tenancy : 0012_members_reenable_rls.sql
--
-- D0.1 (CHO-1538) follow-up — RE-ENABLE Row-Level Security on the
-- `members` table, closing the cross-tenant data-leak regression.
--
-- BACKGROUND
--   The BE-service post-cutover handoff
--   (docs/m13/handoff-be-service-claude-2026-05-14.md §3 P0) records that
--   `ALTER TABLE members DISABLE ROW LEVEL SECURITY` was applied to the
--   live chora_tenancy DB as a workaround so the mint flow's cross-tenant
--   `ListMembershipsByGCID` query would return rows. That workaround was
--   applied out-of-band — no migration in version control performs it.
--
--   Live-DB reconciliation (2026-05-14, read-only job inspection)
--   CONFIRMED the live state: `members.relrowsecurity = f`. With RLS off,
--   any holder of `chora_tenancy_app_rw` can read EVERY tenant's member
--   directory — a multi-tenant isolation breach.
--
--   Migration 0011 introduced the `list_memberships_by_gcid(uuid)`
--   SECURITY DEFINER function, which is the *correct* mechanism for the
--   cross-tenant directory query (it bypasses RLS only inside its own
--   `SET row_security = off` boundary, only for that one query, only for
--   the app_rw role granted EXECUTE). With that path in place, the
--   table-wide RLS DISABLE is no longer needed and MUST be reverted.
--
-- WHAT THIS DOES
--   1. ALTER TABLE members ENABLE ROW LEVEL SECURITY.
--   2. Defensively re-assert the `tenant_isolation` policy (DROP IF EXISTS
--      + CREATE) so the policy shape is guaranteed identical to
--      0001_initial.sql even if the live policy drifted. The live inspect
--      showed the policy IS still present — this is belt-and-braces.
--
--   After this migration:
--     - Ordinary SELECT/INSERT/UPDATE/DELETE on `members` by app_rw is
--       filtered by `tenant_id = current_setting('chora.tenant_id', true)`
--       — i.e. callers MUST `SET LOCAL chora.tenant_id` (the existing
--       rls.RunInTx contract for tenant-scoped repos).
--     - The cross-tenant directory query continues to work via the 0011
--       SECURITY DEFINER function.
--
-- ORDERING
--   This file is 0012 — it lex-sorts AFTER 0011, so the migrations-runner
--   applies the SECURITY DEFINER function FIRST, then re-enables RLS. The
--   mint flow is therefore never left in a window where RLS is on but the
--   bypass function does not yet exist.
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Author  : tenancy-rls agent (D0.1 Phyllis backend demo-readiness)
-- Date    : 2026-05-14
--
-- IDEMPOTENCY
--   - ENABLE ROW LEVEL SECURITY is a no-op when RLS is already enabled.
--   - DROP POLICY IF EXISTS + CREATE POLICY makes the policy assertion
--     re-runnable. Safe under the migrations-runner dedup table and under
--     session_replication_role='replica'.
--
-- NOTE on session_replication_role
--   The migrations-runner sets `session_replication_role = 'replica'`
--   before applying each file (to bypass FORCE-RLS + triggers for DDL).
--   That setting affects DML row visibility, NOT the DDL statements here:
--   ALTER TABLE ... ENABLE RLS and CREATE POLICY are catalog changes that
--   apply regardless. The post-apply state is RLS-enabled.
--
-- HARD RULE: cross-database queries forbidden. Touches only the
-- chora_tenancy-local `members` table.
-- =============================================================================

BEGIN;

-- 1) Re-enable RLS on members. No-op if already enabled.
ALTER TABLE members ENABLE ROW LEVEL SECURITY;

-- 2) Defensively re-assert the tenant_isolation policy. Identical shape to
--    0001_initial.sql §members. DROP IF EXISTS keeps this re-runnable and
--    self-heals any drift in the live policy definition.
DROP POLICY IF EXISTS tenant_isolation ON members;
CREATE POLICY tenant_isolation ON members
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   -- RLS is back ON for members.
--   SELECT relname, relrowsecurity, relforcerowsecurity
--     FROM pg_class WHERE relname = 'members';
--   -- expect: relrowsecurity = t
--
--   -- The tenant_isolation policy is present and unchanged.
--   SELECT policyname, cmd, qual FROM pg_policies WHERE tablename = 'members';
--   -- expect: tenant_isolation | ALL | (tenant_id = (current_setting('chora.tenant_id'::text, true))::uuid)
--
--   -- Cross-tenant directory query STILL works (via the 0011 function).
--   SET ROLE chora_tenancy_app_rw;
--   SELECT count(*) FROM list_memberships_by_gcid('<some-gcid>'::uuid);  -- > 0 for a real user
--   -- An inline cross-tenant SELECT, by contrast, is now RLS-filtered:
--   SELECT count(*) FROM members WHERE gcid = '<some-gcid>';            -- 0 (no chora.tenant_id set)
--   RESET ROLE;
-- =============================================================================
