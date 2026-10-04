-- =============================================================================
-- chora-tenancy : 0040_add_on_subscriptions_force_rls.down.sql
--
-- Reverts row E4's RLS hardening on `add_on_subscriptions`: restores the
-- pre-0040 posture, RLS enabled but NOT forced, and the policy cast without
-- its NULLIF guard.
--
-- ⚠ REVERTING THIS RE-OPENS A BYPASS. After this runs, the table-owning
--   `chora_tenancy_migrate` role reads and writes every tenant's entitlements
--   with no tenant context, and an empty `chora.tenant_id` raises 22P02 instead
--   of returning zero rows. It is written because a migration must be
--   revertable, not because reverting is advisable. If the goal is to unblock
--   an owner-connection query, set `chora.tenant_id` in that query's
--   transaction instead; that is what every other tenant-scoped table in this
--   estate already requires.
--
-- ORDER: the policy is restored BEFORE force is dropped, so the table is never
-- momentarily policy-less while still forced (which would deny everything).
--
-- Date: 2026-09-03
-- =============================================================================

BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON add_on_subscriptions;

-- The pre-0040 expression, unguarded, exactly as migration 0012 left it.
CREATE POLICY tenant_isolation ON add_on_subscriptions
    FOR ALL
    USING (tenant_id = (current_setting('chora.tenant_id', true))::uuid);

ALTER TABLE add_on_subscriptions NO FORCE ROW LEVEL SECURITY;

COMMENT ON TABLE add_on_subscriptions IS NULL;

COMMIT;
