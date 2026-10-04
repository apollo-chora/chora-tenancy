-- =============================================================================
-- chora-tenancy : 0023_members_multi_role_unique.down.sql
--
-- Reverses 0023_members_multi_role_unique.up.sql.
--
-- NOTE: This rollback FAILS if any tenant currently holds >1 role per
-- (tenant_id, gcid) — the old constraint can't be re-established without
-- de-duplication. Operators must reconcile multi-role members first
-- (consolidate to a single role or soft-delete the extras) before running
-- this down migration.
-- =============================================================================

BEGIN;

ALTER TABLE members DROP CONSTRAINT members_tenant_gcid_role_key;

ALTER TABLE members
  ADD CONSTRAINT members_tenant_id_gcid_key
  UNIQUE (tenant_id, gcid);

COMMIT;
