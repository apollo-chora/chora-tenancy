-- =============================================================================
-- chora-tenancy : 0036_one_live_owner_per_tenant.down.sql
--
-- Drops the partial unique index. Nothing else: 0036.up moved no data, so
-- there is no data to restore, and the repository guards that enforce the
-- other half of the invariant are code, not schema.
--
-- Dropping it does not make two owners appear. It only stops the database
-- refusing them, which leaves the Go guards as the sole enforcement.
-- =============================================================================

DROP INDEX IF EXISTS uq_members_one_live_owner_per_tenant;
