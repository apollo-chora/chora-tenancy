-- =============================================================================
-- chora-tenancy : 0029_tenants_is_synthetic.down.sql
-- Reverts 0029_tenants_is_synthetic.up.sql (CHO-2020 / ADR-222 D1).
-- =============================================================================

BEGIN;

DROP INDEX IF EXISTS idx_tenants_synthetic;

ALTER TABLE tenants
    DROP COLUMN IF EXISTS is_synthetic;

COMMIT;
