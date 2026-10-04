-- =============================================================================
-- chora-tenancy : 0015_org_unit.down.sql
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS trg_org_unit_updated_at ON org_unit;
DROP TABLE IF EXISTS org_unit;

COMMIT;
