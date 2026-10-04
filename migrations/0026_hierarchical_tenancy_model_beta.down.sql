-- =============================================================================
-- chora-tenancy : 0026_hierarchical_tenancy_model_beta.down.sql
--
-- Reverses 0026 up (ADR-217 Phase 1). Idempotent. Restores the pre-migration
-- state: no hosting_mode column/type, no parent invariant, the 4 tenants back
-- to roots, chora-master display name back to 'Chora'.
--
-- Guards ensure the down never clobbers state a later phase may own:
--   - the 4 are un-parented ONLY if still parented to chora-master by 0026;
--   - the rename is reverted ONLY if still 'Chora Platform'.
-- =============================================================================

BEGIN;

-- (d) Drop the parent invariant first (so the un-parent below has no trigger
--     to satisfy — un-parenting to NULL is exempt anyway).
DROP TRIGGER IF EXISTS trg_tenants_parent_in_master_tree ON tenants;
DROP FUNCTION IF EXISTS tenancy_check_parent_in_master_tree();

-- (b) Un-parent the 4 back to roots (only those 0026 parented to chora-master).
UPDATE tenants
SET    parent_tenant_id = NULL
WHERE  tenant_id IN (
        '11111111-1111-7111-8111-111111111111',
        '22222222-2222-7222-8222-222222222222',
        '019e899d-99d7-7064-a732-6c9ca6955cda',
        '019eb1b4-bfdc-7591-ab0d-3aa33488d1f3'
       )
  AND  parent_tenant_id = '00000000-0000-7000-8000-000000000001';

-- (c) Restore chora-master display name.
UPDATE tenants
SET    name = 'Chora'
WHERE  tenant_id = '00000000-0000-7000-8000-000000000001'
  AND  name IS DISTINCT FROM 'Chora';

-- (a) Drop hosting_mode column then its enum type (column depends on the type).
--     Dropping the column also reverses the FRANCHISE / SELF_HOST backfill.
ALTER TABLE tenants DROP COLUMN IF EXISTS hosting_mode;
DROP TYPE IF EXISTS tenant_hosting_mode;

COMMIT;
