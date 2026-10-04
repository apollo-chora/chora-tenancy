-- =============================================================================
-- chora-tenancy : 0021_chora_master_public_tenant.down.sql
--
-- Best-effort reverse of 0021 (soft-delete semantics per ddd-enforcement).
-- Notes:
--   - PostgreSQL cannot remove an enum value: 'author' stays on
--     tenant_member_role (harmless — rows referencing it are soft-deleted
--     below).
--   - The single-role UNIQUE(tenant_id, gcid) is only restorable when no
--     live multi-role rows exist; other tenants may have gained multi-role
--     members since 0021. The constraint restore is attempted, not
--     guaranteed.
-- =============================================================================

UPDATE add_on_subscriptions
SET    status = 'cancelled', cancelled_at = now(), updated_at = now()
WHERE  tenant_id = '00000000-0000-7000-8000-000000000001'
  AND  add_on_id = 'a7d00006-0000-7000-8000-000000000006'
  AND  status = 'active';

UPDATE members
SET    deleted_at = now(), updated_at = now()
WHERE  tenant_id = '00000000-0000-7000-8000-000000000001'
  AND  deleted_at IS NULL;

UPDATE tenants
SET    deleted_at = now(), updated_at = now()
WHERE  tenant_id = '00000000-0000-7000-8000-000000000001'
  AND  deleted_at IS NULL;

ALTER TABLE add_ons DROP COLUMN IF EXISTS code;

DROP INDEX IF EXISTS uq_members_tenant_gcid_role;
-- Best-effort: fails when live multi-role rows exist (see header note).
ALTER TABLE members ADD CONSTRAINT members_tenant_id_gcid_key UNIQUE (tenant_id, gcid);
