-- =============================================================================
-- chora-tenancy : 0019_tenants_wizard_completed_at.down.sql
--
-- Reverses 0019 by dropping the `wizard_completed_at` column. Data loss is
-- acceptable on rollback because no other table or query depends on the
-- column at the SQL layer (consumers read it via the application via
-- tenant.Tenant struct; rollback implies the Phase C wizard ship is being
-- backed out anyway).
-- =============================================================================

BEGIN;

ALTER TABLE tenants DROP COLUMN IF EXISTS wizard_completed_at;

COMMIT;
