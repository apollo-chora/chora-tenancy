-- =============================================================================
-- chora-tenancy : 0033_add_ons_canonical_codes.down.sql
--
-- Restores the migration-0021 slug values for the four add-on codes 0033
-- repaired. This reinstates the CHO-2414 defect (the boot hydrator can no
-- longer resolve those four add-ons and /h/addons goes empty for tenants
-- holding them), so it exists for reversibility, not as a remedy.
--
-- The reverse of a no-op stays a no-op: the WHERE clauses are keyed on the
-- canonical value, so running this twice changes nothing.
-- =============================================================================

BEGIN;

UPDATE add_ons SET code = 'training_management_suite'   WHERE code = 'tms';
UPDATE add_ons SET code = 'content_management_suite'    WHERE code = 'cms';
UPDATE add_ons SET code = 'familiar_companion'          WHERE code = 'familiar';
UPDATE add_ons SET code = 'ai_assist_tier_4_guardrail_' WHERE code = 'ai_assist';

COMMIT;
