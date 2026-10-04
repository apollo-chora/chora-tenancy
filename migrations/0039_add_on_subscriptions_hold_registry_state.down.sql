-- =============================================================================
-- chora-tenancy : 0039_add_on_subscriptions_hold_registry_state.down.sql
--
-- Reverts row E4 slice 1 as far as PostgreSQL permits.
--
-- ⚠ THIS DOWN IS DELIBERATELY PARTIAL, AND SAYS SO RATHER THAN PRETENDING.
--   PostgreSQL has no `ALTER TYPE ... DROP VALUE`. The five labels added by
--   the up migration CANNOT be removed. Reverting them would mean recreating
--   `entitlement_status` with the original two labels and rewriting every
--   dependent column, which would fail outright on any row already carrying
--   one of the new labels and would silently destroy state on the rest. A
--   revert that destroys data is worse than a revert that is honest about
--   its limit, so this drops the COLUMNS only.
--
--   Leaving the labels is safe: an enum label nothing writes is inert. The
--   pre-0039 binary only ever wrote `active` and `cancelled`.
--
-- ORDER: columns are dropped after the comments they carry, which Postgres
-- removes with the column, so no explicit COMMENT cleanup is needed.
--
-- Date: 2026-09-03
-- =============================================================================

BEGIN;

ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS owner_gcid;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS version;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS grace_ends_at;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS scheduled_tier;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS scheduled_effective_at;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS next_renewal_at;
ALTER TABLE add_on_subscriptions DROP COLUMN IF EXISTS deactivated_at;

-- The five enum labels stay. See the header: there is no DROP VALUE, and
-- rebuilding the type would destroy the rows that use them.

COMMIT;
