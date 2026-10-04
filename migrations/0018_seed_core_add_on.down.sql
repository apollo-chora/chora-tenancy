-- =============================================================================
-- chora-tenancy : 0018_seed_core_add_on.down.sql
-- =============================================================================
--
-- Guarded by BOTH `add_on_id` and `name` so an unrelated 'core'-named row
-- (e.g. a future seed someone re-keyed) is not accidentally dropped. Down
-- migration is destructive of the bootstrap path — any tenant whose
-- `add_on_subscriptions` row references this add_on_id will be left dangling
-- relative to the FK (ON DELETE RESTRICT) so the DELETE will fail until
-- subscriptions are removed. That's the safe default.
--
-- =============================================================================

BEGIN;

DELETE FROM add_ons
WHERE add_on_id = 'a7d00000-0000-7000-8000-000000000000'
  AND name = 'core';

COMMIT;
