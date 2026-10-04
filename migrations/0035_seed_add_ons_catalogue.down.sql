-- =============================================================================
-- chora-tenancy : 0035_seed_add_ons_catalogue.down.sql
--
-- Reverts 0035: removes the eleven seeded catalogue rows and puts the PvP
-- Arena code back to the slug it carried before.
--
-- WHAT THIS DELIBERATELY REFUSES TO DO
--
-- 1. It never deletes a row that anything subscribes to. `add_on_subscriptions`
--    references `add_ons(add_on_id)`, so a blind DELETE would either raise a
--    foreign-key violation or, worse under a cascade, take a tenant's paid
--    subscription with it. The guard is a NOT EXISTS on the subscription
--    table, so a partially-adopted catalogue reverts to exactly the rows
--    nobody is using and leaves the rest standing.
--
-- 2. It never touches `core` or `cplus_social`. Those are seeded by 0018 and
--    0021 and are not this migration's to remove.
--
-- 3. It deletes BY ID, not by code or name. The ids are the fixed ones 0035
--    inserted, so a row that happened to be created out of band under the
--    same code is not swept up by a revert of a migration that did not
--    create it.
--
-- The `pvp_arena` restore is deliberately last and deliberately conditional:
-- if the PvP Arena row is one of the rows a tenant subscribes to, it stays,
-- and its code goes back to the pre-0035 value so the estate is exactly as
-- 0033 left it.
--
-- Reverting is therefore SAFE but not always COMPLETE, and that is the right
-- trade: a revert that silently destroys a subscription would be worse than
-- one that leaves a row behind and says so in the row count.
-- =============================================================================

BEGIN;

DELETE FROM add_ons a
 WHERE a.add_on_id IN (
        'a7d00010-0000-7000-8000-000000000010'::uuid,
        'a7d00011-0000-7000-8000-000000000011'::uuid,
        'a7d00012-0000-7000-8000-000000000012'::uuid,
        'a7d00013-0000-7000-8000-000000000013'::uuid,
        'a7d00014-0000-7000-8000-000000000014'::uuid,
        'a7d00015-0000-7000-8000-000000000015'::uuid,
        'a7d00016-0000-7000-8000-000000000016'::uuid,
        'a7d00017-0000-7000-8000-000000000017'::uuid,
        'a7d00018-0000-7000-8000-000000000018'::uuid,
        'a7d00019-0000-7000-8000-000000000019'::uuid,
        'a7d0001a-0000-7000-8000-00000000001a'::uuid
       )
   AND NOT EXISTS (
        SELECT 1 FROM add_on_subscriptions s WHERE s.add_on_id = a.add_on_id
       );

-- Restore the pre-0035 slug on whatever PvP Arena row survives (either a row
-- that predates 0035 on the live estate, or one this revert could not remove
-- because a tenant subscribes to it).
UPDATE add_ons SET code = 'pvp_arena' WHERE code = 'pvp-arena';

COMMIT;
