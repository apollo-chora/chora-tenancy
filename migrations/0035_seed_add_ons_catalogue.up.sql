-- =============================================================================
-- chora-tenancy : 0035_seed_add_ons_catalogue.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Track         : UX refactor Phase B, package B2 (day zero)
-- Date          : 2026-09-02
--
-- Seeds the add-on catalogue so a FRESH deployment matches the running
-- binary, and repairs the last slug-shaped code migration 0033 left alone.
--
-- WHY
-- The catalogue lives in two places and nothing reconciled them:
--
--   * the binary, in internal/domain/add_on.NewSeedCatalogue() (13 plans),
--     which the marketplace, the H+ setup wizard and the SubscriptionRegistry
--     all read;
--   * the `add_ons` table, which `add_on_subscriptions` references by
--     add_on_id and the boot hydrator joins for the code.
--
-- Migrations seeded exactly TWO of those rows: `core` (0018_seed_core_add_on)
-- and `cplus_social` (0021_chora_master_public_tenant item 5). Every other
-- row on the live estate was created out of band, which is why 0021's
-- backfill had rows to slugify at all. Measured on a throwaway PostgreSQL
-- 18.6 with 0001 through 0034 applied: `SELECT code, name FROM add_ons`
-- returns 2 rows. So on a brand new environment the H+ Add-Ons and
-- Marketplace screens offer eleven plans the database cannot represent, and
-- the failure is SILENT: an unknown code does not raise, the marketplace
-- simply renders the plan as unavailable and the hydrator simply skips it.
--
-- WHAT THIS DOES
--   1. Repairs `pvp_arena` to `pvp-arena`. Migration 0033 repaired four
--      slug codes and deliberately exempted this one, because at the time
--      there was no catalogue entry to point it at. There is: the seed
--      catalogue has registered `pvp-arena` since commit c26199f50 and the
--      FE setup wizard offers it. A hyphen and an underscore are one
--      character apart, and the drift costs a paying tenant their plan.
--   2. Inserts the eleven plans a fresh database is missing, with the
--      display names, prices and currency the binary uses.
--   3. Fails loud if any canonical code is still unreachable afterwards.
--
-- THE ONE EXEMPTION, DELIBERATE AND UNCHANGED
-- The catalogue calls the baseline plan `base`; the row is named and coded
-- `core`. It is NOT renamed here and NO `base` row is created, for the two
-- reasons 0033 already recorded: pg.BootstrapRepository resolves the
-- baseline inside the bootstrap transaction with
-- `SELECT add_on_id FROM add_ons WHERE name = 'core'`, and
-- pg.TranslateAddOnCode maps `core` to `base` at read time. Creating a
-- second baseline row would strand every existing subscription and give a
-- future writer two rows to choose between. The boot reconciler honours the
-- same bridge (addon.LegacyBaseCode).
--
-- DISPLAY NAMES MATCH THE LIVE ROWS, NOT TODAY'S VOCABULARY
-- `familiar` is seeded as "Familiar Companion", which is what
-- NewSeedCatalogue says and what 0021's slug `familiar_companion` implies
-- the live row is called. ADR-254 renamed the product to Companion, but the
-- idempotency key here is `name`, so using the newer word would INSERT A
-- DUPLICATE on the live estate instead of skipping. Renaming the display
-- text is a separate change with its own reconciliation.
--
-- IDEMPOTENCY
-- Guarded on BOTH unique keys. `add_ons.name` is UNIQUE
-- (add_ons_name_key, 0001) and `add_ons.code` is UNIQUE (uq_add_ons_code,
-- 0021), so a row that exists under either key must not be inserted again.
-- `ON CONFLICT (name) DO NOTHING` covers the first; the `WHERE NOT EXISTS`
-- on code covers the second, which matters because a live row's display
-- name could have been edited since the backfill. Re-applying this
-- migration against an already-seeded estate is a no-op.
--
-- RLS
-- `add_ons` is a GLOBAL catalogue with no RLS. Verified on the same
-- throwaway instance: `pg_class.relrowsecurity` and `relforcerowsecurity`
-- are both false. So no `SET LOCAL chora.tenant_id` is required, and this
-- migration adds no policy. It is not an RLS-bypass surface and does not
-- touch the ADR-165/184/192/255 chain.
--
-- IDs
-- Fixed UUIDv7-shaped ids in the `a7d000NN` family, continuing the sequence
-- 0018 (`a7d00000-...-000000000000`) and 0021 (`a7d00006-...-000000000006`)
-- established. Fixed rather than generated so the down migration can name
-- exactly what this file inserted.
--
-- Rows this migration is expected to change on a FRESH database: 11 inserts.
-- On the live estate as 0033 left it: 1 update (pvp_arena) + 6 inserts
-- (campus-ops, marketplace, daily_dose, kg_hexagonal, course_application,
-- governance_dashboard); the other five already exist under their names.
-- =============================================================================

BEGIN;

-- 1. Repair the last slug code. Keyed on the CURRENT value, so re-running is
--    a no-op, and scoped so it cannot touch a row that already holds the
--    canonical code.
UPDATE add_ons SET code = 'pvp-arena' WHERE code = 'pvp_arena';

-- 2. Seed the plans a fresh database is missing.
--
--    capabilities is left '{}', matching the `core` seed in 0018. The
--    runtime catalogue is the Go seed, and the marketplace reads its
--    entitlements from there; this column is a read-time convenience for FE
--    feature gating that no current consumer populates for these plans.
--    Setting invented capability strings here would be a fabrication.
INSERT INTO add_ons (add_on_id, name, description, monthly_price_cents, currency, capabilities, public, self_hosted_only, code)
SELECT v.add_on_id::uuid, v.name, v.description, v.monthly_price_cents, v.currency, '{}'::text[], TRUE, FALSE, v.code
FROM (VALUES
    ('a7d00010-0000-7000-8000-000000000010', 'Training Management Suite', 'Course, class, booking, rostering and certification administration on the R+ surface.', 4900::bigint, 'SGD', 'tms'),
    ('a7d00011-0000-7000-8000-000000000011', 'Content Management Suite', 'Atom authoring, revisions, collections and question banks on the A+ creator surface.', 2900::bigint, 'SGD', 'cms'),
    ('a7d00012-0000-7000-8000-000000000012', 'PvP Arena', 'Head-to-head learner duels with matchmaking and seasonal ladders.', 1900::bigint, 'SGD', 'pvp-arena'),
    ('a7d00013-0000-7000-8000-000000000013', 'Familiar Companion', 'Per-learner Companion with growth stages, memory and in-flow chat.', 990::bigint, 'SGD', 'familiar'),
    ('a7d00014-0000-7000-8000-000000000014', 'Campus Operations', 'Live classroom, polling, attendance and digital signage for campus delivery.', 9900::bigint, 'SGD', 'campus-ops'),
    ('a7d00015-0000-7000-8000-000000000015', 'Marketplace Add-Ons', 'Tenant-facing marketplace for browsing and subscribing to optional plans.', 1490::bigint, 'SGD', 'marketplace'),
    ('a7d00016-0000-7000-8000-000000000016', 'AI Assist (Tier-4 Guardrail)', 'Guardrail-screened AI Assist generation (Cloud Model Armor + Cloud DLP + regex baseline + output validators).', 1490::bigint, 'SGD', 'ai_assist'),
    ('a7d00017-0000-7000-8000-000000000017', 'Familiar Daily Dose', 'Daily 5-atom dose composed deterministically from TopicRetention; Notifications subscriber drives in-app banner.', 590::bigint, 'SGD', 'daily_dose'),
    ('a7d00018-0000-7000-8000-000000000018', 'Hexagonal Knowledge Graph', 'Per-user hexagonal Knowledge Graph fog with 1 focal + 6 LLM-generated neighbours (per ADR-143).', 990::bigint, 'SGD', 'kg_hexagonal'),
    ('a7d00019-0000-7000-8000-000000000019', 'Course Application + Checkout', 'Stripe Checkout for paid course applications + interview + tax invoice + confirmation flow.', 2490::bigint, 'SGD', 'course_application'),
    ('a7d0001a-0000-7000-8000-00000000001a', 'Governance Dashboard (O+)', 'O+ Observability+ baseline IMDA evidence dashboard (D1 accountability / D2 transparency / D3 safety / D4 fairness per ADR-141).', 4900::bigint, 'SGD', 'governance_dashboard')
) AS v(add_on_id, name, description, monthly_price_cents, currency, code)
WHERE NOT EXISTS (SELECT 1 FROM add_ons a WHERE a.code = v.code)
ON CONFLICT (name) DO NOTHING;

-- 3. Fail loud if any canonical code is still unreachable.
--
--    This is the point of the migration, not decoration. A seed that half
--    applies and reports success is exactly the silent failure the whole
--    package exists to close. `base` is satisfied by the `core` row per the
--    exemption above; every other code must have a row of its own.
DO $$
DECLARE
    unreachable TEXT;
BEGIN
    SELECT string_agg(c.code, ', ' ORDER BY c.code)
      INTO unreachable
      FROM (VALUES
        ('base'), ('tms'), ('cms'), ('pvp-arena'), ('familiar'),
        ('campus-ops'), ('marketplace'), ('ai_assist'), ('daily_dose'),
        ('kg_hexagonal'), ('course_application'), ('governance_dashboard'),
        ('cplus_social')
      ) AS c(code)
     WHERE NOT EXISTS (
        SELECT 1 FROM add_ons a
         WHERE a.code = c.code
            OR (c.code = 'base' AND a.code = 'core')
     );
    IF unreachable IS NOT NULL THEN
        RAISE EXCEPTION
            '0035: add_ons is still missing a row for these canonical codes: %',
            unreachable;
    END IF;
END $$;

COMMIT;
