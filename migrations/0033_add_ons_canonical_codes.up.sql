-- =============================================================================
-- chora-tenancy : 0033_add_ons_canonical_codes.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-2414
-- Date          : 2026-08-24
--
-- Repairs add_ons.code to the canonical CHO-1745 catalogue codes.
--
-- WHY
-- Migration 0021 added add_ons.code with the stated intent of giving the FE a
-- "stable machine code for FE feature gating ... so FeatureFlagService
-- .isEnabled('<code>') works against live data instead of fail-loud false".
-- Its backfill was:
--
--     UPDATE add_ons
--     SET    code = lower(regexp_replace(name, '[^a-zA-Z0-9]+', '_', 'g'))
--     WHERE  code IS NULL;
--
-- That is a slug of the human display name, not the canonical code. So the
-- live catalogue holds `training_management_suite` where both the in-memory
-- seed catalogue (internal/domain/add_on.NewSeedCatalogue) and the FE setup
-- wizard (chora-web .../setup-wizard.component.ts AVAILABLE_ADD_ONS) say
-- `tms`. Only the row inserted explicitly by 0021 item 5 (cplus_social) and
-- the legacy `core` row ever carried a code the runtime recognises.
--
-- Consequence, measured on the live estate 2026-08-24: the boot subscription
-- hydrator replayed 2 of 9 active subscriptions and /h/addons rendered
-- "No add-ons activated yet" for a tenant with four active, billed
-- subscriptions.
--
-- SAFETY
-- - add_on_subscriptions references add_ons(add_on_id), never the code, so no
--   foreign key is disturbed.
-- - uq_add_ons_code is a UNIQUE index. None of the target codes (tms, cms,
--   familiar, ai_assist) is present, so no collision is possible. The
--   WHERE clauses below are keyed on the CURRENT slug value, so re-running
--   this migration is a no-op.
-- - `core` is deliberately NOT renamed: pg.BootstrapRepository resolves the
--   baseline add-on with `SELECT add_on_id FROM add_ons WHERE name = 'core'`,
--   and pg.TranslateAddOnCode already maps the code `core` to `base` at read
--   time. Renaming it here would change nothing and risk that lookup.
-- - `pvp_arena` is deliberately NOT renamed: it has NO counterpart in the
--   seed catalogue at all, so there is no canonical code to point it at.
--   Inventing a sellable SKU (price, tiers, capabilities) is a product
--   decision, not a defect fix. Tenant 22222222-2222-7222-8222-222222222222
--   holds an active subscription to it, so after this migration the hydrator
--   still skips exactly that one row, loudly and by name. Raised to the owner
--   as a separate decision item.
-- - add_ons is a global catalogue with no RLS, and the runner connects as the
--   owning migrate role, so no SET LOCAL chora.tenant_id is required.
--
-- Rows this migration is expected to change on the live estate: 4.
-- =============================================================================

BEGIN;

UPDATE add_ons SET code = 'tms'       WHERE code = 'training_management_suite';
UPDATE add_ons SET code = 'cms'       WHERE code = 'content_management_suite';
UPDATE add_ons SET code = 'familiar'  WHERE code = 'familiar_companion';
UPDATE add_ons SET code = 'ai_assist' WHERE code = 'ai_assist_tier_4_guardrail_';

-- Fail loud if the repair left a subscribed add-on on a slug-shaped code that
-- the runtime catalogue cannot key on. `core` and `pvp_arena` are the two
-- adjudicated exemptions documented above; anything else appearing here means
-- a new add-on was seeded by display-name slug and this migration needs a new
-- line rather than the estate quietly regressing.
DO $$
DECLARE
    stragglers TEXT;
BEGIN
    SELECT string_agg(DISTINCT a.name || ' (code=' || COALESCE(a.code, '<NULL>') || ')', ', ')
      INTO stragglers
      FROM add_ons a
      JOIN add_on_subscriptions s ON s.add_on_id = a.add_on_id
     WHERE s.status = 'active'::entitlement_status
       AND s.ended_at IS NULL
       AND COALESCE(a.code, '') NOT IN (
            'base', 'core', 'tms', 'cms', 'familiar', 'ai_assist',
            'cplus_social', 'daily_dose', 'kg_hexagonal',
            'course_application', 'governance_dashboard', 'marketplace',
            'pvp_arena'
       );
    IF stragglers IS NOT NULL THEN
        RAISE EXCEPTION
            '0033: active subscriptions still reference non-canonical add-on codes: %',
            stragglers;
    END IF;
END $$;

COMMIT;
