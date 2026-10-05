-- =============================================================================
-- chora-tenancy : 0017_drop_purchases_after_extract.up.sql
--
-- Domain        : Tenancy + Billing (supporting)
-- Database      : chora_tenancy
-- Author        : ADR-164 Wave 1 Stage D follow-on
-- Date          : 2026-05-24
--
-- ADR-164 Wave 1 Stage F follow-on — DROP the source chora_tenancy tables
-- whose contents have been extracted to chora_payments via migration 0016
-- + the extract-purchases-to-payments Go binary:
--
--   chora_tenancy.familiar_egg_purchases   → DROPPED (now in chora_payments)
--   chora_tenancy.familiar_egg_catalog     → KEPT (catalog is tenant-config,
--                                             not payment correlation)
--   chora_tenancy.tenant_mana_pool_topups  → DROPPED (now in chora_payments
--                                             as tenant_mana_topups)
--
-- WARNING: this migration is DESTRUCTIVE. DO NOT apply until ALL of these
-- are GREEN:
--
--   1. Migration 0016 applied (no-op marker present in chora_tenancy
--      migration history).
--   2. The extract binary has been run with EXTRACT_MODE=apply.
--   3. Row-count parity validated per the 0016 runbook.
--   4. Stage F end-to-end smokes for both FamiliarEgg + ManaTopUp flows
--      have run GREEN against chora_payments.
--   5. The 24h soak window post-Stage F has passed (per ADR-164
--      §"Risks + mitigations" R2 — Stripe redelivery cutover window).
--
-- This migration MUST be a separate explicit operator step, NOT auto-
-- applied alongside Stage D code. The migrations-runner job
-- skips migrations matching a `--stop-at` flag; the canonical Stage D
-- apply runs `--stop-at 0016`, leaving this migration unstaged until the
-- Stage G follow-on explicitly applies it.
--
-- Reversal: 0017_drop_purchases_after_extract.down.sql re-creates the
-- DROPPED tables WITHOUT data (data must be re-extracted from
-- chora_payments via the reverse-direction Go binary).
-- =============================================================================

BEGIN;

-- --------------------------------------------------------------------------
-- Drop familiar_egg_purchases (chora-tenancy side).
-- chora_payments.familiar_egg_purchases is now the canonical home.
-- --------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_familiar_egg_purchases_purchaser;
DROP INDEX IF EXISTS idx_familiar_egg_purchases_state;
DROP INDEX IF EXISTS idx_familiar_egg_purchases_stripe_session;

DROP TABLE IF EXISTS familiar_egg_purchases CASCADE;

-- --------------------------------------------------------------------------
-- Drop tenant_mana_pool_topups (chora-tenancy side).
-- chora_payments.tenant_mana_topups is now the canonical home.
--
-- The tenant_mana_pools + tenant_mana_allocations tables STAY — they hold
-- the canonical balance + grant aggregates that chora-tenancy owns.
-- chora-payments only owns the Stripe-correlation row for the top-up
-- transaction; the balance update is materialised by the chora-tenancy
-- payments subscriber (internal/adapter/events/payments_subscriber.go).
-- --------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_tenant_mana_topups_pool;
DROP INDEX IF EXISTS idx_tenant_mana_topups_tenant;
DROP INDEX IF EXISTS idx_tenant_mana_topups_envelope;

DROP TABLE IF EXISTS tenant_mana_pool_topups CASCADE;

-- --------------------------------------------------------------------------
-- Verification.
-- --------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name = 'familiar_egg_purchases'
    ) THEN
        RAISE EXCEPTION 'chora-tenancy 0017: familiar_egg_purchases still present after DROP';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name = 'tenant_mana_pool_topups'
    ) THEN
        RAISE EXCEPTION 'chora-tenancy 0017: tenant_mana_pool_topups still present after DROP';
    END IF;
    RAISE NOTICE 'chora-tenancy 0017: extract source tables dropped successfully';
END$$;

COMMIT;
