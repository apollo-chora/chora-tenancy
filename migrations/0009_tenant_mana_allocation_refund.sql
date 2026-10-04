-- =============================================================================
-- chora-tenancy : 0009_tenant_mana_allocation_refund.sql
--
-- Domain        : Tenancy + Billing combined (supporting/platform)
-- Database      : chora_tenancy
-- Date          : 2026-05-13
-- ADR           : adr-149-familiar-growth-stage-model.md §"Unhatched egg expiry"
--                 + adr-142-per-user-economy-mana.md (extends 0003 schema)
--
-- Adds the platform-side "egg hard-expiry refund" path to the tenant subsidy
-- track (ADR-142 0003 schema):
--
--   1. tenant_mana_allocation_reason enum gains `egg_hard_expiry_refund`
--      — surfaces as canonical reason in the AllocationGranted event payload
--      so chora-identity (the wallet owner) can attribute the credit correctly.
--
--   2. tenant_mana_allocations.source_pool_id becomes NULLABLE specifically
--      for the egg-refund reason — platform refunds are NOT sourced from a
--      tenant pool (refunds come from the chora-side ledger, not from a
--      tenant's mana balance). CHECK constraint enforces:
--         a. egg_hard_expiry_refund → source_pool_id IS NULL
--         b. every other reason → source_pool_id IS NOT NULL (existing FK)
--
--   3. tenant_mana_allocations gains an `idempotency_key VARCHAR(128) NULL`
--      column with a partial UNIQUE INDEX (only when key is non-null). The
--      familiar_egg_sweeper uses this to de-dup refund attempts across
--      sweeper retries / DLQ replays. The same column also unblocks future
--      idempotency-keyed allocation paths (manual grant retry, etc.).
--
-- This extension is back-compat:
--   - Existing rows (no idempotency_key, with source_pool_id) keep their
--     constraints.
--   - New refund rows can carry NULL source_pool_id + a non-null
--     idempotency_key.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Extend the reason enum
--
-- PostgreSQL forbids `ALTER TYPE ... ADD VALUE` inside an explicit transaction
-- block (errors with: `cannot run inside a transaction block`). Native
-- `IF NOT EXISTS` makes this idempotent — re-applying produces no error if
-- the value already exists. Runs OUTSIDE the §2-§3 transaction wrapper below.
-- -----------------------------------------------------------------------------

ALTER TYPE tenant_mana_allocation_reason
    ADD VALUE IF NOT EXISTS 'egg_hard_expiry_refund';

BEGIN;

-- -----------------------------------------------------------------------------
-- 2. Relax source_pool_id NOT NULL for egg-refund rows
-- -----------------------------------------------------------------------------

ALTER TABLE tenant_mana_allocations
    ALTER COLUMN source_pool_id DROP NOT NULL;

-- Belt-and-braces invariant: refund reason MUST have NULL source_pool_id;
-- every other reason MUST have a non-null source_pool_id (existing FK still
-- applies when non-null).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'tenant_mana_allocations'::regclass
          AND conname = 'chk_allocation_refund_source_pool'
    ) THEN
        ALTER TABLE tenant_mana_allocations
            ADD CONSTRAINT chk_allocation_refund_source_pool CHECK (
                (allocation_reason = 'egg_hard_expiry_refund' AND source_pool_id IS NULL)
                OR
                (allocation_reason <> 'egg_hard_expiry_refund' AND source_pool_id IS NOT NULL)
            );
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 3. Add idempotency_key + partial UNIQUE INDEX
-- -----------------------------------------------------------------------------

ALTER TABLE tenant_mana_allocations
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128);

-- Partial unique index — only rows with a non-null idempotency_key are
-- de-duplicated. Existing rows with NULL keep their pre-0009 semantics.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_tenant_mana_alloc_idempotency
    ON tenant_mana_allocations (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMIT;
