-- =============================================================================
-- chora-tenancy : 0029_tenants_is_synthetic.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-2020 (P0) — Tenant CSV mass-import foundations
-- ADRs          : ADR-221 (importer) + ADR-222 D1 (synthetic tenants)
-- Date          : 2026-07-03
--
-- tenants.is_synthetic marks demo/load-test tenants minted by the CSV
-- importer's synthetic modes. Classification, NOT access control:
--   - gates the ADR-222 D2 GCIP exception (synthetic_login accounts are a
--     hard 4xx outside is_synthetic tenants) and D3 teardown (closure-saga
--     fan-out is FORBIDDEN for real-mode batches);
--   - selects the entitlement posture (GQ-12: synthetic => unlock-all
--     add-ons + Stripe skipped; real => 'core' bootstrap parity);
--   - future: excluded-from-real-analytics flag (horizon).
--
-- Constant default => PG11+ fast-default (no table rewrite). `tenants` is
-- the RLS-FREE registry (0001) — no policy work here; the owner-run
-- migrate role writes it directly (same posture as 0026 hosting_mode).
-- =============================================================================

BEGIN;

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN NOT NULL DEFAULT FALSE;

-- Fleet ops scan: "list all synthetic tenants" (teardown sweeps, expiry
-- sweep P5, analytics exclusion). Partial — real tenants dominate.
CREATE INDEX IF NOT EXISTS idx_tenants_synthetic
    ON tenants (created_at)
    WHERE is_synthetic;

COMMIT;

-- =============================================================================
-- VERIFICATION (manual, after apply):
--   SELECT count(*) FROM tenants WHERE is_synthetic;          -- 0 on day one
--   \d+ tenants                                               -- column + partial index present
-- =============================================================================
