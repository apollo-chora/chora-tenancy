-- =============================================================================
-- chora-tenancy : 0019_tenants_wizard_completed_at.up.sql
--
-- Domain        : Tenancy + Billing
-- Database      : chora_tenancy
-- Contract      : chora-contracts/openapi/tenancy-admin.yaml
--                 (`PATCH /api/v1/tenants/me/finish-setup`)
--                 services/chora-tenancy/internal/domain/tenant/tenant.go
--                 (`Tenant.FinishWizard`)
-- Story         : CHO-1682 — Tenant Lifecycle | Setup Wizard | Phase C
--                 (Identity provider + Apply / finish-setup — BE+BFF)
-- Date          : 2026-06-07
--
-- Purpose:
--   Setup Wizard step 4 Apply stamps a `wizard_completed_at` timestamp on
--   the tenant row so the platform can tell whether a tenant has finished
--   onboarding. The BFF aggregator at POST /api/v1/tenants/setup calls
--   PATCH /api/v1/tenants/me/finish-setup AFTER the chora-identity IdP
--   provider write succeeds (CHO-1682 fan-out contract — failed identity
--   blocks tenancy finish; failed tenancy after identity-success surfaces
--   502 + the caller may retry the whole call since both endpoints are
--   idempotent).
--
-- Semantics: SET-ONCE. The Tenant.FinishWizard domain method refuses to
-- overwrite a non-NULL timestamp; the HTTP handler uses the returned
-- `newly` flag to decide whether to emit
-- `chora.tenancy.tenant_wizard_completed.v1` on the outbox. Re-finishing
-- is a 200 OK no-op that returns the existing timestamp.
--
-- Scope: tenants table is tenant-id-keyed (one row per tenant) — adding a
-- NULL-by-default column is a pure additive schema change. No backfill is
-- needed; existing rows correctly report "wizard not yet finished" until
-- the H+ admin re-runs (or completes for the first time) the wizard. No
-- index added — finish-setup is a single-row UPDATE keyed by primary key.
--
-- HARD RULE: cross-database queries forbidden — this migration touches
-- only chora_tenancy.tenants. Cross-domain side effects (identity audit,
-- billing entitlement unlocks) flow through events.
-- =============================================================================

BEGIN;

ALTER TABLE tenants
    ADD COLUMN wizard_completed_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN tenants.wizard_completed_at IS
    'Setup Wizard step 4 Apply completion timestamp (CHO-1682). Set-once at chora-tenancy/internal/domain/tenant.Tenant.FinishWizard. NULL until the H+ admin finishes the wizard via PATCH /api/v1/tenants/me/finish-setup.';

COMMIT;
