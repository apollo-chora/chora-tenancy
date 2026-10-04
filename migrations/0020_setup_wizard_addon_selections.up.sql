-- 0020_setup_wizard_addon_selections.up.sql — CHO-1692.
--
-- Persists the Setup Wizard step 2 (CHO-1664) add-on selections so the
-- Phase D re-entry hydration (CHO-1692) can pre-fill the wizard's
-- selectedAddOns Set after a service restart. Distinct from
-- add_on_subscriptions which models admin-side lifecycle (with add_on_id
-- → add_ons UUID joins, status transitions, billing snapshots). The
-- wizard captures DECLARED INTENT against the in-memory catalogue codes
-- ("base", "tms", "cms", ...) which is the source of truth for which
-- features the tenant ticked during onboarding.
--
-- RLS: same tenant_isolation pattern as siblings in 0001_initial.sql.
-- Composite PK ensures idempotent-additive upserts (per the wizard's
-- contract: re-clicking Next is a no-op for already-selected codes).
CREATE TABLE setup_wizard_addon_selections (
    tenant_id    UUID                     NOT NULL,
    add_on_code  VARCHAR(64)              NOT NULL,
    status       VARCHAR(32)              NOT NULL DEFAULT 'pending_activation',
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, add_on_code)
);

ALTER TABLE setup_wizard_addon_selections ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON setup_wizard_addon_selections
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Index for fast tenant lookups (PK already covers, but explicit for
-- ListByTenant which is the hot read-path for the wizard's mount).
CREATE INDEX idx_setup_wizard_addon_selections_tenant
    ON setup_wizard_addon_selections (tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON setup_wizard_addon_selections TO chora_tenancy_app_rw;
