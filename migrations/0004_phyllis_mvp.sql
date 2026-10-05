-- =============================================================================
-- chora-tenancy : 0004_phyllis_mvp.sql
--
-- Domain        : Tenancy + Billing combined (supporting/platform)
-- Database      : chora_tenancy
-- Date          : 2026-05-09
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 1 D1) — H+ Hub+
--
-- Phyllis MVP §5.2 + S2.2 fill-gaps:
--
--   1. Add light-wizard fields to tenants:
--      - slug              UNIQUE (idempotency key for POST /v1/tenants)
--      - country           ISO 3166-1 alpha-2 (default 'SG')
--      - default_currency  ISO 4217 (default 'SGD')
--      - stripe_customer_id  populated by /golive
--      - activated_at      stamped by /golive
--
--   2. Add 'pending_activation' to tenant_status enum so freshly-created
--      light-wizard tenants start outside 'active' until /golive flips them.
--
--   3. addon_usage_events  stage table — high-volume per-event log written
--      from per-service usage emitters. The nightly rollup job
--      (cmd/usage-rollup) walks this table and
--      aggregates into addon_usage_daily then emits
--      chora.tenancy.addon.usage_recorded.v1 per (tenant, addon, date).
--
--   4. addon_usage_rollups view  — convenience read alias for STITCH-H-ADD-3
--      so the H+ usage analytics screen does not have to join the daily
--      table to addons + tenants in every render.
--
--   5. (Catalog alignment is a Go-code concern — handled by
--      NewSeedCatalogue in internal/domain/add_on/add_on.go which now seeds
--      the 12 Phyllis-MVP-scope add-ons. No SQL seed exists.)
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. tenants — add light-wizard fields
-- -----------------------------------------------------------------------------

-- Postgres ENUM cannot ALTER ADD VALUE inside a transaction in older
-- versions; for PG 15+ this is fine. Wrap defensively for migration tools
-- that batch-up DDL.
ALTER TYPE tenant_status ADD VALUE IF NOT EXISTS 'pending_activation' BEFORE 'active';

ALTER TABLE tenants
    ADD COLUMN slug                VARCHAR(64)     NULL,
    ADD COLUMN country             CHAR(2)         NOT NULL DEFAULT 'SG',
    ADD COLUMN default_currency    CHAR(3)         NOT NULL DEFAULT 'SGD',
    ADD COLUMN stripe_customer_id  VARCHAR(64)     NULL,
    ADD COLUMN activated_at        TIMESTAMPTZ     NULL,
    ADD CONSTRAINT chk_tenants_country_iso     CHECK (country ~ '^[A-Z]{2}$'),
    ADD CONSTRAINT chk_tenants_currency_iso    CHECK (default_currency ~ '^[A-Z]{3}$'),
    ADD CONSTRAINT chk_tenants_slug_format     CHECK (
        slug IS NULL OR slug ~ '^[a-z0-9](?:[a-z0-9-]{1,62}[a-z0-9])?$'
    );

-- Slug is UNIQUE per non-deleted tenant; allows soft-deleted tenants to
-- free their slugs for reuse via partial index.
CREATE UNIQUE INDEX idx_tenants_slug_unique
    ON tenants (slug)
    WHERE slug IS NOT NULL AND deleted_at IS NULL;

CREATE UNIQUE INDEX idx_tenants_stripe_customer_id_unique
    ON tenants (stripe_customer_id)
    WHERE stripe_customer_id IS NOT NULL AND deleted_at IS NULL;

-- -----------------------------------------------------------------------------
-- 3. addon_usage_events — high-volume stage table
-- -----------------------------------------------------------------------------

CREATE TABLE addon_usage_events (
    event_id           UUID                          PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          UUID                          NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    add_on_id          UUID                          NOT NULL REFERENCES add_ons(add_on_id) ON DELETE RESTRICT,
    occurred_at        TIMESTAMPTZ                   NOT NULL DEFAULT now(),
    bucket_date        DATE                          NOT NULL,
    seats_used_delta   BIGINT                        NOT NULL DEFAULT 0,
    quota_consumed_delta BIGINT                      NOT NULL DEFAULT 0,
    api_calls_delta    BIGINT                        NOT NULL DEFAULT 0,
    error_count_delta  BIGINT                        NOT NULL DEFAULT 0,
    -- Idempotency key for the rollup job (per envelope idempotency_key).
    source_event_id    UUID                          NULL,
    created_at         TIMESTAMPTZ                   NOT NULL DEFAULT now()
);

CREATE INDEX idx_addon_usage_events_bucket
    ON addon_usage_events (bucket_date, tenant_id, add_on_id);

CREATE INDEX idx_addon_usage_events_tenant_addon
    ON addon_usage_events (tenant_id, add_on_id, occurred_at DESC);

ALTER TABLE addon_usage_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON addon_usage_events
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- 4. addon_usage_rollups — convenience view over addon_usage_daily
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW addon_usage_rollups AS
SELECT
    d.tenant_id,
    d.add_on_id,
    -- NOTE: add_ons table has no `code` column (only `name`). Until a future
    -- migration adds a machine-readable code, both columns return `name`.
    -- Frontend can dedupe; view stays present for H+ analytics screen.
    a.name      AS addon_code,
    a.name      AS addon_display_name,
    d.bucket_date,
    d.seats_used,
    d.seats_used_peak,
    d.quota_consumed,
    d.api_calls,
    d.error_count,
    d.last_recorded_at
FROM addon_usage_daily d
JOIN add_ons a ON a.add_on_id = d.add_on_id;

COMMENT ON VIEW addon_usage_rollups IS
    'STITCH-H-ADD-3 convenience view: addon_usage_daily JOIN add_ons for the H+ usage analytics screen.';

COMMIT;
