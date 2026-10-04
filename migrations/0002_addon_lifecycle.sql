-- =============================================================================
-- chora-tenancy : 0002_addon_lifecycle.sql
--
-- Domain        : Tenancy + Billing combined (supporting/platform)
-- Database      : chora_tenancy
-- Date          : 2026-05-09
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 1 D1) — H+ Hub+
--
-- Adds H+ add-on lifecycle support powering 5 Stitch screens:
--   STITCH-H-ADD-1 : management dashboard sub-tab
--   STITCH-H-ADD-2 : deactivation modal with structured reason audit
--   STITCH-H-ADD-3 : usage analytics screen
--   STITCH-H-ADD-4 : in-place upgrade/downgrade flow
--   STITCH-H-ADD-5 : marketplace tile detail expansion
--
-- Adds:
--   1. Structured deactivation reason on add_on_subscriptions
--      (reason_code enum + reason_text free-form + requested_by_gcid +
--      effective_at deferral).
--   2. Tier columns on add_on_subscriptions (current_tier + plan_family).
--   3. addon_lifecycle_events append-only audit table for the dashboard
--      sub-tab + deactivation reason analytics.
--   4. Indexes for the new query patterns.
--
-- All cross-DB FKs forbidden (per ddd-enforcement). GCIDs stored as UUID
-- without FK; cross-domain validation via Pub/Sub events.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- ENUMs
-- -----------------------------------------------------------------------------

-- Mirrors AddonDeactivationReason in proto/events/tenancy/addon.proto AND
-- the OpenAPI DeactivateAddonRequest.reason enum. Keep the three in sync.
CREATE TYPE addon_deactivation_reason AS ENUM (
    'no_longer_needed',
    'cost',
    'consolidation',
    'migration',
    'compliance',
    'other'
);

-- Lifecycle event kinds — append-only audit dimension.
CREATE TYPE addon_lifecycle_event_kind AS ENUM (
    'activated',
    'upgraded',
    'downgraded',
    'deactivation_requested',
    'deactivated'
);

-- -----------------------------------------------------------------------------
-- add_on_subscriptions — extend with tier + structured deactivation fields
-- -----------------------------------------------------------------------------
ALTER TABLE add_on_subscriptions
    ADD COLUMN current_tier             VARCHAR(64)  NULL,
    ADD COLUMN plan_family               VARCHAR(64)  NULL,
    ADD COLUMN deactivation_reason       addon_deactivation_reason NULL,
    ADD COLUMN deactivation_reason_text  TEXT         NULL,
    ADD COLUMN deactivation_requested_by UUID         NULL,            -- GCID; cross-DB ref to chora_identity
    ADD COLUMN deactivation_requested_at TIMESTAMPTZ  NULL,
    ADD COLUMN deactivation_effective_at TIMESTAMPTZ  NULL,
    ADD CONSTRAINT chk_deactivation_reason_other_text
        CHECK (
            deactivation_reason IS DISTINCT FROM 'other'
            OR (deactivation_reason_text IS NOT NULL AND length(deactivation_reason_text) > 0)
        );

CREATE INDEX idx_add_on_subs_tier
    ON add_on_subscriptions (plan_family, current_tier)
    WHERE status = 'active';

CREATE INDEX idx_add_on_subs_deact_reason
    ON add_on_subscriptions (deactivation_reason)
    WHERE deactivation_reason IS NOT NULL;

CREATE INDEX idx_add_on_subs_deact_effective
    ON add_on_subscriptions (deactivation_effective_at)
    WHERE deactivation_effective_at IS NOT NULL;

-- -----------------------------------------------------------------------------
-- addon_lifecycle_events — append-only audit log
-- -----------------------------------------------------------------------------
--
-- Append-only (no UPDATE / DELETE in product code; cold-archive only).
-- Drives:
--   - STITCH-H-ADD-1 dashboard timeline sub-tab
--   - STITCH-H-ADD-2 reason analytics
--   - STITCH-H-ADD-4 upgrade/downgrade history
-- -----------------------------------------------------------------------------
CREATE TABLE addon_lifecycle_events (
    event_id              UUID                          PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID                          NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    add_on_id             UUID                          NOT NULL REFERENCES add_ons(add_on_id) ON DELETE RESTRICT,
    subscription_id       UUID                          NULL REFERENCES add_on_subscriptions(subscription_id) ON DELETE RESTRICT,
    kind                  addon_lifecycle_event_kind    NOT NULL,
    from_tier             VARCHAR(64)                   NULL,
    to_tier               VARCHAR(64)                   NULL,
    deactivation_reason   addon_deactivation_reason     NULL,
    reason_text           TEXT                          NULL,
    billing_delta_cents   BIGINT                        NULL,
    schedule_id           VARCHAR(128)                  NULL,
    requested_by_gcid     UUID                          NULL,            -- cross-DB ref to chora_identity
    effective_at          TIMESTAMPTZ                   NULL,
    occurred_at           TIMESTAMPTZ                   NOT NULL DEFAULT now(),
    -- Pub/Sub event correlation per envelope conventions.
    event_envelope_id     UUID                          NULL,
    traceparent           VARCHAR(64)                   NULL,
    created_at            TIMESTAMPTZ                   NOT NULL DEFAULT now(),
    -- 'other' must carry reason_text — mirrors the OpenAPI + proto rule.
    CONSTRAINT chk_addon_lifecycle_other_text CHECK (
        deactivation_reason IS DISTINCT FROM 'other'
        OR (reason_text IS NOT NULL AND length(reason_text) > 0)
    ),
    -- Tier transition events MUST carry both from_tier and to_tier.
    CONSTRAINT chk_addon_lifecycle_tier_transition CHECK (
        kind NOT IN ('upgraded', 'downgraded')
        OR (from_tier IS NOT NULL AND to_tier IS NOT NULL AND from_tier <> to_tier)
    )
);

CREATE INDEX idx_addon_lifecycle_tenant_addon
    ON addon_lifecycle_events (tenant_id, add_on_id, occurred_at DESC);

CREATE INDEX idx_addon_lifecycle_kind
    ON addon_lifecycle_events (kind, occurred_at DESC);

CREATE INDEX idx_addon_lifecycle_reason
    ON addon_lifecycle_events (deactivation_reason)
    WHERE deactivation_reason IS NOT NULL;

CREATE INDEX idx_addon_lifecycle_envelope
    ON addon_lifecycle_events (event_envelope_id)
    WHERE event_envelope_id IS NOT NULL;

ALTER TABLE addon_lifecycle_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON addon_lifecycle_events
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- addon_usage_daily — pre-aggregated rollup for STITCH-H-ADD-3
-- -----------------------------------------------------------------------------
--
-- Daily bucket rollup populated by a Cloud Run Job consuming
-- chora.tenancy.addon.usage_recorded.v1 events. Powers the time-series
-- chart in the usage analytics screen without scanning the high-volume
-- raw event topic on every load.
-- -----------------------------------------------------------------------------
CREATE TABLE addon_usage_daily (
    tenant_id          UUID         NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    add_on_id          UUID         NOT NULL REFERENCES add_ons(add_on_id) ON DELETE RESTRICT,
    bucket_date        DATE         NOT NULL,
    seats_used         BIGINT       NOT NULL DEFAULT 0 CHECK (seats_used >= 0),
    seats_used_peak    BIGINT       NOT NULL DEFAULT 0 CHECK (seats_used_peak >= 0),
    quota_consumed     BIGINT       NOT NULL DEFAULT 0 CHECK (quota_consumed >= 0),
    api_calls          BIGINT       NOT NULL DEFAULT 0 CHECK (api_calls >= 0),
    error_count        BIGINT       NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    last_recorded_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, add_on_id, bucket_date)
);

CREATE INDEX idx_addon_usage_daily_addon_date
    ON addon_usage_daily (add_on_id, bucket_date DESC);

CREATE TRIGGER trg_addon_usage_daily_updated_at
    BEFORE UPDATE ON addon_usage_daily
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE addon_usage_daily ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON addon_usage_daily
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
