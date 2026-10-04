-- =============================================================================
-- chora-tenancy : 0017_drop_purchases_after_extract.down.sql
--
-- Reverse companion to 0017_drop_purchases_after_extract.up.sql — re-creates
-- the dropped chora_tenancy tables EMPTY. Operator must then re-run the
-- extract binary with DIRECTION=reverse to re-populate from chora_payments.
--
-- This file is a stripped-down copy of the schema-creation parts of
-- migrations 0007 (familiar_egg_purchases) + 0003 (tenant_mana_pool_topups).
-- It deliberately OMITS the seed data + the catalog table — the catalog
-- was never DROPPED in the .up companion.
-- =============================================================================

BEGIN;

-- --------------------------------------------------------------------------
-- familiar_egg_purchases — re-created EMPTY (data lives in chora_payments).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_egg_purchases (
    purchase_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID         NOT NULL,
    purchaser_gcid        UUID         NOT NULL,
    egg_sku               TEXT         NOT NULL,
    suggested_focal_atom_id  UUID,
    state                 TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN (
            'checkout_started', 'paid', 'payment_failed', 'provisioned',
            'refunded', 'expired'
        )),
    amount_cents          BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid     BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_paid >= 0),
    amount_cents_refunded BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_refunded >= 0),
    currency              TEXT         NOT NULL CHECK (length(currency) = 3),
    stripe_session_id     TEXT         NOT NULL,
    stripe_payment_intent_id  TEXT,
    stripe_charge_id      TEXT,
    stripe_refund_id      TEXT,
    stripe_checkout_url   TEXT,
    stripe_failure_code   TEXT,
    stripe_failure_message TEXT,
    refund_reason         TEXT
        CHECK (refund_reason IN (
            'hard_expiry_unhatched', 'support_initiated', 'tenant_policy',
            'customer_request', 'duplicate_charge', 'fraud'
        ) OR refund_reason IS NULL),
    refund_credit_only    BOOLEAN      NOT NULL DEFAULT FALSE,
    checkout_started_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at               TIMESTAMPTZ,
    failed_at             TIMESTAMPTZ,
    provisioned_at        TIMESTAMPTZ,
    refunded_at           TIMESTAMPTZ,
    expired_at            TIMESTAMPTZ,
    soft_expiry_at        TIMESTAMPTZ  NOT NULL,
    hard_expiry_at        TIMESTAMPTZ  NOT NULL,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (stripe_session_id)
);

CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_purchaser
    ON familiar_egg_purchases (tenant_id, purchaser_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_state
    ON familiar_egg_purchases (state, hard_expiry_at)
    WHERE state IN ('paid', 'provisioned');
CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_stripe_session
    ON familiar_egg_purchases (stripe_session_id);

CREATE TRIGGER trg_familiar_egg_purchases_updated_at
    BEFORE UPDATE ON familiar_egg_purchases
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE familiar_egg_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE familiar_egg_purchases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON familiar_egg_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- --------------------------------------------------------------------------
-- tenant_mana_pool_topups — re-created EMPTY.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_mana_pool_topups (
    topup_id                      UUID                              PRIMARY KEY DEFAULT gen_random_uuid(),
    pool_id                       UUID                              NOT NULL
        REFERENCES tenant_mana_pools(pool_id) ON DELETE RESTRICT,
    tenant_id                     UUID                              NOT NULL
        REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    units_credited                BIGINT                            NOT NULL CHECK (units_credited > 0),
    charged_cents                 BIGINT                            NOT NULL CHECK (charged_cents > 0),
    currency                      VARCHAR(3)                        NOT NULL,
    stripe_payment_intent_id      VARCHAR(128)                      NOT NULL UNIQUE,
    auto_renew                    BOOLEAN                           NOT NULL DEFAULT FALSE,
    topped_up_by_gcid             UUID                              NULL,
    balance_after_units           BIGINT                            NOT NULL CHECK (balance_after_units >= 0),
    event_envelope_id             UUID                              NULL,
    traceparent                   VARCHAR(64)                       NULL,
    recorded_at                   TIMESTAMPTZ                       NOT NULL DEFAULT now(),
    created_at                    TIMESTAMPTZ                       NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_mana_topups_pool
    ON tenant_mana_pool_topups (pool_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_tenant_mana_topups_tenant
    ON tenant_mana_pool_topups (tenant_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_tenant_mana_topups_envelope
    ON tenant_mana_pool_topups (event_envelope_id) WHERE event_envelope_id IS NOT NULL;

ALTER TABLE tenant_mana_pool_topups ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_mana_pool_topups
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_tenancy_app') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON familiar_egg_purchases TO chora_tenancy_app';
    END IF;
END$$;

COMMIT;
