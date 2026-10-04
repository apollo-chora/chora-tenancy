-- =============================================================================
-- chora-tenancy : 0003_tenant_mana_pool.sql
--
-- Domain        : Tenancy + Billing combined (supporting/platform)
-- Database      : chora_tenancy
-- Author        : agent-a7b2cf0388593b37a (BE-USR-3)
-- Date          : 2026-05-09
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 1 D1) — H+ Hub+
-- ADR           : adr-142-per-user-economy-mana.md (Q2 = v1, tenant subsidy)
--
-- Adds the tenant subsidy track (corporate L&D / cohort sponsorship use case):
--
--   1. tenant_mana_pools           — primary aggregate; one row per tenant.
--                                    Tracks balance, monthly auto-renew,
--                                    auto-allocation policy, lifetime totals.
--   2. tenant_mana_allocations     — append-only audit row; one row per
--                                    grant of mana from a pool to a learner.
--                                    Status transitions (pending → claimed |
--                                    revoked | expired) recorded in place;
--                                    NEVER UPDATEd beyond status + tracking.
--   3. tenant_mana_pool_topups     — append-only audit; one row per Stripe
--                                    PaymentIntent that funded the pool.
--                                    Drives the H+ "Mana Pool Funding"
--                                    history view + reconciliation.
--
-- All cross-DB FKs are forbidden (per `.claude/rules/ddd-enforcement.md`).
-- GCIDs are stored as UUID (recipient learner / actor admin) without FK;
-- cross-domain validation is via Pub/Sub events (chora.identity.* +
-- chora.tenancy.* event taxonomy).
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Idempotent guards (re-running migration is safe in dev environments)
-- -----------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tenant_mana_allocation_policy') THEN
        CREATE TYPE tenant_mana_allocation_policy AS ENUM (
            'manual',
            'on_enrollment',
            'equal_split',
            'tier_based'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tenant_mana_allocation_status') THEN
        CREATE TYPE tenant_mana_allocation_status AS ENUM (
            'pending',
            'claimed',
            'expired',
            'revoked'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tenant_mana_allocation_reason') THEN
        CREATE TYPE tenant_mana_allocation_reason AS ENUM (
            'manual',
            'on_enrollment',
            'equal_split',
            'tier_based',
            'promo'
        );
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- tenant_mana_pools — primary aggregate (1 per tenant)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_mana_pools (
    pool_id                       UUID                              PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                     UUID                              NOT NULL UNIQUE
        REFERENCES tenants(tenant_id) ON DELETE RESTRICT,

    -- Live balance + monotonic counters.
    balance_units                 BIGINT                            NOT NULL DEFAULT 0
        CHECK (balance_units >= 0),
    lifetime_topped_up_units      BIGINT                            NOT NULL DEFAULT 0
        CHECK (lifetime_topped_up_units >= 0),
    lifetime_allocated_units      BIGINT                            NOT NULL DEFAULT 0
        CHECK (lifetime_allocated_units >= 0),

    -- Auto-renew config; 0 disables auto-renew.
    monthly_topup_units           BIGINT                            NOT NULL DEFAULT 0
        CHECK (monthly_topup_units >= 0),
    monthly_topup_currency        VARCHAR(3)                        NULL,

    -- Auto-allocation policy + per-policy parameters.
    auto_allocation_policy        tenant_mana_allocation_policy     NOT NULL DEFAULT 'manual',
    units_on_enrollment           BIGINT                            NULL
        CHECK (units_on_enrollment IS NULL OR units_on_enrollment >= 0),
    tier_amounts_json             JSONB                             NULL,
    allocation_ttl_days           INTEGER                           NULL
        CHECK (allocation_ttl_days IS NULL OR allocation_ttl_days >= 0),

    -- OCC + lifecycle.
    version                       BIGINT                            NOT NULL DEFAULT 1,
    created_by_gcid               UUID                              NOT NULL,
    created_at                    TIMESTAMPTZ                       NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ                       NOT NULL DEFAULT now(),
    last_topped_up_at             TIMESTAMPTZ                       NULL,
    deleted_at                    TIMESTAMPTZ                       NULL,

    -- on_enrollment policy MUST carry units_on_enrollment.
    CONSTRAINT chk_pool_on_enrollment_units CHECK (
        auto_allocation_policy <> 'on_enrollment'
        OR (units_on_enrollment IS NOT NULL AND units_on_enrollment > 0)
    ),
    -- tier_based policy MUST carry tier_amounts_json.
    CONSTRAINT chk_pool_tier_based_amounts CHECK (
        auto_allocation_policy <> 'tier_based'
        OR (tier_amounts_json IS NOT NULL AND jsonb_typeof(tier_amounts_json) = 'object')
    )
);

CREATE INDEX IF NOT EXISTS idx_tenant_mana_pools_tenant
    ON tenant_mana_pools (tenant_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tenant_mana_pools_low_balance
    ON tenant_mana_pools (balance_units)
    WHERE deleted_at IS NULL AND monthly_topup_units > 0;

CREATE TRIGGER trg_tenant_mana_pools_updated_at
    BEFORE UPDATE ON tenant_mana_pools
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE tenant_mana_pools ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_mana_pools;
CREATE POLICY tenant_isolation ON tenant_mana_pools
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- tenant_mana_allocations — append-only audit row (one per grant)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_mana_allocations (
    allocation_id                 UUID                              PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                     UUID                              NOT NULL
        REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    source_pool_id                UUID                              NOT NULL
        REFERENCES tenant_mana_pools(pool_id) ON DELETE RESTRICT,
    -- Recipient learner GCID (cross-DB ref to chora_identity; NO FK).
    gcid                          UUID                              NOT NULL,
    units                         BIGINT                            NOT NULL
        CHECK (units > 0),

    status                        tenant_mana_allocation_status     NOT NULL DEFAULT 'pending',
    allocation_reason             tenant_mana_allocation_reason     NOT NULL,
    -- Admin actor GCID; NULL/empty for auto-policy issuance.
    allocated_by_gcid             UUID                              NULL,

    allocated_at                  TIMESTAMPTZ                       NOT NULL DEFAULT now(),
    expires_at                    TIMESTAMPTZ                       NULL,
    claimed_at                    TIMESTAMPTZ                       NULL,
    revoked_at                    TIMESTAMPTZ                       NULL,
    revoked_by_gcid               UUID                              NULL,
    revoked_reason                VARCHAR(512)                      NULL,

    -- Tracking populated on first debit (Claim).
    first_debit_entry_id          VARCHAR(128)                      NULL,
    remaining_units               BIGINT                            NULL
        CHECK (remaining_units IS NULL OR remaining_units >= 0),

    updated_at                    TIMESTAMPTZ                       NOT NULL DEFAULT now(),

    -- Manual reason MUST carry allocated_by_gcid.
    CONSTRAINT chk_allocation_manual_actor CHECK (
        allocation_reason <> 'manual' OR allocated_by_gcid IS NOT NULL
    ),
    -- Revoke transition MUST carry actor + reason.
    CONSTRAINT chk_allocation_revoke_fields CHECK (
        status <> 'revoked'
        OR (revoked_at IS NOT NULL AND revoked_by_gcid IS NOT NULL AND revoked_reason IS NOT NULL)
    ),
    -- Claim transition MUST carry first_debit_entry_id + claimed_at.
    CONSTRAINT chk_allocation_claim_fields CHECK (
        status <> 'claimed'
        OR (claimed_at IS NOT NULL AND first_debit_entry_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_tenant_mana_alloc_gcid
    ON tenant_mana_allocations (tenant_id, gcid);
CREATE INDEX IF NOT EXISTS idx_tenant_mana_alloc_status
    ON tenant_mana_allocations (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_tenant_mana_alloc_expires_at
    ON tenant_mana_allocations (expires_at)
    WHERE expires_at IS NOT NULL AND status = 'pending';
CREATE INDEX IF NOT EXISTS idx_tenant_mana_alloc_pool
    ON tenant_mana_allocations (source_pool_id, allocated_at DESC);

CREATE TRIGGER trg_tenant_mana_allocations_updated_at
    BEFORE UPDATE ON tenant_mana_allocations
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE tenant_mana_allocations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_mana_allocations;
CREATE POLICY tenant_isolation ON tenant_mana_allocations
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- tenant_mana_pool_topups — append-only audit (one row per Stripe charge)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_mana_pool_topups (
    topup_id                      UUID                              PRIMARY KEY DEFAULT gen_random_uuid(),
    pool_id                       UUID                              NOT NULL
        REFERENCES tenant_mana_pools(pool_id) ON DELETE RESTRICT,
    tenant_id                     UUID                              NOT NULL
        REFERENCES tenants(tenant_id) ON DELETE RESTRICT,

    units_credited                BIGINT                            NOT NULL
        CHECK (units_credited > 0),
    charged_cents                 BIGINT                            NOT NULL
        CHECK (charged_cents > 0),
    currency                      VARCHAR(3)                        NOT NULL,
    stripe_payment_intent_id      VARCHAR(128)                      NOT NULL UNIQUE,

    auto_renew                    BOOLEAN                           NOT NULL DEFAULT FALSE,
    topped_up_by_gcid             UUID                              NULL, -- admin actor; NULL for auto-renew
    balance_after_units           BIGINT                            NOT NULL CHECK (balance_after_units >= 0),

    -- Pub/Sub envelope correlation (per envelope conventions).
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
DROP POLICY IF EXISTS tenant_isolation ON tenant_mana_pool_topups;
CREATE POLICY tenant_isolation ON tenant_mana_pool_topups
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
