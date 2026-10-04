-- =============================================================================
-- chora-tenancy : 0007_familiar_egg_purchases.sql
--
-- Domain        : Tenancy + Billing (supporting)
-- Database      : chora_tenancy
-- Author        : Iter G.1 — Familiar Growth Stage Model (2026-05-13)
-- Architecture  : docs/architecture/adrs/adr-149-familiar-growth-stage-model.md
--                 chora-contracts/proto/events/tenancy/familiar_egg.proto
--
-- Stripe-tracked Familiar Egg purchase lifecycle (ADR-149 §"Egg purchase").
--
-- Egg lifecycle (purchase path):
--   1. chora-tenancy.POST /api/familiar-eggs/checkout creates row in state
--      'checkout_started' + Stripe Checkout session.
--   2. Stripe webhook checkout.session.completed → transition to 'paid' +
--      publish chora.tenancy.familiar_egg.payment_succeeded.v1.
--   3. chora-consumption subscribes, provisions Stage-0 familiar_instances
--      row, publishes chora.consumption.familiar.egg_purchased.v1 (which
--      back-references purchase_id).
--   4. Hard-expiry sweeper (60d unhatched) → 'refunded' (credit_only=true)
--      + publish chora.tenancy.familiar_egg.refunded.v1.
--
-- Free-egg paths (trial / subscription_inclusion / tenant_grant) DO NOT
-- create rows here — they go straight to chora-consumption.familiar_instances
-- provisioning with source != 'purchase'.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- familiar_egg_purchases — Stripe-tracked purchase records
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_egg_purchases (
    purchase_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),  -- UUIDv7 in app layer
    tenant_id             UUID         NOT NULL,
    purchaser_gcid        UUID         NOT NULL,

    -- Egg SKU from the tenant's catalog (resolved at checkout-time; copied
    -- here for audit even if catalog later changes).
    egg_sku               TEXT         NOT NULL,

    -- Pre-bound focal-atom suggestion from the egg SKU. May be NULL if the
    -- SKU is "open-ended" (learner picks from KG at hatching).
    suggested_focal_atom_id  UUID,

    -- Lifecycle state.
    state                 TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN (
            'checkout_started',     -- Stripe session created, awaiting payment
            'paid',                 -- checkout.session.completed received
            'payment_failed',       -- Stripe failure reported
            'provisioned',          -- chora-consumption acked + provisioned familiar_instances row
            'refunded',             -- full or partial refund processed
            'expired'               -- unhatched + 60d past hard_expiry; auto-refund-credit issued
        )),

    -- Pricing snapshot.
    amount_cents          BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid     BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_paid >= 0),
    amount_cents_refunded BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_refunded >= 0),
    currency              TEXT         NOT NULL CHECK (length(currency) = 3),  -- ISO 4217

    -- Stripe correlation handles.
    stripe_session_id     TEXT         NOT NULL,
    stripe_payment_intent_id  TEXT,
    stripe_charge_id      TEXT,
    stripe_refund_id      TEXT,
    stripe_checkout_url   TEXT,
    stripe_failure_code   TEXT,
    stripe_failure_message TEXT,

    -- Refund metadata.
    refund_reason         TEXT
        CHECK (refund_reason IN (
            'hard_expiry_unhatched',
            'support_initiated',
            'tenant_policy',
            'customer_request',
            'duplicate_charge',
            'fraud'
        ) OR refund_reason IS NULL),
    refund_credit_only    BOOLEAN      NOT NULL DEFAULT FALSE,  -- TRUE = mana_credit refund vs real money

    -- Lifecycle timestamps.
    checkout_started_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at               TIMESTAMPTZ,
    failed_at             TIMESTAMPTZ,
    provisioned_at        TIMESTAMPTZ,
    refunded_at           TIMESTAMPTZ,
    expired_at            TIMESTAMPTZ,

    -- Egg expiry deadlines (snapshotted at checkout from SKU defaults).
    soft_expiry_at        TIMESTAMPTZ  NOT NULL,
    hard_expiry_at        TIMESTAMPTZ  NOT NULL,

    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (stripe_session_id),

    -- Sanity: if state is 'paid', paid_at must be set.
    CHECK ((state != 'paid'         OR paid_at IS NOT NULL)),
    CHECK ((state != 'payment_failed' OR failed_at IS NOT NULL)),
    CHECK ((state != 'provisioned'  OR provisioned_at IS NOT NULL)),
    CHECK ((state != 'refunded'     OR refunded_at IS NOT NULL)),
    CHECK ((state != 'expired'      OR expired_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_purchaser
    ON familiar_egg_purchases (tenant_id, purchaser_gcid, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_state
    ON familiar_egg_purchases (state, hard_expiry_at)
    WHERE state IN ('paid', 'provisioned');  -- expiry sweep candidates

CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_stripe_session
    ON familiar_egg_purchases (stripe_session_id);

-- Updated-at trigger (assumes tenancy_set_updated_at() exists from 0001).
CREATE TRIGGER trg_familiar_egg_purchases_updated_at
    BEFORE UPDATE ON familiar_egg_purchases
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE familiar_egg_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE familiar_egg_purchases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON familiar_egg_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMENT ON TABLE familiar_egg_purchases IS
    'ADR-149 Stripe-tracked Familiar Egg purchases. Free-egg paths (trial/inclusion/grant) do NOT create rows here.';

-- -----------------------------------------------------------------------------
-- familiar_egg_catalog — per-tenant + platform-default egg SKUs
--
-- Tenant admins curate their own egg catalog via H+ marketplace UI; platform
-- defaults (e.g., "egg.standard.v1", "egg.trial.v1") are seeded by ops.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_egg_catalog (
    sku                   TEXT         PRIMARY KEY,           -- e.g., "egg.skillflow-cspo.v1"
    tenant_id             UUID,                                -- NULL = platform-global
    display_name          TEXT         NOT NULL,
    description           TEXT,
    price_cents           BIGINT       NOT NULL CHECK (price_cents >= 0),
    currency              TEXT         NOT NULL CHECK (length(currency) = 3),

    -- Pre-bound focal-atom hint. UUIDv7 of a LearningAtom (validated at
    -- catalog-create time; this DB has no FK to chora_creation).
    suggested_focal_atom_id  UUID,

    -- Breed lootbox probability distribution (ADR-149 §Breed lootbox).
    -- Shape: { "owl": 25.0, "fox": 20.0, "cat": 15.0, "dragon": 15.0,
    --          "phoenix": 10.0, "turtle": 7.0, "wolf": 5.0, "raven": 3.0 }
    -- Weights must sum to 100.0 (CHECK constraint below). Used by
    -- chora-consumption.FamiliarGrowth.HatchEgg to roll the breed at hatching.
    -- ALSO exposed publicly via PreviewEggOdds for IMDA D2 transparency.
    breed_distribution    JSONB        NOT NULL DEFAULT '{}'::jsonb,

    -- Cached sum of breed_distribution weights (must be 100.0 for active
    -- SKUs). Updated by trigger on breed_distribution change.
    breed_distribution_total  NUMERIC(6,2) NOT NULL DEFAULT 0,

    -- Last-modified timestamp for the breed_distribution specifically.
    -- Exposed via PreviewEggOdds.distribution_updated_at so consumers
    -- can detect mid-purchase odds changes.
    breed_distribution_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Whether this SKU is available for direct purchase (vs only included
    -- in subscription tiers / tenant grants).
    purchasable           BOOLEAN      NOT NULL DEFAULT TRUE,

    -- Whether this SKU is the tenant's "trial egg" (one-per-user-ever).
    is_trial              BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Default expiry windows (days). soft typically 30, hard typically 60.
    soft_expiry_days      INTEGER      NOT NULL DEFAULT 30 CHECK (soft_expiry_days > 0),
    hard_expiry_days      INTEGER      NOT NULL DEFAULT 60 CHECK (hard_expiry_days >= soft_expiry_days),

    -- Active window for the SKU itself (e.g., seasonal eggs).
    available_from        TIMESTAMPTZ,
    available_until       TIMESTAMPTZ,

    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at            TIMESTAMPTZ,

    CHECK (NOT is_trial OR price_cents = 0),  -- trial eggs MUST be free

    -- breed_distribution_total must be 100.0 for any non-deleted SKU.
    -- Tenants can stage drafts at total != 100, but those rows MUST be
    -- soft-deleted before going live. App-layer enforces transition.
    CHECK (deleted_at IS NOT NULL OR breed_distribution_total = 100.0)
);

-- Trigger: keep breed_distribution_total in sync with the JSONB sum, and
-- bump breed_distribution_updated_at on any breed_distribution change.
CREATE OR REPLACE FUNCTION familiar_egg_catalog_breed_distribution_sync()
RETURNS TRIGGER AS $$
DECLARE
    total NUMERIC(6,2) := 0;
    species_key TEXT;
    species_weight NUMERIC(6,2);
BEGIN
    IF NEW.breed_distribution IS DISTINCT FROM OLD.breed_distribution OR TG_OP = 'INSERT' THEN
        -- Sum all values in the JSONB object.
        FOR species_key, species_weight IN
            SELECT key, (value::TEXT)::NUMERIC(6,2)
            FROM jsonb_each_text(NEW.breed_distribution)
        LOOP
            -- Validate species_key against canonical enum.
            IF species_key NOT IN ('owl','fox','cat','dragon','phoenix','turtle','wolf','raven') THEN
                RAISE EXCEPTION 'invalid species in breed_distribution: %', species_key;
            END IF;
            IF species_weight < 0 THEN
                RAISE EXCEPTION 'negative weight for species %: %', species_key, species_weight;
            END IF;
            total := total + species_weight;
        END LOOP;
        NEW.breed_distribution_total := total;
        NEW.breed_distribution_updated_at := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_familiar_egg_catalog_breed_distribution_sync
    BEFORE INSERT OR UPDATE OF breed_distribution ON familiar_egg_catalog
    FOR EACH ROW EXECUTE FUNCTION familiar_egg_catalog_breed_distribution_sync();

CREATE INDEX IF NOT EXISTS idx_familiar_egg_catalog_tenant
    ON familiar_egg_catalog (tenant_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_familiar_egg_catalog_platform
    ON familiar_egg_catalog (sku)
    WHERE tenant_id IS NULL AND deleted_at IS NULL;

CREATE TRIGGER trg_familiar_egg_catalog_updated_at
    BEFORE UPDATE ON familiar_egg_catalog
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

-- Catalog is mostly platform-managed + tenant-curated. RLS allows tenant_id
-- match OR tenant_id IS NULL (platform SKUs visible to all tenants).
ALTER TABLE familiar_egg_catalog ENABLE ROW LEVEL SECURITY;
ALTER TABLE familiar_egg_catalog FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_or_platform_visibility ON familiar_egg_catalog
    FOR SELECT USING (
        tenant_id IS NULL
        OR tenant_id = current_setting('chora.tenant_id', true)::uuid
    );
CREATE POLICY tenant_admin_mutation ON familiar_egg_catalog
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMENT ON TABLE familiar_egg_catalog IS
    'ADR-149 egg SKU catalog. tenant_id NULL = platform-global; tenant_id set = tenant-curated. Trial eggs MUST be free.';

-- -----------------------------------------------------------------------------
-- Seed platform-default egg SKUs.
-- -----------------------------------------------------------------------------
INSERT INTO familiar_egg_catalog
    (sku, tenant_id, display_name, description, price_cents, currency,
     purchasable, is_trial, soft_expiry_days, hard_expiry_days,
     breed_distribution)
VALUES
    ('egg.trial.v1',     NULL, 'Trial Egg',
        'Your first Familiar — free to try. Uniform odds across all breeds. One per learner, ever.',
        0, 'SGD', FALSE, TRUE, 30, 60,
        '{"owl":12.5,"fox":12.5,"cat":12.5,"dragon":12.5,"phoenix":12.5,"turtle":12.5,"wolf":12.5,"raven":12.5}'::jsonb),
    ('egg.standard.v1',  NULL, 'Standard Egg',
        'A versatile Familiar egg. Pick any focal atom from your Knowledge Graph at hatching. Common breeds favored.',
        999, 'SGD', TRUE, FALSE, 30, 60,
        '{"owl":25.0,"fox":20.0,"cat":18.0,"dragon":12.0,"phoenix":8.0,"turtle":7.0,"wolf":6.0,"raven":4.0}'::jsonb),
    ('egg.premium.v1',   NULL, 'Premium Egg',
        'A premium Familiar egg with rarer breeds weighted. Includes Source Revelation preview boost at Stage 3.',
        2999, 'SGD', TRUE, FALSE, 45, 90,
        '{"owl":10.0,"fox":10.0,"cat":10.0,"dragon":22.0,"phoenix":22.0,"turtle":8.0,"wolf":10.0,"raven":8.0}'::jsonb)
ON CONFLICT (sku) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Grants (re-runnable safe).
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_tenancy_app') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON familiar_egg_purchases TO chora_tenancy_app';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON familiar_egg_catalog TO chora_tenancy_app';
    END IF;
END$$;

COMMIT;
