-- =============================================================================
-- chora-tenancy : 0001_initial.sql
--
-- Domain        : Tenancy + Billing combined (supporting/platform)
-- Database      : chora_tenancy
-- Author        : agent-a5e52e89b73ede1d2 (db-migrations-11-services)
-- Date          : 2026-05-08
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 1 D1) — H+ Hub+
--
-- Aggregates owned by this database:
--   - Tenants (parent/child franchise hierarchy + branding + self_hosted)
--   - Members (per-tenant directory cache, GCID-keyed)
--   - AddOn catalog (composable add-on plans — NO tier hierarchy per CLAUDE.md §1)
--   - Tenant entitlements (active feature flags per tenant)
--   - Invoices (per-period billing snapshots)
--   - Bulk invites (tenant onboarding)
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE OR REPLACE FUNCTION tenancy_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- ENUMs
-- -----------------------------------------------------------------------------
CREATE TYPE tenant_status        AS ENUM ('active', 'suspended', 'closed');
CREATE TYPE entitlement_status   AS ENUM ('active', 'cancelled');
CREATE TYPE invoice_status       AS ENUM ('draft', 'open', 'paid', 'void', 'uncollectible');
CREATE TYPE invite_status        AS ENUM ('pending', 'accepted', 'rejected', 'expired');
CREATE TYPE payment_method_status AS ENUM ('active', 'detached');
CREATE TYPE tenant_member_role   AS ENUM ('owner', 'admin', 'instructor', 'learner', 'auditor');

-- -----------------------------------------------------------------------------
-- tenants — primary aggregate; supports parent/child franchise hierarchy
-- -----------------------------------------------------------------------------
CREATE TABLE tenants (
    tenant_id            UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 VARCHAR(256)   NOT NULL,
    parent_tenant_id     UUID           NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    branding             JSONB          NOT NULL DEFAULT '{}'::jsonb,
    self_hosted          BOOLEAN        NOT NULL DEFAULT FALSE,
    status               tenant_status  NOT NULL DEFAULT 'active',
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ    NOT NULL DEFAULT now(),
    deleted_at           TIMESTAMPTZ    NULL,
    CHECK (parent_tenant_id IS NULL OR parent_tenant_id <> tenant_id)
);

CREATE INDEX idx_tenants_parent ON tenants (parent_tenant_id) WHERE parent_tenant_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX idx_tenants_status ON tenants (status) WHERE deleted_at IS NULL;

CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

-- tenants table is the registry — RLS not enabled on the parent registry itself
-- (Hub+ admin reads cross-tenant for marketplace + parent rollup views).

-- -----------------------------------------------------------------------------
-- members — per-tenant directory of GCIDs with role + suspension flag
-- -----------------------------------------------------------------------------
CREATE TABLE members (
    member_id            UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID                 NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    gcid                 UUID                 NOT NULL,                  -- cross-DB ref to chora_identity
    role                 tenant_member_role   NOT NULL DEFAULT 'learner',
    suspended_at         TIMESTAMPTZ          NULL,
    invited_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    joined_at            TIMESTAMPTZ          NULL,
    created_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    deleted_at           TIMESTAMPTZ          NULL,
    UNIQUE (tenant_id, gcid)
);

CREATE INDEX idx_members_gcid     ON members (gcid) WHERE deleted_at IS NULL;
CREATE INDEX idx_members_role     ON members (role) WHERE deleted_at IS NULL;
CREATE INDEX idx_members_tenant   ON members (tenant_id) WHERE deleted_at IS NULL;

CREATE TRIGGER trg_members_updated_at
    BEFORE UPDATE ON members
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE members ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON members
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- add_ons — flat catalog (NO tier hierarchy per CLAUDE.md §1)
-- -----------------------------------------------------------------------------
CREATE TABLE add_ons (
    add_on_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 VARCHAR(128) NOT NULL UNIQUE,
    description          TEXT         NOT NULL DEFAULT '',
    monthly_price_cents  BIGINT       NOT NULL DEFAULT 0 CHECK (monthly_price_cents >= 0),
    currency             VARCHAR(3)   NOT NULL DEFAULT 'SGD',
    capabilities         TEXT[]       NOT NULL DEFAULT '{}',
    public               BOOLEAN      NOT NULL DEFAULT TRUE,
    self_hosted_only     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_add_ons_caps_gin ON add_ons USING GIN (capabilities);

CREATE TRIGGER trg_add_ons_updated_at
    BEFORE UPDATE ON add_ons
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

-- -----------------------------------------------------------------------------
-- add_on_subscriptions (= TenantEntitlement) — composable, NOT tier pricing
-- -----------------------------------------------------------------------------
CREATE TABLE add_on_subscriptions (
    subscription_id            UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                  UUID                 NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    add_on_id                  UUID                 NOT NULL REFERENCES add_ons(add_on_id) ON DELETE RESTRICT,
    status                     entitlement_status   NOT NULL DEFAULT 'active',
    monthly_price_cents_snap   BIGINT               NOT NULL DEFAULT 0 CHECK (monthly_price_cents_snap >= 0),
    started_at                 TIMESTAMPTZ          NOT NULL DEFAULT now(),
    ended_at                   TIMESTAMPTZ          NULL,
    cancelled_at               TIMESTAMPTZ          NULL,
    created_at                 TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ          NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, add_on_id)
);

CREATE INDEX idx_add_on_subs_tenant ON add_on_subscriptions (tenant_id) WHERE status = 'active';
CREATE INDEX idx_add_on_subs_addon  ON add_on_subscriptions (add_on_id);
CREATE INDEX idx_add_on_subs_status ON add_on_subscriptions (status);

CREATE TRIGGER trg_add_on_subs_updated_at
    BEFORE UPDATE ON add_on_subscriptions
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE add_on_subscriptions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON add_on_subscriptions
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- invoices — per-period billing snapshots
-- -----------------------------------------------------------------------------
CREATE TABLE invoices (
    invoice_id           UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID            NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    period_start         TIMESTAMPTZ     NOT NULL,
    period_end           TIMESTAMPTZ     NOT NULL,
    amount_cents         BIGINT          NOT NULL DEFAULT 0 CHECK (amount_cents >= 0),
    currency             VARCHAR(3)      NOT NULL DEFAULT 'SGD',
    status               invoice_status  NOT NULL DEFAULT 'draft',
    line_items           JSONB           NOT NULL DEFAULT '[]'::jsonb,
    stripe_invoice_id    VARCHAR(64)     NULL,
    generated_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    paid_at              TIMESTAMPTZ     NULL,
    created_at           TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CHECK (period_end > period_start)
);

CREATE INDEX idx_invoices_tenant ON invoices (tenant_id);
CREATE INDEX idx_invoices_status ON invoices (status);
CREATE INDEX idx_invoices_period ON invoices (period_start, period_end);
CREATE INDEX idx_invoices_stripe ON invoices (stripe_invoice_id) WHERE stripe_invoice_id IS NOT NULL;

CREATE TRIGGER trg_invoices_updated_at
    BEFORE UPDATE ON invoices
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON invoices
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- payment_methods — Stripe payment method handles (stub for M11; real M12+)
-- -----------------------------------------------------------------------------
CREATE TABLE payment_methods (
    payment_method_id        UUID                  PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                UUID                  NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    stripe_payment_method_id VARCHAR(64)           NOT NULL,
    status                   payment_method_status NOT NULL DEFAULT 'active',
    created_at               TIMESTAMPTZ           NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ           NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, stripe_payment_method_id)
);

CREATE INDEX idx_payment_methods_tenant ON payment_methods (tenant_id);

CREATE TRIGGER trg_payment_methods_updated_at
    BEFORE UPDATE ON payment_methods
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE payment_methods ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON payment_methods
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- bulk_invites — onboarding queue (no GCID until accepted)
-- -----------------------------------------------------------------------------
CREATE TABLE bulk_invites (
    invite_id            UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID                NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    gcid_invitee         UUID                NULL,                          -- populated on accept
    invitee_email        VARCHAR(320)        NOT NULL,
    role                 tenant_member_role  NOT NULL DEFAULT 'learner',
    status               invite_status       NOT NULL DEFAULT 'pending',
    invited_at           TIMESTAMPTZ         NOT NULL DEFAULT now(),
    accepted_at          TIMESTAMPTZ         NULL,
    expires_at           TIMESTAMPTZ         NOT NULL,
    invite_token_hash    CHAR(64)            NOT NULL,                      -- SHA-256 hex
    created_at           TIMESTAMPTZ         NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ         NOT NULL DEFAULT now()
);

CREATE INDEX idx_bulk_invites_tenant ON bulk_invites (tenant_id);
CREATE INDEX idx_bulk_invites_email  ON bulk_invites (invitee_email);
CREATE INDEX idx_bulk_invites_status ON bulk_invites (status);
CREATE UNIQUE INDEX idx_bulk_invites_token ON bulk_invites (invite_token_hash);

CREATE TRIGGER trg_bulk_invites_updated_at
    BEFORE UPDATE ON bulk_invites
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE bulk_invites ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON bulk_invites
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
