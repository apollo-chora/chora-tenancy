-- =============================================================================
-- chora-tenancy : 0031_tenant_external_egress_policy.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-2148 (Far Sight egress governance write-path)
-- ADR           : ADR-220 D4 (tenant gating, franchise default OFF)
--                 ADR-231 D6 (fail-closed gateway egress gate + O+ kill-switch)
-- Canon         : PLAN.md §4.2.4 "Egress governance model" (resolved 2026-07-11)
--
-- The SOURCE OF TRUTH for whether a tenant's learners may reach the open web
-- through the model-gateway's grounded-search egress (Far Sight / Seeker).
--
-- Why here and not in chora_observability: an egress entitlement is
-- DETERMINISTIC TENANT ADMINISTRATION, so chora-tenancy owns it (H+ writes it,
-- next to the add-ons and entitlements it already owns). chora-observability
-- holds only a READ-PROJECTION (`external_egress_policy`, obs mig 0014) which
-- the model-gateway consults fail-closed on every grounded call. The projection
-- is fed by `chora.tenancy.external_egress_policy.updated.v1` — cross-DB writes
-- are FORBIDDEN, so that event is the ONLY bridge between the two databases.
--
-- ⚠ NO BACKFILL — deliberately. The safety posture is DEFAULT-DENY: a tenant
-- with no row is OFF. That is exactly how ADR-220 D4's "franchise tenants
-- default OFF" (schools, minors) is enforced — absence IS the denial, so there
-- is nothing to seed and no way to accidentally seed a franchise tenant ON.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS tenant_external_egress_policies (
    -- tenant_id is the identity: exactly one policy per tenant, no surrogate key
    -- (mirrors the chora_observability read-copy, whose PK is also tenant_id).
    tenant_id           UUID        PRIMARY KEY
        REFERENCES tenants(tenant_id) ON DELETE RESTRICT,

    egress_enabled      BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Per-tenant/day grounded-call budget. Every grounded call is a PRICED
    -- external-egress action (ADR-231 D6), so the ceiling is bounded: an
    -- unbounded ceiling is a cost hole a tenant could open on itself. The upper
    -- bound mirrors external_egress.MaxDailyCallCeiling in the Go domain.
    daily_call_ceiling  INTEGER     NOT NULL DEFAULT 50
        CHECK (daily_call_ceiling >= 0 AND daily_call_ceiling <= 1000),

    -- Monotonic. Increments ONLY on a real change (an identical write is a
    -- no-op). Carried on the policy-changed event so the chora-observability
    -- projection can reject an OUT-OF-ORDER Pub/Sub delivery: a stale event
    -- whose version <= the projected version is discarded, never applied.
    version             BIGINT      NOT NULL DEFAULT 1,

    -- The admin who made the most recent change. External web egress is
    -- governance-sensitive: every change names its actor so the audit trail can
    -- always answer "who turned this on, for which tenant, and when"
    -- (PLAN.md §4.2.4 safety net b; IMDA D1 accountability).
    -- Cross-domain ref to chora_identity — UUID, NO FK (cross-DB FKs forbidden).
    updated_by_gcid     UUID        NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenant_external_egress_policies IS
    'Source of truth for the per-tenant external web-egress entitlement (Far Sight). '
    'Default-deny: no row => OFF. Projected into chora_observability.external_egress_policy '
    'by chora.tenancy.external_egress_policy.updated.v1. ADR-220 D4 / ADR-231 D6.';

CREATE TRIGGER trg_tenant_external_egress_policies_updated_at
    BEFORE UPDATE ON tenant_external_egress_policies
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE tenant_external_egress_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_external_egress_policies FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_external_egress_policies;
CREATE POLICY tenant_isolation ON tenant_external_egress_policies
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
