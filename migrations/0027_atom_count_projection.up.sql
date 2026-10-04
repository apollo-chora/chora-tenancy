-- =============================================================================
-- chora-tenancy : 0027_atom_count_projection.up.sql
--
-- Domain   : Tenancy + Billing (supporting/platform)
-- Database : chora_tenancy
-- Story    : CHO-2011 (ADR-217 Debt 3) — per-tenant atom-count read projection
-- Date     : 2026-07-02
--
-- Populates ChildTenantSummary.atom_count in the franchise-hierarchy read
-- (tenant_hierarchy_repository.go), which was hard-0 because atoms live in
-- chora_creation and cross-DB reads are forbidden (ddd-enforcement). This is
-- the sanctioned event-fed projection: the atom_count subscriber folds
-- chora.creation.atom.created.v1 (+1) / chora.creation.atom.archived.v1 (-1)
-- into a per-tenant counter, read under each child's RLS scope like user_count.
--
-- Cross-DB-clean: NO FK to chora_creation; tenant_id is an opaque UUID ref
-- validated upstream. Forward-only: atoms created before this projection went
-- live are not counted (no historical backfill — cross-DB is forbidden). The
-- counter stores a raw signed fold (commutative +1/-1); the READ clamps with
-- GREATEST(atom_count, 0) so an isolated archive (whose create predates the
-- projection) never surfaces a negative count.
--
-- RLS: tenant-isolated, FORCE'd, platform-sentinel-aware (mirrors 0024
-- transaction_ledger) so a future operator span-all read works under the
-- NOBYPASSRLS app role. Grants: applied by 9999_grant_app_roles.sql (last in
-- lex order via GRANT ... ON ALL TABLES).
-- =============================================================================

BEGIN;

CREATE TABLE tenant_atom_counts (
    tenant_id   UUID        PRIMARY KEY,
    atom_count  BIGINT      NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE tenant_atom_counts ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_atom_counts FORCE ROW LEVEL SECURITY;

-- Platform-sentinel-aware isolation (mirrors transaction_ledger, 0024): the
-- blessed 'platform' operator GUC reads all tenants; otherwise the row must
-- match the session tenant. Empty/blank GUC → NULL → matches nothing.
CREATE POLICY tenant_atom_counts_isolation ON tenant_atom_counts
    FOR ALL USING (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    );

CREATE TRIGGER trg_tenant_atom_counts_updated_at
    BEFORE UPDATE ON tenant_atom_counts
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

COMMIT;
