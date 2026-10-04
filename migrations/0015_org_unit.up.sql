-- =============================================================================
-- chora-tenancy : 0015_org_unit.up.sql
--
-- Domain        : Tenancy + Billing (7 supporting)
-- Database      : chora_tenancy
-- Author        : ADR-133 (GCID identity portability) §"OrgUnit"
-- Architecture  : docs/architecture/adrs/adr-133-gcid-identity-portability.md
--                 docs/references/ddd-aggregate-map.md §3.7 (Tenancy aggregates) + §10 (audit A2)
-- Audit reference: ~/.claude/plans/transient-hugging-dewdrop.md §4.1 A2 + §16.2 I.2
--
-- Purpose:
--   Author the per-tenant org_unit hierarchy table that ADR-133 commits but
--   earlier audits flagged as missing. OrgUnit is self-referential (parent_unit_id
--   FK back into org_unit). The unit_type enum partitions the hierarchy by
--   organisational role (headquarters / franchise / branch / campus / department).
--
-- Aggregate boundary:
--   `OrgUnit` is a separate aggregate root in Tenancy per ADR-133. A tenant
--   may host an arbitrary tree of org_units; TenantMembership rows in
--   chora_identity carry an optional `org_unit_id` to bind a (gcid, tenant_id)
--   to a specific node in the tree. Cross-domain reference is UUID-only with
--   no FK constraint (chora_identity sits in a separate database).
--
-- Soft-delete per .claude/rules/ddd-enforcement.md §5.
-- RLS-enabled — composes with multi-tenant-rls skill.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- org_unit — per-tenant organisational hierarchy (self-referential)
-- -----------------------------------------------------------------------------
CREATE TABLE org_unit (
    unit_id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID         NOT NULL REFERENCES tenants(tenant_id) ON DELETE RESTRICT,
    parent_unit_id        UUID         REFERENCES org_unit(unit_id) ON DELETE RESTRICT,

    unit_type             TEXT         NOT NULL
        CHECK (unit_type IN (
            'headquarters',
            'franchise',
            'branch',
            'campus',
            'department'
        )),
    name                  VARCHAR(256) NOT NULL,
    code                  VARCHAR(64),
    metadata              JSONB        NOT NULL DEFAULT '{}'::jsonb,

    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at            TIMESTAMPTZ,

    CHECK (parent_unit_id IS NULL OR parent_unit_id <> unit_id)
);

CREATE INDEX idx_org_unit_tenant
    ON org_unit (tenant_id, unit_type)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_org_unit_parent
    ON org_unit (parent_unit_id)
    WHERE parent_unit_id IS NOT NULL AND deleted_at IS NULL;

-- Per-tenant unique code where code is supplied (partial index).
CREATE UNIQUE INDEX idx_org_unit_tenant_code_unique
    ON org_unit (tenant_id, code)
    WHERE code IS NOT NULL AND deleted_at IS NULL;

CREATE TRIGGER trg_org_unit_updated_at
    BEFORE UPDATE ON org_unit
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE org_unit ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_unit FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON org_unit
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- Grants handled by services/chora-tenancy/migrations/9999_grant_app_roles.sql.

COMMIT;
