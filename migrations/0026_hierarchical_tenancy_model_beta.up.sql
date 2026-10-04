-- =============================================================================
-- chora-tenancy : 0026_hierarchical_tenancy_model_beta.up.sql
--
-- ADR-217 (Hierarchical tenancy, Model β) — Phase 1: schema/data.
--   AMENDS ADR-182 D3 (private tenants: independent → children of chora-master).
--   Epic CHO-2002 / story CHO-2003.
--
--   (a) Persist hosting_mode. Proto HostingMode (tenancy.proto) + OpenAPI
--       (tenancy-admin.yaml) both spell the values UPPERCASE
--       [PLATFORM_HOSTED, WHITE_LABEL, FRANCHISE, SELF_HOST], so the DB enum
--       mirrors the wire exactly (Phase-2 Go mapping is a pass-through). This
--       is a DELIBERATE divergence from the lowercase tenant_status convention
--       — the API boundary already case-translates `state` (ACTIVE↔active), so
--       matching the wire here removes a second translation. Classification /
--       reporting only, NOT access control (ADR-217 D5); `self_hosted` stays.
--
--   (b) Re-parent the 4 active customer tenants under chora-master (franchisor
--       root) and classify them FRANCHISE (ADR-217 D2/D6). Targets 4 EXPLICIT
--       UUIDs only — never a broad UPDATE — so daleleung@hotmail (public
--       chora-master member) and the phyllis-*/rls-* pending_activation test
--       rows are left untouched.
--
--   (c) Rename chora-master display name 'Chora' → 'Chora Platform' (ADR-217 D1).
--       The slug 'chora-master' and id ...-001 are IMMUTABLE — untouched.
--
--   (d) Parent invariant (ADR-217 D6): a non-root tenant's parent chain MUST
--       terminate at chora-master. Root (parent NULL) is EXEMPT — this is what
--       keeps ADR-182 D2 auto-enrol (which writes `members`, never `tenants`)
--       and the still-root-creating bootstrap path working until Phase 2
--       rewires onboarding. Cycles + foreign-root parents are rejected. Enforced
--       by a BEFORE INSERT OR UPDATE OF parent_tenant_id trigger (depth-bounded).
--
-- Idempotent + reversible (companion 0026_..._.down.sql). `tenants` is the
-- RLS-FREE registry (0001) so these writes need no SET LOCAL chora.tenant_id;
-- the runner connects as the OWNER migrate role.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- (a) hosting_mode enum type + column
-- ---------------------------------------------------------------------------
-- CREATE TYPE has no IF NOT EXISTS — guard for re-apply safety (stale-GCS
-- re-apply gotcha). PUBLIC holds USAGE on new types by default, so app_rw /
-- app_ro can read+write the column without an extra GRANT.
DO $$
BEGIN
    CREATE TYPE tenant_hosting_mode AS ENUM
        ('PLATFORM_HOSTED', 'WHITE_LABEL', 'FRANCHISE', 'SELF_HOST');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Constant default → PG11+ fast-default (no table rewrite).
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS hosting_mode tenant_hosting_mode
        NOT NULL DEFAULT 'PLATFORM_HOSTED';

-- Classification backfill: a self-hosted tenant is SELF_HOST (0 rows in dev
-- today; kept for environment-independence). FRANCHISE for the 4 (below) wins.
UPDATE tenants
SET    hosting_mode = 'SELF_HOST'
WHERE  self_hosted = TRUE
  AND  hosting_mode = 'PLATFORM_HOSTED';

-- ---------------------------------------------------------------------------
-- (d) Parent invariant — installed BEFORE the re-parenting below so the
--     migration's own data change is validated by the very invariant it adds.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION tenancy_check_parent_in_master_tree()
RETURNS TRIGGER AS $$
DECLARE
    -- chora-master id is IMMUTABLE (ADR-182 D1 / ADR-217 D1); referencing the
    -- fixed seed UUID (migration 0021) is the sanctioned pattern — a stable
    -- domain constant, not environment config.
    master_id CONSTANT uuid := '00000000-0000-7000-8000-000000000001';
    cur       uuid := NEW.parent_tenant_id;
    hops      int  := 0;
    max_hops  CONSTANT int := 64;   -- depth-bounded (ADR-217 guardrail)
BEGIN
    -- Root is exempt: a NULL parent is always allowed (chora-master + any
    -- legacy/bootstrap root that Phase 2 will later re-home).
    IF NEW.parent_tenant_id IS NULL THEN
        RETURN NEW;
    END IF;

    -- Direct self-parent (defensive; a table CHECK also blocks it).
    IF NEW.parent_tenant_id = NEW.tenant_id THEN
        RAISE EXCEPTION 'tenant % cannot be its own parent', NEW.tenant_id
            USING ERRCODE = 'check_violation';
    END IF;

    -- Walk the parent chain upward; it MUST terminate at chora-master.
    WHILE cur IS NOT NULL LOOP
        IF cur = master_id THEN
            RETURN NEW;                        -- resolved within the master tree
        END IF;
        IF cur = NEW.tenant_id THEN            -- new edge closes a loop
            RAISE EXCEPTION 'tenant % parent assignment would create a cycle', NEW.tenant_id
                USING ERRCODE = 'check_violation';
        END IF;
        hops := hops + 1;
        IF hops > max_hops THEN
            RAISE EXCEPTION 'tenant % parent chain exceeds max depth % (cycle or too deep)',
                NEW.tenant_id, max_hops USING ERRCODE = 'check_violation';
        END IF;
        SELECT t.parent_tenant_id INTO cur FROM tenants t WHERE t.tenant_id = cur;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'tenant % parent % does not exist', NEW.tenant_id, cur
                USING ERRCODE = 'foreign_key_violation';
        END IF;
    END LOOP;

    -- Chain ended at a NULL parent that was not chora-master → a foreign root.
    RAISE EXCEPTION
        'tenant % parent_tenant_id must resolve to chora-master (%); chain terminated at a foreign root',
        NEW.tenant_id, master_id USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_tenants_parent_in_master_tree
    BEFORE INSERT OR UPDATE OF parent_tenant_id ON tenants
    FOR EACH ROW EXECUTE FUNCTION tenancy_check_parent_in_master_tree();

-- ---------------------------------------------------------------------------
-- (c) Rename chora-master display name (idempotent; slug/id untouched).
-- ---------------------------------------------------------------------------
UPDATE tenants
SET    name = 'Chora Platform'
WHERE  tenant_id = '00000000-0000-7000-8000-000000000001'
  AND  name IS DISTINCT FROM 'Chora Platform';

-- ---------------------------------------------------------------------------
-- (b) Re-parent the 4 active customer tenants under chora-master + FRANCHISE.
--     Guarded on parent_tenant_id IS NULL so a re-run never clobbers a later
--     (Phase 2 multi-level) re-parenting; the id set is EXACTLY these four.
-- ---------------------------------------------------------------------------
UPDATE tenants
SET    parent_tenant_id = '00000000-0000-7000-8000-000000000001',
       hosting_mode     = 'FRANCHISE'
WHERE  tenant_id IN (
        '11111111-1111-7111-8111-111111111111',  -- Mighty Mind Tuition Agency (mtm-singapore)
        '22222222-2222-7222-8222-222222222222',  -- Chen's Coaching (chen-coaching)
        '019e899d-99d7-7064-a732-6c9ca6955cda',  -- ACE
        '019eb1b4-bfdc-7591-ab0d-3aa33488d1f3'   -- MTM Phyllis Academy
       )
  AND  parent_tenant_id IS NULL
  AND  deleted_at IS NULL;

COMMIT;
