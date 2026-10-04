-- =============================================================================
-- chora-tenancy : 0011_list_memberships_by_gcid_security_definer.sql
--
-- D0.1 (CHO-1538) — RLS bypass for the cross-tenant membership directory
-- query that the post-Stage-2-cutover mint flow depends on.
--
-- PROBLEM
--   The auth mint flow (`POST /api/v1/auth/session/mint` →
--   chora-identity `/v1/identity/resolve` → chora-tenancy gRPC
--   `ListMembershipsByGCID`) is a CROSS-TENANT directory lookup: "given a
--   GCID, what tenants does this user belong to?". It has NO tenant
--   context — there is no `chora.tenant_id` to set, because resolving the
--   tenant set is the whole point of the call.
--
--   `chora_tenancy.members` carries the `tenant_isolation` RLS policy
--   (`tenant_id = current_setting('chora.tenant_id', true)::uuid`,
--   declared in 0001_initial.sql). The chora-tenancy gRPC server's pg pool
--   authenticates as `chora_tenancy_app_rw`, which does NOT have the
--   BYPASSRLS attribute. So an inline `SELECT ... FROM members WHERE
--   gcid = $1` is filtered to ZERO rows whenever RLS is enabled — mint
--   then returns `403 AUTH_NO_TENANT_MEMBERSHIP` for every user.
--
--   Live-DB reconciliation (2026-05-14, read-only Cloud Run Job inspect):
--   `members.relrowsecurity = f` — RLS had been DISABLED on the live DB
--   as an out-of-band workaround (NOT captured in any migration). That is
--   a cross-tenant data-leak regression: any holder of the app_rw role
--   could read every tenant's directory. This migration + the follow-up
--   0012 close that regression properly.
--
-- FIX
--   A `SECURITY DEFINER` function `list_memberships_by_gcid(uuid)` owned by
--   the migrate role (`chora_tenancy_migrate` — the table owner, which is
--   exempt from RLS). With `SET row_security = off` in its config, the
--   function body's read of `members` runs WITHOUT the `tenant_isolation`
--   policy applied — exactly what a cross-tenant directory query needs.
--   `EXECUTE` is granted to `chora_tenancy_app_rw` so the gRPC server can
--   call it; nothing else changes about the app_rw role's RLS posture.
--   Ordinary `SELECT`s on `members` from app_rw remain fully RLS-governed
--   (re-enabled in 0012).
--
--   The function body is byte-equivalent to the inline SELECT it replaces
--   in services/chora-tenancy/internal/adapter/pg/membership_repository.go
--   (same ARRAY_AGG roles aggregation, same tenants JOIN for slug, same
--   soft-delete + suspension filters, same joined_at-DESC ordering) — only
--   the RLS boundary moves.
--
-- SCHEMA REFERENCE (verified live 2026-05-14)
--   members  : member_id, tenant_id, gcid, role (enum tenant_member_role),
--              suspended_at, invited_at, joined_at, created_at, updated_at,
--              deleted_at. RLS policy `tenant_isolation`.
--   tenants  : tenant_id PK, name, ..., slug (nullable, 0004_phyllis_mvp),
--              deleted_at. No RLS (registry table — 0001 §tenants comment).
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Author  : tenancy-rls agent (D0.1 Phyllis backend demo-readiness)
-- Date    : 2026-05-14
--
-- IDEMPOTENCY
--   - CREATE OR REPLACE FUNCTION — re-apply is a no-op.
--   - GRANT EXECUTE — idempotent by design.
--   - Safe to re-run under the migrations-runner (chora_runner_schema_
--     migrations dedup) and under session_replication_role='replica'.
--
-- HARD RULE: cross-database queries forbidden. This function only reads
-- chora_tenancy-local tables (members, tenants).
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- list_memberships_by_gcid(p_gcid uuid)
--
-- Returns one row per (gcid, tenant_id) pair the user belongs to, across
-- ALL tenants, with every role held in that tenant aggregated into
-- roles[]. SECURITY DEFINER + `SET row_security = off` so the cross-tenant
-- read is not filtered by the `tenant_isolation` RLS policy on members.
--
-- Returning zero rows is a valid result ("user has no memberships yet") —
-- the gRPC adapter surfaces that as an empty list, NOT an error.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION list_memberships_by_gcid(p_gcid uuid)
RETURNS TABLE (
    tenant_id            uuid,
    tenant_slug          text,
    roles                text[],
    joined_or_invited_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET row_security = off
SET search_path = public, pg_temp
AS $$
BEGIN
    -- NOTE on the explicit ::text cast on tenant_slug:
    --   tenants.slug is VARCHAR(64) and tenants.name is VARCHAR(256), so
    --   COALESCE(t.slug, lower(replace(t.name, ' ', '-'))) yields
    --   `character varying`. A PL/pgSQL `RETURNS TABLE` function enforces
    --   EXACT type matching on RETURN QUERY (unlike a bare SELECT), so an
    --   unqualified varchar column raises:
    --     ERROR: structure of query does not match function result type
    --   The `::text` cast pins it to the declared `tenant_slug text`
    --   column. m.tenant_id (uuid), the ARRAY_AGG (text[]) and the
    --   COALESCE(MAX(...)) (timestamptz) already match their declared
    --   types exactly.
    RETURN QUERY
        SELECT
            m.tenant_id                                                AS tenant_id,
            COALESCE(t.slug, lower(replace(t.name, ' ', '-')))::text    AS tenant_slug,
            ARRAY_AGG(DISTINCT m.role::text)                            AS roles,
            COALESCE(MAX(m.joined_at), MAX(m.invited_at))               AS joined_or_invited_at
        FROM   members m
        JOIN   tenants t
          ON   t.tenant_id = m.tenant_id
         AND   t.deleted_at IS NULL
        WHERE  m.gcid = p_gcid
          AND  m.deleted_at IS NULL
          AND  m.suspended_at IS NULL
        GROUP BY m.tenant_id, t.slug, t.name
        ORDER BY COALESCE(MAX(m.joined_at), MAX(m.invited_at)) DESC;
END;
$$;

COMMENT ON FUNCTION list_memberships_by_gcid(uuid) IS
    'D0.1 / CHO-1538: SECURITY DEFINER cross-tenant membership directory '
    'lookup for the auth mint flow. Bypasses the tenant_isolation RLS '
    'policy on members (SET row_security = off) — the mint directory query '
    'has no tenant context. EXECUTE granted to chora_tenancy_app_rw only.';

-- The chora-tenancy gRPC server (pg pool authenticates as app_rw) is the
-- sole sanctioned caller. app_ro is intentionally NOT granted EXECUTE —
-- the directory query is a privileged path, not a general read.
GRANT EXECUTE ON FUNCTION list_memberships_by_gcid(uuid) TO chora_tenancy_app_rw;

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   -- Function exists, is SECURITY DEFINER, has row_security off.
--   SELECT p.proname, p.prosecdef AS security_definer, p.proconfig
--     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--    WHERE n.nspname = 'public' AND p.proname = 'list_memberships_by_gcid';
--   -- expect: security_definer = t, proconfig contains 'row_security=off'
--
--   -- app_rw can call it cross-tenant with NO chora.tenant_id set.
--   SET ROLE chora_tenancy_app_rw;
--   SELECT * FROM list_memberships_by_gcid('<some-gcid>'::uuid);
--   RESET ROLE;
-- =============================================================================
