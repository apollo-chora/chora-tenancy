-- =============================================================================
-- chora-tenancy : 0038_list_memberships_include_suspended.up.sql
--
-- UX refactor Track U, E3 slice 8: close the suspended-owner gap in the
-- closure ownership pre-flight.
--
-- PROBLEM
--   `list_memberships_by_gcid(uuid)` (migration 0011) filters
--   `m.suspended_at IS NULL`. The one-live-owner partial unique index
--   (0036) and the membership write guards deliberately do NOT consider
--   suspension. So the two disagree about what "holds the tenant" means:
--   a suspended owner still holds the tenant's only live owner row, while
--   the directory read cannot see it.
--
--   The closure ownership pre-flight (E3 slice 3) is built on that read.
--   A suspended owner would therefore pass the pre-flight, close their
--   account, and orphan the organisation the pre-flight exists to protect.
--
--   NOT REACHABLE TODAY, recorded so the next reader does not go hunting:
--   nothing in the repo writes `members.suspended_at` outside a test. The
--   v2 suspend handler writes an in-memory store, not this table. This
--   migration pins the behaviour BEFORE a suspend path exists, which is
--   the only cheap moment to do it.
--
-- FIX
--   A two-argument overload carrying an explicit `p_include_suspended`
--   flag, and the one-argument form rewritten as a thin wrapper that
--   delegates with FALSE.
--
--   WHY A WRAPPER AND NOT A DEFAULT PARAMETER: adding
--   `p_include_suspended boolean DEFAULT false` to the existing signature
--   creates a SECOND function rather than replacing the first (CREATE OR
--   REPLACE cannot change arity). `SELECT list_memberships_by_gcid($1)`
--   would then match both candidates and Postgres raises 42725,
--   "function is not unique". Two distinct arities with NO default is
--   unambiguous, keeps every existing caller compiling untouched, and
--   needs no DROP, so there is no window in which the function is absent
--   from a live database mid-deploy.
--
--   DEFAULT STAYS FALSE ON PURPOSE. Mint is the other caller of this read
--   (`POST /api/v1/auth/session/mint`), and mint MUST keep excluding
--   suspended members: a suspended member must not authenticate back into
--   the tenant that suspended them. Only the closure pre-flight opts in.
--
-- NO NEW RLS SURFACE. Both functions keep the SECURITY DEFINER +
-- `SET row_security = off` posture that 0011 established and that
-- `.claude/rules/ddd-enforcement.md` rule 2 accounts for as a narrow
-- SECURITY DEFINER function, NOT as one of the four sanctioned
-- RLS-bypass surfaces. The bypass-surface count is unchanged at four.
-- The widening here is over `suspended_at`, never over `tenant_id`.
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Date    : 2026-09-03
--
-- IDEMPOTENCY
--   CREATE OR REPLACE FUNCTION for both arities; GRANT EXECUTE is
--   idempotent. Safe to re-run under the migrations runner.
--
-- HARD RULE: cross-database queries forbidden. Reads only chora_tenancy
-- tables (members, tenants).
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- list_memberships_by_gcid(p_gcid uuid, p_include_suspended boolean)
--
-- The implementation. Identical to 0011 in every respect except the
-- suspension predicate, which is now driven by the flag:
--
--   p_include_suspended = FALSE -> `m.suspended_at IS NULL`  (0011 behaviour)
--   p_include_suspended = TRUE  -> no suspension filter at all
--
-- The soft-delete filters are NOT negotiable and are not touched: both
-- `m.deleted_at IS NULL` and `t.deleted_at IS NULL` still apply on both
-- paths. Suspension is reversible; deletion is not, and a soft-deleted
-- member does not hold the tenant.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION list_memberships_by_gcid(
    p_gcid              uuid,
    p_include_suspended boolean
)
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
    -- The ::text cast on tenant_slug is load-bearing: RETURNS TABLE
    -- enforces EXACT type matching on RETURN QUERY, and
    -- COALESCE(varchar, varchar) yields `character varying`, which raises
    -- "structure of query does not match function result type" without it.
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
          AND  (p_include_suspended OR m.suspended_at IS NULL)
        GROUP BY m.tenant_id, t.slug, t.name
        ORDER BY COALESCE(MAX(m.joined_at), MAX(m.invited_at)) DESC;
END;
$$;

COMMENT ON FUNCTION list_memberships_by_gcid(uuid, boolean) IS
    'E3 slice 8: cross-tenant membership directory lookup with an explicit '
    'suspension flag. FALSE reproduces migration 0011 exactly (mint, which '
    'must keep excluding suspended members); TRUE is for the closure '
    'ownership pre-flight, which must see a suspended owner because that '
    'owner still holds the tenant. SECURITY DEFINER + row_security=off as '
    'per 0011: this is a narrow SECURITY DEFINER function, NOT a fifth '
    'RLS-bypass surface. The widening is over suspended_at, never tenant_id.';

-- -----------------------------------------------------------------------------
-- list_memberships_by_gcid(p_gcid uuid)
--
-- Backward-compatible wrapper. Every existing caller keeps working with no
-- change and no ambiguity, because the two arities are distinct and neither
-- declares a DEFAULT.
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
    RETURN QUERY SELECT * FROM list_memberships_by_gcid(p_gcid, false);
END;
$$;

COMMENT ON FUNCTION list_memberships_by_gcid(uuid) IS
    'D0.1 / CHO-1538, rewritten by 0038 as a wrapper delegating to '
    'list_memberships_by_gcid(uuid, boolean) with include_suspended=false. '
    'Behaviour is unchanged from migration 0011.';

-- app_ro is intentionally still NOT granted EXECUTE: the directory query is
-- a privileged path, not a general read.
GRANT EXECUTE ON FUNCTION list_memberships_by_gcid(uuid, boolean) TO chora_tenancy_app_rw;
GRANT EXECUTE ON FUNCTION list_memberships_by_gcid(uuid)          TO chora_tenancy_app_rw;

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   -- Both arities exist, both SECURITY DEFINER, both row_security off.
--   SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args,
--          p.prosecdef AS security_definer, p.proconfig
--     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--    WHERE n.nspname = 'public' AND p.proname = 'list_memberships_by_gcid'
--    ORDER BY args;
--   -- expect two rows, both security_definer = t, both row_security=off
--
--   -- The one-arg form is NOT ambiguous (would raise 42725 if it were).
--   SELECT count(*) FROM list_memberships_by_gcid(gen_random_uuid());
-- =============================================================================
