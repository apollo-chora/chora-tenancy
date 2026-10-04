-- =============================================================================
-- chora-tenancy : 0038_list_memberships_include_suspended.down.sql
--
-- Reverts E3 slice 8: drops the two-argument overload and restores the
-- one-argument function to its migration 0011 body.
--
-- ORDER MATTERS. The one-arg form is a WRAPPER that calls the two-arg form,
-- so the wrapper must be restored to a self-contained body BEFORE the
-- two-arg overload is dropped. Dropping first would leave the one-arg
-- function present but raising "function list_memberships_by_gcid(uuid,
-- boolean) does not exist" on every call, which is worse than either state:
-- mint would fail closed with an error that names a function nobody expects
-- to be involved.
--
-- Date: 2026-09-03
-- =============================================================================

BEGIN;

-- 1. Restore the one-arg function to the self-contained 0011 body, so it no
--    longer depends on the overload about to be dropped.
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
    'policy on members (SET row_security = off) - the mint directory query '
    'has no tenant context. EXECUTE granted to chora_tenancy_app_rw only.';

GRANT EXECUTE ON FUNCTION list_memberships_by_gcid(uuid) TO chora_tenancy_app_rw;

-- 2. Now the overload has no dependents and can go.
DROP FUNCTION IF EXISTS list_memberships_by_gcid(uuid, boolean);

COMMIT;
