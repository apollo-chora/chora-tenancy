-- =============================================================================
-- chora-tenancy : 0036_one_live_owner_per_tenant.up.sql
--
-- UX refactor R21, work item S7-B1: state the ownership invariant in the
-- database, not only in Go.
--
-- Ownership is one row in `members` with role='owner'. The repository guards
-- shipped alongside this migration refuse to strip it or to remove the last
-- one, and they take a row lock on the tenant's owner rows so the check and
-- the write cannot be separated. Within one process that is sound. What Go
-- cannot do is state the invariant itself, so a hand-written UPDATE, a future
-- service that forgets the guard, or a migration nobody read carefully can
-- still leave a tenant with two owners.
--
-- This index says: at most ONE live owner row per tenant. The Go guards say:
-- never zero, and never lose one by accident. Neither half can be expressed
-- where the other lives, which is why both exist.
--
-- Predicate, both halves load-bearing:
--   role = 'owner'        without it the index would cap every tenant at ONE
--                         MEMBER, which is catastrophic and would be caught
--                         immediately, but is worth naming anyway.
--   deleted_at IS NULL    without it a soft-deleted former owner would block
--                         the next one forever, and since the platform never
--                         hard-deletes, the ownership handover could never
--                         complete even once.
--
-- ⚠ CONSEQUENCE FOR THE HANDOVER (S7-B4, not built here). A partial unique
-- INDEX cannot be deferred: Postgres defers CONSTRAINTs, and a unique
-- constraint cannot carry a WHERE clause. So the transfer transaction MUST
-- soft-delete the outgoing owner row BEFORE inserting the incoming one.
-- Insert-then-delete violates this index mid-transaction and fails. The
-- first-launch spec 13.3.3 lists the insert first; read that as an unordered
-- list of effects, not as the statement order.
--
-- ⚠ This migration REFUSES rather than repairs. If a tenant already holds two
-- live owner rows, the DO block raises with the offending tenant ids. Deleting
-- the "surplus" row automatically would resolve the conflict by destroying
-- exactly the fact this work exists to protect, and no migration should make
-- that call unattended. Verified on the local Postgres 18 mirror before
-- writing this: zero tenants hold more than one live owner row, which is
-- expected because only tenant bootstrap ever writes the role and it writes
-- one.
--
-- No data is moved. Members is untouched.
-- =============================================================================

DO $$
DECLARE
    offending text;
BEGIN
    SELECT string_agg(tenant_id::text, ', ')
    INTO   offending
    FROM (
        SELECT tenant_id
        FROM   members
        WHERE  role = 'owner'
          AND  deleted_at IS NULL
        GROUP  BY tenant_id
        HAVING count(*) > 1
    ) AS d;

    IF offending IS NOT NULL THEN
        RAISE EXCEPTION
            'cannot enforce one live owner per tenant: these tenants already hold more than one live members row with role=owner: %. Decide which GCID owns each of them and soft-delete the others (set deleted_at, never DELETE) before re-running this migration. Do not resolve this automatically: ownership is not admin-grantable, so the wrong choice cannot be undone through any API.',
            offending;
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_members_one_live_owner_per_tenant
    ON members (tenant_id)
    WHERE role = 'owner' AND deleted_at IS NULL;

COMMENT ON INDEX uq_members_one_live_owner_per_tenant IS
    'S7-B1 (UX refactor R21): at most one live owner row per tenant. Complements the repository guards, which enforce the other half (never zero, never stripped by a role edit). The ownership handover must soft-delete the outgoing owner row before inserting the incoming one: this index cannot be deferred.';
