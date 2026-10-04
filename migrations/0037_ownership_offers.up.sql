-- =============================================================================
-- chora-tenancy : 0037_ownership_offers.up.sql
--
-- UX refactor E3, work items S7-B2 to B7: the two-party ownership handover.
--
-- Ownership is one row in `members` with role='owner'. It is the only role no
-- admin API can grant, and it carries the tenant-deletion authority. So it does
-- not move by editing a role: that call is ReplaceRoles, which soft-deletes
-- every row outside the new set, and 'owner' can never be in that set, so a
-- role edit destroys ownership instead of moving it. The 0036 guards refuse
-- exactly that, which is why the handover needs a mechanism of its own.
--
-- This table is that mechanism's consent record. A handover writes nothing to
-- `members` until an offer here reaches 'accepted'. Assigning the deletion
-- authority for an organisation to somebody who never agreed would be a
-- liability transfer without consent, so the nominee has to say yes, including
-- on the platform-operator override.
--
-- SHAPE. Deliberately mirrors chora_identity.pending_invites (live since its
-- migration 0026) rather than inventing one: a pending row, a TTL, terminal
-- states, and a partial-unique index so a re-offer is safe.
--
-- NO SOFT DELETE COLUMN, ON PURPOSE. Rows here are never deleted at all: every
-- outcome is a terminal STATUS and the row stays as the record of it. That is
-- stricter than the platform's soft-delete rule, not a departure from it, and
-- it matches the pending_invites precedent, which also has no deleted_at.
--
-- ⚠ THE ORDERING CONSTRAINT THIS TABLE INHERITS. 0036 created
-- uq_members_one_live_owner_per_tenant, a PARTIAL UNIQUE INDEX, and a partial
-- unique index cannot be deferred: Postgres defers CONSTRAINTs, and a unique
-- constraint cannot carry a WHERE clause. So the accept transaction MUST
-- soft-delete the outgoing owner row BEFORE inserting the incoming one.
-- Insert-then-delete violates the index mid-transaction and fails. Proven on
-- the local Postgres 18 mirror: delete-then-insert commits, the reverse does
-- not. The first-launch spec 13.3.3 lists the insert first; read that as an
-- unordered list of effects, not as the statement order.
--
-- No data is moved. `members` is untouched by this migration.
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ownership_offer_status') THEN
        CREATE TYPE ownership_offer_status AS ENUM
            ('pending', 'accepted', 'declined', 'revoked', 'expired');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ownership_offer_initiator') THEN
        -- Only these two. A tenant admin can never start a handover: ownership
        -- is the one role that is not admin-grantable, and letting an admin
        -- nominate would be self-promotion by another name.
        CREATE TYPE ownership_offer_initiator AS ENUM ('owner', 'operator');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS ownership_offers (
    offer_id     UUID                      PRIMARY KEY DEFAULT uuidv7(),
    tenant_id    UUID                      NOT NULL,
    -- The GCID losing ownership. Recorded even on the operator override, where
    -- the outgoing owner may be unreachable or closing, so the audit trail
    -- names both ends whatever happens next.
    from_gcid    UUID                      NOT NULL,
    to_gcid      UUID                      NOT NULL,
    initiated_by UUID                      NOT NULL,
    initiator    ownership_offer_initiator NOT NULL,
    -- Mandatory on the operator override (CHECK below), optional otherwise.
    reason       TEXT                      NULL,
    -- Shown to the nominee. Never used as the audit reason.
    note         TEXT                      NULL,
    status       ownership_offer_status    NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ               NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ               NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ               NOT NULL,
    responded_at TIMESTAMPTZ               NULL,
    responded_by UUID                      NULL,

    -- Self-nomination is a no-op that looks like an action: the owner would
    -- see a pending handover that can never change anything.
    CONSTRAINT ck_ownership_offers_not_self CHECK (from_gcid <> to_gcid),
    -- The override without a reason is unauditable. btrim so whitespace does
    -- not pass for an explanation.
    CONSTRAINT ck_ownership_offers_operator_reason CHECK (
        initiator <> 'operator' OR btrim(coalesce(reason, '')) <> ''
    ),
    -- A TTL that has already elapsed at insert would create an offer nobody can
    -- accept, indistinguishable from a bug in the clock.
    CONSTRAINT ck_ownership_offers_ttl_future CHECK (expires_at > created_at)
);

-- At most ONE pending offer per tenant. Two racing offers would each try to
-- write an owner row, and 0036 would fail the second one at commit with an
-- index violation the caller cannot interpret; refusing the second OFFER
-- instead gives them a reason. Terminal offers do not block a fresh one, which
-- is what makes a re-offer after a decline safe.
CREATE UNIQUE INDEX IF NOT EXISTS uq_ownership_offers_one_pending_per_tenant
    ON ownership_offers (tenant_id)
    WHERE status = 'pending';

-- The nominee's inbox: "which offers am I being asked to answer".
CREATE INDEX IF NOT EXISTS idx_ownership_offers_to_gcid
    ON ownership_offers (to_gcid, status);
-- The reaper's sweep: pending rows past their TTL.
CREATE INDEX IF NOT EXISTS idx_ownership_offers_expiry
    ON ownership_offers (expires_at)
    WHERE status = 'pending';

CREATE OR REPLACE TRIGGER trg_ownership_offers_updated_at
    BEFORE UPDATE ON ownership_offers
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

-- Same tenant isolation as `members` and `add_on_subscriptions`. The app role
-- chora_tenancy_app_rw is NOBYPASSRLS (0013), so every read and write here runs
-- inside a SET LOCAL chora.tenant_id transaction or sees nothing. The operator
-- override is authorised by the JWT role and still runs inside
-- RunInTenantTx(target_tenant): it is authorisation, not an RLS bypass, and it
-- adds no fifth bypass surface to the ADR-165/184/192/255 chain.
ALTER TABLE ownership_offers ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'ownership_offers' AND policyname = 'tenant_isolation'
    ) THEN
        CREATE POLICY tenant_isolation ON ownership_offers
            FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
    END IF;
END
$$;

COMMENT ON TABLE ownership_offers IS
    'E3 (UX refactor S7-B2): two-party tenant ownership handover. A handover writes nothing to members until an offer here reaches accepted. One pending offer per tenant; the accept transaction must soft-delete the outgoing owner row BEFORE inserting the incoming one, because 0036 uq_members_one_live_owner_per_tenant is a partial unique index and cannot be deferred.';
