-- =============================================================================
-- chora-tenancy : 0024_transaction_ledger.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-1937 (Wave B1) — Contextual Transaction History (ADR-205)
-- Date          : 2026-06-29
--
-- The CQRS READ projection for the unified transaction timeline (payments +
-- mana), owned by chora_tenancy (Billing) per ADR-205 resolved-decision #1.
-- Fed by Pub/Sub event subscribers (B2 / CHO-1938); served scope-aware via
-- gRPC (B3 / CHO-1939). Cross-DB-clean: NO FKs to other domains; cross-domain
-- ids are opaque UUID/text refs validated upstream (DDD invariant).
--
--   transaction_ledger          — one row per logical ledger entry
--                                 (purchase | mana_topup | mana_spend_daily)
--   transaction_ledger_detail   — per-call mana-spend rows under a
--                                 mana_spend_daily parent (drill-down)
--   transaction_ledger_summary  — per (tenant, day) KPI view (summary-first)
--
-- RLS: tenant-isolated, FORCE'd. The policy is PLATFORM-SENTINEL-AWARE — when
-- the session GUC `chora.tenant_id` = 'platform' (the cross-tenant operator
-- context blessed by libs/chora-common/rls.ValidateTenantID + envelope.proto)
-- the policy returns ALL tenants' rows. This is what makes operator span-all
-- (ADR-165, B3) actually work under FORCE RLS + a NOBYPASSRLS app role,
-- avoiding the empty-result class of bug (CHO-1931) that the old payments
-- ctx-skip path hit. The operator read path MUST emit the cross-tenant audit
-- event BEFORE issuing the query (IMDA D1, B6).
--
-- Grants: app_rw / app_ro privileges are applied by 9999_grant_app_roles.sql
-- (runs last in lex order; covers these tables via GRANT ... ON ALL TABLES +
-- ALTER DEFAULT PRIVILEGES). No explicit GRANT needed here.
-- =============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- transaction_ledger — one row per logical ledger entry
-- ----------------------------------------------------------------------------
CREATE TABLE transaction_ledger (
    ledger_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    occurred_at    TIMESTAMPTZ  NOT NULL,
    tenant_id      UUID         NOT NULL,
    learner_gcid   UUID         NULL,            -- NULL = tenant-level (not learner-scoped)
    kind           TEXT         NOT NULL CHECK (kind IN ('purchase', 'mana_topup', 'mana_spend_daily')),
    source_domain  TEXT         NOT NULL,        -- payments | identity | observability
    source_ref_id  TEXT         NOT NULL,        -- purchase_id | topup_id | 'YYYY-MM-DD' rollup key
    label          TEXT         NOT NULL DEFAULT '',
    currency       TEXT         NULL,            -- fiat only; NULL for mana rows
    amount_minor   BIGINT       NULL,            -- fiat only; minor units (e.g. cents)
    mana_units     BIGINT       NULL,            -- mana only; signed (topup +, spend -)
    status         TEXT         NOT NULL CHECK (status IN ('captured', 'refunded', 'failed', 'expired', 'posted')),
    metadata_jsonb JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Natural key for idempotent upsert from event subscribers (B2). learner_gcid
-- is COALESCE'd to the nil UUID so tenant-level rows (NULL gcid) still dedupe.
-- A mana_spend_daily row accumulates many token_usage events into ONE
-- (tenant, learner, day) row; a purchase/topup is one row per source_ref_id.
CREATE UNIQUE INDEX uq_transaction_ledger_natural_key
    ON transaction_ledger (
        tenant_id, kind, source_domain, source_ref_id,
        COALESCE(learner_gcid, '00000000-0000-0000-0000-000000000000'::uuid)
    );

-- Read indexes (ADR-205 D5.5) — newest-first within each scope.
CREATE INDEX idx_transaction_ledger_tenant_time
    ON transaction_ledger (tenant_id, occurred_at DESC);
CREATE INDEX idx_transaction_ledger_tenant_learner_time
    ON transaction_ledger (tenant_id, learner_gcid, occurred_at DESC);
CREATE INDEX idx_transaction_ledger_tenant_kind_status_time
    ON transaction_ledger (tenant_id, kind, status, occurred_at DESC);

CREATE TRIGGER trg_transaction_ledger_updated_at
    BEFORE UPDATE ON transaction_ledger
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE transaction_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_ledger FORCE ROW LEVEL SECURITY;
-- Policy expression: 'platform' GUC => all rows (operator span-all); a real
-- tenant uuid => that tenant's rows. The nested NULLIF maps BOTH the empty
-- string '' (a custom GUC that was set then reset in the session reads back as
-- '' rather than NULL) AND the 'platform' sentinel to NULL before the ::uuid
-- cast, so an unset/empty context fails CLOSED (0 rows) instead of erroring
-- with 22P02. ApplySession only ever emits a uuid or 'platform'.
CREATE POLICY transaction_ledger_tenant_isolation ON transaction_ledger
    FOR ALL
    USING (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    )
    WITH CHECK (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    );

-- ----------------------------------------------------------------------------
-- transaction_ledger_detail — per-call mana-spend rows (drill-down)
-- ----------------------------------------------------------------------------
CREATE TABLE transaction_ledger_detail (
    detail_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ledger_id      UUID         NOT NULL REFERENCES transaction_ledger (ledger_id) ON DELETE CASCADE,
    tenant_id      UUID         NOT NULL,
    learner_gcid   UUID         NULL,
    occurred_at    TIMESTAMPTZ  NOT NULL,
    action_code    TEXT         NOT NULL,
    mana_units     BIGINT       NOT NULL,        -- signed per-call spend
    model          TEXT         NULL,
    trace_id       TEXT         NULL,
    metadata_jsonb JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_transaction_ledger_detail_ledger
    ON transaction_ledger_detail (ledger_id);
CREATE INDEX idx_transaction_ledger_detail_tenant_learner_time
    ON transaction_ledger_detail (tenant_id, learner_gcid, occurred_at DESC);

ALTER TABLE transaction_ledger_detail ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_ledger_detail FORCE ROW LEVEL SECURITY;
CREATE POLICY transaction_ledger_detail_tenant_isolation ON transaction_ledger_detail
    FOR ALL
    USING (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    )
    WITH CHECK (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    );

-- ----------------------------------------------------------------------------
-- transaction_ledger_summary — per (tenant, day) KPI view (summary-first, D5.3)
--
-- A VIEW first (materialise later if volume demands). security_invoker = true
-- (PG15+) so the QUERYING role's RLS context applies — NOT the view owner's —
-- keeping tenant isolation + the platform sentinel intact through the view.
-- The precise per-currency KPI map is computed in the gRPC GetSummary (B3);
-- this view is the coarse daily rollup.
-- ----------------------------------------------------------------------------
CREATE VIEW transaction_ledger_summary
    WITH (security_invoker = true) AS
SELECT
    tenant_id,
    date_trunc('day', occurred_at)::date                                     AS day,
    count(*)                                                                  AS entry_count,
    count(*) FILTER (WHERE kind = 'purchase')                                AS purchase_count,
    coalesce(sum(amount_minor) FILTER (WHERE currency IS NOT NULL), 0)        AS amount_minor_total,
    coalesce(sum(mana_units)   FILTER (WHERE kind = 'mana_topup'), 0)         AS mana_topped_up,
    coalesce(-sum(mana_units)  FILTER (WHERE kind = 'mana_spend_daily'), 0)   AS mana_spent
FROM transaction_ledger
GROUP BY tenant_id, date_trunc('day', occurred_at)::date;

COMMIT;
