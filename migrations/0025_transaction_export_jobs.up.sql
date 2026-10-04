-- =============================================================================
-- chora-tenancy : 0025_transaction_export_jobs.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-1941 (Wave B5) — Contextual Transaction History async
--                 export (ADR-205 D5.6)
-- Date          : 2026-06-29
--
-- Backs the async export of the transaction_ledger projection (large result
-- sets time out a synchronous stream). A CreateTransactionExport RPC inserts a
-- PENDING row; a background worker claims it (across tenants, under the
-- 'platform' sentinel), reads the ledger under the JOB's scope, builds the
-- CSV/JSON, uploads to GCS, signs a URL, and flips the row to READY (or FAILED).
--
-- RLS: tenant-isolated + FORCE'd, PLATFORM-SENTINEL-AWARE (identical policy to
-- transaction_ledger, migration 0024). The owning service is chora_tenancy_app_rw
-- (NOBYPASSRLS). Scope → tenant_id slot:
--   - learner / tenant / master-single-franchisee → the concrete tenant UUID.
--   - master span-all → the NIL uuid sentinel (00000000-...-0); only the
--     operator context (chora.tenant_id = 'platform') can see it, since no real
--     tenant's GUC equals the nil uuid. The worker claims under 'platform'.
-- A learner additionally only sees their own job (the gRPC layer adds a
-- learner_gcid predicate, mirroring the read path).
--
-- Grants applied by 9999_grant_app_roles.sql (runs last; covers new tables).
-- =============================================================================

BEGIN;

CREATE TABLE transaction_export_jobs (
    job_id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID         NOT NULL,   -- concrete tenant | nil-uuid (span-all)
    owner_gcid           UUID         NOT NULL,   -- caller who created the job
    learner_gcid         UUID         NULL,       -- learner scope: the bound learner
    scope                TEXT         NOT NULL CHECK (scope IN ('learner', 'tenant', 'master')),
    franchisee_tenant_id UUID         NULL,       -- master single-franchisee selector echo
    filters_jsonb        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    format               TEXT         NOT NULL CHECK (format IN ('csv', 'json')),
    status               TEXT         NOT NULL DEFAULT 'pending'
                                      CHECK (status IN ('pending', 'building', 'ready', 'failed')),
    gcs_object           TEXT         NULL,        -- object path once uploaded
    download_url         TEXT         NULL,        -- signed URL (status=ready)
    expires_at           TIMESTAMPTZ  NULL,        -- signed-URL expiry (status=ready)
    error                TEXT         NULL,        -- failure reason (status=failed)
    attempt_count        INT          NOT NULL DEFAULT 0,
    last_attempt_at      TIMESTAMPTZ  NULL,        -- claim-reservation + crashed-worker reaper
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Worker claim path: oldest pending first.
CREATE INDEX idx_transaction_export_jobs_pending
    ON transaction_export_jobs (created_at)
    WHERE status = 'pending';

-- Poll path: a caller's jobs newest-first.
CREATE INDEX idx_transaction_export_jobs_owner
    ON transaction_export_jobs (tenant_id, owner_gcid, created_at DESC);

CREATE TRIGGER trg_transaction_export_jobs_updated_at
    BEFORE UPDATE ON transaction_export_jobs
    FOR EACH ROW EXECUTE FUNCTION tenancy_set_updated_at();

ALTER TABLE transaction_export_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_export_jobs FORCE ROW LEVEL SECURITY;
-- Platform-sentinel-aware (matches transaction_ledger): 'platform' GUC => all
-- rows (worker + operator span-all); a real tenant uuid => that tenant's rows;
-- empty/unset => fail closed (nested NULLIF guard, no 22P02).
CREATE POLICY transaction_export_jobs_tenant_isolation ON transaction_export_jobs
    FOR ALL
    USING (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    )
    WITH CHECK (
        current_setting('chora.tenant_id', true) = 'platform'
        OR tenant_id = NULLIF(NULLIF(current_setting('chora.tenant_id', true), ''), 'platform')::uuid
    );

COMMIT;
