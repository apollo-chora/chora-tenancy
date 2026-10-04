-- =============================================================================
-- chora-tenancy : 0025_transaction_export_jobs.down.sql  (reverts 0025 up)
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS trg_transaction_export_jobs_updated_at ON transaction_export_jobs;
DROP TABLE IF EXISTS transaction_export_jobs;

COMMIT;
