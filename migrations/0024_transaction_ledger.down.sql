-- =============================================================================
-- chora-tenancy : 0024_transaction_ledger.down.sql  (reverse of 0024 up)
-- Story: CHO-1937 (Wave B1) — ADR-205. Drops the unified ledger read projection.
-- Indexes, triggers and RLS policies drop with their owning tables.
-- =============================================================================

BEGIN;

DROP VIEW  IF EXISTS transaction_ledger_summary;
DROP TABLE IF EXISTS transaction_ledger_detail;
DROP TABLE IF EXISTS transaction_ledger;

COMMIT;
