-- =============================================================================
-- chora-tenancy : 0016_extract_purchases_to_payments.down.sql
--
-- Reverse companion to 0016_extract_purchases_to_payments.up.sql.
-- Since 0016 is a no-op migration marker (data move performed by the
-- Go binary at services/chora-tenancy/cmd/extract-purchases-to-payments),
-- the .down companion is also a no-op marker.
--
-- To actually reverse the data move, run the extract binary with
-- DIRECTION=reverse — see the runbook in the .up.sql header.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    RAISE NOTICE 'chora-tenancy 0016 DOWN: ADR-164 Stage D no-op marker — reverse data move via cmd/extract-purchases-to-payments/main.go DIRECTION=reverse';
END$$;

COMMIT;
