-- =============================================================================
-- chora-tenancy : 0031_tenant_external_egress_policy.down.sql
--
-- Reverses 0031. Dropping the source-of-truth table reverts every tenant to the
-- default-deny posture (no row => egress OFF) the moment the projection stops
-- being fed — the safe direction, so no data-preserving unwind is needed.
--
-- The chora_observability read-copy (external_egress_policy) is NOT touched
-- here: it is a different database and cross-DB writes are forbidden. Rolling
-- this back leaves the last-projected rows in observability until an operator
-- clears them.
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS trg_tenant_external_egress_policies_updated_at
    ON tenant_external_egress_policies;
DROP POLICY IF EXISTS tenant_isolation ON tenant_external_egress_policies;
DROP TABLE IF EXISTS tenant_external_egress_policies;

COMMIT;
