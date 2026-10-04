-- =============================================================================
-- chora-tenancy : 0027_atom_count_projection.down.sql  (reverse of 0027 up)
-- Story: CHO-2011 (ADR-217 Debt 3). Drops the per-tenant atom-count projection.
-- The RLS policy + updated_at trigger drop with the owning table.
-- =============================================================================

BEGIN;

DROP TABLE IF EXISTS tenant_atom_counts;

COMMIT;
