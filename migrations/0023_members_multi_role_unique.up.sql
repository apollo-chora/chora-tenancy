-- =============================================================================
-- chora-tenancy : 0023_members_multi_role_unique.up.sql
--
-- Domain        : Tenancy + Billing
-- Database      : chora_tenancy
-- Contract      : services/chora-tenancy/internal/adapter/pg/membership_write_repository.go
--                 (`MembershipWriteRepository.UpsertRoles`)
-- Story         : CHO-1809 — multi-role memberships: widen unique constraint
--                 from (tenant_id, gcid) to (tenant_id, gcid, role) so a
--                 single (tenant, user) pair can hold ≥1 role.
-- Date          : 2026-06-20
--
-- Purpose:
--   The Go code at MembershipWriteRepository.UpsertRoles loops over the
--   roles[] argument and issues
--
--     INSERT INTO members (tenant_id, gcid, role, ...)
--     VALUES (...) ON CONFLICT (tenant_id, gcid, role) DO NOTHING
--
--   for each role, so a single tenant member may carry several role rows
--   (e.g. LEARNER + INSTRUCTOR). The ON CONFLICT clause requires a
--   non-partial unique constraint on EXACTLY (tenant_id, gcid, role).
--
--   The original 0001 migration shipped a constraint UNIQUE (tenant_id,
--   gcid) — narrower than the code expects. As a result, every
--   `POST /api/v1/admin/tenant-members` invite failed with SQLSTATE 42P10
--   ("there is no unique or exclusion constraint matching the ON CONFLICT
--   specification") the first time the loop body ran, even for a single
--   role. The H+ Members page surfaced this as a generic 502 from the
--   BFF; the underlying upstream message identified the missing
--   constraint at the SQL layer.
--
-- IDEMPOTENCY REWRITE (2026-06-22):
--   The original migration did a bare `DROP CONSTRAINT members_tenant_id_gcid_key`
--   (no IF EXISTS) + `ADD CONSTRAINT ...`. On the live chora_tenancy DB that
--   legacy (tenant_id, gcid) constraint NEVER existed (the table already enforces
--   (tenant_id, gcid, role) uniqueness via the UNIQUE INDEX
--   `uq_members_tenant_gcid_role`). The bare DROP threw `constraint ... does not
--   exist` — a hard error (NOT "does not exist, skipping") that FATAL'd the
--   migrations runner at chora-tenancy (2nd service), stranding every downstream
--   DB's migrations (same wedge class as the chora-identity/0022 sibling; both
--   are the 2026-06-21 multi-role batch). This rewrite is fully idempotent +
--   reality-aligned:
--     1. Drop the legacy narrow (tenant_id, gcid) uniqueness in EITHER form
--        (constraint or standalone index) with IF EXISTS — no-op when absent.
--     2. Ensure (tenant_id, gcid, role) uniqueness via CREATE UNIQUE INDEX
--        IF NOT EXISTS, matching the name the live schema already uses. A unique
--        index satisfies `ON CONFLICT (tenant_id, gcid, role)` identically to a
--        constraint. No-op on the live DB; converges any drifted env. (dup_rows
--        verified 0 on the live DB before this lands.)
--
-- Compatibility notes:
--   - Existing rows trivially satisfy the (tenant_id, gcid, role) uniqueness.
--   - Soft delete: rows with `deleted_at IS NOT NULL` still occupy the
--     uniqueness slot (unchanged from the original intent).
-- =============================================================================

BEGIN;

-- 1. Drop the legacy narrow (tenant_id, gcid) uniqueness if present, in either
--    form. IF EXISTS makes both a no-op when absent (the live chora_tenancy case)
--    so the runner never sees a hard error.
ALTER TABLE members DROP CONSTRAINT IF EXISTS members_tenant_id_gcid_key;
DROP INDEX IF EXISTS members_tenant_id_gcid_key;

-- 2. Ensure the (tenant_id, gcid, role) uniqueness the UpsertRoles upsert needs.
--    CREATE UNIQUE INDEX IF NOT EXISTS is fully idempotent; the name matches the
--    index already present on the live DB, so this is a no-op there and a
--    convergence step on any env missing it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_members_tenant_gcid_role
  ON members (tenant_id, gcid, role);

COMMIT;
