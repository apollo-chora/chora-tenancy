-- =============================================================================
-- chora-tenancy : 0032_closure_pseudonymisation_state.up.sql
--
-- Domain        : Tenancy + Billing (supporting/platform)
-- Database      : chora_tenancy
-- Story         : CHO-2198 (W0-F1 durability + W0-F5 error-honesty)
--
-- Durable ack/dedup state for the federated account-closure saga (Tier 3
-- D11 / ADR-184 / ADR-186), replacing the process-local
-- events.InMemoryClosureRepo (W0-F1 UNGATED-DEFECT — see
-- docs/references/w0-f1-inmemory-inventory.md §6 item 4). Today the repo is
-- nested only under `pubsubClient != nil`, never gated on pool health, so
-- this ack/dedup state is lost on every pod restart, which can
-- duplicate-process or permanently stall the closure saga for this domain.
--
-- What this table is NOT: it does not itself redact any PII column. The
-- closure subscriber (internal/adapter/events/closure_subscriber.go) still
-- decides WHAT to tokenise from config/PII_Closure_Map.yaml at read time;
-- neither the prior in-memory repo NOR this pg-backed ClosureRepository
-- executes a real per-table UPDATE against tenant_memberships /
-- stripe_customer_mappings / tenant_invitations / addon_usage_events —
-- both only durably record THAT a (tenant, gcid) pair has been processed
-- and HOW MANY columns the map declared (rows_touched is a
-- declared-intent count from the PII map, not an actual per-row
-- UPDATE-affected count). Real per-table redaction is a separate, deeper
-- gap confirmed to apply identically across ALL 9 closure-saga services
-- (see CHO-2198 durability report). This migration only fixes DURABILITY
-- of the ack/dedup signal.
--
-- Row semantics: INSERT-once via ON CONFLICT (tenant_id, gcid) DO NOTHING,
-- never UPDATEd by application code — existence of a (tenant_id, gcid) row
-- IS the "pseudonymised" flag (mirrors the in-memory repo's
-- `pseudonymed[gcid] = true`, now tenant-scoped: the in-memory version
-- collapsed the key to gcid-only, a latent cross-tenant dedup collision
-- fixed alongside this migration — see closure_subscriber.go). No
-- deleted_at: this is append-only operational/ack state, not domain
-- content (same soft-delete exemption class as outbox_events /
-- idempotency_keys per ddd-enforcement.md §Soft Deletes Exceptions), and
-- MUST NOT be reversed by anything short of the saga's own compensation
-- path — which, per the account-closure-saga skill, is refused once
-- pseudonymisation has been ack'd (point of no return).
--
-- RLS: ENABLE + FORCE (matches this service's own most recent RLS
-- precedent, 0031_tenant_external_egress_policy.up.sql) with a STRICT
-- tenant_isolation policy — chora_tenancy_app_rw is NOBYPASSRLS
-- (0013_app_rw_nobypassrls.sql), and every write in this service's pg
-- package that touches an RLS-protected table already runs inside a
-- RunInTenantTx-opened transaction (SET LOCAL chora.tenant_id BEFORE the
-- user query — see internal/adapter/pg/closure_repository.go +
-- membership_write_repository.go), so — unlike chora-a2a-gateway — a
-- strict policy is safe and correct here, not a permissive-when-unset
-- escape hatch.
--
-- Date : 2026-07-15
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS closure_pseudonymisation_state (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID         NOT NULL,
    gcid             UUID         NOT NULL,
    rows_touched     INTEGER      NOT NULL DEFAULT 0,
    pseudonymised_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- One durable ack per (tenant, gcid) — the natural idempotency key the
    -- ClosureRepository.Pseudonymise port operates on.
    UNIQUE (tenant_id, gcid)
);

-- Cross-tenant admin/debug lookup path ("has this GCID been closed in ANY
-- tenant this domain has rows for?"). The (tenant_id, gcid) UNIQUE
-- constraint above already covers the tenant-scoped lookup RLS/app queries
-- use; this is the gcid-only shape for O+ closure-pipeline visibility
-- (account-closure-saga skill "Admin O+ closure-pipeline visibility").
CREATE INDEX IF NOT EXISTS idx_closure_pseudonymisation_state_gcid
    ON closure_pseudonymisation_state (gcid);

ALTER TABLE closure_pseudonymisation_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE closure_pseudonymisation_state FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON closure_pseudonymisation_state;
CREATE POLICY tenant_isolation ON closure_pseudonymisation_state
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Self-contained grants (9999_grant_app_roles.sql already ran against the
-- live DB; new tables created afterwards need explicit grants — same
-- convention as 0031_tenant_external_egress_policy.up.sql; ALTER DEFAULT
-- PRIVILEGES alone has previously caused 42501 on targeted/backdated
-- migrations, see
-- reusable_gotcha_targeted_migration_skips_9999_grants_and_tracker_lineage_drift).
GRANT SELECT, INSERT, UPDATE, DELETE ON closure_pseudonymisation_state TO chora_tenancy_app_rw;
GRANT SELECT ON closure_pseudonymisation_state TO chora_tenancy_app_ro;

COMMIT;
