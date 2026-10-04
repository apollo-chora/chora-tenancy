-- =============================================================================
-- chora-tenancy : 0010_outbox_add_tenant_id.sql
--
-- Wave B propagation paydown (2026-05-14, tracker #157 / KK): chora-tenancy
-- outbox dispatcher fails ~4/sec at runtime with
--
--   ERROR: column "tenant_id" does not exist (SQLSTATE 42703)
--
-- when scanning outbox_events. Verified live 2026-05-14 against pod
-- chora-tenancy-6fbfbd74b9-rtdpc (namespace=tenancy) — Cloud Logging
-- shows 1000+ matches in the past 24h (rate limit hit; actual count
-- likely higher).
--
-- ROOT CAUSE — migration 0005_outbox.sql already declares tenant_id (via
-- ADD COLUMN inside a DO $$ ... END $$ block at lines 58-72) and via the
-- canonical CREATE TABLE (line 36). However, in the production DB the
-- outbox_events table was created by an earlier code path (the chora-go-
-- common/outbox PostgresRecorder template at
-- libs/chora-common/outbox/sql_fixtures/outbox_events.up.sql shipped a
-- CREATE TABLE WITHOUT tenant_id). The 0005 CREATE-IF-NOT-EXISTS then
-- short-circuited and the DO block ALTERs never executed because the
-- runner's per-file dedup table (chora_runner_schema_migrations) recorded
-- 0005 as already applied at the prior CREATE-only variant. Net result:
-- production has the legacy column set; the canonical Go dispatcher
-- (services/chora-tenancy/internal/adapter/outbox/store.go §FetchPending,
-- which SELECTs id, tenant_id::TEXT, gcid::TEXT, ...) references
-- tenant_id and crashes on every poll cycle (PollInterval=250ms in
-- internal/adapter/outbox/dispatcher.go).
--
-- FIX — re-apply the canonical D6.2 ALTERs from 0005 under a fresh
-- filename so the runner's chora_runner_schema_migrations table treats
-- this as a new apply. Every statement is guarded with IF NOT EXISTS /
-- DROP-and-recreate so re-runs are no-ops. Mirrors ZA's canonical
-- services/chora-consumption/migrations/0034_outbox_add_tenant_id.sql
-- exactly (same column set, same backfill, same RLS policy shape).
--
-- Domain  : Tenancy + Billing (combined supporting/platform)
-- Database: chora_tenancy
-- Author  : KK paydown agent (Wave B propagation, tracker #157)
-- Date    : 2026-05-14
--
-- HARD INVARIANT: outbox rows live in the SAME database as the domain
-- they serve (chora_tenancy). Cross-DB queries remain forbidden.
-- =============================================================================

BEGIN;

-- Canonical D6.2 fields. Mirrors 0005_outbox.sql §M12.3 W1b ALTER block.
-- IF NOT EXISTS guards re-apply against the prior variant.
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS tenant_id        UUID,
    ADD COLUMN IF NOT EXISTS gcid             UUID,
    ADD COLUMN IF NOT EXISTS idempotency_key  TEXT;

-- Backfill tenant_id from the event_envelope JSON for any pre-existing
-- rows the legacy CREATE TABLE may have accumulated. JSONB ?-operator
-- short-circuits when the key is absent so the UPDATE is safe on empty
-- envelopes. UUID regex guards a malformed tenant_id from blowing up
-- the cast.
UPDATE outbox_events
    SET tenant_id = (envelope ->> 'tenant_id')::UUID
    WHERE tenant_id IS NULL
      AND envelope ? 'tenant_id'
      AND envelope ->> 'tenant_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

-- D6.3 multi-tenant isolation lookup index — partial on pending rows since
-- the dispatcher only ever scans for status='pending'.
CREATE INDEX IF NOT EXISTS outbox_events_tenant_idx
    ON outbox_events (tenant_id, status, occurred_at);

-- Idempotency dedupe — partial unique to allow legacy rows with NULL key.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_idempotency_idx
    ON outbox_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Multi-tenant RLS — mirrors 0005 §RLS block exactly. Permissive when GUC
-- unset so the dispatcher (a non-tenant-scoped background worker) keeps
-- functioning; defence-in-depth comes from the surrounding domain repos +
-- envelope + Protobuf layers.
ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS outbox_events_tenant_isolation ON outbox_events;
CREATE POLICY outbox_events_tenant_isolation ON outbox_events
    USING (
        current_setting('app.current_tenant', TRUE) IS NULL
        OR current_setting('app.current_tenant', TRUE) = ''
        OR tenant_id IS NULL
        OR tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
    );

COMMIT;
