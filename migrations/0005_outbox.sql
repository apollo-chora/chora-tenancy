-- =============================================================================
-- chora-tenancy : 0005_outbox.sql
--
-- Adds the transactional outbox primitive to chora_tenancy per the
-- Phyllis Wave-B service-wiring directive (`feedback_resilience_priority`)
-- and the M12.3 Wave-2 D6.2 producer-side durability uplift
-- (`feedback_d6_resilience_first_class` + `agentic-resilience-d6` skill
-- Pillar 2).
--
-- Domain  : Tenancy + Billing (combined supporting/platform domain)
-- Database: chora_tenancy
-- Author  : agent-adc76a416bb263224 (Phyllis A-Service-Wiring-Identity)
--           + M12.3 W2b (D6 columns + RLS + idempotency_key uniqueness)
-- Date    : 2026-05-11 (origin) / 2026-05-12 (D6 uplift)
--
-- HARD INVARIANT: outbox rows live in the SAME database as the domain
-- they serve (chora_tenancy). Cross-DB queries remain forbidden — the
-- background Dispatcher publishes to the event bus and downstream
-- subscribers consume via the event bus.
--
-- D6.2 deltas (2026-05-12):
--   - tenant_id (UUID, top-level + indexed) — D6.3 multi-tenant chaos
--     isolation requires indexable per-row tenant attribution.
--   - gcid (UUID, nullable) — subject attribution.
--   - idempotency_key UNIQUE — producer-side dedupe on the dispatcher
--     poll path. Mirrors chora-creation/chora-guardrail outbox shape.
--   - aggregate_type defaults to 'tenant' for existing rows.
--   - RLS enabled with permissive policy when app.current_tenant GUC
--     is unset (POC + dispatcher path), enforced when GUC is set.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS outbox_events (
    id              TEXT        PRIMARY KEY,
    tenant_id       UUID        NOT NULL,                    -- D6.3 isolation
    gcid            UUID,                                    -- subject (nullable for system events)
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    topic           TEXT        NOT NULL,
    payload         BYTEA       NOT NULL,
    envelope        JSONB       NOT NULL,
    idempotency_key TEXT        NOT NULL,                    -- UNIQUE via index below
    occurred_at     TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ALTER block — for environments where 0005 was applied pre-D6 uplift.
-- The CREATE TABLE above is idempotent (IF NOT EXISTS), so this block
-- back-fills the new D6 columns / constraints on existing tables.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='public' AND table_name='outbox_events'
                   AND column_name='tenant_id') THEN
        ALTER TABLE outbox_events ADD COLUMN tenant_id UUID;
        -- back-fill from envelope if any pre-D6 rows exist (none expected
        -- in the build-out phase but keep idempotent semantics).
        UPDATE outbox_events
           SET tenant_id = (envelope->>'tenant_id')::UUID
         WHERE tenant_id IS NULL AND envelope ? 'tenant_id';
        -- enforce NOT NULL only when the table has zero ungated rows.
        IF NOT EXISTS (SELECT 1 FROM outbox_events WHERE tenant_id IS NULL) THEN
            ALTER TABLE outbox_events ALTER COLUMN tenant_id SET NOT NULL;
        END IF;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='public' AND table_name='outbox_events'
                   AND column_name='gcid') THEN
        ALTER TABLE outbox_events ADD COLUMN gcid UUID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='public' AND table_name='outbox_events'
                   AND column_name='idempotency_key') THEN
        ALTER TABLE outbox_events ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';
    END IF;
END $$;

-- Dispatcher poll query — find next pending event by occurred_at.
CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (occurred_at ASC) WHERE status = 'pending';

-- D6.3 multi-tenant isolation lookup: dispatcher MAY filter per-tenant.
CREATE INDEX IF NOT EXISTS outbox_events_tenant_idx
    ON outbox_events (tenant_id, status, occurred_at);

-- Per-aggregate read.
CREATE INDEX IF NOT EXISTS outbox_events_aggregate_idx
    ON outbox_events (aggregate_type, aggregate_id, occurred_at DESC);

-- Per-topic dispatcher worker mode.
CREATE INDEX IF NOT EXISTS outbox_events_topic_idx
    ON outbox_events (topic, status);

-- Idempotency dedupe — re-emission of the same business event collapses
-- on this unique index.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_idempotency_idx
    ON outbox_events (idempotency_key);

CREATE TABLE IF NOT EXISTS outbox_poll_checkpoints (
    worker_id                  TEXT        NOT NULL,
    topic                      TEXT        NOT NULL,
    last_processed_outbox_id   TEXT        NOT NULL,
    last_processed_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker_id, topic)
);

CREATE INDEX IF NOT EXISTS outbox_poll_checkpoints_topic_idx
    ON outbox_poll_checkpoints (topic, updated_at DESC);

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    outbox_event_id   TEXT        PRIMARY KEY REFERENCES outbox_events(id),
    failure_reason    TEXT        NOT NULL,
    attempt_count     INT         NOT NULL,
    worker_id         TEXT        NOT NULL,
    deadlettered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    resolution_note   TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS outbox_dead_letters_unresolved_idx
    ON outbox_dead_letters (deadlettered_at DESC) WHERE resolved_at IS NULL;

-- ----------------------------------------------------------------------------
-- D6.3 RLS — multi-tenant isolation indexed on tenant_id.
--
-- chora-tenancy outbox rows are inherently multi-tenant (tenant lifecycle
-- + addon lifecycle + Stripe webhook fan-out all carry tenant_id).
-- Production wires the GUC via per-connection SET LOCAL app.current_tenant.
-- The policy remains permissive when the GUC is unset to keep the
-- dispatcher loop functional (the dispatcher runs as a background worker
-- without per-request tenant context). Application-level tenant
-- enforcement is preserved at the events.Recorder.PublishWithError seam
-- (which validates `tenant_id` on every emit).
-- ----------------------------------------------------------------------------
ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS outbox_events_tenant_isolation ON outbox_events;
CREATE POLICY outbox_events_tenant_isolation ON outbox_events
    USING (
        current_setting('app.current_tenant', TRUE) IS NULL
        OR current_setting('app.current_tenant', TRUE) = ''
        OR tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
    );

-- idempotency_keys — multi-pod webhook + cross-domain command dedupe.
-- Schema matches chora-common/idempotent.PostgresStore (Mark/Seen/Process)
-- so chora-billing-webhook can wire the production store directly.
-- (Retained for legacy harnesses; the canonical version lives in
-- 0006_idempotency_keys.up.sql with the stored procedure.)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT        PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_at        TIMESTAMPTZ NOT NULL,
    result_hash   TEXT        NULL
);

CREATE INDEX IF NOT EXISTS idempotency_keys_ttl_idx
    ON idempotency_keys (ttl_at);

COMMIT;
