-- =============================================================================
-- chora-tenancy : 0008_stripe_webhook_events.sql
--
-- Domain        : Tenancy + Billing (supporting)
-- Database      : chora_tenancy
-- Author        : Iter G.4 PROD-C — Familiar Egg pg wiring (2026-05-13)
-- Architecture  : docs/architecture/adrs/adr-149-familiar-growth-stage-model.md
--
-- Stripe webhook idempotency table. Stripe retries failed-delivery webhooks
-- with the same event.ID; this table is the durable de-dup record so a
-- redelivery cannot double-process (re-publish outbox events, re-mark a
-- purchase paid, re-issue refund credits).
--
-- RLS-DISABLED rationale: this is a GLOBAL operational table — Stripe events
-- are not tenant-scoped at receive time (tenant_id is in the event payload,
-- not the row). The handler routes the event AFTER the de-dup gate; any
-- per-tenant access control happens at the FamiliarEgg purchase-row layer.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- stripe_webhook_events — durable de-dup for Stripe webhook retries
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stripe_webhook_events (
    event_id          TEXT         PRIMARY KEY,   -- Stripe's evt_xxx identifier
    event_type        TEXT         NOT NULL,      -- e.g. checkout.session.completed
    received_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ,                -- NULL == received but not yet processed
    processing_error  TEXT                        -- NULL == no failure recorded
);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_received
    ON stripe_webhook_events (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_unprocessed
    ON stripe_webhook_events (received_at)
    WHERE processed_at IS NULL;

-- RLS intentionally DISABLED — global operational table; not tenant-scoped.
-- (No `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` clause here.)

COMMENT ON TABLE stripe_webhook_events IS
    'ADR-149 Stripe webhook event de-dup. Global operational table (RLS-disabled). Stripe retries with same event.ID must not double-process.';
COMMENT ON COLUMN stripe_webhook_events.event_id IS
    'Stripe event.ID (evt_xxx). UNIQUE — duplicate INSERTs surface as 23505.';
COMMENT ON COLUMN stripe_webhook_events.processed_at IS
    'Set by handler after successful processing. NULL = received but in-flight or failed.';
COMMENT ON COLUMN stripe_webhook_events.processing_error IS
    'Set when the handler returns an error post-receipt. Helps ops debug stuck rows.';

-- -----------------------------------------------------------------------------
-- Grants (re-runnable safe). Mirrors 0007_familiar_egg_purchases.sql pattern.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_tenancy_app') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON stripe_webhook_events TO chora_tenancy_app';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_tenancy_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON stripe_webhook_events TO chora_tenancy_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_tenancy_app_ro') THEN
        EXECUTE 'GRANT SELECT ON stripe_webhook_events TO chora_tenancy_app_ro';
    END IF;
END$$;

COMMIT;
