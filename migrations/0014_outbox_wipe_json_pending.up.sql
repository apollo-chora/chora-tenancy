-- 0014_outbox_wipe_json_pending.up.sql
--
-- Outbox payload encoding migration (codebase-wide JSON→binary-protobuf fix).
--
-- Background
-- ----------
-- Pre-fix outbox rows on chora_tenancy.outbox_events held JSON-marshalled
-- payload bytes that a BINARY-contracted topic rejects at publish time with
-- "Invalid binary proto message". The dispatcher
-- retries up to MaxAttempts (default 5) then deadletters; the row never
-- publishes successfully.
--
-- Fix
-- ---
-- internal/adapter/events/protomarshal now emits canonical binary protobuf
-- bytes for every chora.tenancy.* topic that has a registered binary
-- proto contract (BINARY encoding):
--
--   * chora.tenancy.tenant.created.v1
--   * chora.tenancy.addon.activated.v1
--   * chora.tenancy.addon.deactivated.v1
--   * chora.tenancy.addon.deactivation_requested.v1
--   * chora.tenancy.addon.upgraded.v1
--   * chora.tenancy.addon.downgraded.v1
--   * chora.tenancy.addon.usage_recorded.v1
--   * chora.tenancy.familiar_egg.checkout_started.v1
--   * chora.tenancy.familiar_egg.payment_succeeded.v1
--   * chora.tenancy.familiar_egg.payment_failed.v1
--   * chora.tenancy.familiar_egg.refunded.v1
--
-- New rows written after the fix carry binary bytes and publish cleanly.
-- This migration drains pre-fix JSON-payload rows out of the pending queue
-- so the dispatcher stops retrying them; rows that NEVER successfully
-- published (still 'pending') are safe to mark 'failed' — no downstream
-- subscriber ever saw them.
--
-- Replay strategy
-- ---------------
-- We mark-failed rather than translate-and-retry: the producer-side
-- handlers (v1_handlers / v2_handlers / addon_screens_handlers /
-- familiar_egg_handlers / familiar_egg_publisher) are idempotent on
-- envelope.idempotency_key — re-triggering the user flow (or the Stripe
-- webhook replay path for familiar_egg.*) will emit a fresh correctly-
-- encoded outbox row. Translating JSON-decoded fields back into the typed
-- proto would be more error-prone than re-emission, and these topics
-- carry observability + IMDA-D1 evidence semantics that tolerate the gap.
--
-- Familiar Egg note
-- -----------------
-- The chora.tenancy.familiar_egg.* topics had no registered binary contract
-- at migration time. Until they get one, the rows publish as opaque bytes;
-- the encoder is in place so when the contract lands the bytes are
-- immediately valid.
--
-- Idempotent: re-running is a no-op (the WHERE clause matches no rows
-- after the first pass).
UPDATE outbox_events
SET status          = 'failed',
    last_error      = 'codebase-wide outbox protobuf encoding fix #33 — pre-fix JSON-payload row drained',
    last_attempt_at = now()
WHERE status = 'pending'
  AND topic IN (
    'chora.tenancy.tenant.created.v1',
    'chora.tenancy.addon.activated.v1',
    'chora.tenancy.addon.deactivated.v1',
    'chora.tenancy.addon.deactivation_requested.v1',
    'chora.tenancy.addon.upgraded.v1',
    'chora.tenancy.addon.downgraded.v1',
    'chora.tenancy.addon.usage_recorded.v1',
    'chora.tenancy.familiar_egg.checkout_started.v1',
    'chora.tenancy.familiar_egg.payment_succeeded.v1',
    'chora.tenancy.familiar_egg.payment_failed.v1',
    'chora.tenancy.familiar_egg.refunded.v1'
  );
