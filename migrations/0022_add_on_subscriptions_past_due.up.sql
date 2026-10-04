-- =============================================================================
-- chora-tenancy : 0022_add_on_subscriptions_past_due.up.sql
--
-- Domain  : Tenancy + Billing
-- Author  : Chora Platform Team (Team 3)
-- Stage   : CHO-1776 (Phase 2b of the Stripe Subscription lifecycle story —
--           backs the FE dunning banner on /h/billing).
--
-- Adds the past_due flag to add_on_subscriptions. Driven out-of-band by
-- chora-tenancy's CHO-1775 payments subscriber, which flips the flag
-- true on `chora.payments.tenant_addon_purchase.subscription_payment_failed.v1`
-- and clears it on `.payment_recovered.v1`.
--
-- Backwards-compatible:
--   - Default false; existing rows immediately reflect the "not past due"
--     state with no migration of historic rows.
--   - The flag is independent of status — a row stays ACTIVE during
--     dunning so existing access continues; only the FE banner changes.
-- =============================================================================

BEGIN;

ALTER TABLE add_on_subscriptions
    ADD COLUMN past_due BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN add_on_subscriptions.past_due IS 'Stripe dunning flag. CHO-1776; set by chora.payments.tenant_addon_purchase.subscription_payment_failed.v1 subscriber, cleared by .payment_recovered.v1. Independent of status — a row stays ACTIVE during dunning so existing access continues, only the FE banner changes.';

COMMIT;
