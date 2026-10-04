BEGIN;
ALTER TABLE add_on_subscriptions DROP COLUMN past_due;
COMMIT;
