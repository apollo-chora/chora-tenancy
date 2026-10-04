-- =============================================================================
-- chora-tenancy : 0037_ownership_offers.down.sql
--
-- Reverses 0037. Drops the consent record for the ownership handover.
--
-- ⚠ This is destructive in a way the up is not. Every pending offer is a
-- handover somebody has been asked to answer and has not yet answered, and
-- every terminal row is the record of a handover that happened. Dropping the
-- table discards both. Ownership itself is untouched: it lives in `members`,
-- and an accepted handover has already moved it there.
--
-- The enums are dropped only if nothing else uses them, which is why the drops
-- are ordered table first.
-- =============================================================================

DROP TRIGGER IF EXISTS trg_ownership_offers_updated_at ON ownership_offers;
DROP TABLE IF EXISTS ownership_offers;
DROP TYPE IF EXISTS ownership_offer_status;
DROP TYPE IF EXISTS ownership_offer_initiator;
