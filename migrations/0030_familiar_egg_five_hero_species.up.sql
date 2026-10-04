-- 0030_familiar_egg_five_hero_species.up.sql
--
-- CHO-2032 — reconcile the egg-gacha breed taxonomy to the 5 canonical hero
-- species {owl, fox, dragon, phoenix, penguin}.
--
-- The 0007 egg catalogue seeded an 8-breed gacha (cat/turtle/wolf/raven) that
-- the Grimoire layer in chora-consumption never supported: those breeds have
-- no species Path (species_paths, consumption 0061), no art, and are absent
-- from the sovereign-acquire hero set + the 0046 species DB default. Hatching
-- one produced a familiar that stage-up-errored into an EMPTY Grimoire
-- (verified live: a wolf familiar reached stage 2 with zero skill grants).
--
-- This migration:
--   1. Narrows the breed_distribution validation trigger to the 5 heroes,
--      adding penguin (the 5th hero — already storable/rollable everywhere
--      else) and retiring cat/turtle/wolf/raven.
--   2. Reseeds the three platform egg SKUs to 5-hero distributions (the 0007
--      seed used ON CONFLICT DO NOTHING, so the rows already exist → UPDATE).
--
-- Mirrors growth.CanonicalSpecies + familiar_egg.canonicalSpecies (Go).
-- Additive on the proto side: FAMILIAR_SPECIES_PENGUIN = 9 (v1-safe).

BEGIN;

-- 1. Trigger CHECK → 5 heroes (penguin in; cat/turtle/wolf/raven out).
CREATE OR REPLACE FUNCTION familiar_egg_catalog_breed_distribution_sync()
RETURNS TRIGGER AS $$
DECLARE
    total NUMERIC(6,2) := 0;
    species_key TEXT;
    species_weight NUMERIC(6,2);
BEGIN
    IF NEW.breed_distribution IS DISTINCT FROM OLD.breed_distribution OR TG_OP = 'INSERT' THEN
        FOR species_key, species_weight IN
            SELECT key, (value::TEXT)::NUMERIC(6,2)
            FROM jsonb_each_text(NEW.breed_distribution)
        LOOP
            -- 5 canonical hero species only (CHO-2032).
            IF species_key NOT IN ('owl','fox','dragon','phoenix','penguin') THEN
                RAISE EXCEPTION 'invalid species in breed_distribution: % (heroes: owl|fox|dragon|phoenix|penguin)', species_key;
            END IF;
            IF species_weight < 0 THEN
                RAISE EXCEPTION 'negative weight for species %: %', species_key, species_weight;
            END IF;
            total := total + species_weight;
        END LOOP;
        NEW.breed_distribution_total := total;
        NEW.breed_distribution_updated_at := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Reseed the platform egg SKUs (tenant_id IS NULL) to 5-hero odds.
--    Each sums to 100.0 (the trigger recomputes breed_distribution_total).
--    Platform rows are UPDATE-locked by the tenant_admin_mutation RLS policy
--    (`tenant_id = GUC` is never true for a NULL tenant_id), so a migration
--    toggles NO FORCE while it reseeds them, exactly as 0007 seeded them.
ALTER TABLE familiar_egg_catalog NO FORCE ROW LEVEL SECURITY;

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":20.0,"fox":20.0,"penguin":20.0,"dragon":20.0,"phoenix":20.0}'::jsonb
 WHERE sku = 'egg.trial.v1' AND tenant_id IS NULL;

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":28.0,"fox":27.0,"penguin":25.0,"dragon":12.0,"phoenix":8.0}'::jsonb
 WHERE sku = 'egg.standard.v1' AND tenant_id IS NULL;

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":15.0,"fox":13.0,"penguin":12.0,"dragon":30.0,"phoenix":30.0}'::jsonb
 WHERE sku = 'egg.premium.v1' AND tenant_id IS NULL;

ALTER TABLE familiar_egg_catalog FORCE ROW LEVEL SECURITY;

-- Any tenant-authored SKU still carrying a retired breed is now invalid data;
-- it would fail the trigger on its next breed_distribution UPDATE. Surface it
-- loudly rather than silently leaving a hatch-breaking row.
DO $$
DECLARE
    bad_count INT;
BEGIN
    SELECT count(*) INTO bad_count
    FROM familiar_egg_catalog c,
         jsonb_object_keys(c.breed_distribution) AS k
    WHERE k NOT IN ('owl','fox','dragon','phoenix','penguin')
      AND c.deleted_at IS NULL;
    IF bad_count > 0 THEN
        RAISE WARNING 'CHO-2032: % egg catalogue row(s) still reference a retired breed — reseed them to the 5 heroes', bad_count;
    END IF;
END $$;

COMMIT;
