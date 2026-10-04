-- 0030_familiar_egg_five_hero_species.down.sql
--
-- Revert the breed-distribution validation trigger + platform egg seeds to the
-- legacy 8-breed gacha taxonomy (0007 state). NOTE: this re-permits breeds
-- (cat/turtle/wolf/raven) that have no species Path/art — a familiar hatched
-- from a rolled non-hero breed will stage-up into an empty Grimoire again.

BEGIN;

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
            IF species_key NOT IN ('owl','fox','cat','dragon','phoenix','turtle','wolf','raven') THEN
                RAISE EXCEPTION 'invalid species in breed_distribution: %', species_key;
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

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":12.5,"fox":12.5,"cat":12.5,"dragon":12.5,"phoenix":12.5,"turtle":12.5,"wolf":12.5,"raven":12.5}'::jsonb
 WHERE sku = 'egg.trial.v1' AND tenant_id IS NULL;

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":25.0,"fox":20.0,"cat":18.0,"dragon":12.0,"phoenix":8.0,"turtle":7.0,"wolf":6.0,"raven":4.0}'::jsonb
 WHERE sku = 'egg.standard.v1' AND tenant_id IS NULL;

UPDATE familiar_egg_catalog
   SET breed_distribution = '{"owl":10.0,"fox":10.0,"cat":10.0,"dragon":22.0,"phoenix":22.0,"turtle":8.0,"wolf":10.0,"raven":8.0}'::jsonb
 WHERE sku = 'egg.premium.v1' AND tenant_id IS NULL;

COMMIT;
