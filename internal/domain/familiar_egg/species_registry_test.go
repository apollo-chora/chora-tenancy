// Species-registry drift guards for the egg catalogue (CHO-2037).
//
// Tenancy owns the egg SKUs whose breed_distribution the gacha rolls from,
// but consumption owns the species canon — the two drifted once already
// (8-breed egg catalogue vs 5-hero Grimoire, CHO-2032) and hatched familiars
// with dead Grimoires. These guards pin BOTH tenancy copies (the Go
// validator and the 0030 SQL trigger + platform SKU reseeds) to
// chora-contracts/companion/species_registry.json.
//
// In-package: canonicalSpecies is deliberately unexported.
package familiar_egg

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"testing"
)

type registrySpecies struct {
	Key       string `json:"key"`
	WireValue int32  `json:"wire_value"`
	Status    string `json:"status"`
}

type speciesRegistryDoc struct {
	SchemaVersion int               `json:"schema_version"`
	Species       []registrySpecies `json:"species"`
}

func loadSpeciesRegistry(t *testing.T) speciesRegistryDoc {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("species registry: runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	for {
		candidate := filepath.Join(dir, "chora-contracts", "companion", "species_registry.json")
		if raw, err := os.ReadFile(candidate); err == nil {
			var doc speciesRegistryDoc
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("species registry %s: malformed JSON: %v", candidate, err)
			}
			if len(doc.Species) == 0 {
				t.Fatalf("species registry %s: empty species list", candidate)
			}
			return doc
		}
		// Stop AT the monorepo root (the directory holding go.work). Without
		// this the walk climbs past a git worktree into whatever checkout
		// contains it, reads THAT tree's copy and reports green on a file this
		// tree does not have: exactly how the missed half of the ADR-254 D9
		// rename hid, because the parent checkout still had the old path.
		if _, gerr := os.Stat(filepath.Join(dir, "go.work")); gerr == nil {
			t.Fatalf("%s not found at the monorepo root %s", candidate, dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("chora-contracts/companion/species_registry.json not found walking up from the test dir: the canonical species roster is missing (see docs/references/companion-species-onboarding.md)")
		}
		dir = parent
	}
}

func registryHeroSet(doc speciesRegistryDoc) map[string]bool {
	out := map[string]bool{}
	for _, s := range doc.Species {
		if s.Status == "hero" {
			out[s.Key] = true
		}
	}
	return out
}

func diffSpeciesSets(got, want map[string]bool) (missing, extra []string) {
	for k := range want {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	for k := range got {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func TestSpeciesRegistry_EggValidatorMatchesHeroes(t *testing.T) {
	heroes := registryHeroSet(loadSpeciesRegistry(t))
	got := map[string]bool{}
	for k := range canonicalSpecies {
		got[k] = true
	}
	missing, extra := diffSpeciesSets(got, heroes)
	if len(missing) > 0 || len(extra) > 0 {
		t.Errorf("familiar_egg.canonicalSpecies drifted from species registry heroes: missing %v, extra %v; follow docs/references/companion-species-onboarding.md", missing, extra)
	}
}

// eggSpeciesMigration pins the LATEST tenancy migration that hardcodes the
// hero roster (breed_distribution trigger + platform SKU reseeds). A new
// species needs a NEW migration re-issuing the trigger + reseeds — and a bump
// here; a stale entry fails because the old file's roster no longer matches.
const eggSpeciesMigration = "0030_familiar_egg_five_hero_species.up.sql"

func readEggMigration(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations", eggSpeciesMigration)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("eggSpeciesMigration points at %s but it cannot be read (%v) — update the manifest to the latest species-bearing migration", eggSpeciesMigration, err)
	}
	return string(raw)
}

var (
	notInListRe    = regexp.MustCompile(`NOT IN \(([^)]+)\)`)
	quotedRe       = regexp.MustCompile(`'([a-z_]+)'`)
	distributionRe = regexp.MustCompile(`SET breed_distribution = '(\{[^']+\})'::jsonb`)
)

func TestSpeciesRegistry_Migration0030MatchesHeroes(t *testing.T) {
	heroes := registryHeroSet(loadSpeciesRegistry(t))
	sql := readEggMigration(t)

	// Trigger CHECK + the bad-row audit both enumerate the roster via
	// `NOT IN (...)` — every occurrence must equal the hero set.
	lists := notInListRe.FindAllStringSubmatch(sql, -1)
	if len(lists) < 2 {
		t.Fatalf("%s: expected >=2 `NOT IN (...)` species lists (trigger + audit), found %d — SQL moved; update this guard + the manifest", eggSpeciesMigration, len(lists))
	}
	for i, m := range lists {
		got := map[string]bool{}
		for _, q := range quotedRe.FindAllStringSubmatch(m[1], -1) {
			got[q[1]] = true
		}
		missing, extra := diffSpeciesSets(got, heroes)
		if len(missing) > 0 || len(extra) > 0 {
			t.Errorf("%s: NOT IN list #%d drifted from registry heroes: missing %v, extra %v", eggSpeciesMigration, i+1, missing, extra)
		}
	}

	// Platform SKU reseeds: every breed_distribution key must be a hero
	// (a non-hero key would hatch a familiar with no Path — dead Grimoire).
	dists := distributionRe.FindAllStringSubmatch(sql, -1)
	if len(dists) < 3 {
		t.Fatalf("%s: expected >=3 platform SKU breed_distribution reseeds, found %d — SQL moved; update this guard + the manifest", eggSpeciesMigration, len(dists))
	}
	for i, m := range dists {
		var dist map[string]float64
		if err := json.Unmarshal([]byte(m[1]), &dist); err != nil {
			t.Fatalf("%s: SKU reseed #%d is not valid JSON: %v", eggSpeciesMigration, i+1, err)
		}
		var offRoster []string
		for k := range dist {
			if !heroes[k] {
				offRoster = append(offRoster, k)
			}
		}
		sort.Strings(offRoster)
		if len(offRoster) > 0 {
			t.Errorf("%s: SKU reseed #%d rolls non-hero species %v; follow docs/references/companion-species-onboarding.md", eggSpeciesMigration, i+1, offRoster)
		}
	}
}
