// Package config_test holds the RED-phase TDD specs for the PII Closure Map
// loader used by chora-tenancy and the federated closure saga.
package config_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/config"
)

func TestLoadPIIClosureMap_FromBytes_ParsesYAML(t *testing.T) {
	t.Parallel()
	yaml := `
domain: chora_tenancy
version: "1.0"
fields_to_tokenize:
  - table: tenant_memberships
    columns:
      - column: invited_by_display_name
        strategy: tombstone_string
        value: "Former member"
retention_days_by_jurisdiction:
  EU: 2557
  default: 2557
on_creator_closure:
  strategy: tokenise_authorship_keep_atom
  show_authorship_as: "Former member"
`
	m, err := config.LoadFromBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Domain != "chora_tenancy" {
		t.Fatalf("domain: %q", m.Domain)
	}
	if m.Version != "1.0" {
		t.Fatalf("version: %q", m.Version)
	}
	if len(m.FieldsToTokenize) != 1 {
		t.Fatalf("expected 1 table; got %d", len(m.FieldsToTokenize))
	}
	if m.FieldsToTokenize[0].Table != "tenant_memberships" {
		t.Fatalf("table: %q", m.FieldsToTokenize[0].Table)
	}
	if len(m.FieldsToTokenize[0].Columns) != 1 {
		t.Fatalf("expected 1 column; got %d", len(m.FieldsToTokenize[0].Columns))
	}
	if m.FieldsToTokenize[0].Columns[0].Column != "invited_by_display_name" {
		t.Fatalf("column name: %q", m.FieldsToTokenize[0].Columns[0].Column)
	}
	if m.FieldsToTokenize[0].Columns[0].Strategy != "tombstone_string" {
		t.Fatalf("strategy: %q", m.FieldsToTokenize[0].Columns[0].Strategy)
	}
	if m.RetentionDaysByJurisdiction["EU"] != 2557 {
		t.Fatalf("EU retention: %d", m.RetentionDaysByJurisdiction["EU"])
	}
	if m.OnCreatorClosure.Strategy != "tokenise_authorship_keep_atom" {
		t.Fatalf("creator closure strategy: %q", m.OnCreatorClosure.Strategy)
	}
}

func TestLoadPIIClosureMap_FromFile_TenancyManifest(t *testing.T) {
	t.Parallel()
	// Smoke test: the actual chora-tenancy/config/PII_Closure_Map.yaml must
	// load cleanly with no validation errors.
	m, err := config.LoadFromFile("../../config/PII_Closure_Map.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Domain != "chora_tenancy" {
		t.Fatalf("expected domain chora_tenancy, got %q", m.Domain)
	}
	if len(m.FieldsToTokenize) == 0 {
		t.Fatalf("expected at least one tokenize entry")
	}
}

func TestLoadPIIClosureMap_RejectsEmptyDomain(t *testing.T) {
	t.Parallel()
	yaml := `
version: "1.0"
fields_to_tokenize: []
`
	_, err := config.LoadFromBytes([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "domain") {
		t.Fatalf("expected domain-required error; got %v", err)
	}
}

func TestLoadPIIClosureMap_RejectsBadStrategy(t *testing.T) {
	t.Parallel()
	yaml := `
domain: chora_tenancy
version: "1.0"
fields_to_tokenize:
  - table: x
    columns:
      - column: y
        strategy: zap_to_void
        value: ""
`
	_, err := config.LoadFromBytes([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "strategy") {
		t.Fatalf("expected strategy validation error; got %v", err)
	}
}

func TestLoadPIIClosureMap_AGIDApplicableInferred(t *testing.T) {
	t.Parallel()
	// Tenancy: AGID NOT applicable (TenantMembership is GCID-only;
	// the saga rejects AGID upstream — but defence-in-depth here too).
	yaml := `
domain: chora_tenancy
version: "1.0"
fields_to_tokenize: []
`
	m, err := config.LoadFromBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// chora-tenancy domain → AGID NOT applicable.
	if m.AGIDApplicable {
		t.Fatalf("AGID should not be applicable for chora_tenancy")
	}
}

func TestLoadPIIClosureMap_AllowedStrategies(t *testing.T) {
	t.Parallel()
	allowed := []string{
		"tombstone_string",
		"tombstone_email",
		"drop",
		"hash",
		"preserve",
		"tokenize",
		"encrypt",
		"cascade_delete",
	}
	for _, s := range allowed {
		if !config.IsAllowedStrategy(s) {
			t.Errorf("strategy %q should be allowed", s)
		}
	}
	if config.IsAllowedStrategy("zap_to_void") {
		t.Errorf("strategy zap_to_void should NOT be allowed")
	}
}

func TestLoadPIIClosureMap_FromFile_MissingFile(t *testing.T) {
	t.Parallel()
	if _, err := config.LoadFromFile("no-such-pii-manifest.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadPIIClosureMap_FromBytes_BadYAML(t *testing.T) {
	t.Parallel()
	if _, err := config.LoadFromBytes([]byte("{{{{not yaml")); err == nil {
		t.Fatal("expected yaml parse error")
	}
}
