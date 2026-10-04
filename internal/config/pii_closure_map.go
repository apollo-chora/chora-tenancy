// Package config provides configuration loaders for chora-tenancy.
//
// PIIClosureMap is the canonical loader for config/PII_Closure_Map.yaml,
// the per-domain federated-closure-saga PII tokenisation declaration. Each
// domain (5 core + 6 supporting) owns its own map. Loaded at startup; the
// adapter/events ClosureSubscriber consumes it during Handle().
//
// Source-of-truth:
//   - .claude/skills/account-closure-saga/SKILL.md
//   - .claude/rules/ddd-enforcement.md "Account Closure"
//
// Hexagonal note: this is a CONFIG package — neither domain nor adapter.
// Loaded once at startup and injected into the ClosureSubscriber.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// PIIClosureMap mirrors the canonical PII_Closure_Map.yaml schema.
type PIIClosureMap struct {
	Domain                     string                  `yaml:"domain"`
	Version                    string                  `yaml:"version"`
	FieldsToTokenize           []TableSpec             `yaml:"fields_to_tokenize"`
	RetentionDaysByJurisdiction map[string]int          `yaml:"retention_days_by_jurisdiction"`
	OnCreatorClosure           CreatorClosureSpec      `yaml:"on_creator_closure"`

	// AGIDApplicable is inferred from the domain: only chora_a2a applies
	// (per ddd-enforcement invariant #10 — agents have no lifecycle).
	AGIDApplicable bool `yaml:"-"`
}

// TableSpec declares which columns of a table to tokenise.
type TableSpec struct {
	Table   string       `yaml:"table"`
	Columns []ColumnSpec `yaml:"columns"`
}

// ColumnSpec declares the per-column tokenisation strategy.
type ColumnSpec struct {
	Column   string `yaml:"column"`
	Strategy string `yaml:"strategy"`
	Value    string `yaml:"value"`
}

// CreatorClosureSpec declares behaviour when the closing user is a creator
// of artefacts owned by this domain (e.g., atom author in chora_creation).
type CreatorClosureSpec struct {
	Strategy          string `yaml:"strategy"`
	ShowAuthorshipAs  string `yaml:"show_authorship_as"`
}

// allowedStrategies enumerates the canonical action verbs across all domains.
//
// Per the agent task brief:
//   - pseudonymise / tombstone_string / tombstone_email — replace with deterministic hash or constant
//   - drop                — clear column
//   - hash                — store SHA-256(value + tenant_master_key)
//   - preserve            — leave as-is
//   - tokenize            — replace with random token + DEK-encrypted mapping
//   - encrypt             — encrypt with user DEK (post-shred unrecoverable)
//   - cascade_delete      — hard-delete the row (rare; non-audit)
var allowedStrategies = map[string]struct{}{
	"tombstone_string": {},
	"tombstone_email":  {},
	"drop":             {},
	"hash":             {},
	"preserve":         {},
	"tokenize":         {},
	"encrypt":          {},
	"cascade_delete":   {},
}

// IsAllowedStrategy reports whether s is one of the canonical strategies.
func IsAllowedStrategy(s string) bool {
	_, ok := allowedStrategies[strings.TrimSpace(s)]
	return ok
}

// LoadFromFile reads a PII_Closure_Map.yaml from disk and parses it.
func LoadFromFile(path string) (*PIIClosureMap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return LoadFromBytes(data)
}

// LoadFromBytes parses raw YAML bytes into a PIIClosureMap and validates.
func LoadFromBytes(data []byte) (*PIIClosureMap, error) {
	var m PIIClosureMap
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("config: yaml parse: %w", err)
	}
	if err := validate(&m); err != nil {
		return nil, err
	}
	// AGID applicability is inferred from domain — only chora_a2a domain
	// participates in AGID closure (per ddd-enforcement invariant #10
	// agents have no lifecycle saga; for AGID-targeted closure the A2A
	// domain deactivates the agent + revokes contracts).
	m.AGIDApplicable = (m.Domain == "chora_a2a")
	return &m, nil
}

func validate(m *PIIClosureMap) error {
	if strings.TrimSpace(m.Domain) == "" {
		return errors.New("config: PII_Closure_Map.domain required")
	}
	for _, t := range m.FieldsToTokenize {
		for _, c := range t.Columns {
			if !IsAllowedStrategy(c.Strategy) {
				return fmt.Errorf("config: PII_Closure_Map.fields_to_tokenize[%s.%s] strategy %q not allowed", t.Table, c.Column, c.Strategy)
			}
		}
	}
	return nil
}
