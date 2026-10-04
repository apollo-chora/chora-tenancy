// Tests for the runtime entry point of the extract binary —
// env parsing + null-time projection. The cross-DB streaming logic
// (runExtract / extractEggs / extractManaTopUps / validateParity) is
// exercised by the chora-infra migrations-runner integration job;
// these unit tests cover the pure-Go helpers + the env contract.
package main

import (
	"os"
	"testing"
	"time"
)

func TestParseConfig_RequiresSourceDSN(t *testing.T) {
	t.Setenv(envSourceDSN, "")
	t.Setenv(envDestDSN, "postgres://x")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for missing %s", envSourceDSN)
	}
}

func TestParseConfig_RequiresDestDSN(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://x")
	t.Setenv(envDestDSN, "")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for missing %s", envDestDSN)
	}
}

func TestParseConfig_DefaultsToDryRunForward(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://src")
	t.Setenv(envDestDSN, "postgres://dst")
	t.Setenv(envMode, "")
	t.Setenv(envDirection, "")
	t.Setenv(envManaDefaultSKU, "")
	t.Setenv(envParityTolerance, "")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cfg.mode != modeDryRun {
		t.Errorf("default mode wrong: got %q want %q", cfg.mode, modeDryRun)
	}
	if cfg.direction != directionForward {
		t.Errorf("default direction wrong: got %q want %q", cfg.direction, directionForward)
	}
	if cfg.manaDefaultSKU != defaultManaLegacySKU {
		t.Errorf("default mana sku wrong: got %q want %q", cfg.manaDefaultSKU, defaultManaLegacySKU)
	}
	if cfg.parityTolerance != 0 {
		t.Errorf("default parity tolerance wrong: got %d want 0", cfg.parityTolerance)
	}
}

func TestParseConfig_RejectsBogusMode(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://s")
	t.Setenv(envDestDSN, "postgres://d")
	t.Setenv(envMode, "garbage")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for bogus mode")
	}
}

func TestParseConfig_RejectsBogusDirection(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://s")
	t.Setenv(envDestDSN, "postgres://d")
	t.Setenv(envDirection, "sideways")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for bogus direction")
	}
}

func TestParseConfig_RejectsNegativeTolerance(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://s")
	t.Setenv(envDestDSN, "postgres://d")
	t.Setenv(envParityTolerance, "-1")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for negative tolerance")
	}
}

func TestParseConfig_AcceptsToleranceInt(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://s")
	t.Setenv(envDestDSN, "postgres://d")
	t.Setenv(envParityTolerance, "3")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cfg.parityTolerance != 3 {
		t.Errorf("tolerance not parsed: got %d", cfg.parityTolerance)
	}
}

func TestParseConfig_NormalisesCase(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://s")
	t.Setenv(envDestDSN, "postgres://d")
	t.Setenv(envMode, "  APPLY ")
	t.Setenv(envDirection, "REVERSE")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cfg.mode != modeApply {
		t.Errorf("apply mode not normalised: %q", cfg.mode)
	}
	if cfg.direction != directionReverse {
		t.Errorf("direction not normalised: %q", cfg.direction)
	}
}

func TestPgxNullTime_Pointer(t *testing.T) {
	var n pgxNullTime
	if n.AsPointer() != nil {
		t.Fatalf("zero-value pointer should be nil")
	}
	tm := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	if err := n.Scan(tm); err != nil {
		t.Fatalf("scan: %v", err)
	}
	p := n.AsPointer()
	if p == nil || !p.Equal(tm) {
		t.Fatalf("pointer mismatch: got %v", p)
	}
}

func TestPgxNullTime_ScanNil(t *testing.T) {
	var n pgxNullTime
	if err := n.Scan(nil); err != nil {
		t.Fatalf("scan nil: %v", err)
	}
	if n.AsPointer() != nil {
		t.Fatalf("scan(nil) should not mark valid")
	}
}

func TestPgxNullTime_ScanPtr(t *testing.T) {
	var n pgxNullTime
	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := n.Scan(&tm); err != nil {
		t.Fatalf("scan ptr: %v", err)
	}
	if !n.T.Equal(tm) {
		t.Fatalf("ptr scan mismatch")
	}
	var nilP *time.Time
	if err := n.Scan(nilP); err != nil {
		t.Fatalf("scan nil ptr: %v", err)
	}
	if n.AsPointer() != nil {
		t.Fatalf("nil ptr should not mark valid")
	}
}

func TestPgxNullTime_ScanUnsupported(t *testing.T) {
	var n pgxNullTime
	if err := n.Scan(42); err == nil {
		t.Fatalf("expected error for non-time type")
	}
}

// Final sanity — ensure env-driven happy path config returns valid struct.
func TestParseConfig_HappyPath(t *testing.T) {
	t.Setenv(envSourceDSN, "postgres://src")
	t.Setenv(envDestDSN, "postgres://dst")
	t.Setenv(envMode, "apply")
	t.Setenv(envDirection, "forward")
	t.Setenv(envManaDefaultSKU, "mana.tenant_topup.v2")
	t.Setenv(envParityTolerance, "0")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cfg.sourceDSN != "postgres://src" || cfg.destDSN != "postgres://dst" {
		t.Errorf("DSNs not propagated: %+v", cfg)
	}
	if cfg.manaDefaultSKU != "mana.tenant_topup.v2" {
		t.Errorf("override sku not honoured")
	}
	// quick sanity that os.Setenv unmount works
	_ = os.Unsetenv(envParityTolerance)
}
