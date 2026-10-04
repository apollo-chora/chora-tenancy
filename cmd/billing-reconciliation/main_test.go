// main_test.go — unit tests for the billing-reconciliation Cloud Run Job
// entrypoint's pure helpers. main() itself needs a full config/env + live
// bus, so coverage targets pickReconciliationDay (the cron-day decision).
package main

import (
	"testing"
	"time"
)

func TestPickReconciliationDay_UnsetDefaultsToYesterday(t *testing.T) {
	t.Setenv("RECONCILIATION_DAY", "")
	now := time.Date(2026, 8, 1, 14, 30, 0, 0, time.UTC)
	got := pickReconciliationDay(now)
	want := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestPickReconciliationDay_ValidEnv(t *testing.T) {
	t.Setenv("RECONCILIATION_DAY", "2026-06-15")
	now := time.Date(2026, 8, 1, 14, 30, 0, 0, time.UTC)
	got := pickReconciliationDay(now)
	want := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestPickReconciliationDay_InvalidEnvFallsBack(t *testing.T) {
	t.Setenv("RECONCILIATION_DAY", "not-a-date")
	now := time.Date(2026, 8, 1, 14, 30, 0, 0, time.UTC)
	got := pickReconciliationDay(now)
	want := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected fallback %v, got %v", want, got)
	}
}
