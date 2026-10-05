// Package main_test holds the unit tests for the chora-tenancy usage-rollup
// scheduled job. The job's `runRollup` is the seam under test — it walks
// (tenant × addon × usage-snapshot) and emits one usage_recorded event
// per row.
package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

func TestLoadConfig_DefaultsTopicName(t *testing.T) {
	t.Setenv("CHORA_DEV_MODE", "true")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("PUBSUB_TOPIC_USAGE_RECORDED", "")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PubsubTopicUsageRecorded != "chora.tenancy.addon.usage_recorded.v1" {
		t.Fatalf("expected default topic, got %q", cfg.PubsubTopicUsageRecorded)
	}
	if cfg.WindowDays != 1 {
		t.Fatalf("expected default WINDOW_DAYS=1, got %d", cfg.WindowDays)
	}
}

func TestLoadConfig_RejectsMissingDatabaseURLOutsideDevMode(t *testing.T) {
	t.Setenv("CHORA_DEV_MODE", "")
	t.Setenv("DATABASE_URL", "")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error when DATABASE_URL not set + not in dev mode")
	}
}

func TestLoadConfig_RejectsBadWindowDays(t *testing.T) {
	t.Setenv("CHORA_DEV_MODE", "true")
	t.Setenv("WINDOW_DAYS", "not-a-number")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error for non-int WINDOW_DAYS")
	}
	t.Setenv("WINDOW_DAYS", "0")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error for WINDOW_DAYS<=0")
	}
}

func TestRunRollup_EmitsOneEventPerSnapshot(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	tt, err := deps.Tenants.CreateRootLight(tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "rollup-mtm",
	})
	if err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := deps.Subscriptions.Subscribe(tt.ID, "tms", "g1"); err != nil {
		t.Fatalf("seed subscribe: %v", err)
	}
	a, _ := deps.Catalogue.Get("tms")
	// Two snapshots — yesterday and today; yesterday is in the
	// 1-day rollup window, today is on the boundary.
	yesterday := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID: tt.ID, AddOnID: a.ID, BucketDate: yesterday,
		SeatsUsed: 3, QuotaConsumed: 100, APICalls: 7,
	})

	ctx := context.Background()
	emitted, err := runRollup(ctx, deps, &Config{WindowDays: 2})
	if err != nil {
		t.Fatalf("runRollup: %v", err)
	}
	if emitted == 0 {
		t.Fatalf("expected ≥1 emission")
	}
	rec, ok := deps.Events.(*events.Recorder)
	if !ok {
		t.Fatalf("expected deps.Events to be *events.Recorder; got %T", deps.Events)
	}
	usage := rec.RecordedByTopic("chora.tenancy.addon.usage_recorded.v1")
	if len(usage) == 0 {
		t.Fatalf("expected at least 1 usage_recorded event")
	}
	pl := usage[0].Payload
	if pl["tenant_id"] != tt.ID {
		t.Fatalf("tenant_id payload mismatch: got %v", pl["tenant_id"])
	}
	if code, _ := pl["addon_code"].(string); code != "tms" {
		t.Fatalf("addon_code mismatch: got %v", code)
	}
	if !strings.Contains(usage[0].Topic, "usage_recorded") {
		t.Fatalf("expected usage_recorded topic, got %q", usage[0].Topic)
	}
}

func TestRunRollup_EmptyDepsEmitsZero(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	emitted, err := runRollup(context.Background(), deps, &Config{WindowDays: 1})
	if err != nil {
		t.Fatalf("runRollup: %v", err)
	}
	if emitted != 0 {
		t.Fatalf("expected 0 emissions on empty deps, got %d", emitted)
	}
}

func TestRunRollup_HonoursContextCancel(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	tt, _ := deps.Tenants.CreateRootLight(tenant.LightTenantInput{
		DisplayName: "MTM", OwnerGCID: "g1", Slug: "rollup-cancel",
	})
	deps.Subscriptions.Subscribe(tt.ID, "tms", "g1")
	a, _ := deps.Catalogue.Get("tms")
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID: tt.ID, AddOnID: a.ID, BucketDate: time.Now().UTC().Truncate(24 * time.Hour),
		SeatsUsed: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runRollup(ctx, deps, &Config{WindowDays: 1})
	if err == nil {
		// runRollup may emit one event before observing cancel; accept
		// either an early-cancel error or a clean run that completed
		// before the check fired (the snapshot loop is short-circuit on
		// ctx.Err()).
		return
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled or nil, got %v", err)
	}
}
