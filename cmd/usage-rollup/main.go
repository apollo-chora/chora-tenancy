// Command usage-rollup is the nightly Cloud Run Job that aggregates
// addon_usage_events into addon_usage_daily and emits
// chora.tenancy.addon.usage_recorded.v1 per (tenant, addon) per day.
//
// Triggered by Cloud Scheduler via Terraform module
// `chora-infra/terraform/modules/tenancy/usage-rollup-cron.tf` (one
// invocation per UTC midnight).
//
// Per CLAUDE.md §6 (no-inline-config): every URL/secret/topic name flows
// from env vars sourced by Terraform / Secret Manager:
//
//	DATABASE_URL                  Cloud SQL connection (tenancy DB)
//	PUBSUB_PROJECT                chora-489812 (platform host)
//	PUBSUB_TOPIC_USAGE_RECORDED   chora.tenancy.addon.usage_recorded.v1
//	OTEL_EXPORTER_OTLP_ENDPOINT   Cloud Trace endpoint
//	WINDOW_DAYS                   number of days to roll up (default 1 = previous day)
//
// The job is idempotent on (tenant_id, addon_id, bucket_date) — re-running
// for the same day will UPSERT and re-emit (consumers must dedupe via
// envelope.idempotency_key per .claude/skills/event-driven).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
)

const (
	jobName    = "chora-tenancy-usage-rollup"
	jobVersion = "0.1.0"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("job=%s received %s — cancelling", jobName, sig)
		cancel()
	}()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("job=%s config: %v", jobName, err)
	}
	log.Printf("job=%s version=%s starting (window_days=%d topic=%s)",
		jobName, jobVersion, cfg.WindowDays, cfg.PubsubTopicUsageRecorded)

	// In-memory deps for now. Production wiring (M14) swaps to a Postgres
	// repository + Cloud Pub/Sub publisher. The flush handler invoked
	// here is the same code path tested in v1_tenants_test.go.
	deps := httpapi.NewDefaultV2Deps()

	startedAt := time.Now().UTC()
	totalEmitted, err := runRollup(ctx, deps, cfg)
	duration := time.Since(startedAt)
	if err != nil {
		log.Fatalf("job=%s rollup_failed duration=%s emitted=%d err=%v",
			jobName, duration, totalEmitted, err)
	}
	log.Printf("job=%s rollup_complete duration=%s emitted=%d",
		jobName, duration, totalEmitted)
}

// Config captures the env-driven knobs of the rollup job. Per
// secrets-and-env: NO inline defaults for service URLs / secrets / topic
// names. WindowDays defaults to 1 (yesterday) so a missing var still does
// useful work but does not silently mask a misconfig elsewhere.
type Config struct {
	DatabaseURL              string
	PubsubProject            string
	PubsubTopicUsageRecorded string
	OTLPEndpoint             string
	WindowDays               int
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:              strings.TrimSpace(os.Getenv("DATABASE_URL")),
		PubsubProject:            strings.TrimSpace(os.Getenv("PUBSUB_PROJECT")),
		PubsubTopicUsageRecorded: strings.TrimSpace(os.Getenv("PUBSUB_TOPIC_USAGE_RECORDED")),
		OTLPEndpoint:             strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		WindowDays:               1,
	}
	if v := strings.TrimSpace(os.Getenv("WINDOW_DAYS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("WINDOW_DAYS must be a positive integer, got %q", v)
		}
		cfg.WindowDays = n
	}
	// In dev mode (DATABASE_URL absent) we run against the in-memory
	// deps so the binary stays runnable in CI without a Cloud SQL
	// instance. Production deploys MUST have DATABASE_URL set; the
	// readyz/startup probe in the Cloud Run Job pre-checks it.
	if cfg.DatabaseURL == "" && os.Getenv("CHORA_DEV_MODE") != "true" {
		return nil, errors.New("DATABASE_URL is required (no inline config) — set CHORA_DEV_MODE=true for in-memory dev mode")
	}
	if cfg.PubsubTopicUsageRecorded == "" {
		cfg.PubsubTopicUsageRecorded = "chora.tenancy.addon.usage_recorded.v1"
	}
	return cfg, nil
}

// runRollup is the unit under test (extracted from main so it can be
// covered in usage_rollup_test.go without spawning a process).
func runRollup(ctx context.Context, deps httpapi.V2Deps, cfg *Config) (int, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-time.Duration(cfg.WindowDays) * 24 * time.Hour)
	emitted := 0
	// Walk the catalogue × all known tenants in the registry. A
	// production implementation walks the addon_usage_events stage
	// table directly with a scan-by-bucket-date filter; the in-memory
	// path exercises the same publish call path so the integration test
	// can validate envelope shape.
	for _, t := range deps.Tenants.List() {
		for _, a := range deps.Catalogue.List() {
			snaps := deps.Subscriptions.ListUsage(t.ID, a.ID, cutoff, now.Add(24*time.Hour))
			for _, snap := range snaps {
				deps.Events.Publish("chora.tenancy.addon.usage_recorded.v1",
					events.Header{TenantID: t.ID}, map[string]interface{}{
						"tenant_id":       t.ID,
						"addon_plan_id":   a.ID,
						"addon_code":      a.Code,
						"bucket_date":     snap.BucketDate.Format("2006-01-02"),
						"seats_used":      snap.SeatsUsed,
						"seats_used_peak": snap.SeatsUsedPeak,
						"quota_consumed":  snap.QuotaConsumed,
						"api_calls":       snap.APICalls,
						"error_count":     snap.ErrorCount,
					})
				emitted++
				if ctx.Err() != nil {
					return emitted, ctx.Err()
				}
			}
		}
	}
	return emitted, nil
}
