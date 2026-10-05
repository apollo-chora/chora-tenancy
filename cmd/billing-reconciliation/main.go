// Package main is the chora-tenancy billing-reconciliation job entrypoint.
//
// Subsumes services/chora-billing-webhook/cmd/reconciliation-job (M12.2
// Batch-1 consolidation). Per locked architecture (CLAUDE.md §1) Tenancy +
// Billing are a single combined supporting domain — daily Stripe vs chora
// event-log reconciliation runs as a scheduled job under chora-tenancy.
//
// Triggered daily by the scheduler with the previous-day date in the
// `RECONCILIATION_DAY` env var (YYYY-MM-DD UTC). When unset, defaults to
// "yesterday UTC".
//
// Pulls Stripe Charges total + chora event-log captured-cents total for
// the day, computes drift, emits:
//
//   - chora.governance.payment_reconciliation.completed.v1   (always)
//   - chora.governance.payment_reconciliation.anomaly.v1     (drift > tolerance)
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/billingpubsub"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/billingstripe"
	"github.com/apollo-chora/chora-tenancy/internal/config"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
	"github.com/apollo-chora/chora-tenancy/internal/observability"
)

const (
	jobName = "chora-tenancy-billing-reconciliation"
	version = "0.3.0"
)

func main() {
	cfg, err := config.LoadBillingReconciliationJob()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	shutdownTrace, err := observability.InitOTLP(jobName, version)
	if err != nil {
		log.Printf("observability init failed (non-fatal): %v", err)
	}
	defer func() {
		if shutdownTrace == nil {
			return
		}
		if err := shutdownTrace(); err != nil {
			log.Printf("trace shutdown error: %v", err)
		}
	}()

	day := pickReconciliationDay(time.Now().UTC())
	log.Printf("recon: day=%s tolerance_bps=%d", day.Format("2006-01-02"), cfg.ToleranceBPS)

	// Bus + emitter (recon writes 2 governance events per run).
	bus := eventbus.NewInMemoryBus()
	defer bus.Close()
	emitter := billingpubsub.NewReconciliationEmitter(billingpubsub.EmitterConfig{
		Bus:           bus,
		SourceProject: cfg.SourceProject,
		SourceService: cfg.SourceService,
	})

	stripe := billingstripe.New(billingstripe.Config{
		APIBase:   cfg.StripeAPIBase,
		SecretKey: cfg.StripeSecretKey,
	})

	// Event-log feeder: subscribes to the bus AND can be pre-loaded from a
	// snapshot. For the job MVP this is a fresh in-memory feeder that only
	// sees events arriving during the run window. Production swaps to a
	// warehouse-backed feeder pulling from the centralised audit ledger.
	feeder := billingpubsub.NewEventLogReplayFeeder()
	defer feeder.Close()
	if err := feeder.Subscribe(ctx, bus); err != nil {
		log.Fatalf("recon: subscribe feeder: %v", err)
	}

	job := reconciliation.NewJob(reconciliation.JobConfig{
		Stripe:       stripe,
		EventLog:     feeder,
		Emitter:      emitter,
		ToleranceBPS: cfg.ToleranceBPS,
	})
	report, err := job.RunDaily(ctx, day)
	if err != nil {
		log.Fatalf("recon: %v", err)
	}
	log.Printf("recon: run_id=%s expected=%d observed=%d delta=%d bps=%d has_drift=%t",
		report.RunID, report.ExpectedCents, report.ObservedCents, report.DeltaCents, report.DeltaBPS(), report.HasDrift())
}

// pickReconciliationDay reads RECONCILIATION_DAY from env (YYYY-MM-DD UTC)
// or falls back to "yesterday UTC" — the standard cron pattern.
func pickReconciliationDay(now time.Time) time.Time {
	v := strings.TrimSpace(os.Getenv("RECONCILIATION_DAY"))
	if v == "" {
		return now.AddDate(0, 0, -1).Truncate(24 * time.Hour)
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		log.Printf("recon: RECONCILIATION_DAY %q invalid; falling back to yesterday", v)
		return now.AddDate(0, 0, -1).Truncate(24 * time.Hour)
	}
	return t.UTC()
}
