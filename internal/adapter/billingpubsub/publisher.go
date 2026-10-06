// Package billingpubsub adapts the chora-common event-bus primitive to the
// billing-webhook + reconciliation flows hosted under chora-tenancy.
//
// Ported from services/chora-billing-webhook/internal/adapter/pubsub as part
// of the M12.2 Batch-1 consolidation.
//
//  1. WebhookPublisher — wraps an event-bus publisher with envelope
//     construction for a webhook.Classification (one event in → one chora
//     event out).
//
//  2. ReconciliationEmitter — implements reconciliation.Emitter on top of an
//     event-bus publisher, so reconciliation Job runs publish two governance
//     events (completed, anomaly).
//
// Production wires the chora-common JetStream bus; the in-memory bus is the
// dev/test drop-in and slots in via the same eventbus.Publisher interface.
//
// JSON payload shape: temporary MVP body. Production swaps to Protobuf via
// the chora-contracts gen/go bundle (M11.4 + schema validation).
package billingpubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/reconciliation"
)

// DefaultSourceService is the canonical source_service envelope value used
// when EmitterConfig.SourceService is left empty.
const DefaultSourceService = "chora-tenancy-billing-webhook"

// Bus is the publisher contract this adapter consumes. The chora-common
// JetStream and in-memory buses both satisfy eventbus.Publisher.
type Bus = eventbus.Publisher

// EmitterConfig is the construction-time wiring for both publishers.
type EmitterConfig struct {
	// Bus is the underlying chora-common publisher.
	Bus Bus

	// SourceProject is the platform source_project stamp
	// (e.g. chora). Read from CHORA_SOURCE_PROJECT env in main().
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// DefaultSourceService when empty.
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// ReconciliationEmitter publishes the reconciliation `completed` + `anomaly`
// events. Implements reconciliation.Emitter.
type ReconciliationEmitter struct {
	cfg EmitterConfig
}

// NewReconciliationEmitter constructs an emitter.
func NewReconciliationEmitter(cfg EmitterConfig) *ReconciliationEmitter {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceService == "" {
		cfg.SourceService = DefaultSourceService
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	return &ReconciliationEmitter{cfg: cfg}
}

// EmitCompleted satisfies reconciliation.Emitter.
func (e *ReconciliationEmitter) EmitCompleted(ctx context.Context, r reconciliation.Report) error {
	return e.publishReport(ctx, reconciliation.CompletedTopic, "accountability", r)
}

// EmitAnomaly satisfies reconciliation.Emitter.
func (e *ReconciliationEmitter) EmitAnomaly(ctx context.Context, r reconciliation.Report) error {
	return e.publishReport(ctx, reconciliation.AnomalyTopic, "safety_and_robustness", r)
}

type reconReportPayload struct {
	RunID          string `json:"run_id"`
	Day            string `json:"day"`
	ExpectedCents  int64  `json:"expected_cents"`
	ObservedCents  int64  `json:"observed_cents"`
	DeltaCents     int64  `json:"delta_cents"`
	DeltaBPS       int    `json:"delta_basis_points"`
	ToleranceBPS   int    `json:"tolerance_basis_points"`
	HasDrift       bool   `json:"has_drift"`
	GeneratedAtUTC string `json:"generated_at"`
}

func (e *ReconciliationEmitter) publishReport(ctx context.Context, topic, dim string, r reconciliation.Report) error {
	now := e.cfg.Now()
	env := cgcenvelope.Build(ctx, cgcenvelope.BuildOpts{
		EventType:          topic,
		SchemaVersion:      1,
		SourceProject:      e.cfg.SourceProject,
		SourceService:      e.cfg.SourceService,
		IdempotencyKey:     fmt.Sprintf("recon:%s:%s", strings.TrimSpace(r.RunID), topic),
		ChoraImdaDimension: dim,
		ImdaLifecycleStage: "post_deploy",
		Now:                func() time.Time { return now },
	})
	// Reconciliation events are platform-scoped (no tenant context).
	env.TenantID = "platform"

	payload := reconReportPayload{
		RunID:          r.RunID,
		Day:            r.Day.UTC().Format("2006-01-02"),
		ExpectedCents:  r.ExpectedCents,
		ObservedCents:  r.ObservedCents,
		DeltaCents:     r.DeltaCents,
		DeltaBPS:       r.DeltaBPS(),
		ToleranceBPS:   r.ToleranceBPS,
		HasDrift:       r.HasDrift(),
		GeneratedAtUTC: now.Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("billingpubsub: marshal recon payload: %w", err)
	}
	if err := e.cfg.Bus.Publish(ctx, topic, env, body); err != nil {
		return fmt.Errorf("billingpubsub: publish recon %s: %w", topic, err)
	}
	return nil
}

// Compile-time port checks.
var _ reconciliation.Emitter = (*ReconciliationEmitter)(nil)
