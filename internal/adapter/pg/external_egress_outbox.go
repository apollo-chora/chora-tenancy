// external_egress_outbox.go — outbox-row constructor for the tenant
// external web-egress entitlement (CHO-2148).
//
// Builds the chora.tenancy.external_egress_policy.updated.v1 row that
// ExternalEgressRepository.Save INSERTs atomically with the policy upsert.
//
// Why a free function rather than the shared outbox.Publisher: the Publisher
// opens its OWN transaction and writes via outbox.Store — that breaks
// atomicity. Here atomicity is the whole point. If the policy row and this
// event can drift apart, a crash between them leaves chora_tenancy saying
// "egress ON" while the chora-observability projection the model-gateway
// actually reads still says OFF — or, worse, leaves the projection ON after
// the tenant opted out. Same reasoning (and same envelope conventions) as
// buildBootstrapOutboxRow in bootstrap_outbox.go.
package pg

import (
	"fmt"
	"strconv"
	"time"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
	"github.com/apollo-chora/chora-tenancy/internal/domain/external_egress"
)

// Canonical constants. Keep aligned with:
//   - chora-contracts/proto/events/tenancy/external_egress_policy.proto
//   - chora-contracts/asyncapi/tenancy/external-egress-policy-updated-v1.yaml
//   - chora-infra/topics/topics.yaml
const (
	TopicExternalEgressPolicyUpdated = "chora.tenancy.external_egress_policy.updated.v1"

	aggregateTypeExternalEgressPolicy = "external_egress_policy"
	eventTypeExternalEgressPolicy     = "tenancy.external_egress_policy.updated"
)

// buildExternalEgressOutboxRow constructs the outbox row for a policy change.
//
// The idempotency key is (tenant, version). Version increments only on a real
// change, so a Save retry after a transient pgx error reproduces the SAME key
// and the dispatcher de-duplicates rather than publishing the decision twice.
func buildExternalEgressOutboxRow(
	next external_egress.Policy,
	prev external_egress.Policy,
	now time.Time,
	traceparent string,
) (outbox.Row, error) {
	if next.TenantID == "" {
		return outbox.Row{}, fmt.Errorf("buildExternalEgressOutboxRow: empty tenant_id")
	}
	if next.UpdatedByGCID == "" {
		return outbox.Row{}, fmt.Errorf("buildExternalEgressOutboxRow: empty updated_by_gcid — an egress change must name its actor")
	}

	// Mirror envelope.Build(): mint a synthetic-root traceparent only when the
	// input is empty (a non-HTTP caller). The envelope's Validate() rejects a
	// blank traceparent pre-publish and the row would deadletter.
	if traceparent == "" {
		traceparent = tracing.EnsureTraceparent("")
	}

	eventID := newOutboxUUIDv7()
	idemKey := "external-egress-policy:" + next.TenantID + ":" + strconv.FormatInt(next.Version, 10)

	envelopeProto := &commonv1.EventEnvelope{
		EventId:        eventID,
		IdempotencyKey: idemKey,
		TenantId:       next.TenantID,
		Gcid:           next.UpdatedByGCID,
		OccurredAt:     timestamppb.New(now),
		PublishedAt:    timestamppb.New(now),
		Traceparent:    traceparent,
		Tracestate:     "",
		SourceProject:  outboxSourceProject,
		SourceService:  outboxSourceService,
		SchemaVersion:  outboxSchemaVersion,
	}

	payloadProto := &tenancyv1.ExternalEgressPolicyUpdated{
		Envelope:                 envelopeProto,
		TenantId:                 next.TenantID,
		EgressEnabled:            next.EgressEnabled,
		DailyCallCeiling:         next.DailyCallCeiling,
		Version:                  next.Version,
		UpdatedByGcid:            next.UpdatedByGCID,
		PreviousEgressEnabled:    prev.EgressEnabled,
		PreviousDailyCallCeiling: prev.DailyCallCeiling,
		UpdatedAt:                timestamppb.New(next.UpdatedAt),
	}

	payloadBytes, err := proto.Marshal(payloadProto)
	if err != nil {
		return outbox.Row{}, fmt.Errorf("buildExternalEgressOutboxRow: marshal payload: %w", err)
	}

	envMap := map[string]string{
		"event_id":        eventID,
		"idempotency_key": idemKey,
		"tenant_id":       next.TenantID,
		"gcid":            next.UpdatedByGCID,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"published_at":    now.Format(time.RFC3339Nano),
		"traceparent":     traceparent,
		"tracestate":      "",
		"source_project":  outboxSourceProject,
		"source_service":  outboxSourceService,
		"schema_version":  strconv.Itoa(int(outboxSchemaVersion)),
	}

	return outbox.Row{
		ID:             eventID,
		TenantID:       next.TenantID,
		GCID:           next.UpdatedByGCID,
		AggregateType:  aggregateTypeExternalEgressPolicy,
		AggregateID:    next.TenantID,
		EventType:      eventTypeExternalEgressPolicy,
		Topic:          TopicExternalEgressPolicyUpdated,
		Payload:        payloadBytes,
		Envelope:       envMap,
		IdempotencyKey: idemKey,
		OccurredAt:     now,
	}, nil
}
