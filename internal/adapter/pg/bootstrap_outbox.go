// bootstrap_outbox.go — outbox-row constructor for the H+ Setup-Tenant
// Phase 2 producer (CHO-1630).
//
// Builds the chora.tenancy.tenant.bootstrapped.v1 outbox row that
// BootstrapRepository.Persist INSERTs atomically with the three
// bootstrap rows. Pure-function — no DB, no time injection in caller.
//
// Why a free function: the bootstrap Persist transaction needs to emit
// the outbox row inside its own tx (atomicity). The shared
// outbox.Publisher opens its own context and writes via outbox.Store —
// that breaks atomicity. So we duplicate the envelope + payload shape
// here, deliberately keeping it in lockstep with
// internal/adapter/outbox/publisher.go (same source_project /
// source_service / schema_version conventions).
package pg

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"time"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"

	"github.com/apollo-chora/chora-common/env"
)

// newOutboxUUIDv7 mints a UUIDv7 string for the outbox row primary key
// + event_id. Mirrors the same generator outbox.Publisher uses.
func newOutboxUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Outbox-row canonical constants. Keep aligned with the AsyncAPI spec at
// chora-contracts/asyncapi/tenancy/tenant-bootstrapped-v1.yaml and the
// Protobuf at chora-contracts/proto/events/tenancy/tenant_bootstrapped.proto.
const (
	TopicTenantBootstrapped      = "chora.tenancy.tenant.bootstrapped.v1"
	aggregateTypeTenantBootstrap = "tenant"
	eventTypeTenantBootstrap     = "tenancy.tenant.bootstrapped"

	outboxSourceService = "chora-tenancy"
	outboxSchemaVersion = int32(1)
)

// Outbox-row canonical constants. Keep aligned with the AsyncAPI spec at
// chora-contracts/asyncapi/tenancy/tenant-bootstrapped-v1.yaml and the
// Protobuf at chora-contracts/proto/events/tenancy/tenant_bootstrapped.proto.
//
// Was a hardcoded literal until 2026-09-02 (CHO-2419). No manifest could reach
// it, so a second org stamped every event with chora-489812 and any consumer
// filtering on source_project would have been filtering on a lie.
// CHORA_SOURCE_PROJECT is now set on every event-emitting service; the literal
// stays as the fallback so this estate is provably unchanged.
var outboxSourceProject = env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812")

// buildBootstrapOutboxRow constructs the outbox row for the
// chora.tenancy.tenant.bootstrapped.v1 event. A non-empty `traceparent`
// (from the inbound HTTP request via the handler's r.Context) is propagated
// unchanged; an empty `traceparent` (a background flush from a non-HTTP
// context) is replaced with a freshly-minted synthetic-root traceparent so
// the mandatory envelope field is always populated — else the shared envelope
// Validate() rejects the row pre-publish and it deadletters.
//
// The idempotency key is stable for the same Bundle so that a Persist
// retry (e.g. transient pgx error before COMMIT) produces the same key
// and the dispatcher de-duplicates the event downstream.
func buildBootstrapOutboxRow(b *bootstrap.Bundle, now time.Time, traceparent string) (outbox.Row, error) {
	if b == nil {
		return outbox.Row{}, fmt.Errorf("buildBootstrapOutboxRow: nil bundle")
	}

	// Event-fabric (2026-07-01) Class-A fix: mirror envelope.Build() — mint a
	// synthetic-root traceparent only when the input is empty (non-HTTP flush).
	if traceparent == "" {
		traceparent = tracing.EnsureTraceparent("")
	}

	eventID := newOutboxUUIDv7()
	idemKey := "tenant-bootstrapped:" + b.Tenant.ID

	// 1. Build the canonical EventEnvelope proto. The mandatory fields
	// list comes from CLAUDE.md §6 ("Event envelope mandatory fields").
	envelopeProto := &commonv1.EventEnvelope{
		EventId:        eventID,
		IdempotencyKey: idemKey,
		TenantId:       b.Tenant.ID,
		Gcid:           b.Tenant.OwnerGCID,
		OccurredAt:     timestamppb.New(now),
		PublishedAt:    timestamppb.New(now),
		Traceparent:    traceparent,
		Tracestate:     "",
		SourceProject:  outboxSourceProject,
		SourceService:  outboxSourceService,
		SchemaVersion:  outboxSchemaVersion,
	}

	// 2. Build the per-aggregate proto payload.
	payloadProto := &tenancyv1.TenantBootstrapped{
		Envelope:       envelopeProto,
		TenantId:       b.Tenant.ID,
		OwnerGcid:      b.Tenant.OwnerGCID,
		DisplayName:    b.Tenant.Name,
		OwnerMemberId:  b.OwnerMember.ID,
		EntitlementId:  b.Entitlement.ID,
		BootstrappedAt: timestamppb.New(b.CreatedAt),
	}

	payloadBytes, err := proto.Marshal(payloadProto)
	if err != nil {
		return outbox.Row{}, fmt.Errorf("buildBootstrapOutboxRow: marshal payload: %w", err)
	}

	// 3. Build the JSONB envelope attribute set. Mirrors the
	// outbox.Publisher envMap shape so subscribers see identical
	// attribute keys whether the row is emitted from here or from the
	// shared publisher.
	envMap := map[string]string{
		"event_id":        eventID,
		"idempotency_key": idemKey,
		"tenant_id":       b.Tenant.ID,
		"gcid":            b.Tenant.OwnerGCID,
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
		TenantID:       b.Tenant.ID,
		GCID:           b.Tenant.OwnerGCID,
		AggregateType:  aggregateTypeTenantBootstrap,
		AggregateID:    b.Tenant.ID,
		EventType:      eventTypeTenantBootstrap,
		Topic:          TopicTenantBootstrapped,
		Payload:        payloadBytes,
		Envelope:       envMap,
		IdempotencyKey: idemKey,
		OccurredAt:     now,
	}, nil
}
