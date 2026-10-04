// bootstrap_outbox_test.go — RED-phase unit tests for the outbox-row
// constructor used by BootstrapRepository.Persist (CHO-1630 Phase 2).
//
// Pure-function tests — no DB. Validates the wire shape of the
// chora.tenancy.tenant.bootstrapped.v1 outbox row that is INSERTed
// atomically with the three bootstrap rows.
package pg

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

func sampleBundle(t *testing.T) *bootstrap.Bundle {
	t.Helper()
	b, err := bootstrap.New(bootstrap.Input{
		Name:      "Outbox Sample Tenant",
		OwnerGCID: "01935f12-0000-7000-8000-0000000000aa",
	})
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}
	return b
}

func TestBuildBootstrapOutboxRow_topicIsCanonical(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	const want = "chora.tenancy.tenant.bootstrapped.v1"
	if row.Topic != want {
		t.Errorf("topic = %q, want %q", row.Topic, want)
	}
	if row.AggregateType != "tenant" {
		t.Errorf("aggregate_type = %q, want %q", row.AggregateType, "tenant")
	}
	if row.EventType != "tenancy.tenant.bootstrapped" {
		t.Errorf("event_type = %q, want %q", row.EventType, "tenancy.tenant.bootstrapped")
	}
}

func TestBuildBootstrapOutboxRow_envelopeMandatoryFields(t *testing.T) {
	b := sampleBundle(t)
	now := time.Date(2026, 6, 1, 14, 0, 0, 0, time.UTC)
	row, err := buildBootstrapOutboxRow(b, now, "00-abc-xyz-01")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}

	env := row.Envelope
	if env == nil {
		t.Fatal("envelope nil")
	}

	for _, k := range []string{
		"event_id", "idempotency_key", "tenant_id", "gcid",
		"occurred_at", "published_at", "traceparent", "tracestate",
		"source_project", "source_service", "schema_version",
	} {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope missing mandatory field %q", k)
		}
	}

	if env["tenant_id"] != b.Tenant.ID {
		t.Errorf("envelope.tenant_id = %q, want %q", env["tenant_id"], b.Tenant.ID)
	}
	if env["gcid"] != b.Tenant.OwnerGCID {
		t.Errorf("envelope.gcid = %q, want %q", env["gcid"], b.Tenant.OwnerGCID)
	}
	if env["source_service"] != "chora-tenancy" {
		t.Errorf("envelope.source_service = %q, want chora-tenancy", env["source_service"])
	}
	if env["source_project"] != "chora-489812" {
		t.Errorf("envelope.source_project = %q, want chora-489812", env["source_project"])
	}
	if env["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %q, want \"1\"", env["schema_version"])
	}
	if env["traceparent"] != "00-abc-xyz-01" {
		t.Errorf("envelope.traceparent = %q, want %q", env["traceparent"], "00-abc-xyz-01")
	}
	if env["occurred_at"] != now.Format(time.RFC3339Nano) {
		t.Errorf("envelope.occurred_at = %q, want %q", env["occurred_at"], now.Format(time.RFC3339Nano))
	}
}

func TestBuildBootstrapOutboxRow_payloadDecodesToProto(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}

	var msg tenancyv1.TenantBootstrapped
	if err := proto.Unmarshal(row.Payload, &msg); err != nil {
		t.Fatalf("payload not a valid TenantBootstrapped proto: %v", err)
	}

	if msg.TenantId != b.Tenant.ID {
		t.Errorf("proto.tenant_id = %q, want %q", msg.TenantId, b.Tenant.ID)
	}
	if msg.OwnerGcid != b.Tenant.OwnerGCID {
		t.Errorf("proto.owner_gcid = %q, want %q", msg.OwnerGcid, b.Tenant.OwnerGCID)
	}
	if msg.DisplayName != b.Tenant.Name {
		t.Errorf("proto.display_name = %q, want %q", msg.DisplayName, b.Tenant.Name)
	}
	if msg.OwnerMemberId != b.OwnerMember.ID {
		t.Errorf("proto.owner_member_id = %q, want %q", msg.OwnerMemberId, b.OwnerMember.ID)
	}
	if msg.EntitlementId != b.Entitlement.ID {
		t.Errorf("proto.entitlement_id = %q, want %q", msg.EntitlementId, b.Entitlement.ID)
	}
	if msg.BootstrappedAt == nil {
		t.Fatal("proto.bootstrapped_at should be set")
	}
	if msg.Envelope == nil {
		t.Fatal("proto.envelope should be set (mandatory field 1)")
	}
	if msg.Envelope.TenantId != b.Tenant.ID {
		t.Errorf("proto.envelope.tenant_id = %q, want %q", msg.Envelope.TenantId, b.Tenant.ID)
	}
}

// TestBuildBootstrapOutboxRow_mintsTraceparentWhenEmpty is the event-fabric
// (2026-07-01) Class-A regression guard. A background flush from a non-HTTP
// context passes an empty traceparent, but the envelope's traceparent is
// mandatory — without it the shared envelope Validate() rejects the row
// pre-publish ("envelope: traceparent is required") and
// chora.tenancy.tenant.bootstrapped.v1 deadletters. The constructor mints a
// synthetic-root traceparent onto BOTH the JSONB envelope attributes and the
// embedded proto envelope when the input is empty.
func TestBuildBootstrapOutboxRow_mintsTraceparentWhenEmpty(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	if row.Envelope["traceparent"] == "" {
		t.Error("envelope.traceparent empty on empty input; want a minted W3C traceparent")
	}

	var msg tenancyv1.TenantBootstrapped
	if err := proto.Unmarshal(row.Payload, &msg); err != nil {
		t.Fatalf("payload not a valid TenantBootstrapped proto: %v", err)
	}
	if msg.GetEnvelope().GetTraceparent() == "" {
		t.Error("payload envelope.traceparent empty on empty input; the embedded proto envelope must also carry the minted traceparent")
	}
}

func TestBuildBootstrapOutboxRow_aggregateIdIsTenantID(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	if row.AggregateID != b.Tenant.ID {
		t.Errorf("aggregate_id = %q, want tenant.ID %q", row.AggregateID, b.Tenant.ID)
	}
}

func TestBuildBootstrapOutboxRow_idempotencyKeyDerivedFromTenantID(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	// The idempotency key MUST be stable for the same Bundle so that a
	// retry of Persist produces the same key. We use the tenant ID since
	// it is freshly minted (UUIDv7) and unique-per-bootstrap.
	if row.IdempotencyKey != "tenant-bootstrapped:"+b.Tenant.ID {
		t.Errorf("idempotency_key = %q, want %q", row.IdempotencyKey, "tenant-bootstrapped:"+b.Tenant.ID)
	}
}

func TestBuildBootstrapOutboxRow_idIsUUIDv7Shape(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	if len(row.ID) != 36 {
		t.Errorf("row.ID length = %d, want 36 (UUIDv7 shape)", len(row.ID))
	}
	if row.ID[14] != '7' {
		t.Errorf("row.ID 13th nibble = %q, want '7'", string(row.ID[14]))
	}
}

func TestBuildBootstrapOutboxRow_tenantIDAndGCIDPopulated(t *testing.T) {
	b := sampleBundle(t)
	row, err := buildBootstrapOutboxRow(b, time.Now().UTC(), "")
	if err != nil {
		t.Fatalf("buildBootstrapOutboxRow: %v", err)
	}
	if row.TenantID != b.Tenant.ID {
		t.Errorf("row.TenantID = %q, want %q", row.TenantID, b.Tenant.ID)
	}
	if row.GCID != b.Tenant.OwnerGCID {
		t.Errorf("row.GCID = %q, want %q", row.GCID, b.Tenant.OwnerGCID)
	}
}

func TestWithTraceparent_RoundTrip(t *testing.T) {
	// WithTraceparent stamps; traceparentFromContext + the bootstrap row
	// builder echo it into the outbox row.
	ctx := WithTraceparent(context.Background(), "00-aa-bb-01")
	if got := traceparentFromContext(ctx); got != "00-aa-bb-01" {
		t.Fatalf("expected stamped traceparent, got %q", got)
	}
	// Empty traceparent → unchanged ctx (+ no key).
	if got := traceparentFromContext(WithTraceparent(ctx, "")); got != "00-aa-bb-01" {
		t.Fatalf("empty traceparent must not clobber, got %q", got)
	}
	// Nil ctx is a no-op.
	if got := traceparentFromContext(nil); got != "" {
		t.Fatalf("nil ctx must yield empty, got %q", got)
	}
}

func TestBuildBootstrapOutboxRow_NilBundle(t *testing.T) {
	if _, err := buildBootstrapOutboxRow(nil, time.Now().UTC(), "00-aa-01"); err == nil {
		t.Fatal("expected error for nil bundle")
	}
}
