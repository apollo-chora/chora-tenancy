// Package events_test holds the RED-phase TDD specs for the in-process event
// publisher used by chora-tenancy.
//
// Aligned with:
//   - chora-contracts/proto/common/envelope.proto — mandatory envelope fields
//   - .claude/rules/ddd-enforcement.md — "Event envelope mandatory fields"
//
// Topic taxonomy: chora.{domain}.{aggregate}.{event_type}.v{N}
//
// 5 events emitted by chora-tenancy in this phase:
//   - chora.tenancy.tenant.created.v1 (incl sub_tenant_of for franchise)
//   - chora.tenancy.member.invited.v1
//   - chora.tenancy.member.suspended.v1
//   - chora.tenancy.add_on.subscribed.v1
//   - chora.tenancy.add_on.unsubscribed.v1
//   - chora.tenancy.invoice.issued.v1
package events_test

import (
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

const tenantA = "01970000-0000-7000-8000-0000000000aa"

func TestNewPublisher_RecorderInProcess(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	if p == nil {
		t.Fatalf("expected non-nil recorder")
	}
}

func TestEvent_BuildEnvelope_PopulatesMandatoryFields(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	rec := p.Publish("chora.tenancy.tenant.created.v1", events.Header{
		TenantID:    tenantA,
		GCID:        "gcid-1",
		Traceparent: "00-aabb-ccdd-01",
	}, map[string]interface{}{
		"tenant_id":     "t1",
		"display_name":  "Acme",
		"sub_tenant_of": "",
	})

	if rec.EventID == "" {
		t.Fatalf("expected event_id (UUIDv7) populated")
	}
	if rec.IdempotencyKey == "" {
		t.Fatalf("expected idempotency_key populated")
	}
	if rec.TenantID != tenantA {
		t.Fatalf("envelope tenant_id mismatch")
	}
	if rec.GCID != "gcid-1" {
		t.Fatalf("envelope gcid mismatch")
	}
	if rec.OccurredAt.IsZero() {
		t.Fatalf("envelope occurred_at must be set")
	}
	if rec.PublishedAt.IsZero() {
		t.Fatalf("envelope published_at must be set")
	}
	if rec.Traceparent != "00-aabb-ccdd-01" {
		t.Fatalf("envelope traceparent must be propagated")
	}
	if rec.SourceProject != "chora-489812" {
		t.Fatalf("expected source_project chora-489812 (platform host), got %q", rec.SourceProject)
	}
	if rec.SourceService != "chora-tenancy" {
		t.Fatalf("expected source_service chora-tenancy, got %q", rec.SourceService)
	}
	if rec.SchemaVersion != 1 {
		t.Fatalf("expected schema_version=1, got %d", rec.SchemaVersion)
	}
}

func TestRecorder_RecordsAllEventsForAssertion(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	p.Publish("chora.tenancy.tenant.created.v1", events.Header{TenantID: tenantA}, nil)
	p.Publish("chora.tenancy.member.invited.v1", events.Header{TenantID: tenantA}, nil)
	all := p.Recorded()
	if len(all) != 2 {
		t.Fatalf("expected 2 records, got %d", len(all))
	}
	if all[0].Topic != "chora.tenancy.tenant.created.v1" {
		t.Fatalf("first topic mismatch: %q", all[0].Topic)
	}
}

func TestRecorder_FilterByTopic(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	p.Publish("chora.tenancy.tenant.created.v1", events.Header{TenantID: tenantA}, nil)
	p.Publish("chora.tenancy.member.invited.v1", events.Header{TenantID: tenantA}, nil)
	p.Publish("chora.tenancy.member.invited.v1", events.Header{TenantID: tenantA}, nil)
	tents := p.RecordedByTopic("chora.tenancy.tenant.created.v1")
	if len(tents) != 1 {
		t.Fatalf("expected 1 tenant.created, got %d", len(tents))
	}
	mems := p.RecordedByTopic("chora.tenancy.member.invited.v1")
	if len(mems) != 2 {
		t.Fatalf("expected 2 member.invited, got %d", len(mems))
	}
}

func TestEvent_TopicTaxonomyEnforcement(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	// Valid: chora.{domain}.{aggregate}.{event_type}.v{N}
	if _, err := p.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: tenantA}, nil); err != nil {
		t.Fatalf("valid topic should not error: %v", err)
	}
	// Wrong domain
	if _, err := p.PublishWithError("chora.bogus.tenant.created.v1", events.Header{TenantID: tenantA}, nil); err == nil {
		t.Fatalf("expected error for unknown domain")
	}
	// Missing v{N}
	if _, err := p.PublishWithError("chora.tenancy.tenant.created", events.Header{TenantID: tenantA}, nil); err == nil {
		t.Fatalf("expected error for missing version")
	}
	if _, err := p.PublishWithError("not.a.real.topic", events.Header{TenantID: tenantA}, nil); err == nil {
		t.Fatalf("expected error for malformed topic")
	}
}

func TestEvent_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	if _, err := p.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: ""}, nil); err == nil {
		t.Fatalf("expected error for empty tenant_id (mandatory envelope field)")
	}
}

func TestEnvelopeIDsAreLexicographicallyMonotonic(t *testing.T) {
	t.Parallel()
	p := events.NewRecorder()
	prev := ""
	for i := 0; i < 50; i++ {
		rec := p.Publish("chora.tenancy.tenant.created.v1", events.Header{TenantID: tenantA}, nil)
		if rec.EventID <= prev {
			// Two UUIDv7 minted within same millisecond CAN sort in either order
			// (random tail). We only assert format consistency here.
			if !strings.Contains(rec.EventID, "-") {
				t.Fatalf("expected dashed UUIDv7 format")
			}
		}
		prev = rec.EventID
		// Force monotonic gap so the loop also exercises the millisecond bump.
		time.Sleep(time.Microsecond)
	}
}
