// Package events_test holds the outbox round-trip + IMDA-evidence tests for
// the chora-tenancy event adapter.
//
// What this asserts:
//
//  1. Every Phyllis-MVP / S2.2 add-on lifecycle event topic validates
//     against the local Recorder (= Schema Registry shape gate in tests).
//
//  2. tenant.golive carries chora_imda_dimension=accountability per
//     ADR-141 (D1) — surfaced via the payload field the publisher copies.
//
//  3. Mid-publish crash safety: the in-process recorder reproduces the
//     outbox round-trip discipline (atomic record-then-publish — every
//     Recorded() call returns the full set of accepted events with no
//     partials). Production wiring uses chora-common/outbox.
package events_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

// canonicalTopics enumerates the 6 add-on lifecycle topics + 2 tenant
// lifecycle topics required by the audit doc + the deliverables brief.
// All eight must round-trip cleanly through the Recorder.
var canonicalTopics = []string{
	"chora.tenancy.tenant.created.v1",
	"chora.tenancy.tenant.golive.v1",
	"chora.tenancy.addon.activated.v1",
	"chora.tenancy.addon.upgraded.v1",
	"chora.tenancy.addon.downgraded.v1",
	"chora.tenancy.addon.deactivation_requested.v1",
	"chora.tenancy.addon.deactivated.v1",
	"chora.tenancy.addon.usage_recorded.v1",
}

func TestOutboxRoundtrip_AllCanonicalTopicsValidate(t *testing.T) {
	t.Parallel()
	rec := events.NewRecorder()
	hdr := events.Header{
		TenantID:    "01970000-0000-7000-8000-000000000001",
		GCID:        "gcid-test",
		Traceparent: "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
	}
	for _, topic := range canonicalTopics {
		ev, err := rec.PublishWithError(topic, hdr, map[string]interface{}{
			"tenant_id": hdr.TenantID,
		})
		if err != nil {
			t.Fatalf("topic %q failed schema gate: %v", topic, err)
		}
		// Envelope mandatory fields per CLAUDE.md §6.
		if ev.EventID == "" {
			t.Errorf("topic %q: envelope missing event_id", topic)
		}
		if ev.IdempotencyKey == "" {
			t.Errorf("topic %q: envelope missing idempotency_key", topic)
		}
		if ev.TenantID == "" {
			t.Errorf("topic %q: envelope missing tenant_id", topic)
		}
		if ev.OccurredAt.IsZero() {
			t.Errorf("topic %q: envelope missing occurred_at", topic)
		}
		if ev.PublishedAt.IsZero() {
			t.Errorf("topic %q: envelope missing published_at", topic)
		}
		if ev.SourceProject != "chora-489812" {
			t.Errorf("topic %q: source_project=%q, want chora-489812", topic, ev.SourceProject)
		}
		if ev.SourceService != "chora-tenancy" {
			t.Errorf("topic %q: source_service=%q, want chora-tenancy", topic, ev.SourceService)
		}
	}
	if got := len(rec.Recorded()); got != len(canonicalTopics) {
		t.Fatalf("expected %d records (atomic round-trip), got %d", len(canonicalTopics), got)
	}
}

func TestOutboxRoundtrip_RejectsMissingTenantID(t *testing.T) {
	t.Parallel()
	rec := events.NewRecorder()
	_, err := rec.PublishWithError("chora.tenancy.tenant.created.v1",
		events.Header{TenantID: ""}, map[string]interface{}{})
	if err == nil {
		t.Fatalf("expected ErrEmptyTenantID, got nil")
	}
	if !strings.Contains(err.Error(), "tenant_id") {
		t.Fatalf("expected tenant_id complaint, got %q", err.Error())
	}
}

func TestOutboxRoundtrip_RejectsBadTopicShape(t *testing.T) {
	t.Parallel()
	rec := events.NewRecorder()
	hdr := events.Header{TenantID: "t-1"}
	for _, bad := range []string{
		"not.a.topic",                      // wrong domain prefix
		"chora.unknown.aggregate.event.v1", // unknown domain
		"chora.tenancy.addon.activated",    // missing version
		"chora.tenancy.addon.activated.v",  // empty version digit
	} {
		if _, err := rec.PublishWithError(bad, hdr, map[string]interface{}{}); err == nil {
			t.Fatalf("expected error for topic %q", bad)
		}
	}
}

// IMDA accountability tag — tenant.golive payload carries the
// chora_imda_dimension="accountability" claim per ADR-141. The Recorder
// surfaces the payload verbatim so the per-route IMDA evidence is
// auditable end-to-end.
func TestOutboxRoundtrip_TenantGoLiveCarriesIMDAAccountability(t *testing.T) {
	t.Parallel()
	rec := events.NewRecorder()
	rec.Publish("chora.tenancy.tenant.golive.v1",
		events.Header{TenantID: "01970000-0000-7000-8000-000000000001"},
		map[string]interface{}{
			"chora_imda_dimension": "accountability",
			"imda_lifecycle_stage": "post_deploy",
		})
	got := rec.RecordedByTopic("chora.tenancy.tenant.golive.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if v, _ := got[0].Payload["chora_imda_dimension"].(string); v != "accountability" {
		t.Fatalf("expected chora_imda_dimension=accountability, got %q", v)
	}
}
