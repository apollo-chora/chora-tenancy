// events_extra_coverage_test.go — branch coverage for the events package
// surface the existing tests leave sparse: the CloudPublisher wrapper +
// JSON-fallback path, topic validation error families and event-type
// derivation. Uses an in-memory chora-common Recorder stub.
package events_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	cgcoutbox "github.com/apollo-chora/chora-common/outbox"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

// memRecorder is the minimal cgcoutbox.Recorder for CloudPublisher tests.
type memRecorder struct {
	rows []*cgcoutbox.Row
	err  error
}

func (m *memRecorder) Record(_ context.Context, _ cgcoutbox.Tx, row *cgcoutbox.Row) error {
	if m.err != nil {
		return m.err
	}
	m.rows = append(m.rows, row)
	return nil
}
func (m *memRecorder) Claim(context.Context, int) ([]*cgcoutbox.Row, error)   { return nil, nil }
func (m *memRecorder) MarkPublished(context.Context, []string) error          { return nil }
func (m *memRecorder) MarkFailed(context.Context, string, string, bool) error { return nil }

func TestCloudPublisher_Publish_Wrapper(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{AggregateType: "tenant"})
	pe := pub.Publish("chora.tenancy.tenant.created.v1", events.Header{TenantID: "t-1", GCID: "g-1"}, map[string]interface{}{"tenant_id": "t-1"})
	if pe.EventID == "" {
		t.Fatalf("wrapper publish should succeed")
	}
	if len(rec.rows) != 1 || rec.rows[0].AggregateType != "tenant" {
		t.Fatalf("expected 1 recorded row, got %d", len(rec.rows))
	}
	// Wrapper swallows the error (keeps drop-in Recorder shape).
	zero := pub.Publish("bogus", events.Header{TenantID: "t"}, nil)
	if zero.EventID != "" {
		t.Fatalf("expected zero-value on invalid topic")
	}
	if len(rec.rows) != 1 {
		t.Fatalf("invalid topic must not record")
	}
}

func TestCloudPublisher_DefaultAggregateType(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{})
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: "t-1"}, nil); err != nil {
		t.Fatalf("PublishWithError: %v", err)
	}
	if len(rec.rows) != 1 || rec.rows[0].AggregateType != "tenant" {
		t.Fatalf("expected default aggregate type, got %+v", rec.rows)
	}
}

func TestCloudPublisher_JSONFallbackAndWarnOnce(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{AggregateType: "tenant"})
	// A valid topic without a binary encoder JSON-fall-backs with a WARN.
	hdr := events.Header{TenantID: "t-1", GCID: "g-1"}
	pe, err := pub.PublishWithError("chora.notifications.sms.sent.v1", hdr, map[string]interface{}{"to": "+65"})
	if err != nil {
		t.Fatalf("JSON-fallback publish: %v", err)
	}
	if pe.EventID == "" {
		t.Fatalf("expected published event")
	}
	if len(rec.rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	// Second publish of the same topic must not warn again (warn-once map).
	if _, err := pub.PublishWithError("chora.notifications.sms.sent.v1", hdr, map[string]interface{}{"to": "+65"}); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rec.rows))
	}
}

func TestCloudPublisher_Errors(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{AggregateType: "tenant"})
	// Empty tenant.
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{}, nil); !errors.Is(err, events.ErrEmptyTenantID) {
		t.Fatalf("expected ErrEmptyTenantID, got %v", err)
	}
	// Invalid topic.
	if _, err := pub.PublishWithError("bogus", events.Header{TenantID: "t"}, nil); err == nil {
		t.Fatalf("expected topic error")
	}
	// Payload type mismatch on an encoder topic → marshal error (not fallback).
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: "t", GCID: "g"}, map[string]interface{}{"display_name": 42}); err == nil || !strings.Contains(err.Error(), "marshal") {
		t.Fatalf("expected marshal error, got %v", err)
	}
	// Recorder failure propagates.
	rec2 := &memRecorder{err: errors.New("record boom")}
	pub2 := events.NewCloudPublisher(rec2, events.CloudPublisherConfig{AggregateType: "tenant"})
	if _, err := pub2.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: "t"}, nil); !strings.Contains(err.Error(), "record") {
		t.Fatalf("expected record error, got %v", err)
	}
}

func TestValidateTopic(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{AggregateType: "tenant"})
	hdr := events.Header{TenantID: "t-1"}
	// Too few parts.
	if _, err := pub.PublishWithError("chora.tenancy.created", hdr, nil); !errors.Is(err, events.ErrInvalidTopic) {
		t.Fatalf("expected ErrInvalidTopic, got %v", err)
	}
	// Wrong prefix.
	if _, err := pub.PublishWithError("x.tenancy.tenant.created.v1", hdr, nil); err == nil || !strings.Contains(err.Error(), "must start with chora") {
		t.Fatalf("expected prefix error, got %v", err)
	}
	// Unknown domain.
	if _, err := pub.PublishWithError("chora.bogusdomain.x.created.v1", hdr, nil); !errors.Is(err, events.ErrUnknownDomain) {
		t.Fatalf("expected ErrUnknownDomain, got %v", err)
	}
	// Missing version suffix.
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.x", hdr, nil); !errors.Is(err, events.ErrMissingVersion) {
		t.Fatalf("expected ErrMissingVersion, got %v", err)
	}
	// Version suffix with non-digit.
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.vx", hdr, nil); !errors.Is(err, events.ErrMissingVersion) {
		t.Fatalf("expected ErrMissingVersion for vx, got %v", err)
	}
	// Numerical single-part version suffix is fine.
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", hdr, nil); err != nil {
		t.Fatalf("valid topic rejected: %v", err)
	}
}

func TestRecorder_RecordedByTopic(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	if got := r.RecordedByTopic("chora.tenancy.tenant.created.v1"); len(got) != 0 {
		t.Fatalf("expected no records, got %d", len(got))
	}
	_, err := r.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{TenantID: "t1"}, nil)
	if err != nil {
		t.Fatalf("PublishWithError: %v", err)
	}
	_, err = r.PublishWithError("chora.tenancy.addon.activated.v1", events.Header{TenantID: "t1"}, nil)
	if err != nil {
		t.Fatalf("PublishWithError: %v", err)
	}
	got := r.RecordedByTopic("chora.tenancy.tenant.created.v1")
	if len(got) != 1 || got[0].Topic != "chora.tenancy.tenant.created.v1" {
		t.Fatalf("unexpected RecordedByTopic %+v", got)
	}
}

func TestRecorder_ValidationErrors(t *testing.T) {
	t.Parallel()
	r := events.NewRecorder()
	if _, err := r.PublishWithError("nope", events.Header{TenantID: "t"}, nil); err == nil {
		t.Fatalf("expected validation error")
	}
	if _, err := r.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{}, nil); !errors.Is(err, events.ErrEmptyTenantID) {
		t.Fatalf("expected ErrEmptyTenantID, got %v", err)
	}
	if len(r.Recorded()) != 0 {
		t.Fatalf("invalid publishes must not record")
	}
}
