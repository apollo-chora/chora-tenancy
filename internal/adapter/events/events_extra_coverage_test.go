// events_extra_coverage_test.go — branch coverage for the events package
// surface the existing tests leave sparse: topic validation error
// families. Uses the in-process Recorder.
package events_test

import (
	"errors"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
)

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
