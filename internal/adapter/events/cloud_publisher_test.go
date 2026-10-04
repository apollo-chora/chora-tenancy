// cloud_publisher_test.go — verifies the outbox-backed CloudPublisher
// satisfies the local publisher contract and writes durable rows.
package events_test

import (
	"context"
	"testing"

	cgcoutbox "github.com/5007-Capstone/chora/libs/chora-go-common/outbox"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
)

type stubRecorder struct {
	rows []*cgcoutbox.Row
}

func (s *stubRecorder) Record(_ context.Context, _ cgcoutbox.Tx, row *cgcoutbox.Row) error {
	s.rows = append(s.rows, row)
	return nil
}
func (s *stubRecorder) Claim(_ context.Context, _ int) ([]*cgcoutbox.Row, error) {
	return nil, nil
}
func (s *stubRecorder) MarkPublished(_ context.Context, _ []string) error  { return nil }
func (s *stubRecorder) MarkFailed(_ context.Context, _ string, _ string, _ bool) error {
	return nil
}

func TestCloudPublisher_Publish_WritesOutboxRow(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{
		AggregateType: "tenant",
	})

	h := events.Header{
		TenantID:    "01970000-0000-7000-8000-aaaaaaaaaaaa",
		GCID:        "01970000-0000-7000-8000-bbbbbbbbbbbb",
		Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
	if _, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", h, map[string]any{"slug": "phyllis"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rec.rows))
	}
	row := rec.rows[0]
	if row.Topic != "chora.tenancy.tenant.created.v1" {
		t.Errorf("topic = %q", row.Topic)
	}
	if row.AggregateType != "tenant" {
		t.Errorf("aggregate_type = %q", row.AggregateType)
	}
	if row.Envelope.TenantID != h.TenantID {
		t.Errorf("envelope.tenant_id = %q", row.Envelope.TenantID)
	}
}

func TestCloudPublisher_PublishWithError_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	pub := events.NewCloudPublisher(rec, events.CloudPublisherConfig{AggregateType: "tenant"})
	_, err := pub.PublishWithError("chora.tenancy.tenant.created.v1", events.Header{}, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}
