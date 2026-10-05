package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	"github.com/apollo-chora/chora-tenancy/internal/config"
)

const (
	pullGCID   = "01970000-0000-7000-9000-000000000001"
	pullTenant = "01970000-0000-7000-8000-000000000001"
)

func pullPIIMap() *config.PIIClosureMap {
	return &config.PIIClosureMap{
		Domain:  "chora_tenancy",
		Version: "1.0",
		FieldsToTokenize: []config.TableSpec{
			{Table: "tenant_memberships", Columns: []config.ColumnSpec{
				{Column: "invited_by_display_name", Strategy: "tombstone_string", Value: "Former member"},
			}},
		},
	}
}

func pullMsg(payload string) eventbus.Message {
	return eventbus.Message{
		Subject: events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{
			TenantID:    pullTenant,
			GCID:        pullGCID,
			Traceparent: "00-feedface-cafebabe-01",
		},
		Payload: []byte(payload),
	}
}

func TestClosurePullHandler_DispatchesAndAcksOnNewTopic(t *testing.T) {
	t.Parallel()
	repo := events.NewInMemoryClosureRepo()
	rec := events.NewRecorder()
	sub := events.NewClosureSubscriber(repo, rec, pullPIIMap(), nil)

	h := events.ClosurePullHandler(sub)
	err := h(context.Background(), pullMsg(
		`{"saga_id":"saga-1","gcid":"`+pullGCID+`","tenant_id":"`+pullTenant+`"}`,
	))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	acks := rec.RecordedByTopic("chora.tenancy.account.pseudonymised.v1")
	if len(acks) != 1 {
		t.Fatalf("want 1 ack on the reconciled topic, got %d", len(acks))
	}
	// Header traceparent must flow from the message envelope (payload has
	// no traceparent field in tenancy's shape).
	if acks[0].Traceparent != "00-feedface-cafebabe-01" {
		t.Errorf("traceparent = %q", acks[0].Traceparent)
	}
}

func TestClosurePullHandler_MalformedPayloadErrors(t *testing.T) {
	t.Parallel()
	sub := events.NewClosureSubscriber(
		events.NewInMemoryClosureRepo(), events.NewRecorder(), pullPIIMap(), nil,
	)
	h := events.ClosurePullHandler(sub)
	if err := h(context.Background(), pullMsg(`{not-json`)); err == nil {
		t.Fatal("malformed payload must error (nack)")
	}
}

func TestClosurePullHandler_NilGuards(t *testing.T) {
	t.Parallel()
	h := events.ClosurePullHandler(nil)
	if err := h(context.Background(), pullMsg(`{}`)); err == nil {
		t.Fatal("nil subscriber must error")
	}
}

// cloudAckRecorder doubles the lib ClosureAckPublisher's Publish shape.
type cloudAckRecorder struct {
	mu     sync.Mutex
	topics []string
	fail   error
}

func (r *cloudAckRecorder) Publish(
	topic, tenantID, gcid, traceparent string, payload map[string]interface{},
) error {
	if r.fail != nil {
		return r.fail
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics = append(r.topics, topic)
	return nil
}

func TestCloudClosurePublisher_BridgesToAckPublisher(t *testing.T) {
	t.Parallel()
	rec := &cloudAckRecorder{}
	bridge := events.NewCloudClosurePublisher(rec)
	_, err := bridge.PublishWithError(
		"chora.tenancy.account.pseudonymised.v1",
		events.Header{TenantID: pullTenant, GCID: pullGCID, Traceparent: "00-a-b-01"},
		map[string]interface{}{"saga_id": "saga-1"},
	)
	if err != nil {
		t.Fatalf("bridge publish: %v", err)
	}
	if len(rec.topics) != 1 || rec.topics[0] != "chora.tenancy.account.pseudonymised.v1" {
		t.Fatalf("bridge topics = %v", rec.topics)
	}
}

func TestCloudClosurePublisher_PropagatesError(t *testing.T) {
	t.Parallel()
	rec := &cloudAckRecorder{fail: errors.New("pubsub down")}
	bridge := events.NewCloudClosurePublisher(rec)
	_, err := bridge.PublishWithError(
		"chora.tenancy.account.pseudonymised.v1",
		events.Header{TenantID: pullTenant}, map[string]interface{}{},
	)
	if err == nil {
		t.Fatal("inner publish error must propagate (handler nacks)")
	}
}
