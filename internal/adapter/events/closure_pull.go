// Pull-loop adapter binding the federated closure-saga subscriber to the
// eventbus Subscriber (CHO-1719 gap 4). The consumer acks on nil and nacks on
// error (ack-after-processing per D6.2).
package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/apollo-chora/chora-common/eventbus"
)

// ClosurePullHandler adapts the ClosureSubscriber to an eventbus.Handler.
// The orchestrator's JSON fan-out body becomes the payload; the Header
// (tenancy's envelope shape) is projected from the message envelope.
func ClosurePullHandler(s *ClosureSubscriber) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		if s == nil {
			return errors.New("events: closure pull handler not initialised")
		}
		var p PseudonymiseRequestedPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return errors.Join(
				errors.New("events: closure payload decode"), err,
			)
		}
		h := Header{
			TenantID:    msg.Envelope.TenantID,
			GCID:        msg.Envelope.GCID,
			Traceparent: msg.Envelope.Traceparent,
		}
		if h.TenantID == "" {
			h.TenantID = p.TenantID
		}
		return s.Handle(ctx, h, p)
	}
}

// closureAckSink is the lib ClosureAckPublisher's Publish shape
// (eventbus.ClosureAckPublisher satisfies it; tests inject a recorder).
type closureAckSink interface {
	Publish(topic, tenantID, gcid, traceparent string,
		payload map[string]interface{}) error
}

// CloudClosurePublisher bridges tenancy's Recorder-shaped
// PublishWithError port onto the shared eventbus.ClosureAckPublisher so
// closure acks reach the real event bus (the in-memory Recorder never
// leaves the process).
type CloudClosurePublisher struct {
	sink closureAckSink
}

// NewCloudClosurePublisher wraps a closure-ack sink.
func NewCloudClosurePublisher(sink closureAckSink) *CloudClosurePublisher {
	return &CloudClosurePublisher{sink: sink}
}

// PublishWithError satisfies the ClosurePublisher port.
func (p *CloudClosurePublisher) PublishWithError(
	topic string, h Header, payload map[string]interface{},
) (PublishedEvent, error) {
	if p == nil || p.sink == nil {
		return PublishedEvent{}, errors.New(
			"events: CloudClosurePublisher not initialised",
		)
	}
	if err := p.sink.Publish(
		topic, h.TenantID, h.GCID, h.Traceparent, payload,
	); err != nil {
		return PublishedEvent{}, err
	}
	return PublishedEvent{
		Topic:       topic,
		TenantID:    h.TenantID,
		GCID:        h.GCID,
		Traceparent: h.Traceparent,
		Payload:     payload,
	}, nil
}
