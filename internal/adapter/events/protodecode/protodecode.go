// Package protodecode decodes inbound event-bus message bytes into a
// snake_case map[string]any compatible with the legacy json.Unmarshal flow.
//
// Why this package exists
// -----------------------
// Producer-side wave (task #33 / #38) flipped chora-tenancy outbox payloads
// to binary protobuf on Schema-Registry-attached topics. chora-tenancy's
// inbound subscribers (billingpubsub.EventLogReplayFeeder) consume the two
// payment.captured topics from chora-tenancy + chora-identity. Neither
// producer has flipped payment.captured to binary yet (no proto schema
// exists for the payment payload at the time this package landed), but the
// infrastructure is in place so the flip is a single-line addition to
// binaryDecoders below.
//
// Decode strategy
// ---------------
//  1. If the topic is registered for binary decoding (see binaryDecoders),
//     attempt proto.Unmarshal first. On success, project the proto message
//     into a snake_case map[string]any compatible with the legacy JSON shape.
//  2. On binary failure (or unregistered topic), fall back to json.Unmarshal.
//  3. On both-fail, return a wrapped error.
package protodecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"google.golang.org/protobuf/proto"
)

// ErrEmptyPayload is returned when the inbound bytes are empty.
var ErrEmptyPayload = errors.New("protodecode: empty payload")

type binaryDecoder func(payload []byte) (proto.Message, error)
type projector func(msg proto.Message, out map[string]any)

// binaryDecoders is the per-topic registry. Empty initially — payment.captured
// is still JSON on the producer side. Add an entry when chora-tenancy or
// chora-identity flips a payment topic to binary protobuf.
var binaryDecoders = map[string]struct {
	decode  binaryDecoder
	project projector
}{}

// DecodePayloadMap decodes inbound event-bus message bytes into a snake_case
// map[string]any.
//
// Use DecodePayloadMapWithAttrs when the caller has message attributes
// available — the publisher places envelope fields (event_id / tenant_id /
// gcid / traceparent) there, not in the payload body.
func DecodePayloadMap(topic string, payload []byte) (map[string]any, error) {
	return DecodePayloadMapWithAttrs(topic, payload, nil)
}

// DecodePayloadMapWithAttrs decodes inbound event-bus message bytes + the
// publisher-supplied message attributes into a snake_case map[string]any.
//
// Field precedence (high → low):
//  1. Binary proto Envelope (when payload is binary-decodable for this topic)
//  2. Message attributes (publisher's canonical envelope projection)
//  3. JSON payload body
//
// nil attrs ⇒ legacy DecodePayloadMap behaviour. Empty payload ⇒
// ErrEmptyPayload regardless of attrs.
func DecodePayloadMapWithAttrs(topic string, payload []byte, attrs map[string]string) (map[string]any, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyPayload
	}

	if entry, ok := binaryDecoders[topic]; ok {
		msg, err := entry.decode(payload)
		if err == nil && msg != nil && entry.project != nil {
			out := make(map[string]any)
			mergeAttrsEnvelope(attrs, out)
			entry.project(msg, out)
			return out, nil
		}
		warnBinaryFallback(topic, err)
	} else {
		warnUnknownTopic(topic)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("protodecode: topic %q neither binary-decodable nor JSON-decodable: %w", topic, err)
	}
	out := make(map[string]any, len(body)+len(attrs))
	for k, v := range body {
		out[k] = v
	}
	mergeAttrsEnvelope(attrs, out)
	return out, nil
}

// DecodePayloadInto decodes inbound bytes directly into the supplied JSON-
// tagged destination struct. Use when the consumer has a stable typed shape
// (e.g., the payment.captured minimalPaymentPayload) rather than a generic
// map. Tries proto first only if the topic is registered for binary.
//
// Returns ErrEmptyPayload on zero-length input.
func DecodePayloadInto(topic string, payload []byte, dst any) error {
	return DecodePayloadIntoWithAttrs(topic, payload, nil, dst)
}

// DecodePayloadIntoWithAttrs is the attribute-aware variant of
// DecodePayloadInto — envelope fields from msg.Attributes are merged into
// the decoded map before re-marshalling into dst, so dst structs with JSON
// tags for event_id / tenant_id / gcid populate correctly even when the
// producer is on JSON-only.
//
// Returns ErrEmptyPayload on zero-length input regardless of attrs.
func DecodePayloadIntoWithAttrs(topic string, payload []byte, attrs map[string]string, dst any) error {
	if len(payload) == 0 {
		return ErrEmptyPayload
	}

	if entry, ok := binaryDecoders[topic]; ok {
		// For binary-registered topics we go through the map projection so
		// the field-name mapping rules stay in one place, then re-marshal
		// + unmarshal into dst.
		msg, err := entry.decode(payload)
		if err == nil && msg != nil && entry.project != nil {
			out := make(map[string]any)
			mergeAttrsEnvelope(attrs, out)
			entry.project(msg, out)
			bz, mErr := json.Marshal(out)
			if mErr == nil {
				return json.Unmarshal(bz, dst)
			}
		}
		warnBinaryFallback(topic, err)
	}

	// JSON-fallback path. When attrs are present we round-trip via a map
	// so envelope fields can be merged.
	if len(attrs) > 0 {
		var body map[string]any
		if err := json.Unmarshal(payload, &body); err != nil {
			return fmt.Errorf("protodecode: topic %q payload not decodable: %w", topic, err)
		}
		mergeAttrsEnvelope(attrs, body)
		bz, mErr := json.Marshal(body)
		if mErr != nil {
			return fmt.Errorf("protodecode: topic %q re-marshal: %w", topic, mErr)
		}
		return json.Unmarshal(bz, dst)
	}

	if err := json.Unmarshal(payload, dst); err != nil {
		return fmt.Errorf("protodecode: topic %q payload not decodable: %w", topic, err)
	}
	return nil
}

// mergeAttrsEnvelope projects message attributes onto the decoded map's
// canonical envelope keys. Empty / missing attrs are no-ops.
//
// Attribute key mapping (publisher → consumer field name):
//   - attrs["event_id"]      → out["event_id"]
//   - attrs["tenant_id"]     → out["tenant_id"]
//   - attrs["gcid"]          → out["owner_gcid"] + out["gcid"]
//   - attrs["traceparent"]   → out["traceparent"]
//   - attrs["tracestate"]    → out["tracestate"]
//   - attrs["occurred_at"]   → out["occurred_at"]
func mergeAttrsEnvelope(attrs map[string]string, out map[string]any) {
	if len(attrs) == 0 {
		return
	}
	if v := attrs["event_id"]; v != "" {
		out["event_id"] = v
	}
	if v := attrs["tenant_id"]; v != "" {
		out["tenant_id"] = v
	}
	if v := attrs["gcid"]; v != "" {
		out["owner_gcid"] = v
		if _, ok := out["gcid"]; !ok {
			out["gcid"] = v
		}
	}
	if v := attrs["traceparent"]; v != "" {
		out["traceparent"] = v
	}
	if v := attrs["tracestate"]; v != "" {
		out["tracestate"] = v
	}
	if v := attrs["occurred_at"]; v != "" {
		out["occurred_at"] = v
	}
	if v := attrs["published_at"]; v != "" {
		out["published_at"] = v
	}
	if v := attrs["chora_imda_dimension"]; v != "" {
		out["chora_imda_dimension"] = v
	}
	if v := attrs["imda_lifecycle_stage"]; v != "" {
		out["imda_lifecycle_stage"] = v
	}
}

// -----------------------------------------------------------------------------
// One-shot WARN logging
// -----------------------------------------------------------------------------

var (
	warnedFallbackMu sync.Mutex
	warnedFallback   = map[string]bool{}

	warnedUnknownMu sync.Mutex
	warnedUnknown   = map[string]bool{}
)

func warnBinaryFallback(topic string, err error) {
	warnedFallbackMu.Lock()
	defer warnedFallbackMu.Unlock()
	if warnedFallback[topic] {
		return
	}
	warnedFallback[topic] = true
	log.Printf("WARN protodecode: topic %q registered for binary but binary unmarshal failed (%v) — falling back to JSON. Expected during producer-side flip; investigate if persistent.", topic, err)
}

func warnUnknownTopic(topic string) {
	warnedUnknownMu.Lock()
	defer warnedUnknownMu.Unlock()
	if warnedUnknown[topic] {
		return
	}
	warnedUnknown[topic] = true
	// Note: NOT logged for chora-tenancy because payment.captured topics
	// are intentionally JSON-only at this package landing — too noisy.
	_ = topic
}
