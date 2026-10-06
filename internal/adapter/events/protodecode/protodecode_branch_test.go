// protodecode_branch_test.go — white-box (package protodecode) coverage
// for the binary-decoder path (registered topic → proto projection), the
// attr-envelope merging and the both-fail error + empty-payload cases.
// Registers a throwaway binary decoder on a test topic and restores the
// registry afterwards.
package protodecode

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// saveDecoder restores the binaryDecoders map after the test.
func withFakeDecoder(t *testing.T) {
	t.Helper()
	const okTopic = "chora.tenancy.test.binary_ok.v1"
	const failTopic = "chora.tenancy.test.binary_fail.v1"
	prevOk, hadOk := binaryDecoders[okTopic]
	prevFail, hadFail := binaryDecoders[failTopic]
	register := binaryDecoders
	_ = register
	t.Cleanup(func() {
		if hadOk {
			binaryDecoders[okTopic] = prevOk
		} else {
			delete(binaryDecoders, okTopic)
		}
		if hadFail {
			binaryDecoders[failTopic] = prevFail
		} else {
			delete(binaryDecoders, failTopic)
		}
	})
	binaryDecoders[okTopic] = struct {
		decode  binaryDecoder
		project projector
	}{
		decode: func(payload []byte) (proto.Message, error) {
			return &timestamppb.Timestamp{}, nil
		},
		project: func(msg proto.Message, out map[string]any) {
			out["tenant_id"] = "proj-tenant"
			out["kind"] = "test"
		},
	}
	binaryDecoders[failTopic] = struct {
		decode  binaryDecoder
		project projector
	}{
		decode: func(payload []byte) (proto.Message, error) {
			return nil, errors.New("unmarshal boom")
		},
		project: func(msg proto.Message, out map[string]any) {},
	}
}

func TestDecodePayloadIntoWithAttrs_BinaryAndJSONPaths(t *testing.T) {
	withFakeDecoder(t)
	type dstT struct {
		TenantID string `json:"tenant_id"`
		Kind     string `json:"kind"`
		EventID  string `json:"event_id"`
	}
	// Binary path.
	var d1 dstT
	if err := DecodePayloadIntoWithAttrs("chora.tenancy.test.binary_ok.v1", []byte("ok"),
		map[string]string{"event_id": "evt-1"}, &d1); err != nil {
		t.Fatalf("binary into: %v", err)
	}
	if d1.TenantID != "proj-tenant" || d1.Kind != "test" || d1.EventID != "evt-1" {
		t.Fatalf("unexpected dst %+v", d1)
	}
	// Attrs round-trip on JSON only (unregistered topic).
	var d2 dstT
	if err := DecodePayloadIntoWithAttrs("chora.tenancy.json_topic.v1", []byte(`{"tenant_id":"json-t"}`),
		map[string]string{"event_id": "evt-2"}, &d2); err != nil {
		t.Fatalf("attrs into: %v", err)
	}
	if d2.TenantID != "json-t" || d2.EventID != "evt-2" {
		t.Fatalf("unexpected dst %+v", d2)
	}
	// Neither decodable.
	var d3 dstT
	err := DecodePayloadIntoWithAttrs("chora.tenancy.garbage.v1", []byte("not-json"), nil, &d3)
	if err == nil || !strings.Contains(err.Error(), "not decodable") {
		t.Fatalf("expected not-decodable error, got %v", err)
	}
	// Empty payload everywhere.
	for _, f := range []func() error{
		func() error { return DecodePayloadIntoWithAttrs("t", nil, nil, &d3) },
	} {
		if err := f(); !errors.Is(err, ErrEmptyPayload) {
			t.Fatalf("expected ErrEmptyPayload, got %v", err)
		}
	}
}

func TestMergeAttrsEnvelopeFullSet(t *testing.T) {
	t.Parallel()
	out := map[string]any{"gcid": "existing"}
	attrs := map[string]string{
		"event_id": "e1", "tenant_id": "t1", "gcid": "g1",
		"traceparent": "tp", "tracestate": "ts",
		"occurred_at": "oa", "published_at": "pa",
		"chora_imda_dimension": "cd", "imda_lifecycle_stage": "ls",
	}
	mergeAttrsEnvelope(attrs, out)
	if out["gcid"] != "existing" {
		t.Fatalf("existing gcid must win, got %v", out["gcid"])
	}
	if out["owner_gcid"] != "g1" || out["event_id"] != "e1" || out["tenant_id"] != "t1" ||
		out["traceparent"] != "tp" || out["tracestate"] != "ts" ||
		out["occurred_at"] != "oa" || out["published_at"] != "pa" ||
		out["chora_imda_dimension"] != "cd" || out["imda_lifecycle_stage"] != "ls" {
		t.Fatalf("unexpected merge %v", out)
	}
	// Empty attrs are a no-op.
	mergeAttrsEnvelope(nil, out)
}
