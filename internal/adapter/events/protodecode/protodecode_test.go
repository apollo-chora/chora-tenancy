// protodecode_test covers the JSON-fallback path. No binary topics are
// registered for chora-tenancy at this package landing; tests assert the
// future-binary-flip seam is in place via the DecodePayloadInto contract.
package protodecode_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events/protodecode"
)

type minimalPayload struct {
	AmountCents int64     `json:"AmountCents"`
	OccurredAt  time.Time `json:"OccurredAt"`
}

// TestDecodeIntoWithAttrs_JSON_StructPopulated verifies the typed-into
// variant.
func TestDecodeIntoWithAttrs_JSON_StructPopulated(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	bz, _ := json.Marshal(map[string]any{
		"AmountCents": 7777,
		"OccurredAt":  now.Format(time.RFC3339Nano),
	})
	attrs := map[string]string{
		"event_id":  "01971a90-0000-7000-8000-attrs",
		"tenant_id": "tenant-acme",
	}
	var p minimalPayload
	if err := protodecode.DecodePayloadIntoWithAttrs("chora.tenancy.payment.captured.v1", bz, attrs, &p); err != nil {
		t.Fatalf("DecodePayloadIntoWithAttrs: %v", err)
	}
	if p.AmountCents != 7777 {
		t.Errorf("AmountCents = %d", p.AmountCents)
	}
	if !p.OccurredAt.Equal(now) {
		t.Errorf("OccurredAt mismatch")
	}
}

// TestDecodeWithAttrs_EmptyPayload_FailsLoud ensures empty fails loud even
// when attrs are present.
func TestDecodeWithAttrs_EmptyPayload_FailsLoud(t *testing.T) {
	var p minimalPayload
	if err := protodecode.DecodePayloadIntoWithAttrs("x.v1", nil, map[string]string{"event_id": "x"}, &p); err == nil {
		t.Fatal("expected error on nil payload via Into")
	}
}
