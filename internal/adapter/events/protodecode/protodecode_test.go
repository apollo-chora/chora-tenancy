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

func TestDecode_PaymentCaptured_JSONIntoStruct(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	payload := map[string]any{
		"AmountCents": 12345,
		"OccurredAt":  now.Format(time.RFC3339Nano),
	}
	bz, _ := json.Marshal(payload)

	var p minimalPayload
	if err := protodecode.DecodePayloadInto("chora.tenancy.payment.captured.v1", bz, &p); err != nil {
		t.Fatalf("DecodePayloadInto: %v", err)
	}
	if p.AmountCents != 12345 {
		t.Errorf("AmountCents = %d, want 12345", p.AmountCents)
	}
	if !p.OccurredAt.Equal(now) {
		t.Errorf("OccurredAt = %v, want %v", p.OccurredAt, now)
	}
}

func TestDecode_EmptyPayload_FailsLoud(t *testing.T) {
	var p minimalPayload
	if err := protodecode.DecodePayloadInto("chora.tenancy.payment.captured.v1", nil, &p); err == nil {
		t.Fatal("expected error on nil payload")
	}
	if _, err := protodecode.DecodePayloadMap("x.v1", nil); err == nil {
		t.Fatal("expected error on nil payload via map decode")
	}
}

func TestDecode_PayloadMap_JSON(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"foo": "bar"})
	got, err := protodecode.DecodePayloadMap("chora.tenancy.payment.captured.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, _ := got["foo"].(string); v != "bar" {
		t.Errorf("foo = %q", v)
	}
}

// -----------------------------------------------------------------------------
// Debt #3 — JSON-fallback envelope-from-attributes fix
// -----------------------------------------------------------------------------

// TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope asserts the JSON
// fallback path carries attrs through.
func TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"AmountCents": 12345})
	attrs := map[string]string{
		"event_id":    "01971a90-0000-7000-8000-attrs",
		"tenant_id":   "tenant-acme",
		"gcid":        "gcid-payer",
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"topic":       "chora.tenancy.payment.captured.v1",
		"occurred_at": "2026-05-16T10:00:00Z",
	}
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.tenancy.payment.captured.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-attrs" {
		t.Errorf("event_id = %q (want attrs-sourced)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	if v, _ := got["owner_gcid"].(string); v != "gcid-payer" {
		t.Errorf("owner_gcid = %q (want attrs.gcid)", v)
	}
	if v, ok := got["AmountCents"].(float64); !ok || v != 12345 {
		t.Errorf("AmountCents from JSON body lost: %v", got["AmountCents"])
	}
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

// TestDecodeWithAttrs_NilAttrs_BehavesLikeLegacy verifies back-compat.
func TestDecodeWithAttrs_NilAttrs_BehavesLikeLegacy(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"foo": "bar"})
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.tenancy.payment.captured.v1", bz, nil)
	if err != nil {
		t.Fatalf("nil attrs: %v", err)
	}
	if v, _ := got["foo"].(string); v != "bar" {
		t.Errorf("foo = %q", v)
	}
}

// TestDecodeWithAttrs_EmptyPayload_FailsLoud ensures empty fails loud even
// when attrs are present.
func TestDecodeWithAttrs_EmptyPayload_FailsLoud(t *testing.T) {
	if _, err := protodecode.DecodePayloadMapWithAttrs("x.v1", nil, map[string]string{"event_id": "x"}); err == nil {
		t.Fatal("expected error on nil payload")
	}
	var p minimalPayload
	if err := protodecode.DecodePayloadIntoWithAttrs("x.v1", nil, map[string]string{"event_id": "x"}, &p); err == nil {
		t.Fatal("expected error on nil payload via Into")
	}
}
