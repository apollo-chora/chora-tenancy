package observability_test

import (
	"log"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/observability"
)

func TestFromRequest_MintsFreshTraceWhenAbsent(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/healthz/", nil)
	tc := observability.FromRequest(r)
	if len(tc.TraceID) != 32 {
		t.Fatalf("expected 32-char trace_id, got %d", len(tc.TraceID))
	}
	if len(tc.SpanID) != 16 {
		t.Fatalf("expected 16-char span_id, got %d", len(tc.SpanID))
	}
	if tc.TraceFlg != "01" {
		t.Fatalf("expected default trace_flg=01, got %q", tc.TraceFlg)
	}
}

func TestFromRequest_ParsesIncomingTraceparent(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/healthz/", nil)
	r.Header.Set("traceparent", "00-aabbccddeeff00112233445566778899-0102030405060708-01")
	tc := observability.FromRequest(r)
	if tc.TraceID != "aabbccddeeff00112233445566778899" {
		t.Fatalf("trace_id mismatch")
	}
	if tc.SpanID != "0102030405060708" {
		t.Fatalf("span_id mismatch")
	}
}

func TestFromRequest_RejectsMalformedTraceparent(t *testing.T) {
	t.Parallel()
	// Wrong version prefix → mint fresh.
	r := httptest.NewRequest("GET", "/healthz/", nil)
	r.Header.Set("traceparent", "01-aabbccddeeff00112233445566778899-0102030405060708-01")
	tc := observability.FromRequest(r)
	if len(tc.TraceID) != 32 || len(tc.SpanID) != 16 {
		t.Fatalf("expected fresh trace for wrong version, got %+v", tc)
	}
}

func TestFromRequest_RejectsShortTraceparent(t *testing.T) {
	t.Parallel()
	// Too few dash-separated parts → mint fresh.
	r := httptest.NewRequest("GET", "/healthz/", nil)
	r.Header.Set("traceparent", "00-aabbccddeeff0011")
	tc := observability.FromRequest(r)
	if len(tc.TraceID) != 32 || len(tc.SpanID) != 16 {
		t.Fatalf("expected fresh trace for malformed header, got %+v", tc)
	}
}

func TestLogRequest_EmitsStructuredLine(t *testing.T) {
	t.Parallel()
	var buf strings.Builder
	log.SetOutput(&buf)
	defer log.SetOutput(nil)
	tc := observability.TraceContext{
		TraceID:  "aabbccddeeff00112233445566778899",
		SpanID:   "0102030405060708",
		TraceFlg: "01",
	}
	observability.LogRequest(tc, "tenant-1", "gcid-7", "GET", "/api/tenants", 200, 42*time.Millisecond)
	line := buf.String()
	for _, want := range []string{
		"service=chora-tenancy",
		"trace_id=aabbccddeeff00112233445566778899",
		"span_id=0102030405060708",
		"tenant_id=tenant-1",
		"gcid=gcid-7",
		"method=GET",
		"path=/api/tenants",
		"status=200",
		"duration_ms=42",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line missing %q: %s", want, line)
		}
	}
}

func TestTraceparentHeader_Format(t *testing.T) {
	t.Parallel()
	tc := observability.TraceContext{
		TraceID:  "aabbccddeeff00112233445566778899",
		SpanID:   "0102030405060708",
		TraceFlg: "01",
	}
	h := tc.TraceparentHeader()
	if !strings.HasPrefix(h, "00-") {
		t.Fatalf("expected 00- prefix, got %q", h)
	}
	if !strings.Contains(h, "aabbccdd") {
		t.Fatalf("trace_id missing from header")
	}
}
