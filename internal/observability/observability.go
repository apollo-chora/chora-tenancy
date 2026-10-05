// Package observability holds the lightweight observability shim for the
// chora-tenancy service.
//
// The production stack is OTLP-everywhere: spans flow to the standard OTLP
// endpoint (see otlp.go) and structured logs carry a generated trace_id (W3C
// traceparent friendly) so the collector can correlate them. The shim below
// emits those structured log lines and mints/propagates trace context for the
// HTTP surface.
package observability

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// ServiceName is the canonical service identifier emitted on every log line.
const ServiceName = "chora-tenancy"

// TraceContext holds the W3C traceparent fields the shim threads through
// each request.
type TraceContext struct {
	TraceID  string // 16-byte hex (32 chars)
	SpanID   string // 8-byte hex (16 chars)
	TraceFlg string // 2-char hex
}

// FromRequest extracts traceparent (W3C trace context) or mints a fresh one.
//
// Per CLAUDE.md §6, traceparent + tracestate are mandatory in the event
// envelope; the same propagation rules apply to HTTP. The skeleton minimally
// parses the version-00 form: 00-<trace-id>-<span-id>-<flags>.
func FromRequest(r *http.Request) TraceContext {
	if h := r.Header.Get("traceparent"); h != "" {
		parts := strings.Split(h, "-")
		if len(parts) == 4 && parts[0] == "00" {
			return TraceContext{TraceID: parts[1], SpanID: parts[2], TraceFlg: parts[3]}
		}
	}
	return TraceContext{
		TraceID:  randHex(16),
		SpanID:   randHex(8),
		TraceFlg: "01",
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Best-effort fallback so logs still emit.
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> uint(8*(i%8)))
		}
	}
	return hex.EncodeToString(b)
}

// LogRequest emits a single structured-log line per request.
//
// Format: service=<name> trace_id=<id> span_id=<id> tenant_id=<id>
// gcid=<id> method=<m> path=<p> status=<n> duration_ms=<n>.
func LogRequest(tc TraceContext, tenantID, gcid, method, path string, status int, dur time.Duration) {
	log.Printf(
		"service=%s trace_id=%s span_id=%s tenant_id=%s gcid=%s method=%s path=%s status=%d duration_ms=%d",
		ServiceName, tc.TraceID, tc.SpanID, tenantID, gcid, method, path, status, dur.Milliseconds(),
	)
}

// TraceparentHeader formats a TraceContext as a W3C traceparent string.
// Used when the service emits outbound HTTP / gRPC / event envelopes.
func (tc TraceContext) TraceparentHeader() string {
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, tc.TraceFlg)
}
