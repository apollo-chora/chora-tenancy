// Package events implements an in-process event publisher for chora-tenancy.
package events

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/env"
)

const (
	sourceService = "chora-tenancy"
)

//
// Was a hardcoded literal until 2026-09-02 (CHO-2419). No manifest could reach
// it, so a second org stamped every event with chora-489812 and any consumer
// filtering on source_project would have been filtering on a lie.
// CHORA_SOURCE_PROJECT is now set on every event-emitting service; the literal
// stays as the fallback so this estate is provably unchanged.
var sourceProject = env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812")

var validDomains = map[string]struct{}{
	"creation":      {},
	"consumption":   {},
	"delivery":      {},
	"sharing":       {},
	"a2a":           {},
	"identity":      {},
	"tenancy":       {},
	"governance":    {},
	"observability": {},
	"notifications": {},
	"ai_kernel":     {},
	"payments":      {}, // ADR-164 — 8th supporting/platform domain
}

var (
	ErrInvalidTopic   = errors.New("invalid topic")
	ErrUnknownDomain  = errors.New("unknown event domain")
	ErrEmptyTenantID  = errors.New("envelope tenant_id is mandatory")
	ErrMissingVersion = errors.New("topic must end in .v{N}")
)

type Header struct {
	TenantID    string
	GCID        string
	Traceparent string
}

type PublishedEvent struct {
	Topic          string
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	SourceProject  string
	SourceService  string
	SchemaVersion  int
	Payload        map[string]interface{}
}

type Recorder struct {
	mu      sync.Mutex
	records []PublishedEvent
}

func NewRecorder() *Recorder { return &Recorder{} }

func (r *Recorder) Publish(topic string, h Header, payload map[string]interface{}) PublishedEvent {
	rec, _ := r.PublishWithError(topic, h, payload)
	return rec
}

func (r *Recorder) PublishWithError(topic string, h Header, payload map[string]interface{}) (PublishedEvent, error) {
	if err := validateTopic(topic); err != nil {
		return PublishedEvent{}, err
	}
	if strings.TrimSpace(h.TenantID) == "" {
		return PublishedEvent{}, ErrEmptyTenantID
	}
	now := time.Now().UTC()
	rec := PublishedEvent{
		Topic:          topic,
		EventID:        newUUIDv7(),
		IdempotencyKey: newUUIDv7(),
		TenantID:       h.TenantID,
		GCID:           h.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    h.Traceparent,
		SourceProject:  sourceProject,
		SourceService:  sourceService,
		SchemaVersion:  1,
		Payload:        payload,
	}
	r.mu.Lock()
	r.records = append(r.records, rec)
	r.mu.Unlock()
	return rec, nil
}

func (r *Recorder) Recorded() []PublishedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PublishedEvent, len(r.records))
	copy(out, r.records)
	return out
}

func (r *Recorder) RecordedByTopic(topic string) []PublishedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PublishedEvent, 0)
	for _, ev := range r.records {
		if ev.Topic == topic {
			out = append(out, ev)
		}
	}
	return out
}

func validateTopic(topic string) error {
	parts := strings.Split(topic, ".")
	if len(parts) < 5 {
		return fmt.Errorf("%w: %q", ErrInvalidTopic, topic)
	}
	if parts[0] != "chora" {
		return fmt.Errorf("%w: must start with chora.: %q", ErrInvalidTopic, topic)
	}
	if _, ok := validDomains[parts[1]]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownDomain, parts[1])
	}
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "v") || len(last) < 2 {
		return ErrMissingVersion
	}
	for _, c := range last[1:] {
		if c < '0' || c > '9' {
			return ErrMissingVersion
		}
	}
	return nil
}

func newUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
