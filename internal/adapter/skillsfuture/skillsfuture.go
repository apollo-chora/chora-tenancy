// Package skillsfuture implements a sandbox client for the SkillsFuture
// Singapore (SSG) training-credit API per the locked architecture
// (CLAUDE.md §1 "Standards" + project_chora_gcp_stack.md "External
// integrations: SkillsFuture / Singpass").
//
// Two flows are exposed:
//
//  1. Subsidy claim — SubmitClaim() → CheckClaimStatus(), emitting events:
//
//     chora.tenancy.skillsfuture.claim.submitted.v1
//     chora.tenancy.skillsfuture.subsidy.approved.v1
//
//  2. Eligible-course catalogue sync — SyncEligibleCourses(), emitting:
//
//     chora.tenancy.skillsfuture.course.synced.v1 (per course)
//
// SkillsFuture is part of the Content Delivery domain in spirit (training
// catalogue + classroom flow), but the *commercial* claim flow lives in
// Tenancy/Billing because the subsidy reconciles against tenant invoices.
// The two domains coordinate via Pub/Sub events ONLY (no cross-DB).
//
// Endpoint base URL + API key sourced from env (SKILLSFUTURE_API_URL and
// SKILLSFUTURE_API_KEY) — never inline. Sandbox vs production differ only
// by env-injected values; the adapter shape is identical.
package skillsfuture

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/env"
)

// -----------------------------------------------------------------------------
// Config — all values sourced from env vars (no inline config).
// -----------------------------------------------------------------------------

// Config holds the SkillsFuture sandbox client configuration. Sandbox values:
//
//	SKILLSFUTURE_API_URL  → https://stg-api.skillsfuture.gov.sg
//	SKILLSFUTURE_API_KEY  → assigned via SSG developer portal (Secret Manager)
//
// Production switches APIURL to api.skillsfuture.gov.sg.
type Config struct {
	APIURL     string
	APIKey     string
	HTTPClient *http.Client
	Publisher  Publisher
}

// Client is the SkillsFuture sandbox adapter.
type Client struct {
	cfg        Config
	httpClient *http.Client
}

// New constructs a Client. Validation is lazy (per call) — same pattern as
// the Singpass adapter and the Stripe stub, so partially-configured clients
// can still be unit-tested in isolation.
func New(cfg Config) *Client {
	c := &Client{cfg: cfg, httpClient: cfg.HTTPClient}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return c
}

// -----------------------------------------------------------------------------
// Eligible-course catalogue sync.
// -----------------------------------------------------------------------------

// EligibleCourse mirrors the SSG /v1/courses entry. course_id format is
// "TGS-{year}{seq}" — the SSG canonical course code that learners must
// reference when claiming.
type EligibleCourse struct {
	CourseID         string `json:"course_id"`
	Title            string `json:"title"`
	SubsidyAmountSGD int    `json:"subsidy_amount_sgd"`
}

type courseListEnvelope struct {
	Courses []EligibleCourse `json:"courses"`
}

// SyncEligibleCourses fetches the SSG-eligible course catalogue for the given
// tenant. Each returned course produces a chora.tenancy.skillsfuture.course.synced.v1
// event so subscribers (e.g. chora-delivery course catalogue) can update their
// local cache.
//
// traceparent is the W3C trace context propagated from the caller (mandatory
// per CLAUDE.md §6 "Trace context across Pub/Sub").
func (c *Client) SyncEligibleCourses(ctx context.Context, tenantID, traceparent string) ([]EligibleCourse, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return nil, errors.New("skillsfuture: SKILLSFUTURE_API_URL not configured")
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, errors.New("skillsfuture: tenant_id is required")
	}

	endpoint := strings.TrimRight(c.cfg.APIURL, "/") + "/v1/courses"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: build courses request: %w", err)
	}
	c.applyAuth(req)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: courses request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: read courses response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("skillsfuture: courses status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var env courseListEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("skillsfuture: decode courses response: %w", err)
	}

	if c.cfg.Publisher != nil {
		for _, course := range env.Courses {
			envel := newEnvelope(tenantID, "", traceparent, "")
			payload := map[string]any{
				"tenant_id":          tenantID,
				"course_id":          course.CourseID,
				"title":              course.Title,
				"subsidy_amount_sgd": course.SubsidyAmountSGD,
				"synced_at":          envel.OccurredAt.Format(time.RFC3339Nano),
				"source":             "skillsfuture-sg-sandbox",
			}
			if err := c.cfg.Publisher.Publish("chora.tenancy.skillsfuture.course.synced.v1", envel, payload); err != nil {
				return env.Courses, fmt.Errorf("skillsfuture: publish course.synced: %w", err)
			}
		}
	}
	return env.Courses, nil
}

// -----------------------------------------------------------------------------
// SubmitClaim — POST /v1/claims, emits chora.tenancy.skillsfuture.claim.submitted.v1.
// -----------------------------------------------------------------------------

// SubmitClaimInput captures the parameters needed to submit a subsidy claim.
type SubmitClaimInput struct {
	TenantID    string
	GCID        string
	NRIC        string // S/T/F/G-prefixed Singapore NRIC/FIN
	CourseID    string // SSG TGS-XXXXXXX course code
	AmountSGD   int    // claim amount in whole SGD
	Traceparent string
	Tracestate  string
}

// ClaimResult mirrors the SSG /v1/claims response.
type ClaimResult struct {
	ClaimID     string `json:"claim_id"`
	Status      string `json:"status"`
	SubmittedAt string `json:"submitted_at"`
}

// SubmitClaim posts a subsidy claim to the SSG API and emits the
// chora.tenancy.skillsfuture.claim.submitted.v1 event.
func (c *Client) SubmitClaim(ctx context.Context, in SubmitClaimInput) (*ClaimResult, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return nil, errors.New("skillsfuture: SKILLSFUTURE_API_URL not configured")
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, errors.New("skillsfuture: tenant_id is required")
	}
	if strings.TrimSpace(in.GCID) == "" {
		return nil, errors.New("skillsfuture: gcid is required")
	}
	if strings.TrimSpace(in.NRIC) == "" {
		return nil, errors.New("skillsfuture: nric is required")
	}
	if strings.TrimSpace(in.CourseID) == "" {
		return nil, errors.New("skillsfuture: course_id is required")
	}
	if in.AmountSGD <= 0 {
		return nil, errors.New("skillsfuture: amount_sgd must be > 0")
	}

	body, err := json.Marshal(map[string]any{
		"tenant_id":  in.TenantID,
		"gcid":       in.GCID,
		"nric":       in.NRIC,
		"course_id":  in.CourseID,
		"amount_sgd": in.AmountSGD,
	})
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: marshal claim body: %w", err)
	}

	endpoint := strings.TrimRight(c.cfg.APIURL, "/") + "/v1/claims"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: build claim request: %w", err)
	}
	c.applyAuth(req)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: claim request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: read claim response: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("skillsfuture: claim status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var res ClaimResult
	if err := json.Unmarshal(respBody, &res); err != nil {
		return nil, fmt.Errorf("skillsfuture: decode claim response: %w", err)
	}
	if res.ClaimID == "" {
		return nil, errors.New("skillsfuture: claim response empty claim_id")
	}

	if c.cfg.Publisher != nil {
		envel := newEnvelope(in.TenantID, in.GCID, in.Traceparent, in.Tracestate)
		payload := map[string]any{
			"tenant_id":    in.TenantID,
			"gcid":         in.GCID,
			"claim_id":     res.ClaimID,
			"course_id":    in.CourseID,
			"amount_sgd":   in.AmountSGD,
			"status":       res.Status,
			"submitted_at": res.SubmittedAt,
			"source":       "skillsfuture-sg-sandbox",
		}
		if err := c.cfg.Publisher.Publish("chora.tenancy.skillsfuture.claim.submitted.v1", envel, payload); err != nil {
			return &res, fmt.Errorf("skillsfuture: publish claim.submitted: %w", err)
		}
	}
	return &res, nil
}

// -----------------------------------------------------------------------------
// CheckClaimStatus — GET /v1/claims/{id}; on APPROVED emits subsidy.approved.v1.
// -----------------------------------------------------------------------------

// CheckClaimStatusInput captures the polling parameters.
type CheckClaimStatusInput struct {
	TenantID    string
	GCID        string
	ClaimID     string
	Traceparent string
	Tracestate  string
}

// ClaimStatus mirrors the SSG /v1/claims/{id} response.
type ClaimStatus struct {
	ClaimID          string `json:"claim_id"`
	Status           string `json:"status"`
	SubsidyAmountSGD int    `json:"subsidy_amount_sgd"`
	ApprovedAt       string `json:"approved_at"`
}

// CheckClaimStatus polls the SSG API for the current status of a claim and,
// when status transitions to APPROVED, emits
// chora.tenancy.skillsfuture.subsidy.approved.v1 so downstream consumers
// (chora-billing, chora-delivery certificate, learner notification) can react.
//
// The caller is responsible for idempotency at the consumer side — repeated
// calls to CheckClaimStatus on an already-APPROVED claim will emit duplicate
// envelope-versioned events whose idempotency_key downstream handlers
// dedupe on (per data-consistency skill).
func (c *Client) CheckClaimStatus(ctx context.Context, in CheckClaimStatusInput) (*ClaimStatus, error) {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return nil, errors.New("skillsfuture: SKILLSFUTURE_API_URL not configured")
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, errors.New("skillsfuture: tenant_id is required")
	}
	if strings.TrimSpace(in.ClaimID) == "" {
		return nil, errors.New("skillsfuture: claim_id is required")
	}

	endpoint := strings.TrimRight(c.cfg.APIURL, "/") + "/v1/claims/" + in.ClaimID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: build status request: %w", err)
	}
	c.applyAuth(req)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: status request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("skillsfuture: read status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("skillsfuture: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var status ClaimStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("skillsfuture: decode status response: %w", err)
	}

	if status.Status == "APPROVED" && c.cfg.Publisher != nil {
		envel := newEnvelope(in.TenantID, in.GCID, in.Traceparent, in.Tracestate)
		payload := map[string]any{
			"tenant_id":          in.TenantID,
			"gcid":               in.GCID,
			"claim_id":           status.ClaimID,
			"subsidy_amount_sgd": status.SubsidyAmountSGD,
			"approved_at":        status.ApprovedAt,
			"source":             "skillsfuture-sg-sandbox",
		}
		if err := c.cfg.Publisher.Publish("chora.tenancy.skillsfuture.subsidy.approved.v1", envel, payload); err != nil {
			return &status, fmt.Errorf("skillsfuture: publish subsidy.approved: %w", err)
		}
	}
	return &status, nil
}

// -----------------------------------------------------------------------------
// HTTP helpers.
// -----------------------------------------------------------------------------

// applyAuth attaches the Bearer auth header (only if APIKey is set so that
// tests using a partially-configured client still exercise the request path).
func (c *Client) applyAuth(req *http.Request) {
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
}

// -----------------------------------------------------------------------------
// Event publisher port — kept locally so the adapter has no cross-package
// dependency on a particular publisher implementation.
// -----------------------------------------------------------------------------

// EventEnvelope mirrors the mandatory fields of chora.common.v1.EventEnvelope.
type EventEnvelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
}

// Publisher is the port used to emit SkillsFuture events.
type Publisher interface {
	Publish(topic string, env EventEnvelope, payload map[string]any) error
}

// PublishedRecord is the test-friendly capture shape for recorder publishers.
type PublishedRecord struct {
	Topic          string
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	Traceparent    string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
	Payload        map[string]any
}

// SourceProject + SourceService for envelope provenance.
const (
	SourceService = "chora-tenancy"
)

// SourceProject + SourceService for envelope provenance.
//
// Was a hardcoded literal until 2026-09-02 (CHO-2419). No manifest could reach
// it, so a second org stamped every event with chora-489812 and any consumer
// filtering on source_project would have been filtering on a lie.
// CHORA_SOURCE_PROJECT is now set on every event-emitting service; the literal
// stays as the fallback so this estate is provably unchanged.
var SourceProject = env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812")

func newEnvelope(tenantID, gcid, traceparent, tracestate string) EventEnvelope {
	now := time.Now().UTC()
	id := newUUIDv7()
	return EventEnvelope{
		EventID:        id,
		IdempotencyKey: id,
		TenantID:       tenantID,
		GCID:           gcid,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    traceparent,
		Tracestate:     tracestate,
		SourceProject:  SourceProject,
		SourceService:  SourceService,
		SchemaVersion:  1,
	}
}

// newUUIDv7 returns a freshly generated UUIDv7 string (RFC 9562 §5.7).
// Mirrors internal/domain/tenancy.NewUUIDv7 to keep the skeleton dep-free.
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
	// Random tail (10 bytes) — read full 16 then overwrite low 10.
	var randTail [10]byte
	_, _ = rand.Read(randTail[:])
	copy(b[6:], randTail[:])
	// Set version (7) in high nibble of byte 6.
	b[6] = (b[6] & 0x0f) | 0x70
	// Set variant (RFC 4122) in high two bits of byte 8.
	b[8] = (b[8] & 0x3f) | 0x80
	hexstr := hex.EncodeToString(b[:])
	// Format 8-4-4-4-12.
	return hexstr[0:8] + "-" + hexstr[8:12] + "-" + hexstr[12:16] + "-" + hexstr[16:20] + "-" + hexstr[20:32]
}
