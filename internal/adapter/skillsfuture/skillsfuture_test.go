// Package skillsfuture_test holds the RED-phase TDD specs for the
// SkillsFuture Singapore (SSG) sandbox API client.
//
// SkillsFuture is the SG government training-credit programme. The sandbox
// API exposes:
//
//  1. /v1/courses     — list eligible / certified courses for sync
//  2. /v1/claims      — submit a subsidy claim on behalf of a learner
//  3. /v1/claims/{id} — poll for approval status
//
// Three flows in this adapter:
//   - SyncEligibleCourses → calls /v1/courses, emits course.synced.v1 per course
//   - SubmitClaim         → POSTs to /v1/claims, emits claim.submitted.v1
//   - CheckClaimStatus    → GETs /v1/claims/{id}; on APPROVED emits subsidy.approved.v1
//
// Endpoint base from env (SKILLSFUTURE_API_URL); API key from env
// (SKILLSFUTURE_API_KEY) — no inline config.
package skillsfuture_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/skillsfuture"
)

const (
	testTenantID = "01970000-0000-7000-8000-0000000000aa"
	testGCID     = "gcid-1"
)

// -----------------------------------------------------------------------------
// Constructor + config validation.
// -----------------------------------------------------------------------------

func TestSkillsFutureClient_New_RequiresAPIURL(t *testing.T) {
	t.Parallel()
	c := skillsfuture.New(skillsfuture.Config{APIURL: "", APIKey: "k"})
	if _, err := c.SyncEligibleCourses(context.Background(), testTenantID, "00-aa-bb-01"); err == nil {
		t.Fatalf("expected error when SKILLSFUTURE_API_URL not set")
	}
}

// -----------------------------------------------------------------------------
// SyncEligibleCourses — calls /v1/courses, returns slice + emits per-course event.
// -----------------------------------------------------------------------------

func TestSkillsFutureClient_SyncEligibleCourses_ReturnsList(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/courses" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"courses": [
				{"course_id": "TGS-2026000001", "title": "Go Fundamentals", "subsidy_amount_sgd": 500},
				{"course_id": "TGS-2026000002", "title": "Cloud Architecture", "subsidy_amount_sgd": 800}
			]
		}`))
	}))
	defer srv.Close()

	pub := &recordingPublisher{}
	c := skillsfuture.New(skillsfuture.Config{
		APIURL:    srv.URL,
		APIKey:    "test-api-key",
		Publisher: pub,
	})

	courses, err := c.SyncEligibleCourses(context.Background(), testTenantID, "00-aa-bb-01")
	if err != nil {
		t.Fatalf("SyncEligibleCourses: %v", err)
	}
	if len(courses) != 2 {
		t.Fatalf("expected 2 courses, got %d", len(courses))
	}
	if courses[0].CourseID != "TGS-2026000001" {
		t.Errorf("courses[0].CourseID = %q", courses[0].CourseID)
	}
	if courses[1].SubsidyAmountSGD != 800 {
		t.Errorf("courses[1].Subsidy = %d", courses[1].SubsidyAmountSGD)
	}

	// One event published per course.
	got := pub.RecordedByTopic("chora.tenancy.skillsfuture.course.synced.v1")
	if len(got) != 2 {
		t.Fatalf("expected 2 course.synced events, got %d", len(got))
	}
	for _, rec := range got {
		if rec.TenantID != testTenantID {
			t.Errorf("envelope.tenant_id = %q", rec.TenantID)
		}
		if rec.Traceparent != "00-aa-bb-01" {
			t.Errorf("envelope.traceparent = %q", rec.Traceparent)
		}
		if rec.SourceService != "chora-tenancy" {
			t.Errorf("envelope.source_service = %q", rec.SourceService)
		}
		if rec.Payload["course_id"] == nil || rec.Payload["title"] == nil {
			t.Errorf("payload missing fields: %+v", rec.Payload)
		}
		if rec.SchemaVersion < 1 {
			t.Errorf("schema_version = %d", rec.SchemaVersion)
		}
	}
}

func TestSkillsFutureClient_SyncEligibleCourses_PropagatesNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal"}`))
	}))
	defer srv.Close()
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k"})
	if _, err := c.SyncEligibleCourses(context.Background(), testTenantID, "00-aa"); err == nil {
		t.Fatalf("expected error on 500")
	}
}

func TestSkillsFutureClient_SyncEligibleCourses_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	c := skillsfuture.New(skillsfuture.Config{APIURL: "https://x", APIKey: "k"})
	if _, err := c.SyncEligibleCourses(context.Background(), "", "00-aa"); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
}

// -----------------------------------------------------------------------------
// SubmitClaim — POST /v1/claims, emits claim.submitted.v1.
// -----------------------------------------------------------------------------

func TestSkillsFutureClient_SubmitClaim_HappyPath(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.Path != "/v1/claims" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body["nric"] != "S1234567A" {
			t.Errorf("body.nric = %v", body["nric"])
		}
		if body["course_id"] != "TGS-2026000001" {
			t.Errorf("body.course_id = %v", body["course_id"])
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"claim_id":     "CLM-100",
			"status":       "PENDING",
			"submitted_at": "2026-05-09T01:23:45Z",
		})
	}))
	defer srv.Close()

	pub := &recordingPublisher{}
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k", Publisher: pub})
	res, err := c.SubmitClaim(context.Background(), skillsfuture.SubmitClaimInput{
		TenantID:    testTenantID,
		GCID:        testGCID,
		NRIC:        "S1234567A",
		CourseID:    "TGS-2026000001",
		AmountSGD:   500,
		Traceparent: "00-aa-bb-02",
	})
	if err != nil {
		t.Fatalf("SubmitClaim: %v", err)
	}
	if res.ClaimID != "CLM-100" {
		t.Errorf("ClaimID = %q", res.ClaimID)
	}
	if res.Status != "PENDING" {
		t.Errorf("Status = %q", res.Status)
	}
	got := pub.RecordedByTopic("chora.tenancy.skillsfuture.claim.submitted.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 claim.submitted event, got %d", len(got))
	}
	rec := got[0]
	if rec.TenantID != testTenantID || rec.GCID != testGCID {
		t.Errorf("tenant/gcid mismatch: %+v", rec)
	}
	if rec.Payload["claim_id"] != "CLM-100" {
		t.Errorf("payload.claim_id = %v", rec.Payload["claim_id"])
	}
	if rec.Payload["course_id"] != "TGS-2026000001" {
		t.Errorf("payload.course_id = %v", rec.Payload["course_id"])
	}
	if rec.Payload["status"] != "PENDING" {
		t.Errorf("payload.status = %v", rec.Payload["status"])
	}
	if rec.SourceProject == "" || rec.SourceService != "chora-tenancy" {
		t.Errorf("envelope source mismatch: project=%q service=%q", rec.SourceProject, rec.SourceService)
	}
	if rec.IdempotencyKey == "" || rec.EventID == "" {
		t.Errorf("event_id / idempotency_key empty")
	}
}

func TestSkillsFutureClient_SubmitClaim_RejectsEmptyArgs(t *testing.T) {
	t.Parallel()
	c := skillsfuture.New(skillsfuture.Config{APIURL: "https://x", APIKey: "k"})
	cases := []skillsfuture.SubmitClaimInput{
		{TenantID: "", GCID: testGCID, NRIC: "S1234567A", CourseID: "TGS-1", AmountSGD: 100},
		{TenantID: testTenantID, GCID: "", NRIC: "S1234567A", CourseID: "TGS-1", AmountSGD: 100},
		{TenantID: testTenantID, GCID: testGCID, NRIC: "", CourseID: "TGS-1", AmountSGD: 100},
		{TenantID: testTenantID, GCID: testGCID, NRIC: "S1234567A", CourseID: "", AmountSGD: 100},
		{TenantID: testTenantID, GCID: testGCID, NRIC: "S1234567A", CourseID: "TGS-1", AmountSGD: 0},
	}
	for i, in := range cases {
		if _, err := c.SubmitClaim(context.Background(), in); err == nil {
			t.Errorf("case %d: expected error for missing required field", i)
		}
	}
}

func TestSkillsFutureClient_SubmitClaim_PropagatesNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_nric"}`))
	}))
	defer srv.Close()
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k"})
	_, err := c.SubmitClaim(context.Background(), skillsfuture.SubmitClaimInput{
		TenantID: testTenantID, GCID: testGCID, NRIC: "S1234567A", CourseID: "TGS-1", AmountSGD: 100,
	})
	if err == nil {
		t.Fatalf("expected error on 400")
	}
}

// -----------------------------------------------------------------------------
// CheckClaimStatus — GET /v1/claims/{id}, emits subsidy.approved.v1 on APPROVED.
// -----------------------------------------------------------------------------

func TestSkillsFutureClient_CheckClaimStatus_ApprovedEmitsEvent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/claims/CLM-100" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"claim_id": "CLM-100",
			"status": "APPROVED",
			"subsidy_amount_sgd": 500,
			"approved_at": "2026-05-09T02:00:00Z"
		}`))
	}))
	defer srv.Close()

	pub := &recordingPublisher{}
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k", Publisher: pub})
	status, err := c.CheckClaimStatus(context.Background(), skillsfuture.CheckClaimStatusInput{
		TenantID: testTenantID, GCID: testGCID, ClaimID: "CLM-100", Traceparent: "00-aa-bb-03",
	})
	if err != nil {
		t.Fatalf("CheckClaimStatus: %v", err)
	}
	if status.Status != "APPROVED" {
		t.Errorf("Status = %q", status.Status)
	}
	if status.SubsidyAmountSGD != 500 {
		t.Errorf("Subsidy = %d", status.SubsidyAmountSGD)
	}
	got := pub.RecordedByTopic("chora.tenancy.skillsfuture.subsidy.approved.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 subsidy.approved event, got %d", len(got))
	}
	if got[0].Payload["claim_id"] != "CLM-100" {
		t.Errorf("payload.claim_id = %v", got[0].Payload["claim_id"])
	}
	if got[0].Payload["subsidy_amount_sgd"] == nil {
		t.Errorf("payload missing subsidy_amount_sgd")
	}
}

func TestSkillsFutureClient_CheckClaimStatus_PendingDoesNotEmit(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"claim_id":"CLM-100","status":"PENDING","subsidy_amount_sgd":0}`))
	}))
	defer srv.Close()

	pub := &recordingPublisher{}
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k", Publisher: pub})
	if _, err := c.CheckClaimStatus(context.Background(), skillsfuture.CheckClaimStatusInput{
		TenantID: testTenantID, GCID: testGCID, ClaimID: "CLM-100",
	}); err != nil {
		t.Fatalf("CheckClaimStatus: %v", err)
	}
	if got := pub.RecordedByTopic("chora.tenancy.skillsfuture.subsidy.approved.v1"); len(got) != 0 {
		t.Fatalf("expected no events for PENDING, got %d", len(got))
	}
}

func TestSkillsFutureClient_CheckClaimStatus_RejectsEmptyArgs(t *testing.T) {
	t.Parallel()
	c := skillsfuture.New(skillsfuture.Config{APIURL: "https://x", APIKey: "k"})
	cases := []skillsfuture.CheckClaimStatusInput{
		{TenantID: "", GCID: testGCID, ClaimID: "CLM-1"},
		{TenantID: testTenantID, GCID: testGCID, ClaimID: ""},
	}
	for i, in := range cases {
		if _, err := c.CheckClaimStatus(context.Background(), in); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestSkillsFutureClient_CheckClaimStatus_PropagatesNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k"})
	_, err := c.CheckClaimStatus(context.Background(), skillsfuture.CheckClaimStatusInput{
		TenantID: testTenantID, GCID: testGCID, ClaimID: "missing",
	})
	if err == nil {
		t.Fatalf("expected error on 404")
	}
}

func TestSkillsFutureClient_CheckClaimStatus_RejectsBadJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer srv.Close()
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k"})
	if _, err := c.CheckClaimStatus(context.Background(), skillsfuture.CheckClaimStatusInput{
		TenantID: testTenantID, GCID: testGCID, ClaimID: "CLM-1",
	}); err == nil {
		t.Fatalf("expected JSON decode error")
	}
}

// -----------------------------------------------------------------------------
// Test helper — minimal Publisher recorder.
// -----------------------------------------------------------------------------

type recordingPublisher struct {
	records []skillsfuture.PublishedRecord
}

func (r *recordingPublisher) Publish(topic string, env skillsfuture.EventEnvelope, payload map[string]any) error {
	r.records = append(r.records, skillsfuture.PublishedRecord{
		Topic:          topic,
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		Traceparent:    env.Traceparent,
		SourceProject:  env.SourceProject,
		SourceService:  env.SourceService,
		SchemaVersion:  env.SchemaVersion,
		Payload:        payload,
	})
	return nil
}

func (r *recordingPublisher) RecordedByTopic(topic string) []skillsfuture.PublishedRecord {
	out := make([]skillsfuture.PublishedRecord, 0)
	for _, rec := range r.records {
		if rec.Topic == topic {
			out = append(out, rec)
		}
	}
	return out
}

// Transport-level failures exercise the HTTP Do error branches.
func TestSkillsFutureClient_TransportErrors(t *testing.T) {
	t.Parallel()
	mk := func() *skillsfuture.Client {
		return skillsfuture.New(skillsfuture.Config{APIURL: "https://api.skillsfuture.example", APIKey: "k", HTTPClient: &http.Client{Transport: errTransport{}}})
	}
	if _, err := mk().SyncEligibleCourses(context.Background(), testTenantID, "00-aa-bb-01"); err == nil {
		t.Fatal("expected transport error on courses")
	}
	if _, err := mk().SubmitClaim(context.Background(), skillsfuture.SubmitClaimInput{
		TenantID: testTenantID, GCID: testGCID, NRIC: "S1234567A", CourseID: "c-1", AmountSGD: 100,
	}); err == nil {
		t.Fatal("expected transport error on claim submit")
	}
	if _, err := mk().CheckClaimStatus(context.Background(), skillsfuture.CheckClaimStatusInput{
		TenantID: testTenantID, ClaimID: "cl-1",
	}); err == nil {
		t.Fatal("expected transport error on claim status")
	}
}

// Malformed JSON body on the courses endpoint.
func TestSkillsFutureClient_SyncEligibleCourses_RejectsBadJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer srv.Close()
	c := skillsfuture.New(skillsfuture.Config{APIURL: srv.URL, APIKey: "k"})
	if _, err := c.SyncEligibleCourses(context.Background(), testTenantID, "00-aa-bb-01"); err == nil {
		t.Fatal("expected decode error")
	}
}

type errTransport struct{}

func (errTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrServerClosed
}
