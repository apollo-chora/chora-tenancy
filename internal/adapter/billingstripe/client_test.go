// Package billingstripe_test — Stripe Charges API client for the daily
// reconciliation Cloud Run Job. Ported from
// services/chora-billing-webhook/internal/adapter/stripe as part of the
// M12.2 Batch-1 consolidation.
package billingstripe_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/billingstripe"
)

func TestNew_DefaultsApplied(t *testing.T) {
	t.Parallel()
	c := billingstripe.New(billingstripe.Config{APIBase: "https://api.stripe.com", SecretKey: "sk_test_x"})
	if c == nil {
		t.Fatal("New returned nil")
	}
}

func TestFetchCapturedCentsForDay_RejectsEmptyAPIBase(t *testing.T) {
	t.Parallel()
	c := billingstripe.New(billingstripe.Config{APIBase: "", SecretKey: "sk_test_x"})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("expected error on empty APIBase")
	}
}

func TestFetchCapturedCentsForDay_RejectsEmptySecretKey(t *testing.T) {
	t.Parallel()
	c := billingstripe.New(billingstripe.Config{APIBase: "https://api.stripe.com", SecretKey: ""})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("expected error on empty SecretKey")
	}
}

func TestFetchCapturedCentsForDay_HappyPath_SinglePage(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/charges") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("created[gte]") == "" || q.Get("created[lt]") == "" {
			t.Fatalf("missing time-range query params: %s", r.URL.RawQuery)
		}
		if q.Get("limit") != "100" {
			t.Fatalf("limit query = %s", q.Get("limit"))
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "sk_test_xyz" || pass != "" {
			t.Fatalf("basic auth: got user=%q pass=%q ok=%v", user, pass, ok)
		}
		body, _ := json.Marshal(map[string]any{
			"object":   "list",
			"has_more": false,
			"data": []map[string]any{
				{"id": "ch_1", "status": "succeeded", "captured": true, "amount_captured": 100_000, "currency": "usd"},
				{"id": "ch_2", "status": "succeeded", "captured": true, "amount_captured": 50_000, "currency": "usd"},
				{"id": "ch_3", "status": "failed", "captured": false, "amount_captured": 0, "currency": "usd"},
			},
		})
		_, _ = w.Write(body)
	}))
	defer server.Close()

	c := billingstripe.New(billingstripe.Config{
		APIBase:   server.URL,
		SecretKey: "sk_test_xyz",
	})
	day := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	got, err := c.FetchCapturedCentsForDay(context.Background(), day)
	if err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
	if got != 150_000 {
		t.Errorf("got=%d want=150000", got)
	}
}

func TestFetchCapturedCentsForDay_PaginationLoop(t *testing.T) {
	t.Parallel()
	var page atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startingAfter := r.URL.Query().Get("starting_after")
		switch page.Add(1) {
		case 1:
			if startingAfter != "" {
				t.Fatalf("page 1 should have no starting_after, got %q", startingAfter)
			}
			body, _ := json.Marshal(map[string]any{
				"object":   "list",
				"has_more": true,
				"data": []map[string]any{
					{"id": "ch_p1_a", "status": "succeeded", "captured": true, "amount_captured": 100, "currency": "usd"},
					{"id": "ch_p1_b", "status": "succeeded", "captured": true, "amount_captured": 200, "currency": "usd"},
				},
			})
			_, _ = w.Write(body)
		case 2:
			if startingAfter != "ch_p1_b" {
				t.Fatalf("page 2 starting_after = %q; want ch_p1_b", startingAfter)
			}
			body, _ := json.Marshal(map[string]any{
				"object":   "list",
				"has_more": false,
				"data": []map[string]any{
					{"id": "ch_p2_a", "status": "succeeded", "captured": true, "amount_captured": 700, "currency": "usd"},
				},
			})
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected page request: page=%d", page.Load())
		}
	}))
	defer server.Close()

	c := billingstripe.New(billingstripe.Config{APIBase: server.URL, SecretKey: "sk_test_x"})
	got, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
	if got != 1000 {
		t.Errorf("got=%d want=1000", got)
	}
}

func TestFetchCapturedCentsForDay_RetryOn5xx(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			http.Error(w, "stripe down", http.StatusInternalServerError)
			return
		}
		body, _ := json.Marshal(map[string]any{
			"object":   "list",
			"has_more": false,
			"data": []map[string]any{
				{"id": "ch_after_retry", "status": "succeeded", "captured": true, "amount_captured": 42, "currency": "usd"},
			},
		})
		_, _ = w.Write(body)
	}))
	defer server.Close()

	c := billingstripe.New(billingstripe.Config{
		APIBase:        server.URL,
		SecretKey:      "sk_test_x",
		MaxRetries:     3,
		RetryBaseDelay: time.Millisecond,
	})
	got, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
	if got != 42 {
		t.Errorf("got=%d want=42", got)
	}
	if a := attempts.Load(); a < 2 {
		t.Errorf("attempts=%d; want ≥2", a)
	}
}

func TestFetchCapturedCentsForDay_RetriesExhausted(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "stripe down", http.StatusInternalServerError)
	}))
	defer server.Close()

	c := billingstripe.New(billingstripe.Config{
		APIBase:        server.URL,
		SecretKey:      "sk_test_x",
		MaxRetries:     2,
		RetryBaseDelay: time.Millisecond,
	})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("expected error after retries exhausted")
	}
}

func TestFetchCapturedCentsForDay_4xxNotRetried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	c := billingstripe.New(billingstripe.Config{
		APIBase:        server.URL,
		SecretKey:      "sk_test_x",
		MaxRetries:     5,
		RetryBaseDelay: time.Millisecond,
	})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("expected error on 400")
	}
	if a := attempts.Load(); a != 1 {
		t.Errorf("attempts=%d; 4xx must NOT retry", a)
	}
}

func TestFetchCapturedCentsForDay_DayWindow(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gte, _ := strconv.ParseInt(r.URL.Query().Get("created[gte]"), 10, 64)
		lt, _ := strconv.ParseInt(r.URL.Query().Get("created[lt]"), 10, 64)
		want := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC).Unix()
		if gte != want {
			t.Errorf("gte=%d want=%d", gte, want)
		}
		if lt != want+86400 {
			t.Errorf("lt=%d want=%d", lt, want+86400)
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[],"has_more":false}`))
	}))
	defer server.Close()
	c := billingstripe.New(billingstripe.Config{APIBase: server.URL, SecretKey: "sk_test_x"})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
}

// errTransport always fails RoundTrip — exercises the transport-error retry
// branch of doRequestWithRetry.
type errTransport struct{}

func (errTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrServerClosed
}

func TestFetchCapturedCentsForDay_TransportError_RetriesThenFails(t *testing.T) {
	t.Parallel()
	c := billingstripe.New(billingstripe.Config{
		APIBase:        "https://api.stripe.com",
		SecretKey:      "sk_test_x",
		MaxRetries:     2,
		RetryBaseDelay: time.Millisecond,
		HTTPClient:     &http.Client{Transport: errTransport{}},
	})
	if _, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "do ") {
		t.Fatalf("expected transport error, got %v", err)
	}
}

func TestFetchCapturedCentsForDay_429RetriedThenOK(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"ch_1","status":"succeeded","captured":true,"amount_captured":9900,"currency":"sgd","created":1}],"has_more":false}`))
	}))
	defer srv.Close()
	c := billingstripe.New(billingstripe.Config{
		APIBase:        srv.URL,
		SecretKey:      "sk_test_x",
		MaxRetries:     3,
		RetryBaseDelay: time.Millisecond,
	})
	total, err := c.FetchCapturedCentsForDay(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("FetchCapturedCentsForDay: %v", err)
	}
	if total != 9900 {
		t.Fatalf("expected 9900, got %d", total)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 HTTP calls (1 retry), got %d", hits.Load())
	}
}

func TestFetchCapturedCentsForDay_CancelDuringRetry(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately so the retry sleep observes a closed context.
	cancel()
	c := billingstripe.New(billingstripe.Config{
		APIBase:        srv.URL,
		SecretKey:      "sk_test_x",
		MaxRetries:     3,
		RetryBaseDelay: time.Hour, // would block forever unless ctx cancels
	})
	if _, err := c.FetchCapturedCentsForDay(ctx, time.Now().UTC()); err == nil {
		t.Fatal("expected error on cancelled context")
	}
}
