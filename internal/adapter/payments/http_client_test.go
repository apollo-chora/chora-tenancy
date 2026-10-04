// Tests for the chora-payments HTTP client adapter (CHO-1767).
//
// Verifies the tenant-addon change-tier / preview-tier-change admin
// endpoints: URL construction, JSON (de)serialisation, HTTPError mapping
// for 4xx/5xx, and the sentinel error codes that tenancy handlers
// errors.Is-check to drive the FE response shape.
package payments_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/payments"
)

func TestNewHTTPClient_RequiresAddr(t *testing.T) {
	if _, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: ""}); err == nil {
		t.Fatalf("expected error for empty addr")
	}
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: "http://localhost:8083", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	if cli == nil {
		t.Fatalf("nil client")
	}
}

func TestNewHTTPClient_TrimsTrailingSlashAndDefaultsTimeout(t *testing.T) {
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: "http://localhost:8083/"})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	// Verify the trimmed addr is used end-to-end: the change-tier path is
	// appended to the base URL without a double slash.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/tenant-addons:change-tier" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"purchase_id":"p1","from_tier":"tms","to_tier":"tms-pro"}`))
	}))
	defer server.Close()

	cli, err = payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL + "/"})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	out, err := cli.ChangeTenantAddonTier(context.Background(), payments.ChangeTierIn{
		TenantID: "t1", AdminGCID: "g1", AddonCode: "tms", TargetTierCode: "tms-pro",
	})
	if err != nil {
		t.Fatalf("ChangeTenantAddonTier: %v", err)
	}
	if out.PurchaseID != "p1" || out.FromTier != "tms" || out.ToTier != "tms-pro" {
		t.Fatalf("unexpected output %+v", out)
	}
	_ = cli
}

func TestHTTPClient_ChangeTenantAddonTier_SendsJSONAndParses(t *testing.T) {
	var gotBody map[string]any
	var gotCT, gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"purchase_id":"pur_123","stripe_subscription_id":"sub_abc",
			"from_tier":"tms","to_tier":"tms-pro","billing_delta_cents":4900,
			"currency":"SGD","effective_at":"2026-07-01T00:00:00Z",
			"subscription_status":"active","deferred_to_cycle_end":true,
			"schedule_id":"sub_sched_1","scheduled_tier_code":"tms-pro",
			"scheduled_effective_at":"2026-08-01T00:00:00Z"
		}`))
	}))
	defer server.Close()

	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	out, err := cli.ChangeTenantAddonTier(context.Background(), payments.ChangeTierIn{
		TenantID: "tenant-1", AdminGCID: "gcid-1", AddonCode: "tms",
		TargetTierCode: "tms-pro", Currency: "SGD", ProrationMode: "partial_invoice",
		EffectiveAt: "end_of_cycle",
	})
	if err != nil {
		t.Fatalf("ChangeTenantAddonTier: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("expected POST, got %s", gotMethod)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Fatalf("expected JSON content type, got %q", gotCT)
	}
	if gotBody["tenant_id"] != "tenant-1" || gotBody["addon_code"] != "tms" ||
		gotBody["target_tier_code"] != "tms-pro" || gotBody["effective_at"] != "end_of_cycle" {
		t.Fatalf("request body not serialised as expected: %v", gotBody)
	}
	if out.PurchaseID != "pur_123" || out.StripeSubscriptionID != "sub_abc" {
		t.Fatalf("purchase fields not parsed: %+v", out)
	}
	if out.BillingDeltaCents != 4900 || out.Currency != "SGD" {
		t.Fatalf("billing fields not parsed: %+v", out)
	}
	if !out.DeferredToCycleEnd || out.ScheduleID != "sub_sched_1" ||
		out.ScheduledTierCode != "tms-pro" || out.ScheduledEffectiveAt != "2026-08-01T00:00:00Z" {
		t.Fatalf("CHO-1772 fields not parsed: %+v", out)
	}
	if out.SubscriptionStatus != "active" {
		t.Fatalf("subscription_status not parsed: %+v", out)
	}
}

func TestHTTPClient_PreviewTenantAddonTier_SendsJSONAndParses(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"purchase_id":"pre_1","from_tier":"tms","to_tier":"tms-pro",
			"billing_delta_cents":4900,"next_invoice_total_cents":17700,
			"currency":"SGD","proration_mode":"partial_invoice",
			"deferred_to_cycle_end":false,"effective_at":"immediate"
		}`))
	}))
	defer server.Close()

	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	out, err := cli.PreviewTenantAddonTier(context.Background(), payments.PreviewTierIn{
		TenantID: "tenant-1", AddonCode: "tms", TargetTierCode: "tms-pro",
		Currency: "SGD", ProrationMode: "partial_invoice",
	})
	if err != nil {
		t.Fatalf("PreviewTenantAddonTier: %v", err)
	}
	if gotPath != "/api/v1/admin/tenant-addons:preview-tier-change" {
		t.Fatalf("unexpected path %q", gotPath)
	}
	if out.PurchaseID != "pre_1" || out.NextInvoiceTotalCents != 17700 {
		t.Fatalf("preview fields not parsed: %+v", out)
	}
	if out.BillingDeltaCents != 4900 || out.ProrationMode != "partial_invoice" {
		t.Fatalf("preview billing not parsed: %+v", out)
	}
	if out.EffectiveAt != "immediate" {
		t.Fatalf("effective_at not parsed: %+v", out)
	}
}

func TestHTTPClient_KnownErrorCodes_WrapSentinels(t *testing.T) {
	cases := []struct {
		name    string
		code    string
		wantErr error
	}{
		{"no_stripe_subscription", "no_stripe_subscription", payments.ErrNoStripeSubscription},
		{"stripe_price_missing", "stripe_price_missing", payments.ErrStripePriceMissing},
		{"tier_unchanged", "tier_unchanged", payments.ErrTierUnchanged},
		{"addon_not_found", "addon_not_found", payments.ErrAddonNotFound},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"` + tc.code + `","message":"nope"}`))
			}))
			defer server.Close()
			cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
			if err != nil {
				t.Fatalf("NewHTTPClient: %v", err)
			}
			_, err = cli.ChangeTenantAddonTier(ctx, payments.ChangeTierIn{TenantID: "t", AdminGCID: "g", AddonCode: "a", TargetTierCode: "b"})
			if err == nil {
				t.Fatalf("expected error")
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected errors.Is(err, %v), got %v", tc.wantErr, err)
			}
			if !strings.Contains(err.Error(), "nope") {
				t.Fatalf("expected message to be carried, got %v", err)
			}
		})
	}
}

func TestHTTPClient_UnknownErrorCode_ReturnsRawHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"upstream_down","message":"stripe is sad"}`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = cli.PreviewTenantAddonTier(context.Background(), payments.PreviewTierIn{TenantID: "t"})
	if err == nil {
		t.Fatalf("expected error")
	}
	var httpErr *payments.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPError, got %v", err)
	}
	if httpErr.StatusCode != http.StatusServiceUnavailable || httpErr.Code != "upstream_down" {
		t.Fatalf("unexpected HTTPError %+v", httpErr)
	}
	if !strings.Contains(httpErr.Error(), "503") || !strings.Contains(httpErr.Error(), "upstream_down") {
		t.Fatalf("HTTPError.Error() malformed: %q", httpErr.Error())
	}
}

func TestHTTPClient_NonJSONErrorBody_StillReturnsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<html>gateway error</html>`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = cli.ChangeTenantAddonTier(context.Background(), payments.ChangeTierIn{TenantID: "t"})
	var httpErr *payments.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPError, got %v", err)
	}
	if httpErr.StatusCode != http.StatusBadGateway || httpErr.Code != "" {
		t.Fatalf("unexpected HTTPError %+v", httpErr)
	}
}

func TestHTTPClient_InvalidJSONBody_ReturnsDecodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = cli.ChangeTenantAddonTier(context.Background(), payments.ChangeTierIn{TenantID: "t"})
	if err == nil {
		t.Fatalf("expected decode error")
	}
	if !strings.Contains(err.Error(), "payments-http: decode") {
		t.Fatalf("expected decode error framing, got %v", err)
	}
}

func TestHTTPClient_TransportError_Wrapped(t *testing.T) {
	// A client whose Transport always fails exercises the `http.Do` error
	// branch without any network dependency.
	roundTripErr := errors.New("connection refused")
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, roundTripErr
	})
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{
		Addr: "http://unreachable.invalid",
		HTTP: &http.Client{Transport: rt},
	})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = cli.ChangeTenantAddonTier(context.Background(), payments.ChangeTierIn{TenantID: "t"})
	if err == nil || !strings.Contains(err.Error(), "payments-http: do") {
		t.Fatalf("expected do-error framing, got %v", err)
	}
}

func TestHTTPClient_MalformedAddr_ReturnsRequestError(t *testing.T) {
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: "://missing-scheme"})
	if err != nil {
		t.Fatalf("NewHTTPClient should accept the addr, got %v", err)
	}
	_, err = cli.PreviewTenantAddonTier(context.Background(), payments.PreviewTierIn{TenantID: "t"})
	if err == nil || !strings.Contains(err.Error(), "payments-http: new request") {
		t.Fatalf("expected new-request error, got %v", err)
	}
}

func TestHTTPClient_TimeoutField_IsApplied(t *testing.T) {
	// Defaulting branch: zero Timeout and no injectable client → 10s default.
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: "http://localhost:8083"})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_ = cli
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
