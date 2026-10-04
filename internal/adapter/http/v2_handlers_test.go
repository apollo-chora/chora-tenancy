// Package httpapi_test contains tests for the v2 HTTP handlers that wire
// the new sub-domains (tenant lifecycle + members + add-ons + billing).
//
// We mount the v2 server on /v2/* routes so it does NOT collide with the
// existing legacy `/api/*` routes (those are still tested in handlers_test.go).
package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
)

const (
	v2tenantHeader = "01970000-0000-7000-8000-000000000099"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newV2Server() http.Handler {
	return httpapi.NewV2Server(httpapi.NewDefaultV2Deps())
}

func doV2(t *testing.T, srv http.Handler, method, path string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", v2tenantHeader)
	req.Header.Set("X-GCID", "gcid-test")
	// CHO-2077: root-tenant create/list (/v2/tenants) is platform_operator-only;
	// stamp the operator mesh role. The /v2/tenants/{id}/* sub-routes this helper
	// also drives carry no role gate, so it is harmless there.
	req.Header.Set("x-mesh-user-roles", "platform_operator")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

func doV2List(t *testing.T, srv http.Handler, method, path string) (*httptest.ResponseRecorder, []interface{}, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-Tenant-Id", v2tenantHeader)
	req.Header.Set("X-GCID", "gcid-test")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	out := map[string]interface{}{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	items, _ := out["items"].([]interface{})
	return rec, items, out
}

// ---------------------------------------------------------------------------
// 1. Tenant lifecycle
// ---------------------------------------------------------------------------

func TestV2_CreateTenant_ReturnsCreated(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	rec, body := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme School",
		"owner_gcid":   "gcid-owner-1",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if _, ok := body["id"].(string); !ok {
		t.Fatalf("expected id in response")
	}
}

func TestV2_GetTenant_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	rec, _ := doV2(t, srv, "GET", "/v2/tenants/no-such-id", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestV2_PatchTenant_UpdatesDisplayNameAndBranding(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, body := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme",
		"owner_gcid":   "gcid-owner-1",
	})
	id, _ := body["id"].(string)
	rec, patched := doV2(t, srv, "PATCH", "/v2/tenants/"+id, map[string]interface{}{
		"display_name": "Acme Renamed",
		"branding": map[string]interface{}{
			"primary_color_hex": "#00B4FF",
			"logo_url":          "https://cdn.acme.com/logo.png",
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %v)", rec.Code, patched)
	}
	if dn, _ := patched["display_name"].(string); dn != "Acme Renamed" {
		t.Fatalf("expected updated display_name, got %q", dn)
	}
}

// Sub-tenant creation under existing parent → 201; under unknown parent → 404.
func TestV2_CreateSubTenant_ReturnsCreated(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Parent",
		"owner_gcid":   "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, sub := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/sub-tenants", map[string]interface{}{
		"display_name": "Sub",
		"owner_gcid":   "gcid-2",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, sub)
	}
	if pid, _ := sub["parent_tenant_id"].(string); pid != parentID {
		t.Fatalf("expected parent_tenant_id=%s, got %q", parentID, pid)
	}
}

func TestV2_CreateSubTenant_404OnUnknownParent(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	rec, _ := doV2(t, srv, "POST", "/v2/tenants/no-such-parent/sub-tenants", map[string]interface{}{
		"display_name": "Sub",
		"owner_gcid":   "gcid-1",
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// 2. Member management
// ---------------------------------------------------------------------------

func TestV2_BulkInviteMembers_DedupesAndReports(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme",
		"owner_gcid":   "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, body := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "alice@acme.com", "roles": []string{"learner"}},
			{"email": "ALICE@acme.com", "roles": []string{"learner"}}, // duplicate
			{"email": "bob@acme.com", "roles": []string{"instructor"}},
		},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if c, _ := body["invited_count"].(float64); int(c) != 2 {
		t.Fatalf("expected invited_count=2, got %v", body["invited_count"])
	}
	if d, _ := body["duplicate_count"].(float64); int(d) != 1 {
		t.Fatalf("expected duplicate_count=1, got %v", body["duplicate_count"])
	}
}

func TestV2_BulkInviteMembers_RejectsInvalidEmail(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	_, body := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "bad-email", "roles": []string{"learner"}},
		},
	})
	if r, _ := body["rejected_count"].(float64); int(r) != 1 {
		t.Fatalf("expected rejected_count=1, got %v", body["rejected_count"])
	}
}

func TestV2_ListMembers_FiltersByRole(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "a@acme.com", "roles": []string{"learner"}},
			{"email": "b@acme.com", "roles": []string{"instructor"}},
		},
	})
	rec, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/members?role=instructor")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 instructor, got %d", len(items))
	}
}

func TestV2_UpdateMemberRole(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "a@acme.com", "roles": []string{"learner"}},
		},
	})
	_, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/members")
	if len(items) != 1 {
		t.Fatalf("expected 1 invited member")
	}
	first := items[0].(map[string]interface{})
	memberID, _ := first["id"].(string)
	rec, body := doV2(t, srv, "PATCH", "/v2/tenants/"+parentID+"/members/"+memberID, map[string]interface{}{
		"roles": []string{"instructor"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %v)", rec.Code, body)
	}
}

func TestV2_SuspendMember_RequiresReason(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "a@acme.com", "roles": []string{"learner"}},
		},
	})
	_, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/members")
	first := items[0].(map[string]interface{})
	memberID, _ := first["id"].(string)
	// Empty reason → 400
	rec, _ := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members/"+memberID+":suspend", map[string]interface{}{
		"reason": "",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty reason, got %d", rec.Code)
	}
	// Valid reason → 200
	rec, _ = doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members/"+memberID+":suspend", map[string]interface{}{
		"reason": "policy violation",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid suspend, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// 3. Add-Ons (composable, NOT tier pricing)
// ---------------------------------------------------------------------------

func TestV2_ListAddOnSubscriptions_StartsEmpty(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/add-ons")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 active subs initially, got %d", len(items))
	}
}

func TestV2_SubscribeAddOn_ReturnsCreated(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, body := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "tms",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "active" {
		t.Fatalf("expected active, got %q", got)
	}
}

func TestV2_SubscribeAddOn_IsIdempotent(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "tms",
	})
	rec, _ := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "tms",
	})
	// Either 201 (idempotent create) or 200 (idempotent re-subscribe) accepted.
	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("expected 201/200 for idempotent re-subscribe, got %d", rec.Code)
	}
}

func TestV2_UnsubscribeAddOn_BaseRejected(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "base",
	})
	rec, _ := doV2(t, srv, "DELETE", "/v2/tenants/"+parentID+"/add-ons/base", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsubscribe of base, got %d", rec.Code)
	}
}

func TestV2_UnsubscribeAddOn_GraceWindow(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "tms",
	})
	rec, body := doV2(t, srv, "DELETE", "/v2/tenants/"+parentID+"/add-ons/tms", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for unsubscribe with grace, got %d (body: %v)", rec.Code, body)
	}
}

func TestV2_UnsubscribeAddOn_UnknownReturns404(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, _ := doV2(t, srv, "DELETE", "/v2/tenants/"+parentID+"/add-ons/no-such-add-on", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// 4. Billing
// ---------------------------------------------------------------------------

func TestV2_GetBillingUsage_ReturnsAggregated(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{"add_on_id": "tms"})
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{"add_on_id": "cms"})
	rec, body := doV2(t, srv, "GET", "/v2/tenants/"+parentID+"/billing/usage", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %v)", rec.Code, body)
	}
	if total, _ := body["total_cents"].(float64); int64(total) <= 0 {
		t.Fatalf("expected positive total_cents, got %v", body["total_cents"])
	}
	lines, _ := body["lines"].([]interface{})
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
}

func TestV2_GetBillingInvoices_StartsEmpty(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/billing/invoices")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 invoices initially, got %d", len(items))
	}
}

func TestV2_AttachPaymentMethod_ReturnsMockToken(t *testing.T) {
	t.Parallel()
	srv := newV2Server()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	rec, body := doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/billing/payment-method", map[string]interface{}{
		"stripe_payment_method_id": "pm_test_card_4242",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %v)", rec.Code, body)
	}
	if tok, _ := body["mock_token"].(string); tok == "" {
		t.Fatalf("expected mock_token in response")
	}
}

// ---------------------------------------------------------------------------
// 5. Event emission audit
// ---------------------------------------------------------------------------

func TestV2_CreateTenant_EmitsCreatedEvent(t *testing.T) {
	t.Parallel()
	srv, recorder := newV2ServerWithRecorder()
	_, _ = doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	got := recorder.RecordedByTopic("chora.tenancy.tenant.created.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 tenant.created event, got %d", len(got))
	}
}

func TestV2_BulkInvite_EmitsMemberInvitedEvents(t *testing.T) {
	t.Parallel()
	srv, recorder := newV2ServerWithRecorder()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{
			{"email": "a@acme.com", "roles": []string{"learner"}},
			{"email": "b@acme.com", "roles": []string{"learner"}},
		},
	})
	got := recorder.RecordedByTopic("chora.tenancy.member.invited.v1")
	if len(got) != 2 {
		t.Fatalf("expected 2 member.invited events, got %d", len(got))
	}
}

func TestV2_SubscribeAddOn_EmitsSubscribedEvent(t *testing.T) {
	t.Parallel()
	srv, recorder := newV2ServerWithRecorder()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{
		"add_on_id": "tms",
	})
	got := recorder.RecordedByTopic("chora.tenancy.add_on.subscribed.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 add_on.subscribed event, got %d", len(got))
	}
}

func TestV2_UnsubscribeAddOn_EmitsUnsubscribedEvent(t *testing.T) {
	t.Parallel()
	srv, recorder := newV2ServerWithRecorder()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/add-ons", map[string]interface{}{"add_on_id": "tms"})
	doV2(t, srv, "DELETE", "/v2/tenants/"+parentID+"/add-ons/tms", nil)
	got := recorder.RecordedByTopic("chora.tenancy.add_on.unsubscribed.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 add_on.unsubscribed event, got %d", len(got))
	}
}

func TestV2_SuspendMember_EmitsSuspendedEvent(t *testing.T) {
	t.Parallel()
	srv, recorder := newV2ServerWithRecorder()
	_, parent := doV2(t, srv, "POST", "/v2/tenants", map[string]interface{}{
		"display_name": "Acme", "owner_gcid": "gcid-1",
	})
	parentID, _ := parent["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members:bulk-invite", map[string]interface{}{
		"invites": []map[string]interface{}{{"email": "a@acme.com", "roles": []string{"learner"}}},
	})
	_, items, _ := doV2List(t, srv, "GET", "/v2/tenants/"+parentID+"/members")
	memberID, _ := items[0].(map[string]interface{})["id"].(string)
	doV2(t, srv, "POST", "/v2/tenants/"+parentID+"/members/"+memberID+":suspend", map[string]interface{}{
		"reason": "policy violation",
	})
	got := recorder.RecordedByTopic("chora.tenancy.member.suspended.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 member.suspended event, got %d", len(got))
	}
}

// newV2ServerWithRecorder returns a server wired with the canonical
// *events.Recorder so tests can assert on emitted topics via
// RecordedByTopic(). Post the debt #50 hexagonal port narrowing the
// production httpapi.Publisher interface no longer carries Recorded() /
// RecordedByTopic() spy methods — the recorder reference is kept as a
// typed value for the test surface.
func newV2ServerWithRecorder() (http.Handler, *events.Recorder) {
	deps := httpapi.NewDefaultV2Deps()
	recorder, ok := deps.Events.(*events.Recorder)
	if !ok {
		panic("newV2ServerWithRecorder: NewDefaultV2Deps did not return *events.Recorder; got " +
			reflect.TypeOf(deps.Events).String())
	}
	return httpapi.NewV2Server(deps), recorder
}
