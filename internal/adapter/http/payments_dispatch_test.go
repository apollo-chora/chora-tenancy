// payments_dispatch_test.go — CHO-1767. Pins the
// handleAdminChangeAddonTier / handleAdminPreviewTierChange dispatch
// path through the new chora-payments HTTP client (CHO-1764+CHO-1765).
package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
)

// fakePaymentsHTTP records calls + returns canned responses/errors.
type fakePaymentsHTTP struct {
	changeIn     httpapi.TenancyPaymentsChangeTierIn
	previewIn    httpapi.TenancyPaymentsPreviewIn
	changeOut    httpapi.TenancyPaymentsChangeTierOut
	previewOut   httpapi.TenancyPaymentsPreviewOut
	changeErr    error
	previewErr   error
	changeCalls  int
	previewCalls int
}

func (f *fakePaymentsHTTP) ChangeTenantAddonTier(_ context.Context, in httpapi.TenancyPaymentsChangeTierIn) (httpapi.TenancyPaymentsChangeTierOut, error) {
	f.changeCalls++
	f.changeIn = in
	return f.changeOut, f.changeErr
}

func (f *fakePaymentsHTTP) PreviewTenantAddonTier(_ context.Context, in httpapi.TenancyPaymentsPreviewIn) (httpapi.TenancyPaymentsPreviewOut, error) {
	f.previewCalls++
	f.previewIn = in
	return f.previewOut, f.previewErr
}

func newAdminServerWithPaymentsHTTP(t *testing.T, fake *fakePaymentsHTTP) (http.Handler, httpapi.V2Deps) {
	t.Helper()
	deps := httpapi.NewDefaultV2Deps()
	deps.PaymentsHTTP = fake
	srv := httpapi.NewV2Server(deps)
	return srv, deps
}

// CHO-1780 follow-up — when chora-payments responds with
// deferred_to_cycle_end=true (end-of-cycle path), the result card
// MUST surface to_tier=<target tier the user picked> so the user sees
// "Starter → Pro" instead of "Starter → Starter". Bug: PR #91's
// handleAdminChangeAddonTierViaPayments overwrote the response's
// currentTier with the registry's existing CurrentTier (still
// "starter") on the deferred path.
func TestHandleAdminChangeAddonTier_DeferredPath_SurfacesTargetAsToTier(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		changeOut: httpapi.TenancyPaymentsChangeTierOut{
			PurchaseID:           "purchase_def",
			StripeSubscriptionID: "sub_def",
			FromTier:             "starter",
			ToTier:               "pro",
			BillingDeltaCents:    0,
			Currency:             "usd",
			EffectiveAt:          "2026-07-18T00:00:00Z",
			SubscriptionStatus:   "active",
			DeferredToCycleEnd:   true,
			ScheduleID:           "sub_sched_test",
			ScheduledTierCode:    "pro",
			ScheduledEffectiveAt: "2026-07-18T00:00:00Z",
		},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	futureISO := "2026-07-18T00:00:00Z"
	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
			"effective_at":     futureISO,
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["from_tier"].(string); got != "starter" {
		t.Errorf("from_tier = %v, want starter", body["from_tier"])
	}
	if got, _ := body["to_tier"].(string); got != "pro" {
		t.Errorf("to_tier = %v, want pro (target tier; bug: PR #91 echoed CurrentTier=starter)", body["to_tier"])
	}
	if got, _ := body["deferred_to_cycle_end"].(bool); !got {
		t.Errorf("deferred_to_cycle_end = %v, want true", body["deferred_to_cycle_end"])
	}
	if got, _ := body["scheduled_tier_code"].(string); got != "pro" {
		t.Errorf("scheduled_tier_code = %v, want pro", body["scheduled_tier_code"])
	}
}

// Happy path: change-tier dispatches through payments + returns the
// Stripe-derived delta verbatim. Registry is best-effort synced.
func TestHandleAdminChangeAddonTier_DispatchesToPaymentsHTTP(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		changeOut: httpapi.TenancyPaymentsChangeTierOut{
			PurchaseID:           "purchase_xyz",
			StripeSubscriptionID: "sub_xyz",
			FromTier:             "starter",
			ToTier:               "pro",
			BillingDeltaCents:    5000,
			Currency:             "usd",
			EffectiveAt:          "2026-06-16T12:00:00Z",
			SubscriptionStatus:   "active",
		},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if fake.changeCalls != 1 {
		t.Fatalf("payments-HTTP ChangeTenantAddonTier calls=%d, want 1", fake.changeCalls)
	}
	if fake.changeIn.AddonCode != "tms" || fake.changeIn.TargetTierCode != "pro" {
		t.Errorf("payments-HTTP input wrong: %+v", fake.changeIn)
	}
	if got, _ := body["billing_delta_cents"].(float64); int64(got) != 5000 {
		t.Errorf("billing_delta_cents = %v, want 5000 (from payments stub)", body["billing_delta_cents"])
	}
	if got, _ := body["from_tier"].(string); got != "starter" {
		t.Errorf("from_tier = %v, want starter (from payments)", body["from_tier"])
	}
}

// no_stripe_subscription error from payments surfaces as the 422 banner
// the FE displays.
func TestHandleAdminChangeAddonTier_NoStripeSubscription_Returns422(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		changeErr: &httpapi.PaymentsHTTPError{Code: "no_stripe_subscription", Message: "legacy row"},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	// Tell the tenancy handler the default sentinels match the error
	// code (test-only — main.go does this once at boot with the real
	// payments-adapter sentinels).
	httpapi.SetPaymentsErrSentinels(
		errors.New("no_stripe_subscription"),
		errors.New("stripe_price_missing"),
		errors.New("tier_unchanged"),
		errors.New("addon_not_found"),
	)
	defer httpapi.SetPaymentsErrSentinels(nil, nil, nil, nil)

	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["error"].(map[string]interface{})["code"].(string); got != "no_stripe_subscription" {
		t.Errorf("error code = %v, want no_stripe_subscription", body["error"])
	}
}

// stripe_price_missing error surfaces as 422.
func TestHandleAdminChangeAddonTier_StripePriceMissing_Returns422(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		changeErr: &httpapi.PaymentsHTTPError{Code: "stripe_price_missing", Message: "tier=pro currency=usd unknown"},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")
	httpapi.SetPaymentsErrSentinels(
		errors.New("no_stripe_subscription"),
		errors.New("stripe_price_missing"),
		errors.New("tier_unchanged"),
		errors.New("addon_not_found"),
	)
	defer httpapi.SetPaymentsErrSentinels(nil, nil, nil, nil)

	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
	if got, _ := body["error"].(map[string]interface{})["code"].(string); got != "stripe_price_missing" {
		t.Errorf("error code = %v, want stripe_price_missing", body["error"])
	}
}

// Network / unknown error → 502 payments_unavailable.
func TestHandleAdminChangeAddonTier_PaymentsDown_Returns502(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		changeErr: errors.New("connection refused"),
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d (body=%v)", rec.Code, body)
	}
}

// When PaymentsHTTP isn't wired (nil), the handler falls back to the
// legacy direct-registry path — existing CHO-1733 cases still pass.
// This is the regression guard: enabling PaymentsHTTP must not break
// the no-payments smoke / unit harness.
func TestHandleAdminChangeAddonTier_NilPaymentsHTTP_FallsBackToRegistry(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t) // no PaymentsHTTP
	seedActiveSubscription(t, deps, "tms")

	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("legacy path expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["to_tier"].(string); got != "pro" {
		t.Errorf("legacy path to_tier = %v, want pro", body["to_tier"])
	}
}

// Preview dispatch happy path — Stripe-derived delta passed through.
func TestHandleAdminPreviewTierChange_DispatchesToPaymentsHTTP(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		previewOut: httpapi.TenancyPaymentsPreviewOut{
			PurchaseID:            "purchase_xyz",
			FromTier:              "starter",
			ToTier:                "pro",
			BillingDeltaCents:     5000,
			NextInvoiceTotalCents: 9900,
			Currency:              "usd",
			ProrationMode:         "create_prorations",
		},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{
			"target_tier_code": "pro",
			"proration_mode":   "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if fake.previewCalls != 1 {
		t.Fatalf("payments-HTTP PreviewTenantAddonTier calls=%d, want 1", fake.previewCalls)
	}
	if fake.previewIn.AddonCode != "tms" || fake.previewIn.TargetTierCode != "pro" {
		t.Errorf("payments-HTTP input wrong: %+v", fake.previewIn)
	}
	if got, _ := body["billing_delta_cents"].(float64); int64(got) != 5000 {
		t.Errorf("billing_delta_cents = %v, want 5000 (from Stripe preview)", body["billing_delta_cents"])
	}
}

// Preview error mapping mirrors change-tier path.
func TestHandleAdminPreviewTierChange_NoStripeSubscription_Returns422(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		previewErr: &httpapi.PaymentsHTTPError{Code: "no_stripe_subscription"},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")
	httpapi.SetPaymentsErrSentinels(
		errors.New("no_stripe_subscription"),
		errors.New("stripe_price_missing"),
		errors.New("tier_unchanged"),
		errors.New("addon_not_found"),
	)
	defer httpapi.SetPaymentsErrSentinels(nil, nil, nil, nil)

	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{"target_tier_code": "pro"})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
	if got, _ := body["error"].(map[string]interface{})["code"].(string); got != "no_stripe_subscription" {
		t.Errorf("error code = %v", body["error"])
	}
}

// Preview MUST NOT emit upgraded/downgraded events (read-only). The
// existing CHO-1733 regression already pins this; the via-payments
// path must preserve it.
func TestHandleAdminPreviewTierChange_ViaPayments_DoesNotEmitUpgradedEvent(t *testing.T) {
	t.Parallel()
	fake := &fakePaymentsHTTP{
		previewOut: httpapi.TenancyPaymentsPreviewOut{
			FromTier: "starter", ToTier: "pro", BillingDeltaCents: 5000,
			NextInvoiceTotalCents: 9900, Currency: "usd",
			ProrationMode: "create_prorations",
		},
	}
	srv, deps := newAdminServerWithPaymentsHTTP(t, fake)
	seedActiveSubscription(t, deps, "tms")

	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms/preview-tier-change",
		"tenant_admin", map[string]interface{}{"target_tier_code": "pro"})

	emitted := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1")
	if len(emitted) != 0 {
		t.Errorf("preview must not emit upgraded event; got %d", len(emitted))
	}
}

// Compile-time assertion: the v2 deps PaymentsHTTPClient port is the
// same shape as the adapter we wire in cmd/server.
func TestPaymentsHTTPClient_InterfaceShape(t *testing.T) {
	t.Parallel()
	var _ httpapi.PaymentsHTTPClient = (*fakePaymentsHTTP)(nil)
	_ = strings.TrimSpace
}
