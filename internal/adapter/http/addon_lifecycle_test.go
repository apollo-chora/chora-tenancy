// Package httpapi_test holds the tests for the BE-T-ADD-1 add-on lifecycle
// endpoints under /api/v1/admin/tenants/{tenantId}/addons/...
package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

const (
	adminTenantID = "01970000-0000-7000-8000-0000000000ad"
	adminGCID     = "gcid-admin-1"
)

func newAdminServer(t *testing.T) (http.Handler, httpapi.V2Deps) {
	t.Helper()
	deps := httpapi.NewDefaultV2Deps()
	// The grant path refuses without a durable store (it will not debit a
	// pool it cannot record against), so tests supply a double that keeps the
	// real contract: persist the debited pool, refuse a repeated
	// allocation_id without re-debiting, and emit the event.
	//
	// Deliberately NOT in NewDefaultV2Deps: putting an in-memory grant store
	// on the default deps would hand dev mode back the very behaviour
	// CHO-2414's follow-up removed, where a debit is durable and the record
	// of it is a map.
	deps.GrantStore = &fakeGrantStore{deps: &deps, seen: map[string]bool{}}
	srv := httpapi.NewV2Server(deps)
	return srv, deps
}

// fakeGrantStore emulates pg.ManaGrantRepository.SaveGrant for handler tests.
type fakeGrantStore struct {
	deps *httpapi.V2Deps
	seen map[string]bool
	// failWith, when set, is returned instead of persisting.
	failWith error
}

func (f *fakeGrantStore) SaveGrant(
	ctx context.Context,
	p *pool.TenantManaPool,
	a *allocation.TenantManaAllocation,
	ev pg.GrantEvent,
) error {
	if f.failWith != nil {
		return f.failWith
	}
	if f.seen[a.AllocationID] {
		return pg.ErrGrantAlreadyRecorded
	}
	if err := f.deps.PoolRepo.Save(ctx, p); err != nil {
		return err
	}
	f.seen[a.AllocationID] = true
	f.deps.Events.Publish(ev.Topic, events.Header{TenantID: a.TenantID, GCID: ev.GCID}, ev.Payload)
	return nil
}

// eventsRecorder type-asserts deps.Events back to *events.Recorder so
// tests can assert on the recorded ring buffer via Recorded() /
// RecordedByTopic(). Per the hexagonal port narrowing in v2_handlers.go
// the production httpapi.Publisher port has only Publish /
// PublishWithError — spy methods belong on the concrete test recorder.
//
// Tests using NewDefaultV2Deps get the *events.Recorder for free; tests
// that explicitly swap in a different publisher must capture the
// recorder reference at construction time and not call this helper.
func eventsRecorder(t *testing.T, deps httpapi.V2Deps) *events.Recorder {
	t.Helper()
	rec, ok := deps.Events.(*events.Recorder)
	if !ok {
		t.Fatalf("deps.Events: got %T; want *events.Recorder (test expectation)", deps.Events)
	}
	return rec
}

func adminDo(t *testing.T, srv http.Handler, method, path string, role string, body interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("X-Tenant-Id", adminTenantID)
	req.Header.Set("X-GCID", adminGCID)
	if role != "" {
		req.Header.Set("X-Role", role)
		// CHO-2072: the real gateway stamps roles on x-mesh-user-roles, which
		// the fail-closed admin gates now read. Mirror the role there too.
		req.Header.Set("x-mesh-user-roles", role)
	} else {
		// Admin routes always carry a gateway-stamped role in prod; default an
		// admin mesh role so pre-existing tests that omit an explicit role
		// exercise the happy path, not the CHO-2072 fail-closed 403.
		req.Header.Set("x-mesh-user-roles", "tenant_admin")
	}
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

func seedActiveSubscription(t *testing.T, deps httpapi.V2Deps, addOnCode string) *addon.Subscription {
	t.Helper()
	sub, err := deps.Subscriptions.Subscribe(adminTenantID, addOnCode, adminGCID)
	if err != nil {
		t.Fatalf("seed subscribe %q: %v", addOnCode, err)
	}
	return sub
}

func TestGetAddonDetail_ReturnsFullAddOnDetail(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["code"].(string); got != "tms" {
		t.Fatalf("expected code=tms, got %v", body["code"])
	}
	tiers, _ := body["pricing_tiers"].([]interface{})
	if len(tiers) < 3 {
		t.Fatalf("expected ≥3 tiers, got %d", len(tiers))
	}
}

func TestGetAddonDetail_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/no-such-addon",
		"tenant_admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGetAddonDetail_IncludesCurrentSubscriptionWhenActive(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present (body=%v)", body)
	}
	if status, _ := snap["status"].(string); status != "ACTIVE" {
		t.Fatalf("expected status=ACTIVE, got %v", snap["status"])
	}
}

// CHO-1780 — the subscription snapshot surfaces the pending end-of-cycle
// tier change so the marketplace detail page renders the same
// "<scheduled> starting <date>" badge as /h/addons.
func TestGetAddonDetail_SurfacesScheduledTierWhenPending(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	// Relative, never a hardcoded calendar date. The registry schedules only
	// when EffectiveAt is AFTER now, so a fixed date silently converts this
	// test from "schedules" to "acts immediately" the day the clock passes it,
	// which is exactly how this went red on main without a code change.
	effective := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	wantEffectiveDay := effective.Format("2006-01-02")
	if _, err := deps.Subscriptions.SetScheduledTier(adminTenantID, "tms", "pro", effective); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present (body=%v)", body)
	}
	if got, _ := snap["scheduled_tier_code"].(string); got != "pro" {
		t.Errorf("scheduled_tier_code = %v, want pro", snap["scheduled_tier_code"])
	}
	got, _ := snap["scheduled_effective_at"].(string)
	if got == "" {
		t.Fatalf("scheduled_effective_at missing from snapshot: %v", snap)
	}
	if !strings.HasPrefix(got, wantEffectiveDay) {
		t.Errorf("scheduled_effective_at = %q, want %s... ISO", got, wantEffectiveDay)
	}
}

// CHO-1780 follow-up — the MARKETPLACE detail endpoint
// (/api/v1/admin/marketplace/addons/<id>) must ALSO surface
// scheduled_tier_code / scheduled_effective_at when installed +
// scheduled, otherwise the AddonMarketplaceCatalogDetailComponent
// can't render the badge. The existing handler omits
// `current_subscription` by default; this asserts it's populated
// when the tenant has a pending end-of-cycle change.
func TestGetMarketplaceAddonByID_SurfacesScheduledTierWhenInstalledAndPending(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	// Relative, never a hardcoded calendar date. The registry schedules only
	// when EffectiveAt is AFTER now, so a fixed date silently converts this
	// test from "schedules" to "acts immediately" the day the clock passes it,
	// which is exactly how this went red on main without a code change.
	effective := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	wantEffectiveDay := effective.Format("2006-01-02")
	if _, err := deps.Subscriptions.SetScheduledTier(adminTenantID, "tms", "pro", effective); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}
	a, _ := deps.Catalogue.Get("tms")

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)

	if installed, _ := body["is_installed"].(bool); !installed {
		t.Fatalf("expected is_installed=true (pre-condition), body=%v", body)
	}
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present when installed + scheduled; body=%v", body)
	}
	if got, _ := snap["scheduled_tier_code"].(string); got != "pro" {
		t.Errorf("scheduled_tier_code = %v, want pro", snap["scheduled_tier_code"])
	}
	got, _ := snap["scheduled_effective_at"].(string)
	if got == "" {
		t.Fatalf("scheduled_effective_at missing from snapshot: %v", snap)
	}
	if !strings.HasPrefix(got, wantEffectiveDay) {
		t.Errorf("scheduled_effective_at = %q, want %s... ISO", got, wantEffectiveDay)
	}
}

// Marketplace browse view: when the tenant has an installed sub but
// no pending schedule, current_subscription is still populated (so the
// FE can read current_tier consistently) but the scheduled keys are
// omitted.
func TestGetMarketplaceAddonByID_OmitsScheduledFieldsWhenNoneScheduled(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	a, _ := deps.Catalogue.Get("tms")

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons/"+a.ID,
		"tenant_admin", nil)

	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		// Acceptable if the endpoint chooses to omit the whole snapshot
		// when nothing is scheduled — the FE handles either shape.
		return
	}
	if _, present := snap["scheduled_tier_code"]; present {
		t.Errorf("scheduled_tier_code MUST be omitted when no schedule pending; got: %v", snap)
	}
	if _, present := snap["scheduled_effective_at"]; present {
		t.Errorf("scheduled_effective_at MUST be omitted when no schedule pending; got: %v", snap)
	}
}

// Defensive: when there is no schedule pending the keys are omitted
// from the response (NOT emitted as empty strings) so the FE can use
// a truthy `@if` without false positives.
func TestGetAddonDetail_OmitsScheduledFieldsWhenNoneScheduled(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, _ := body["current_subscription"].(map[string]interface{})
	if _, present := snap["scheduled_tier_code"]; present {
		t.Errorf("scheduled_tier_code MUST be omitted when no schedule pending; got: %v", snap)
	}
	if _, present := snap["scheduled_effective_at"]; present {
		t.Errorf("scheduled_effective_at MUST be omitted when no schedule pending; got: %v", snap)
	}
}

// CHO-1782 — the subscription snapshot surfaces the pending end-of-cycle
// deactivation so the three FE panels (/h/addons tile, /h/addons/<id>/detail,
// /h/marketplace/<id>) can render the destructive "Deactivating <date>" pill.
// Mirror of TestGetAddonDetail_SurfacesScheduledTierWhenPending.
func TestGetAddonDetail_SurfacesDeactivationFieldsWhenPending(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	// Relative, never a hardcoded calendar date. The registry schedules only
	// when EffectiveAt is AFTER now, so a fixed date silently converts this
	// test from "schedules" to "acts immediately" the day the clock passes it,
	// which is exactly how this went red on main without a code change.
	effective := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	wantEffectiveDay := effective.Format("2006-01-02")
	if _, _, _, err := deps.Subscriptions.Deactivate(adminTenantID, "tms",
		addon.DeactivationRequest{
			Reason:          addon.ReasonCost,
			EffectiveAt:     &effective,
			RequestedByGCID: adminGCID,
		}); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present (body=%v)", body)
	}
	if status, _ := snap["status"].(string); status != "PENDING_DEACTIVATION" {
		t.Fatalf("status = %v, want PENDING_DEACTIVATION", snap["status"])
	}
	got, _ := snap["deactivation_effective_at"].(string)
	if got == "" {
		t.Fatalf("deactivation_effective_at missing from snapshot: %v", snap)
	}
	if !strings.HasPrefix(got, wantEffectiveDay) {
		t.Errorf("deactivation_effective_at = %q, want %s... ISO", got, wantEffectiveDay)
	}
	if reason, _ := snap["deactivation_reason"].(string); reason != "cost" {
		t.Errorf("deactivation_reason = %v, want cost", snap["deactivation_reason"])
	}
}

// Defensive: when the subscription is ACTIVE (no pending deactivation), the
// deactivation_* keys are omitted from the response so the FE can use a
// truthy `@if` to render the pill.
func TestGetAddonDetail_OmitsDeactivationFieldsWhenActive(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, _ := body["current_subscription"].(map[string]interface{})
	if _, present := snap["deactivation_effective_at"]; present {
		t.Errorf("deactivation_effective_at MUST be omitted on ACTIVE; got: %v", snap)
	}
	if _, present := snap["deactivation_reason"]; present {
		t.Errorf("deactivation_reason MUST be omitted on ACTIVE; got: %v", snap)
	}
}

// CHO-1784 — the subscription snapshot surfaces next_renewal_at so the FE
// deactivate modal can send a real future date (not a +30d client-side
// approximation) as effective_at on the end_of_cycle path. Registry
// populates NextRenewalAt = ActivatedAt + 30d at Subscribe time as the
// first-cycle approximation; later tickets will refresh from Stripe events.
func TestGetAddonDetail_SurfacesNextRenewalAtWhenPopulated(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, ok := body["current_subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_subscription present (body=%v)", body)
	}
	got, _ := snap["next_renewal_at"].(string)
	if got == "" {
		t.Fatalf("next_renewal_at missing from snapshot: %v", snap)
	}
	parsed, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("next_renewal_at = %q not parseable RFC3339Nano: %v", got, err)
	}
	if !parsed.After(time.Now().UTC()) {
		t.Errorf("next_renewal_at = %v, want future (Subscribe defaults to ActivatedAt+30d)", parsed)
	}
}

// Defensive: when NextRenewalAt is nil on the registry row (e.g. legacy
// pre-CHO-1784 rows that survived a hot reload), the key is omitted from
// the response.
func TestGetAddonDetail_OmitsNextRenewalAtWhenNil(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	sub := seedActiveSubscription(t, deps, "tms")
	// Force-clear NextRenewalAt to simulate a row that pre-dated CHO-1784.
	sub.NextRenewalAt = nil

	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", nil)
	snap, _ := body["current_subscription"].(map[string]interface{})
	if _, present := snap["next_renewal_at"]; present {
		t.Errorf("next_renewal_at MUST be omitted when nil on the registry row; got: %v", snap)
	}
}

func TestPatchAddonTier_UpgradeReturnsPositiveDelta(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "pro",
			"proration_mode": "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	delta, _ := body["billing_delta_cents"].(float64)
	if int64(delta) <= 0 {
		t.Fatalf("expected positive billing_delta_cents, got %v", body["billing_delta_cents"])
	}
	if to, _ := body["to_tier"].(string); to != "pro" {
		t.Fatalf("expected to_tier=pro, got %v", body["to_tier"])
	}
}

func TestPatchAddonTier_DowngradeReturnsNegativeDelta(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "enterprise",
			"proration_mode": "create_prorations",
		})
	rec, body := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "starter",
			"proration_mode": "create_prorations",
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	delta, _ := body["billing_delta_cents"].(float64)
	if int64(delta) >= 0 {
		t.Fatalf("expected negative billing_delta_cents, got %v", body["billing_delta_cents"])
	}
}

func TestPatchAddonTier_409WhenIneligible(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(48 * time.Hour).Format(time.RFC3339)
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "no_longer_needed",
			"effective_at": future,
		})
	if got, ok := deps.Subscriptions.GetSubscription(adminTenantID, "tms"); ok && got.Status == addon.StatusActive {
		t.Fatalf("expected pending_deactivation, got %s", got.Status)
	}
	rec, _ := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "pro",
			"proration_mode": "create_prorations",
		})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPatchAddonTier_422OnUnknownTier(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "unobtainium",
			"proration_mode": "create_prorations",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestPatchAddonTier_OptimisticConcurrencyConflict409(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	sub := seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id":   "pro",
			"proration_mode":   "create_prorations",
			"expected_version": sub.Version + 99,
		})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPatchAddonTier_EmitsUpgradedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "pro",
			"proration_mode": "create_prorations",
		})
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.upgraded.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 addon.upgraded event, got %d", len(got))
	}
}

func TestPatchAddonTier_EmitsDowngradedEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "enterprise",
			"proration_mode": "create_prorations",
		})
	adminDo(t, srv, "PATCH",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms",
		"tenant_admin", map[string]interface{}{
			"target_plan_id": "starter",
			"proration_mode": "create_prorations",
		})
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.downgraded.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 addon.downgraded event, got %d", len(got))
	}
}

func TestDeactivateAction_ImmediateReturnsDEACTIVATED202(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "no_longer_needed"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "DEACTIVATED" {
		t.Fatalf("expected status=DEACTIVATED, got %v", body["status"])
	}
}

func TestDeactivateAction_DeferredReturnsDEACTIVATION_SCHEDULED202(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339)
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "cost",
			"effective_at": future,
		})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "DEACTIVATION_SCHEDULED" {
		t.Fatalf("expected DEACTIVATION_SCHEDULED, got %v", body["status"])
	}
}

func TestDeactivateAction_AlreadyDeactivated409(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "no_longer_needed"})
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "no_longer_needed"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestDeactivateAction_OtherReasonWithoutTextReturns422(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":      "other",
			"reason_text": "",
		})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

// CHO-1785 — undoing a pending end-of-cycle deactivation. Reverse of
// the deferred deactivate path: PENDING_DEACTIVATION → ACTIVE with all
// deactivation_* fields cleared. Idempotent on ACTIVE (no-op 200);
// 409 on already-DEACTIVATED rows.
func TestCancelDeactivationEndpoint_HappyPath_FromPendingDeactivation(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(48 * time.Hour).Format(time.RFC3339)
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "no_longer_needed",
			"effective_at": future,
		})
	if got, _ := deps.Subscriptions.GetSubscription(adminTenantID, "tms"); got == nil || got.Status != addon.StatusPendingDeactivation {
		t.Fatalf("pre-condition: expected pending_deactivation")
	}
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:cancel-deactivation",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "ACTIVE" {
		t.Errorf("status = %q, want ACTIVE", got)
	}
	if _, present := body["deactivation_effective_at"]; present {
		t.Errorf("deactivation_effective_at MUST be cleared after undo; got: %v", body["deactivation_effective_at"])
	}
	after, _ := deps.Subscriptions.GetSubscription(adminTenantID, "tms")
	if after.Status != addon.StatusActive {
		t.Errorf("registry Status = %q, want active", after.Status)
	}
}

func TestCancelDeactivationEndpoint_IdempotentOnActive(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:cancel-deactivation",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (idempotent on ACTIVE), got %d (body=%v)", rec.Code, body)
	}
	if got, _ := body["status"].(string); got != "ACTIVE" {
		t.Errorf("status = %q, want ACTIVE", got)
	}
}

func TestCancelDeactivationEndpoint_409WhenAlreadyDeactivated(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	// Immediate deactivate (no effective_at) → StatusDeactivated.
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "no_longer_needed"})
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:cancel-deactivation",
		"tenant_admin", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestCancelDeactivationEndpoint_404OnUnknownAddon(t *testing.T) {
	t.Parallel()
	srv, _ := newAdminServer(t)
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/no-such-addon:cancel-deactivation",
		"tenant_admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestCancelDeactivationEndpoint_403OnLearnerRole(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:cancel-deactivation",
		"learner", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestCancelDeactivationEndpoint_EmitsLifecycleEvent(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(48 * time.Hour).Format(time.RFC3339)
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "cost",
			"effective_at": future,
		})
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:cancel-deactivation",
		"tenant_admin", nil)
	emitted := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_cancelled.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected 1 deactivation_cancelled emit, got %d", len(emitted))
	}
}

func TestDeactivateAction_ComplianceLocked423(t *testing.T) {
	t.Parallel()
	// Per audit-platform-fillgaps.md §9.6: governance_dashboard is the
	// canonical compliance-locked add-on (replaces the old
	// singpass-federation seed which was deferred to post-MVP).
	srv, deps := newAdminServer(t)
	if _, ok := deps.Catalogue.Get("governance_dashboard"); !ok {
		t.Fatalf("governance_dashboard seed missing")
	}
	seedActiveSubscription(t, deps, "governance_dashboard")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/governance_dashboard:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "compliance"})
	if rec.Code != http.StatusLocked {
		t.Fatalf("expected 423 Locked, got %d", rec.Code)
	}
}

func TestDeactivateAction_RBAC_ForbiddenForLearner403(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, _ := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"learner", map[string]interface{}{"reason": "no_longer_needed"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestDeactivateAction_EmitsDeactivationRequestedEventOnDeferred(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	future := time.Now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339)
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{
			"reason":       "cost",
			"effective_at": future,
		})
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivation_requested.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 deactivation_requested event, got %d", len(got))
	}
}

func TestDeactivateAction_EmitsDeactivatedEventOnImmediate(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/tms:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "no_longer_needed"})
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.deactivated.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 deactivated event, got %d", len(got))
	}
}

func TestDeactivateAction_McpGatewayPartialRevoke_EmitsPartialEvent(t *testing.T) {
	t.Parallel()
	deps := httpapi.NewDefaultV2Deps()
	deps.Catalogue.Register(&addon.AddOn{
		ID:          addon.NewUUIDv7(),
		Code:        "mcp_gateway",
		DisplayName: "MCP Gateway",
		Category:    "Integrations",
		PlanFamily:  "mcp_gateway",
		Tiers: []addon.Tier{
			{Code: "starter", DisplayName: "Starter", MonthlyPriceCents: 9900, Currency: "USD"},
		},
		MonthlyPriceCents: 9900,
	})
	deps.A2APartnerActive = func(tenantID, code string) bool { return code == "mcp_gateway" }
	srv := httpapi.NewV2Server(deps)
	if _, err := deps.Subscriptions.Subscribe(adminTenantID, "mcp_gateway", adminGCID); err != nil {
		t.Fatalf("seed mcp_gateway sub: %v", err)
	}
	rec, body := adminDo(t, srv, "POST",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/mcp_gateway:deactivate",
		"tenant_admin", map[string]interface{}{"reason": "consolidation"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%v)", rec.Code, body)
	}
	if pr, _ := body["partial_revoke"].(bool); !pr {
		t.Fatalf("expected partial_revoke=true, got %v", body["partial_revoke"])
	}
	got := eventsRecorder(t, deps).RecordedByTopic("chora.tenancy.addon.partial_revoke_started.v1")
	if len(got) != 1 {
		t.Fatalf("expected 1 partial_revoke_started event, got %d", len(got))
	}
}

func TestGetAddonUsage_ReturnsTimeSeriesAndTopConsumers(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	sub := seedActiveSubscription(t, deps, "tms")
	day1 := time.Now().UTC().Add(-2 * 24 * time.Hour)
	day2 := time.Now().UTC().Add(-1 * 24 * time.Hour)
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID:   adminTenantID,
		AddOnID:    sub.AddOnID,
		BucketDate: day1,
		SeatsUsed:  10, SeatsUsedPeak: 12, QuotaConsumed: 200,
	})
	deps.Subscriptions.RecordUsage(&addon.UsageSnapshot{
		TenantID:   adminTenantID,
		AddOnID:    sub.AddOnID,
		BucketDate: day2,
		SeatsUsed:  15, SeatsUsedPeak: 18, QuotaConsumed: 300,
	})
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/"+sub.AddOnID+"/usage",
		"tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%v)", rec.Code, body)
	}
	ts, _ := body["time_series"].([]interface{})
	if len(ts) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(ts))
	}
	tc, _ := body["top_consumers"].([]interface{})
	if len(tc) == 0 {
		t.Fatalf("expected top_consumers populated")
	}
}

func TestGetAddonUsage_PseudonymizesGCIDs(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	a, _ := deps.Catalogue.Get("tms")
	_, body := adminDo(t, srv, "GET",
		"/api/v1/admin/tenants/"+adminTenantID+"/addons/"+a.ID+"/usage",
		"tenant_admin", nil)
	tc, _ := body["top_consumers"].([]interface{})
	for _, raw := range tc {
		entry, _ := raw.(map[string]interface{})
		pseudo, _ := entry["user_id_pseudonymous"].(string)
		if !strings.HasPrefix(pseudo, "u_") {
			t.Fatalf("expected u_-prefixed pseudonym, got %q", pseudo)
		}
		if strings.HasPrefix(pseudo, "gcid-") {
			t.Fatalf("raw GCID leaked in top_consumers: %q", pseudo)
		}
	}
}

func TestListMarketplaceAddons_PaginatesAndOmitsCurrentSubscription(t *testing.T) {
	t.Parallel()
	srv, deps := newAdminServer(t)
	seedActiveSubscription(t, deps, "tms")
	rec, body := adminDo(t, srv, "GET",
		"/api/v1/admin/marketplace/addons", "tenant_admin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	items, _ := body["items"].([]interface{})
	if len(items) < 8 {
		t.Fatalf("expected ≥8 marketplace items, got %d", len(items))
	}
	for _, raw := range items {
		entry, _ := raw.(map[string]interface{})
		if _, present := entry["current_subscription"]; present {
			t.Fatalf("marketplace items must omit current_subscription")
		}
	}
}
