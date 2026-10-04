// leaf_adapters_test.go — unit tests for the small composition-root
// adapters in cmd/server: registryAddOnActivator (CHO-1740),
// registryEntitlementStore (CHO-1811), paymentsHTTPAdapter (CHO-1767),
// familiarEggCheckoutAdapter (ADR-164) and the credit-issuer wiring
// (Iter G.5).
//
// These adapters are thin translation layers between the http/events
// ports and the domain registry / payments client; tests pin the field
// mapping + the nil-registry fail-loud paths.
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/payments"
	addon "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/add_on"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
	allocation "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_allocation"
	paymentsv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/payments/v1"
)

func newTestRegistry(t *testing.T) *addon.SubscriptionRegistry {
	t.Helper()
	return addon.NewSubscriptionRegistry(addon.NewSeedCatalogue())
}

// ---------------------------------------------------------------------------
// registryAddOnActivator — events.AddOnActivator adapter (CHO-1740)
// ---------------------------------------------------------------------------

func TestRegistryAddOnActivator_Activate_CreatesSubscription(t *testing.T) {
	reg := newTestRegistry(t)
	a := newRegistryAddOnActivator(reg)
	if err := a.Activate(context.Background(), "tenant-1", "tms", "gcid-1", ""); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	sub, ok := reg.GetSubscription("tenant-1", "tms")
	if !ok {
		t.Fatalf("expected subscription after Activate")
	}
	if sub.Status != addon.StatusActive {
		t.Fatalf("expected active subscription, got %s", sub.Status)
	}
}

func TestRegistryAddOnActivator_Activate_ChangesTierWhenRequested(t *testing.T) {
	reg := newTestRegistry(t)
	a := newRegistryAddOnActivator(reg)
	if err := a.Activate(context.Background(), "tenant-1", "tms", "gcid-1", "pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	sub, ok := reg.GetSubscription("tenant-1", "tms")
	if !ok {
		t.Fatalf("expected subscription")
	}
	if sub.CurrentTier != "pro" {
		t.Fatalf("expected tier pro, got %q", sub.CurrentTier)
	}
}

func TestRegistryAddOnActivator_Activate_SameTierIsNoOp(t *testing.T) {
	reg := newTestRegistry(t)
	a := newRegistryAddOnActivator(reg)
	// "pro" is requested on both calls → the second call's ChangeTier must
	// be a no-op (same tier) and still succeed.
	if err := a.Activate(context.Background(), "tenant-1", "tms", "gcid-1", "pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := a.Activate(context.Background(), "tenant-1", "tms", "gcid-1", " pro "); err != nil {
		t.Fatalf("Activate with whitespace-padded same tier: %v", err)
	}
}

func TestRegistryAddOnActivator_Activate_UnknownAddOnFails(t *testing.T) {
	reg := newTestRegistry(t)
	a := newRegistryAddOnActivator(reg)
	err := a.Activate(context.Background(), "tenant-1", "no-such-addon", "gcid-1", "")
	if err == nil {
		t.Fatalf("expected error for unknown add-on")
	}
	if !errors.Is(err, addon.ErrAddOnNotFound) {
		t.Fatalf("expected ErrAddOnNotFound, got %v", err)
	}
}

func TestRegistryAddOnActivator_Activate_NilRegistryFails(t *testing.T) {
	a := &registryAddOnActivator{}
	err := a.Activate(context.Background(), "tenant-1", "tms", "gcid-1", "")
	if err == nil || !strings.Contains(err.Error(), "nil registry") {
		t.Fatalf("expected nil-registry error, got %v", err)
	}
}

func TestRegistryAddOnActivator_Deactivate_SuccessAndNilRegistry(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := reg.Subscribe("tenant-1", "tms", "gcid-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	a := newRegistryAddOnActivator(reg)
	if err := a.Deactivate(context.Background(), "tenant-1", "tms", "gcid-1", "fe_request"); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	sub, _ := reg.GetSubscription("tenant-1", "tms")
	if sub.Status != addon.StatusGracePeriod {
		t.Fatalf("expected grace-period after unsubscribe, got %s", sub.Status)
	}
	if err := (&registryAddOnActivator{}).Deactivate(context.Background(), "t", "tms", "g", ""); err == nil {
		t.Fatalf("expected nil-registry error")
	}
}

func TestRegistryAddOnActivator_Deactivate_MissingSubscriptionFails(t *testing.T) {
	reg := newTestRegistry(t)
	a := newRegistryAddOnActivator(reg)
	if err := a.Deactivate(context.Background(), "tenant-1", "tms", "gcid-1", ""); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestRegistryAddOnActivator_MarkPastDueAndRecovered(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := reg.Subscribe("tenant-1", "tms", "gcid-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	a := newRegistryAddOnActivator(reg)
	if err := a.MarkPastDue("tenant-1", "tms"); err != nil {
		t.Fatalf("MarkPastDue: %v", err)
	}
	sub, _ := reg.GetSubscription("tenant-1", "tms")
	if !sub.PastDue {
		t.Fatalf("expected past_due flag")
	}
	if err := a.MarkRecovered("tenant-1", "tms"); err != nil {
		t.Fatalf("MarkRecovered: %v", err)
	}
	sub, _ = reg.GetSubscription("tenant-1", "tms")
	if sub.PastDue {
		t.Fatalf("expected past_due cleared")
	}
	if err := a.MarkPastDue("tenant-1", "missing"); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
	if err := a.MarkRecovered("tenant-1", "missing"); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestRegistryAddOnActivator_MarkPastDue_NilRegistry(t *testing.T) {
	if err := (&registryAddOnActivator{}).MarkPastDue("t", "tms"); err == nil {
		t.Fatalf("expected nil-registry error")
	}
	if err := (&registryAddOnActivator{}).MarkRecovered("t", "tms"); err == nil {
		t.Fatalf("expected nil-registry error")
	}
}

func TestRegistryAddOnActivator_AnchorDeactivation_BothPaths(t *testing.T) {
	reg := newTestRegistry(t)
	_, err := reg.Subscribe("tenant-1", "tms", "gcid-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	a := newRegistryAddOnActivator(reg)
	ok, err := a.AnchorDeactivation("tenant-1", "tms")
	if err != nil {
		t.Fatalf("AnchorDeactivation: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true on first anchor")
	}
	// Second anchor on an already-deactivated row → idempotent no-op.
	ok, err = a.AnchorDeactivation("tenant-1", "tms")
	if err != nil {
		t.Fatalf("AnchorDeactivation (2nd): %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on duplicate anchor")
	}
	// Missing subscription → error propagates.
	if _, err := a.AnchorDeactivation("tenant-1", "missing"); !errors.Is(err, addon.ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
	if _, err := (&registryAddOnActivator{}).AnchorDeactivation("t", "tms"); err == nil {
		t.Fatalf("expected nil-registry error")
	}
}

func TestRegistryAddOnActivator_PromoteScheduledTier_BothPaths(t *testing.T) {
	reg := newTestRegistry(t)
	if _, err := reg.Subscribe("tenant-1", "tms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	a := newRegistryAddOnActivator(reg)
	if _, err := reg.SetScheduledTier("tenant-1", "tms", "pro", time.Now().Add(24*time.Hour)); err != nil {
		t.Fatalf("SetScheduledTier: %v", err)
	}
	kind, fromTier, toTier, err := a.PromoteScheduledTier("tenant-1", "tms")
	if err != nil {
		t.Fatalf("PromoteScheduledTier: %v", err)
	}
	if kind == "" || fromTier == "" || toTier != "pro" {
		t.Fatalf("unexpected promotion result kind=%q from=%q to=%q", kind, fromTier, toTier)
	}
	// No schedule pending → idempotent empty kind.
	kind, fromTier, toTier, err = a.PromoteScheduledTier("tenant-1", "tms")
	if err != nil {
		t.Fatalf("PromoteScheduledTier (2nd): %v", err)
	}
	if kind != "" || fromTier != "" || toTier != "" {
		t.Fatalf("expected empty kind on no-op, got kind=%q from=%q to=%q", kind, fromTier, toTier)
	}
	if _, _, _, err := (&registryAddOnActivator{}).PromoteScheduledTier("t", "tms"); err == nil {
		t.Fatalf("expected nil-registry error")
	}
}

// ---------------------------------------------------------------------------
// registryEntitlementStore — registry-backed EntitlementStore (CHO-1811)
// ---------------------------------------------------------------------------

// fakeEntitlementStore records delegate calls for the registry-backed store.
// items stands in for the durable add_on_subscriptions rows the PG-backed
// delegate would return; it is nil unless a test seeds it.
type fakeEntitlementStore struct {
	subscribed map[string]bool
	cancelled  map[string]bool
	listed     bool
	listCalls  int
	items      []*tenancy.TenantEntitlement
}

func newFakeEntitlementStore() *fakeEntitlementStore {
	return &fakeEntitlementStore{subscribed: map[string]bool{}, cancelled: map[string]bool{}}
}

func (f *fakeEntitlementStore) Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*tenancy.TenantEntitlement, error) {
	f.subscribed[addOnID] = true
	return &tenancy.TenantEntitlement{TenantID: tenantID, AddOnID: addOnID}, nil
}

func (f *fakeEntitlementStore) Cancel(tenantID, addOnID string) (*tenancy.TenantEntitlement, error) {
	f.cancelled[addOnID] = true
	return &tenancy.TenantEntitlement{TenantID: tenantID, AddOnID: addOnID}, nil
}

func (f *fakeEntitlementStore) ListActiveByTenant(tenantID string) []*tenancy.TenantEntitlement {
	f.listed = true
	f.listCalls++
	return f.items
}

func TestRegistryEntitlementStore_SubscribeCancelDelegate(t *testing.T) {
	delegate := newFakeEntitlementStore()
	reg := newTestRegistry(t)
	s := newRegistryEntitlementStore(reg, delegate)
	if _, err := s.Subscribe("tenant-1", "addon-1", 4900); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !delegate.subscribed["addon-1"] {
		t.Fatalf("Subscribe did not delegate")
	}
	if _, err := s.Cancel("tenant-1", "addon-1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !delegate.cancelled["addon-1"] {
		t.Fatalf("Cancel did not delegate")
	}
}

func TestRegistryEntitlementStore_ListActiveByTenant_MapsRegistryRows(t *testing.T) {
	delegate := newFakeEntitlementStore()
	reg := newTestRegistry(t)
	if _, err := reg.Subscribe("tenant-1", "tms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := reg.Subscribe("tenant-1", "cms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	s := newRegistryEntitlementStore(reg, delegate)
	ents := s.ListActiveByTenant("tenant-1")
	if len(ents) != 2 {
		t.Fatalf("expected 2 entitlements, got %d", len(ents))
	}
	codes := map[string]bool{}
	for _, e := range ents {
		if e.Status != tenancy.EntitlementStatusActive {
			t.Fatalf("expected active status, got %s", e.Status)
		}
		if e.AddOnCode == "" || e.TenantID != "tenant-1" {
			t.Fatalf("mapped entitlement missing fields: %+v", e)
		}
		codes[e.AddOnCode] = true
	}
	if !codes["tms"] || !codes["cms"] {
		t.Fatalf("expected both add-on codes, got %v", codes)
	}
	// Deactivated rows are excluded.
	if _, err := newRegistryAddOnActivator(reg).AnchorDeactivation("tenant-1", "tms"); err != nil {
		t.Fatalf("anchor: %v", err)
	}
	ents = s.ListActiveByTenant("tenant-1")
	if len(ents) != 1 || ents[0].AddOnCode != "cms" {
		t.Fatalf("expected only cms after deactivation, got %+v", ents)
	}
}

// ---------------------------------------------------------------------------
// paymentsHTTPAdapter — CHO-1767 HTTP client → httpapi port adapter
// ---------------------------------------------------------------------------

func TestPaymentsHTTPAdapter_ChangeTenantAddonTier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"purchase_id":"pur_1","stripe_subscription_id":"sub_1",
			"from_tier":"tms","to_tier":"tms-pro","billing_delta_cents":4900,"currency":"SGD",
			"effective_at":"2026-07-01T00:00:00Z","subscription_status":"active",
			"deferred_to_cycle_end":true,"schedule_id":"sched_1",
			"scheduled_tier_code":"tms-pro","scheduled_effective_at":"2026-08-01T00:00:00Z"}`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	a := newPaymentsHTTPAdapter(cli)
	out, err := a.ChangeTenantAddonTier(context.Background(), httpapi.TenancyPaymentsChangeTierIn{
		TenantID: "t1", AdminGCID: "g1", AddonCode: "tms", TargetTierCode: "tms-pro",
		Currency: "SGD", ProrationMode: "partial_invoice", EffectiveAt: "end_of_cycle",
	})
	if err != nil {
		t.Fatalf("ChangeTenantAddonTier: %v", err)
	}
	if out.PurchaseID != "pur_1" || out.ToTier != "tms-pro" || !out.DeferredToCycleEnd || out.ScheduleID != "sched_1" {
		t.Fatalf("unexpected output %+v", out)
	}
}

func TestPaymentsHTTPAdapter_ChangeTenantAddonTier_PropagatesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"no_stripe_subscription","message":"no sub"}`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	a := newPaymentsHTTPAdapter(cli)
	_, err = a.ChangeTenantAddonTier(context.Background(), httpapi.TenancyPaymentsChangeTierIn{TenantID: "t1"})
	if !errors.Is(err, payments.ErrNoStripeSubscription) {
		t.Fatalf("expected ErrNoStripeSubscription, got %v", err)
	}
}

func TestPaymentsHTTPAdapter_PreviewTenantAddonTier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"purchase_id":"pre_1","from_tier":"tms","to_tier":"tms-pro",
			"billing_delta_cents":4900,"next_invoice_total_cents":17700,"currency":"SGD",
			"proration_mode":"partial_invoice","deferred_to_cycle_end":false,"effective_at":"immediate"}`))
	}))
	defer server.Close()
	cli, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	a := newPaymentsHTTPAdapter(cli)
	out, err := a.PreviewTenantAddonTier(context.Background(), httpapi.TenancyPaymentsPreviewIn{
		TenantID: "t1", AddonCode: "tms", TargetTierCode: "tms-pro",
	})
	if err != nil {
		t.Fatalf("PreviewTenantAddonTier: %v", err)
	}
	if out.PurchaseID != "pre_1" || out.NextInvoiceTotalCents != 17700 {
		t.Fatalf("unexpected output %+v", out)
	}
}

// ---------------------------------------------------------------------------
// familiarEggCheckoutAdapter — ADR-164 gRPC client → httpapi port adapter
// ---------------------------------------------------------------------------

type fakePaymentsService struct {
	paymentsv1.UnimplementedPaymentServiceServer
	err error
}

func (f *fakePaymentsService) CreateCompanionEggCheckoutSession(
	_ context.Context, _ *paymentsv1.CreateCompanionEggCheckoutSessionRequest,
) (*paymentsv1.CreateCompanionEggCheckoutSessionResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &paymentsv1.CreateCompanionEggCheckoutSessionResponse{
		PurchaseId:        "11111111-1111-7111-8111-111111111111",
		StripeSessionId:   "cs_test_egg",
		StripeCheckoutUrl: "https://stub.invalid/cs_test_egg",
		State:             paymentsv1.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

func newFamiliarEggClient(t *testing.T, srv paymentsv1.PaymentServiceServer) *payments.GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	paymentsv1.RegisterPaymentServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.GracefulStop()
		_ = lis.Close()
	})
	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(context.Background())
	}
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return payments.NewGRPCClientFromConn(conn)
}

func TestFamiliarEggCheckoutAdapter_CreatesSession(t *testing.T) {
	cli := newFamiliarEggClient(t, &fakePaymentsService{})
	a := newFamiliarEggCheckoutAdapter(cli)
	out, err := a.CreateFamiliarEggCheckoutSession(context.Background(), httpapi.EggCheckoutInput{
		IdempotencyKey: "idem-1",
		TenantID:       "tenant-1",
		LearnerGCID:    "gcid-1",
		EggSKU:         "egg.standard.v1",
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateFamiliarEggCheckoutSession: %v", err)
	}
	if out.PurchaseID == "" || out.StripeSessionID != "cs_test_egg" || out.State != "checkout_started" {
		t.Fatalf("unexpected output %+v", out)
	}
}

func TestFamiliarEggCheckoutAdapter_PropagatesError(t *testing.T) {
	cli := newFamiliarEggClient(t, &fakePaymentsService{err: errors.New("payments boom")})
	a := newFamiliarEggCheckoutAdapter(cli)
	_, err := a.CreateFamiliarEggCheckoutSession(context.Background(), httpapi.EggCheckoutInput{
		IdempotencyKey: "idem-1", TenantID: "tenant-1", LearnerGCID: "g",
		EggSKU: "egg.standard.v1", AmountCents: 1, Currency: "SGD",
	})
	if err == nil || !strings.Contains(err.Error(), "payments boom") {
		t.Fatalf("expected error propagation, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Credit-issuer wiring — newFamiliarEggCreditIssuer (Iter G.5)
// ---------------------------------------------------------------------------

func TestNewFamiliarEggCreditIssuer_NilPublisherFails(t *testing.T) {
	repo := manapool.NewInmemAllocationRepo()
	if _, err := newFamiliarEggCreditIssuer(repo, nil); !errors.Is(err, errCreditIssuerWiringNoPublisher) {
		t.Fatalf("expected no-publisher error, got %v", err)
	}
}

func TestNewFamiliarEggCreditIssuer_IssuesRefundCredit(t *testing.T) {
	repo := manapool.NewInmemAllocationRepo()
	store := tenancyoutbox.NewInMemoryStore()
	pub := tenancyoutbox.NewPublisher(tenancyoutbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: serviceName,
	})
	issuer, err := newFamiliarEggCreditIssuer(repo, pub)
	if err != nil {
		t.Fatalf("newFamiliarEggCreditIssuer: %v", err)
	}
	ctx := context.Background()
	if err := issuer.IssueRefundCredit(ctx, "tenant-1", "gcid-1", 999, "purchase-1"); err != nil {
		t.Fatalf("IssueRefundCredit: %v", err)
	}
	// Allocation row persisted with the derived idempotency key.
	key := manapool.DeriveEggRefundIdempotencyKey("purchase-1")
	alloc, err := repo.GetByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if alloc == nil || alloc.TenantID != "tenant-1" || alloc.Units != 999 {
		t.Fatalf("unexpected allocation %+v", alloc)
	}
	if alloc.Reason != allocation.ReasonEggHardExpiryRefund {
		t.Fatalf("expected egg-refund reason, got %s", alloc.Reason)
	}
	// Granted event landed in the outbox.
	rows, err := store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(rows))
	}
	if rows[0].Topic != "chora.tenancy.tenant_mana_allocation.granted.v1" {
		t.Fatalf("unexpected topic %q", rows[0].Topic)
	}
	// Re-issue with the same purchase → idempotent no-op (no second row).
	if err := issuer.IssueRefundCredit(ctx, "tenant-1", "gcid-1", 999, "purchase-1"); err != nil {
		t.Fatalf("IssueRefundCredit (re-run): %v", err)
	}
	rows, err = store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected still 1 outbox row after idempotent re-run, got %d", len(rows))
	}
}

func TestAllocationGrantedOutboxAdapter_Publishes(t *testing.T) {
	store := tenancyoutbox.NewInMemoryStore()
	pub := tenancyoutbox.NewPublisher(tenancyoutbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: serviceName,
	})
	a := allocationGrantedOutboxAdapter{inner: pub}
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	a.PublishAllocationGranted(manapool.EnvelopeHeader{
		TenantID: "tenant-1", GCID: "gcid-1", Traceparent: "00-abc-123-01",
	}, allocation.AllocationGranted{
		AllocationID:    "alloc-1",
		TenantID:        "tenant-1",
		GCID:            "gcid-1",
		SourcePoolID:    "pool-1",
		Units:           500,
		Reason:          allocation.ReasonEggHardExpiryRefund,
		AllocatedByGCID: "platform",
		AllocatedAt:     at,
	})
	rows, err := store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(rows))
	}
	if rows[0].Topic != "chora.tenancy.tenant_mana_allocation.granted.v1" {
		t.Fatalf("unexpected topic %q", rows[0].Topic)
	}
	// Second publish with an expiry timestamp (the ExpiresAt != nil branch).
	expires := at.Add(24 * time.Hour)
	a.PublishAllocationGranted(manapool.EnvelopeHeader{
		TenantID: "tenant-2", GCID: "gcid-2", Traceparent: "00-abc-456-01",
	}, allocation.AllocationGranted{
		AllocationID:    "alloc-2",
		TenantID:        "tenant-2",
		GCID:            "gcid-2",
		Units:           700,
		Reason:          allocation.ReasonEggHardExpiryRefund,
		AllocatedByGCID: "platform",
		AllocatedAt:     at,
		ExpiresAt:       &expires,
	})
	rows, err = store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 outbox rows, got %d", len(rows))
	}
}
