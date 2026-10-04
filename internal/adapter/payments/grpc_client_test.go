// Tests for the chora-payments gRPC client adapter.
//
// Verifies the chora-tenancy → chora-payments synchronous-write path:
//
//   - CreateFamiliarEggCheckoutSession (originating service: chora-tenancy
//     for ADR-149 FamiliarEgg purchases).
//   - CreateManaTopUpSession (originating service: chora-tenancy for tenant
//     mana pool top-ups initiated by tenant_admin via H+ Setup-Tenant flow).
//
// The tests build an in-process gRPC server backed by an UnimplementedPayment
// ServiceServer override, dial it via bufconn, and assert the Client maps the
// inputs into the canonical Request shapes + returns the parsed Response.
package payments_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/payments"
)

const bufSize = 1024 * 1024

// fakeServer is an in-process PaymentService for tests.
type fakeServer struct {
	paymentsv1.UnimplementedPaymentServiceServer

	gotEggReq     *paymentsv1.CreateCompanionEggCheckoutSessionRequest
	gotManaReq    *paymentsv1.CreateManaTopUpSessionRequest
	eggResp       *paymentsv1.CreateCompanionEggCheckoutSessionResponse
	manaResp      *paymentsv1.CreateManaTopUpSessionResponse
	eggReturnErr  error
	manaReturnErr error
}

func (s *fakeServer) CreateCompanionEggCheckoutSession(
	_ context.Context, in *paymentsv1.CreateCompanionEggCheckoutSessionRequest,
) (*paymentsv1.CreateCompanionEggCheckoutSessionResponse, error) {
	s.gotEggReq = in
	if s.eggReturnErr != nil {
		return nil, s.eggReturnErr
	}
	if s.eggResp != nil {
		return s.eggResp, nil
	}
	return &paymentsv1.CreateCompanionEggCheckoutSessionResponse{
		PurchaseId:        "11111111-1111-7111-8111-111111111111",
		StripeSessionId:   "cs_test_egg",
		StripeCheckoutUrl: "https://stub.invalid/cs_test_egg",
		State:             paymentsv1.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

func (s *fakeServer) CreateManaTopUpSession(
	_ context.Context, in *paymentsv1.CreateManaTopUpSessionRequest,
) (*paymentsv1.CreateManaTopUpSessionResponse, error) {
	s.gotManaReq = in
	if s.manaReturnErr != nil {
		return nil, s.manaReturnErr
	}
	if s.manaResp != nil {
		return s.manaResp, nil
	}
	return &paymentsv1.CreateManaTopUpSessionResponse{
		PurchaseId:        "22222222-2222-7222-8222-222222222222",
		StripeSessionId:   "cs_test_mana",
		StripeCheckoutUrl: "https://stub.invalid/cs_test_mana",
		State:             paymentsv1.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

func newBufconnClient(t *testing.T, srv *fakeServer) (*payments.GRPCClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	paymentsv1.RegisterPaymentServiceServer(gs, srv)
	go func() {
		_ = gs.Serve(lis)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	client := payments.NewGRPCClientFromConn(conn)
	cleanup := func() {
		_ = conn.Close()
		gs.GracefulStop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestNewGRPCClient_RequiresAddr(t *testing.T) {
	if _, err := payments.NewGRPCClient(payments.Config{Addr: ""}); !errors.Is(err, payments.ErrNoAddr) {
		t.Fatalf("expected ErrNoAddr, got %v", err)
	}
}

func TestNewGRPCClientFromEnv_FailLoudWhenUnset(t *testing.T) {
	t.Setenv("CHORA_PAYMENTS_GRPC_ADDR", "")
	if _, err := payments.NewGRPCClientFromEnv(); !errors.Is(err, payments.ErrNoAddr) {
		t.Fatalf("expected ErrNoAddr when env unset, got %v", err)
	}
}

func TestGRPCClient_CreateFamiliarEggCheckoutSession_MapsAndReturns(t *testing.T) {
	srv := &fakeServer{}
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	soft := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	hard := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	out, err := client.CreateFamiliarEggCheckoutSession(ctx, payments.CreateFamiliarEggCheckoutInput{
		IdempotencyKey:       "idem-egg-1",
		TenantID:             "tenant-1",
		LearnerGCID:          "gcid-1",
		EggSKU:               "egg.standard.v1",
		SuggestedFocalAtomID: "atom-1",
		AmountCents:          999,
		Currency:             "SGD",
		SoftExpiryAt:         soft,
		HardExpiryAt:         hard,
		SuccessURL:           "https://chora.site/familiar-eggs/success",
		CancelURL:            "https://chora.site/familiar-eggs/cancel",
	})
	if err != nil {
		t.Fatalf("CreateFamiliarEggCheckoutSession returned err: %v", err)
	}
	if out.PurchaseID == "" || out.StripeSessionID != "cs_test_egg" {
		t.Fatalf("unexpected output: %+v", out)
	}
	if out.StripeCheckoutURL != "https://stub.invalid/cs_test_egg" {
		t.Fatalf("checkout url not propagated: %+v", out)
	}
	if out.State != "checkout_started" {
		t.Fatalf("state not mapped: got %q want checkout_started", out.State)
	}

	got := srv.gotEggReq
	if got == nil {
		t.Fatalf("server did not see a request")
	}
	if got.IdempotencyKey != "idem-egg-1" || got.TenantId != "tenant-1" ||
		got.LearnerGcid != "gcid-1" || got.EggSku != "egg.standard.v1" {
		t.Fatalf("request fields not mapped: %+v", got)
	}
	if got.SuggestedFocalAtomId != "atom-1" || got.AmountCents != 999 || got.Currency != "SGD" {
		t.Fatalf("request fields not mapped: %+v", got)
	}
	if got.SoftExpiryAt == nil || !got.SoftExpiryAt.AsTime().Equal(soft) {
		t.Fatalf("soft expiry not mapped: %v", got.SoftExpiryAt)
	}
	if got.HardExpiryAt == nil || !got.HardExpiryAt.AsTime().Equal(hard) {
		t.Fatalf("hard expiry not mapped: %v", got.HardExpiryAt)
	}
	if got.SuccessUrl == "" || got.CancelUrl == "" {
		t.Fatalf("urls not mapped: %+v", got)
	}
}

func TestGRPCClient_CreateFamiliarEggCheckoutSession_PropagatesError(t *testing.T) {
	boom := errors.New("payments boom")
	srv := &fakeServer{eggReturnErr: boom}
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.CreateFamiliarEggCheckoutSession(ctx, payments.CreateFamiliarEggCheckoutInput{
		IdempotencyKey: "idem-egg-1",
		TenantID:       "tenant-1",
		LearnerGCID:    "gcid-1",
		EggSKU:         "egg.standard.v1",
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err == nil || !strings.Contains(err.Error(), "payments boom") {
		t.Fatalf("expected error to propagate, got %v", err)
	}
}

func TestGRPCClient_CreateFamiliarEggCheckoutSession_RequiresInputs(t *testing.T) {
	srv := &fakeServer{}
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()
	cases := []struct {
		name string
		in   payments.CreateFamiliarEggCheckoutInput
	}{
		{name: "missing idempotency", in: payments.CreateFamiliarEggCheckoutInput{TenantID: "t", LearnerGCID: "g", EggSKU: "s", AmountCents: 1, Currency: "SGD"}},
		{name: "missing tenant", in: payments.CreateFamiliarEggCheckoutInput{IdempotencyKey: "k", LearnerGCID: "g", EggSKU: "s", AmountCents: 1, Currency: "SGD"}},
		{name: "missing gcid", in: payments.CreateFamiliarEggCheckoutInput{IdempotencyKey: "k", TenantID: "t", EggSKU: "s", AmountCents: 1, Currency: "SGD"}},
		{name: "missing sku", in: payments.CreateFamiliarEggCheckoutInput{IdempotencyKey: "k", TenantID: "t", LearnerGCID: "g", AmountCents: 1, Currency: "SGD"}},
		{name: "zero amount", in: payments.CreateFamiliarEggCheckoutInput{IdempotencyKey: "k", TenantID: "t", LearnerGCID: "g", EggSKU: "s", AmountCents: 0, Currency: "SGD"}},
		{name: "missing currency", in: payments.CreateFamiliarEggCheckoutInput{IdempotencyKey: "k", TenantID: "t", LearnerGCID: "g", EggSKU: "s", AmountCents: 1}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.CreateFamiliarEggCheckoutSession(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestGRPCClient_CreateManaTopUpSession_MapsAndReturns(t *testing.T) {
	srv := &fakeServer{}
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := client.CreateManaTopUpSession(ctx, payments.CreateManaTopUpInput{
		IdempotencyKey: "idem-mana-1",
		TenantID:       "tenant-1",
		AdminGCID:      "gcid-admin",
		SKU:            "mana.tenant_topup.standard_v1",
		ManaUnits:      10_000,
		AmountCents:    9999,
		Currency:       "SGD",
		SuccessURL:     "https://chora.site/mana/success",
		CancelURL:      "https://chora.site/mana/cancel",
	})
	if err != nil {
		t.Fatalf("CreateManaTopUpSession returned err: %v", err)
	}
	if out.PurchaseID == "" || out.StripeSessionID != "cs_test_mana" {
		t.Fatalf("unexpected output: %+v", out)
	}
	if out.State != "checkout_started" {
		t.Fatalf("state not mapped: got %q", out.State)
	}
	got := srv.gotManaReq
	if got == nil {
		t.Fatalf("server did not see a request")
	}
	if got.IdempotencyKey != "idem-mana-1" || got.TenantId != "tenant-1" ||
		got.AdminGcid != "gcid-admin" || got.Sku != "mana.tenant_topup.standard_v1" {
		t.Fatalf("request fields not mapped: %+v", got)
	}
	if got.ManaUnits != 10_000 || got.AmountCents != 9999 || got.Currency != "SGD" {
		t.Fatalf("amounts not mapped: %+v", got)
	}
}

func TestGRPCClient_CreateManaTopUpSession_RequiresInputs(t *testing.T) {
	srv := &fakeServer{}
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()
	cases := []struct {
		name string
		in   payments.CreateManaTopUpInput
	}{
		{name: "missing idempotency", in: payments.CreateManaTopUpInput{TenantID: "t", AdminGCID: "g", SKU: "s", ManaUnits: 1, AmountCents: 1, Currency: "SGD"}},
		{name: "missing tenant", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", AdminGCID: "g", SKU: "s", ManaUnits: 1, AmountCents: 1, Currency: "SGD"}},
		{name: "missing admin gcid", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", TenantID: "t", SKU: "s", ManaUnits: 1, AmountCents: 1, Currency: "SGD"}},
		{name: "missing sku", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", TenantID: "t", AdminGCID: "g", ManaUnits: 1, AmountCents: 1, Currency: "SGD"}},
		{name: "zero units", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", TenantID: "t", AdminGCID: "g", SKU: "s", ManaUnits: 0, AmountCents: 1, Currency: "SGD"}},
		{name: "zero amount", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", TenantID: "t", AdminGCID: "g", SKU: "s", ManaUnits: 1, AmountCents: 0, Currency: "SGD"}},
		{name: "missing currency", in: payments.CreateManaTopUpInput{IdempotencyKey: "k", TenantID: "t", AdminGCID: "g", SKU: "s", ManaUnits: 1, AmountCents: 1}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.CreateManaTopUpSession(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestGRPCClient_StateMapping(t *testing.T) {
	cases := []struct {
		in   paymentsv1.PurchaseState
		want string
	}{
		{paymentsv1.PurchaseState_PURCHASE_STATE_UNSPECIFIED, "unspecified"},
		{paymentsv1.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED, "checkout_started"},
		{paymentsv1.PurchaseState_PURCHASE_STATE_PAYMENT_CAPTURED, "payment_captured"},
		{paymentsv1.PurchaseState_PURCHASE_STATE_PAYMENT_FAILED, "payment_failed"},
		{paymentsv1.PurchaseState_PURCHASE_STATE_REFUNDED, "refunded"},
		{paymentsv1.PurchaseState_PURCHASE_STATE_EXPIRED, "expired"},
	}
	for _, tc := range cases {
		if got := payments.PurchaseStateToString(tc.in); got != tc.want {
			t.Errorf("PurchaseStateToString(%v) = %q want %q", tc.in, got, tc.want)
		}
	}
}
