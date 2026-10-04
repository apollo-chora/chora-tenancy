// Package payments is the chora-tenancy adapter for the chora-payments
// gRPC service (ADR-164).
//
// chora-tenancy uses this client for the SYNCHRONOUS-WRITE path of two
// Purchase aggregates it originates:
//
//   - CreateFamiliarEggCheckoutSession — ADR-149 FamiliarEgg purchase
//     (POST /api/familiar-eggs/checkout HTTP handler).
//   - CreateManaTopUpSession — tenant mana pool top-up (POST /api/v1/admin/
//     tenants/{tenantId}/mana-pool:topup HTTP handler).
//
// chora-tenancy NO LONGER owns the Stripe SDK — the canonical Stripe
// integration lives in chora-payments. This client routes the
// originating-service write path to that home over mTLS-backed gRPC
// (Cloud Service Mesh handles the SPIFFE identity + transport).
//
// The async ORIGINATING-SERVICE NOTIFICATION path (Pub/Sub events
// chora.payments.{aggregate}.{event_type}.v1) lives in
// internal/adapter/events/payments_subscriber.go — it is what materialises
// FamiliarInstance provisioning + tenant_mana_pool.balance_units
// credit/debit operations that only chora-tenancy can do.
//
// Per `feedback_no_inline_config`: dial target comes from
// CHORA_PAYMENTS_GRPC_ADDR (e.g., `chora-payments.payments.svc.cluster.
// local:9090` in-cluster, `localhost:9090` for local dev with
// docker-compose). Fail-loud on missing.
//
// Per ADR-164 §D5: this is the synchronous outbound path. Returns
// (purchase_id, checkout_url, session_id, state=CHECKOUT_STARTED).
package payments

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// ErrNoAddr is returned when CHORA_PAYMENTS_GRPC_ADDR is unset or empty.
// Bootstrap MUST fail loud rather than silently default to a stub —
// `feedback_no_stubs_real_wiring`.
var ErrNoAddr = errors.New("payments: CHORA_PAYMENTS_GRPC_ADDR required (no inline default)")

// Config wires the gRPC client.
type Config struct {
	// Addr is the dial target (host:port). REQUIRED.
	Addr string
	// DialTimeout caps the initial NewClient establishment. Defaults to
	// 5 s when zero.
	DialTimeout time.Duration
	// DialOptions appends to the default option set (mainly TLS / TLS-skip
	// in dev). When nil, the client defaults to insecure credentials,
	// matching the canonical in-cluster pattern where Cloud Service Mesh
	// terminates mTLS at the sidecar.
	DialOptions []grpc.DialOption
}

// GRPCClient is the thin wrapper over the generated PaymentServiceClient.
type GRPCClient struct {
	cc  *grpc.ClientConn
	cli paymentsv1.PaymentServiceClient
}

// NewGRPCClient dials chora-payments + constructs the wrapper. Fail-loud
// when Addr is empty.
func NewGRPCClient(cfg Config) (*GRPCClient, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrNoAddr
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	opts := cfg.DialOptions
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(cfg.Addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("payments: dial %s: %w", cfg.Addr, err)
	}
	return &GRPCClient{cc: conn, cli: paymentsv1.NewPaymentServiceClient(conn)}, nil
}

// NewGRPCClientFromConn wraps an externally-managed ClientConn (used by
// tests with bufconn + by callers that want to share a connection pool).
func NewGRPCClientFromConn(cc *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{cc: cc, cli: paymentsv1.NewPaymentServiceClient(cc)}
}

// NewGRPCClientFromEnv reads CHORA_PAYMENTS_GRPC_ADDR + builds a client.
// Fail-loud when unset.
func NewGRPCClientFromEnv() (*GRPCClient, error) {
	addr := strings.TrimSpace(os.Getenv("CHORA_PAYMENTS_GRPC_ADDR"))
	if addr == "" {
		return nil, ErrNoAddr
	}
	return NewGRPCClient(Config{Addr: addr})
}

// Close drains the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c == nil || c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

// -----------------------------------------------------------------------------
// CreateFamiliarEggCheckoutSession
// -----------------------------------------------------------------------------

// CreateFamiliarEggCheckoutInput captures the fields the chora-tenancy
// `/api/familiar-eggs/checkout` handler needs to mint a Stripe Checkout
// session via chora-payments. All fields except Suggested* + URLs are
// required.
type CreateFamiliarEggCheckoutInput struct {
	IdempotencyKey       string
	TenantID             string
	LearnerGCID          string
	EggSKU               string
	SuggestedFocalAtomID string // optional — empty for open-ended SKUs
	AmountCents          int64
	Currency             string // ISO 4217
	SoftExpiryAt         time.Time
	HardExpiryAt         time.Time
	SuccessURL           string // optional; chora-payments env-fallback when empty
	CancelURL            string // optional; chora-payments env-fallback when empty
}

// CreateFamiliarEggCheckoutOutput is the parsed gRPC response.
type CreateFamiliarEggCheckoutOutput struct {
	PurchaseID        string
	StripeSessionID   string
	StripeCheckoutURL string
	State             string // canonical: "checkout_started"
}

// CreateFamiliarEggCheckoutSession invokes chora-payments via gRPC.
func (c *GRPCClient) CreateFamiliarEggCheckoutSession(ctx context.Context, in CreateFamiliarEggCheckoutInput) (CreateFamiliarEggCheckoutOutput, error) {
	if c == nil || c.cli == nil {
		return CreateFamiliarEggCheckoutOutput{}, errors.New("payments: nil GRPCClient")
	}
	if err := validateEggInput(in); err != nil {
		return CreateFamiliarEggCheckoutOutput{}, err
	}
	req := &paymentsv1.CreateCompanionEggCheckoutSessionRequest{
		IdempotencyKey:       in.IdempotencyKey,
		TenantId:             in.TenantID,
		LearnerGcid:          in.LearnerGCID,
		EggSku:               in.EggSKU,
		SuggestedFocalAtomId: in.SuggestedFocalAtomID,
		AmountCents:          in.AmountCents,
		Currency:             in.Currency,
		SuccessUrl:           in.SuccessURL,
		CancelUrl:            in.CancelURL,
	}
	if !in.SoftExpiryAt.IsZero() {
		req.SoftExpiryAt = timestamppb.New(in.SoftExpiryAt.UTC())
	}
	if !in.HardExpiryAt.IsZero() {
		req.HardExpiryAt = timestamppb.New(in.HardExpiryAt.UTC())
	}
	resp, err := c.cli.CreateCompanionEggCheckoutSession(ctx, req)
	if err != nil {
		return CreateFamiliarEggCheckoutOutput{}, fmt.Errorf("payments: CreateFamiliarEggCheckoutSession: %w", err)
	}
	if resp == nil {
		return CreateFamiliarEggCheckoutOutput{}, errors.New("payments: nil response")
	}
	return CreateFamiliarEggCheckoutOutput{
		PurchaseID:        resp.PurchaseId,
		StripeSessionID:   resp.StripeSessionId,
		StripeCheckoutURL: resp.StripeCheckoutUrl,
		State:             PurchaseStateToString(resp.State),
	}, nil
}

func validateEggInput(in CreateFamiliarEggCheckoutInput) error {
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return errors.New("payments: idempotency_key required")
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return errors.New("payments: tenant_id required")
	}
	if strings.TrimSpace(in.LearnerGCID) == "" {
		return errors.New("payments: learner_gcid required")
	}
	if strings.TrimSpace(in.EggSKU) == "" {
		return errors.New("payments: egg_sku required")
	}
	if in.AmountCents <= 0 {
		return errors.New("payments: amount_cents must be > 0")
	}
	if strings.TrimSpace(in.Currency) == "" {
		return errors.New("payments: currency required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// CreateManaTopUpSession
// -----------------------------------------------------------------------------

// CreateManaTopUpInput captures the fields the chora-tenancy
// `mana-pool:topup` handler needs to mint a Stripe Checkout session via
// chora-payments. The tenant_admin's GCID is sent as admin_gcid (the
// originator); the target tenant scope is on TenantID.
type CreateManaTopUpInput struct {
	IdempotencyKey string
	TenantID       string
	AdminGCID      string
	SKU            string
	ManaUnits      int64
	AmountCents    int64
	Currency       string
	SuccessURL     string
	CancelURL      string
}

// CreateManaTopUpOutput is the parsed gRPC response.
type CreateManaTopUpOutput struct {
	PurchaseID        string
	StripeSessionID   string
	StripeCheckoutURL string
	State             string
}

// CreateManaTopUpSession invokes chora-payments via gRPC.
func (c *GRPCClient) CreateManaTopUpSession(ctx context.Context, in CreateManaTopUpInput) (CreateManaTopUpOutput, error) {
	if c == nil || c.cli == nil {
		return CreateManaTopUpOutput{}, errors.New("payments: nil GRPCClient")
	}
	if err := validateManaInput(in); err != nil {
		return CreateManaTopUpOutput{}, err
	}
	resp, err := c.cli.CreateManaTopUpSession(ctx, &paymentsv1.CreateManaTopUpSessionRequest{
		IdempotencyKey: in.IdempotencyKey,
		TenantId:       in.TenantID,
		AdminGcid:      in.AdminGCID,
		Sku:            in.SKU,
		ManaUnits:      in.ManaUnits,
		AmountCents:    in.AmountCents,
		Currency:       in.Currency,
		SuccessUrl:     in.SuccessURL,
		CancelUrl:      in.CancelURL,
	})
	if err != nil {
		return CreateManaTopUpOutput{}, fmt.Errorf("payments: CreateManaTopUpSession: %w", err)
	}
	if resp == nil {
		return CreateManaTopUpOutput{}, errors.New("payments: nil response")
	}
	return CreateManaTopUpOutput{
		PurchaseID:        resp.PurchaseId,
		StripeSessionID:   resp.StripeSessionId,
		StripeCheckoutURL: resp.StripeCheckoutUrl,
		State:             PurchaseStateToString(resp.State),
	}, nil
}

func validateManaInput(in CreateManaTopUpInput) error {
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return errors.New("payments: idempotency_key required")
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return errors.New("payments: tenant_id required")
	}
	if strings.TrimSpace(in.AdminGCID) == "" {
		return errors.New("payments: admin_gcid required")
	}
	if strings.TrimSpace(in.SKU) == "" {
		return errors.New("payments: sku required")
	}
	if in.ManaUnits <= 0 {
		return errors.New("payments: mana_units must be > 0")
	}
	if in.AmountCents <= 0 {
		return errors.New("payments: amount_cents must be > 0")
	}
	if strings.TrimSpace(in.Currency) == "" {
		return errors.New("payments: currency required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// State mapping
// -----------------------------------------------------------------------------

// PurchaseStateToString maps the canonical proto enum to the snake_case
// state string used in chora-tenancy HTTP responses + DB state column.
func PurchaseStateToString(s paymentsv1.PurchaseState) string {
	switch s {
	case paymentsv1.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED:
		return "checkout_started"
	case paymentsv1.PurchaseState_PURCHASE_STATE_PAYMENT_CAPTURED:
		return "payment_captured"
	case paymentsv1.PurchaseState_PURCHASE_STATE_PAYMENT_FAILED:
		return "payment_failed"
	case paymentsv1.PurchaseState_PURCHASE_STATE_REFUNDED:
		return "refunded"
	case paymentsv1.PurchaseState_PURCHASE_STATE_EXPIRED:
		return "expired"
	default:
		return "unspecified"
	}
}
