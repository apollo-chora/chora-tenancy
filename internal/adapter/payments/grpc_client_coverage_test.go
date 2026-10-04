// Coverage tests for the ADR-164 gRPC client — Close + nil-receiver +
// env-driven constructor happy path.
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

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/payments"
)

func TestNewGRPCClient_DialsAndCloses(t *testing.T) {
	// We construct a real bufconn-backed gRPC client so Close has a real
	// connection to drain. Skipping a live network dial keeps the test
	// hermetic.
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()
	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.GracefulStop()

	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(context.Background())
	}
	cli, err := payments.NewGRPCClient(payments.Config{
		Addr:        "passthrough://bufnet",
		DialTimeout: 100 * time.Millisecond,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Calling Close on nil should not panic.
	var nilCli *payments.GRPCClient
	if err := nilCli.Close(); err != nil {
		t.Fatalf("Close on nil should be a no-op, got %v", err)
	}
}

func TestNewGRPCClientFromEnv_ReadsEnv(t *testing.T) {
	t.Setenv("CHORA_PAYMENTS_GRPC_ADDR", "passthrough://invalid")
	cli, err := payments.NewGRPCClientFromEnv()
	if err != nil {
		t.Fatalf("expected success when env set, got %v", err)
	}
	if cli == nil {
		t.Fatalf("nil client")
	}
	_ = cli.Close()
}

func TestGRPCClient_NilReceiver_CreateFamiliarEgg(t *testing.T) {
	var c *payments.GRPCClient
	_, err := c.CreateFamiliarEggCheckoutSession(context.Background(), payments.CreateFamiliarEggCheckoutInput{
		IdempotencyKey: "k", TenantID: "t", LearnerGCID: "g", EggSKU: "s", AmountCents: 1, Currency: "SGD",
	})
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("expected nil-receiver error, got %v", err)
	}
}

func TestGRPCClient_NilReceiver_CreateManaTopUp(t *testing.T) {
	var c *payments.GRPCClient
	_, err := c.CreateManaTopUpSession(context.Background(), payments.CreateManaTopUpInput{
		IdempotencyKey: "k", TenantID: "t", AdminGCID: "g", SKU: "s",
		ManaUnits: 1, AmountCents: 1, Currency: "SGD",
	})
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("expected nil-receiver error, got %v", err)
	}
}

// Sanity that ErrNoAddr is a sentinel — supports `errors.Is` for callers.
func TestErrNoAddr_Is(t *testing.T) {
	wrapped := errors.Join(payments.ErrNoAddr, errors.New("wrapper"))
	if !errors.Is(wrapped, payments.ErrNoAddr) {
		t.Fatalf("ErrNoAddr should be wrappable")
	}
}
