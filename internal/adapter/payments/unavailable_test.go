package payments

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestUnavailableFailsEveryOperationWithCodesUnavailable pins the contract the
// tenancy composition root relies on: when CHORA_PAYMENTS_GRPC_ADDR is unset
// the service still boots and only the payments-backed operations fail, with a
// status the caller can map to HTTP 503.
func TestUnavailableFailsEveryOperationWithCodesUnavailable(t *testing.T) {
	var u Unavailable
	if err := u.Close(); err != nil {
		t.Errorf("Close() = %v; want nil", err)
	}

	_, err := u.CreateFamiliarEggCheckoutSession(context.Background(), CreateFamiliarEggCheckoutInput{})
	if !errors.Is(err, ErrPaymentsNotConfigured) {
		t.Fatalf("checkout err = %v; want ErrPaymentsNotConfigured", err)
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("checkout status = %v; want %v", got, codes.Unavailable)
	}

	_, err = u.CreateManaTopUpSession(context.Background(), CreateManaTopUpInput{})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("top-up status = %v; want %v", got, codes.Unavailable)
	}
}
