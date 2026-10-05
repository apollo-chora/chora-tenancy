package payments

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unavailable is the chora-payments client used when CHORA_PAYMENTS_GRPC_ADDR
// is unset. It satisfies the same method set as GRPCClient and fails every
// operation with codes.Unavailable.
//
// Rationale: only the Familiar Egg checkout and the mana top-up session need
// chora-payments. Tenant reads, membership resolution and every other tenancy
// capability are unaffected, so a missing payments address must not stop the
// whole service from booting — it must only make those two operations report
// unavailable. Failing at the feature boundary keeps the failure loud where it
// matters without turning a feature dependency into a service-wide one.
type Unavailable struct{}

// ErrPaymentsNotConfigured is returned by every Unavailable operation.
var ErrPaymentsNotConfigured = status.Error(codes.Unavailable, "chora-payments is not configured (CHORA_PAYMENTS_GRPC_ADDR unset)")

// CreateFamiliarEggCheckoutSession always fails with codes.Unavailable.
func (Unavailable) CreateFamiliarEggCheckoutSession(context.Context, CreateFamiliarEggCheckoutInput) (CreateFamiliarEggCheckoutOutput, error) {
	return CreateFamiliarEggCheckoutOutput{}, ErrPaymentsNotConfigured
}

// CreateManaTopUpSession always fails with codes.Unavailable.
func (Unavailable) CreateManaTopUpSession(context.Context, CreateManaTopUpInput) (CreateManaTopUpOutput, error) {
	return CreateManaTopUpOutput{}, ErrPaymentsNotConfigured
}

// Close is a no-op; there is no connection to release.
func (Unavailable) Close() error { return nil }
