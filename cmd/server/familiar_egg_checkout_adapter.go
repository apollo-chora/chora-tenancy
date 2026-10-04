package main

import (
	"context"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/payments"
)

// familiarEggCheckoutAdapter adapts the concrete chora-payments gRPC client to
// the httpapi.FamiliarEggCheckoutClient port so the http handler package stays
// decoupled from the payments adapter (ADR-164; mirrors paymentsHTTPAdapter).
// It is a thin field-map — the two structs are field-identical by design.
type familiarEggCheckoutAdapter struct {
	cli *payments.GRPCClient
}

func newFamiliarEggCheckoutAdapter(cli *payments.GRPCClient) familiarEggCheckoutAdapter {
	return familiarEggCheckoutAdapter{cli: cli}
}

func (a familiarEggCheckoutAdapter) CreateFamiliarEggCheckoutSession(ctx context.Context, in httpapi.EggCheckoutInput) (httpapi.EggCheckoutOutput, error) {
	out, err := a.cli.CreateFamiliarEggCheckoutSession(ctx, payments.CreateFamiliarEggCheckoutInput{
		IdempotencyKey:       in.IdempotencyKey,
		TenantID:             in.TenantID,
		LearnerGCID:          in.LearnerGCID,
		EggSKU:               in.EggSKU,
		SuggestedFocalAtomID: in.SuggestedFocalAtomID,
		AmountCents:          in.AmountCents,
		Currency:             in.Currency,
		SoftExpiryAt:         in.SoftExpiryAt,
		HardExpiryAt:         in.HardExpiryAt,
		SuccessURL:           in.SuccessURL,
		CancelURL:            in.CancelURL,
	})
	if err != nil {
		return httpapi.EggCheckoutOutput{}, err
	}
	return httpapi.EggCheckoutOutput{
		PurchaseID:        out.PurchaseID,
		StripeSessionID:   out.StripeSessionID,
		StripeCheckoutURL: out.StripeCheckoutURL,
		State:             out.State,
	}, nil
}
