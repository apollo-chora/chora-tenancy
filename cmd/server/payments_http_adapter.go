// payments_http_adapter.go — CHO-1767. Wires the chora-tenancy
// payments.HTTPClient into the unexported v2_handlers paymentsHTTPClient
// port. Translates between the two struct sets (the port is defined
// in httpapi to keep the package import-free of the payments adapter).
package main

import (
	"context"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/payments"
)

type paymentsHTTPAdapter struct {
	cli *payments.HTTPClientImpl
}

func newPaymentsHTTPAdapter(cli *payments.HTTPClientImpl) *paymentsHTTPAdapter {
	return &paymentsHTTPAdapter{cli: cli}
}

func (a *paymentsHTTPAdapter) ChangeTenantAddonTier(ctx context.Context, in httpapi.TenancyPaymentsChangeTierIn) (httpapi.TenancyPaymentsChangeTierOut, error) {
	out, err := a.cli.ChangeTenantAddonTier(ctx, payments.ChangeTierIn{
		TenantID:       in.TenantID,
		AdminGCID:      in.AdminGCID,
		AddonCode:      in.AddonCode,
		TargetTierCode: in.TargetTierCode,
		Currency:       in.Currency,
		ProrationMode:  in.ProrationMode,
		EffectiveAt:    in.EffectiveAt,
	})
	if err != nil {
		return httpapi.TenancyPaymentsChangeTierOut{}, err
	}
	return httpapi.TenancyPaymentsChangeTierOut{
		PurchaseID:           out.PurchaseID,
		StripeSubscriptionID: out.StripeSubscriptionID,
		FromTier:             out.FromTier,
		ToTier:               out.ToTier,
		BillingDeltaCents:    out.BillingDeltaCents,
		Currency:             out.Currency,
		EffectiveAt:          out.EffectiveAt,
		SubscriptionStatus:   out.SubscriptionStatus,
		DeferredToCycleEnd:   out.DeferredToCycleEnd,
		ScheduleID:           out.ScheduleID,
		ScheduledTierCode:    out.ScheduledTierCode,
		ScheduledEffectiveAt: out.ScheduledEffectiveAt,
	}, nil
}

func (a *paymentsHTTPAdapter) PreviewTenantAddonTier(ctx context.Context, in httpapi.TenancyPaymentsPreviewIn) (httpapi.TenancyPaymentsPreviewOut, error) {
	out, err := a.cli.PreviewTenantAddonTier(ctx, payments.PreviewTierIn{
		TenantID:       in.TenantID,
		AddonCode:      in.AddonCode,
		TargetTierCode: in.TargetTierCode,
		Currency:       in.Currency,
		ProrationMode:  in.ProrationMode,
		EffectiveAt:    in.EffectiveAt,
	})
	if err != nil {
		return httpapi.TenancyPaymentsPreviewOut{}, err
	}
	return httpapi.TenancyPaymentsPreviewOut{
		PurchaseID:            out.PurchaseID,
		FromTier:              out.FromTier,
		ToTier:                out.ToTier,
		BillingDeltaCents:     out.BillingDeltaCents,
		NextInvoiceTotalCents: out.NextInvoiceTotalCents,
		Currency:              out.Currency,
		ProrationMode:         out.ProrationMode,
		DeferredToCycleEnd:    out.DeferredToCycleEnd,
		EffectiveAt:           out.EffectiveAt,
	}, nil
}
