package main

import (
	"context"
	"os"
	"strings"
)

// Stripe credentials are configured directly through environment variables.
// Docker Compose loads them from .env; no cloud secret manager is required.
type stripeCredentials struct {
	APIKey        string
	APIKeySource  string
	WebhookSecret string
	WebhookSource string
}

func resolveStripeCredentials(_ context.Context, _ any) (stripeCredentials, error) {
	apiKey := strings.TrimSpace(os.Getenv("STRIPE_API_KEY"))
	webhook := strings.TrimSpace(os.Getenv("STRIPE_WEBHOOK_SECRET"))
	apiSource := "unset"
	webhookSource := "unset"
	if apiKey != "" {
		apiSource = "env"
	}
	if webhook != "" {
		webhookSource = "env"
	}
	return stripeCredentials{
		APIKey: apiKey, APIKeySource: apiSource,
		WebhookSecret: webhook, WebhookSource: webhookSource,
	}, nil
}

func bootstrapStripeSecretClient(context.Context) (any, func(), error) {
	return nil, nil, nil
}
