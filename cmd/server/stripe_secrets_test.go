package main

import (
	"context"
	"testing"
)

func TestResolveStripeCredentialsFromEnvironment(t *testing.T) {
	t.Setenv("STRIPE_API_KEY", "  sk_test_local  ")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "  whsec_local  ")
	creds, err := resolveStripeCredentials(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.APIKey != "sk_test_local" || creds.APIKeySource != "env" {
		t.Fatalf("unexpected API key resolution: %+v", creds)
	}
	if creds.WebhookSecret != "whsec_local" || creds.WebhookSource != "env" {
		t.Fatalf("unexpected webhook resolution: %+v", creds)
	}
}

func TestResolveStripeCredentialsUnset(t *testing.T) {
	t.Setenv("STRIPE_API_KEY", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")
	creds, err := resolveStripeCredentials(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.APIKeySource != "unset" || creds.WebhookSource != "unset" {
		t.Fatalf("expected unset sources: %+v", creds)
	}
}
