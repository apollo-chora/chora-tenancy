package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	cgcsecrets "github.com/5007-Capstone/chora/libs/chora-go-common/secrets"
)

func TestResolveStripeSecret_ReturnsRawEnvWhenSecretIDUnset(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
	t.Setenv("STRIPE_API_KEY", "sk_test_raw_dev_value")

	val, src, err := resolveStripeSecret(context.Background(), nil, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != "sk_test_raw_dev_value" {
		t.Errorf("value: want raw env; got %q", val)
	}
	if src != "raw-env" {
		t.Errorf("source: want raw-env; got %q", src)
	}
}

func TestResolveStripeSecret_FetchesFromSecretManagerWhenSecretIDSet(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key")
	t.Setenv("STRIPE_API_KEY", "raw-fallback-must-not-be-used")

	stub := cgcsecrets.NewStubClient(map[string]string{
		"chora-stripe-api-key": "sk_test_from_gsm_value",
	})
	val, src, err := resolveStripeSecret(context.Background(), stub, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != "sk_test_from_gsm_value" {
		t.Errorf("value: want GSM value; got %q", val)
	}
	if src != "secret-manager" {
		t.Errorf("source: want secret-manager; got %q", src)
	}
}

func TestResolveStripeSecret_TrimsWhitespaceFromValues(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key")
	stub := cgcsecrets.NewStubClient(map[string]string{
		"chora-stripe-api-key": "  sk_test_padded  \n",
	})
	val, _, err := resolveStripeSecret(context.Background(), stub, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != "sk_test_padded" {
		t.Errorf("value: want trimmed; got %q", val)
	}
}

func TestResolveStripeSecret_RawEnvTrimsWhitespace(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
	t.Setenv("STRIPE_API_KEY", "  sk_test_raw_padded  ")
	val, _, err := resolveStripeSecret(context.Background(), nil, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != "sk_test_raw_padded" {
		t.Errorf("want trimmed raw env value; got %q", val)
	}
}

func TestResolveStripeSecret_ReturnsEmptyWhenBothUnset(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
	t.Setenv("STRIPE_API_KEY", "")

	val, src, err := resolveStripeSecret(context.Background(), nil, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != "" {
		t.Errorf("value: want empty; got %q", val)
	}
	if src != "unset" {
		t.Errorf("source: want unset; got %q", src)
	}
}

func TestResolveStripeSecret_ErrorsWhenSecretIDSetButResolverNil(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key")

	_, _, err := resolveStripeSecret(context.Background(), nil, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err == nil {
		t.Fatal("want error when SECRET_ID set but resolver nil; got nil")
	}
	if !strings.Contains(err.Error(), "Secret Manager client") {
		t.Errorf("error should mention the missing client; got %v", err)
	}
}

func TestResolveStripeSecret_PropagatesSecretManagerNotFound(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key-missing")

	stub := cgcsecrets.NewStubClient(map[string]string{
		"chora-stripe-api-key": "different-secret-not-the-one-we-asked-for",
	})
	_, _, err := resolveStripeSecret(context.Background(), stub, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err == nil {
		t.Fatal("want error for missing secret; got nil")
	}
	if !errors.Is(err, cgcsecrets.ErrSecretNotFound) {
		t.Errorf("error should wrap ErrSecretNotFound; got %v", err)
	}
}

func TestStripeNeedsSecretClient_FalseWhenNoSecretIDs(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET_SECRET_ID", "")
	t.Setenv("STRIPE_PUBLISHABLE_KEY_SECRET_ID", "")
	if stripeNeedsSecretClient() {
		t.Error("want false when no STRIPE_*_SECRET_ID set")
	}
}

func TestStripeNeedsSecretClient_TrueWhenAnySecretIDSet(t *testing.T) {
	for _, key := range []string{
		"STRIPE_API_KEY_SECRET_ID",
		"STRIPE_WEBHOOK_SECRET_SECRET_ID",
		"STRIPE_PUBLISHABLE_KEY_SECRET_ID",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
			t.Setenv("STRIPE_WEBHOOK_SECRET_SECRET_ID", "")
			t.Setenv("STRIPE_PUBLISHABLE_KEY_SECRET_ID", "")
			t.Setenv(key, "some-secret-name")
			if !stripeNeedsSecretClient() {
				t.Errorf("want true when %s set", key)
			}
		})
	}
}

func TestResolveStripeCredentials_HappyPathMixedSources(t *testing.T) {
	// API key from GSM, webhook from raw env.
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key")
	t.Setenv("STRIPE_WEBHOOK_SECRET_SECRET_ID", "")
	t.Setenv("STRIPE_API_KEY", "raw-fallback-must-not-be-used")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_raw_dev_value")

	stub := cgcsecrets.NewStubClient(map[string]string{
		"chora-stripe-api-key": "sk_test_from_gsm",
	})

	creds, err := resolveStripeCredentials(context.Background(), stub)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if creds.APIKey != "sk_test_from_gsm" {
		t.Errorf("APIKey: got %q", creds.APIKey)
	}
	if creds.APIKeySource != "secret-manager" {
		t.Errorf("APIKeySource: got %q", creds.APIKeySource)
	}
	if creds.WebhookSecret != "whsec_raw_dev_value" {
		t.Errorf("WebhookSecret: got %q", creds.WebhookSecret)
	}
	if creds.WebhookSource != "raw-env" {
		t.Errorf("WebhookSource: got %q", creds.WebhookSource)
	}
}

func TestResolveStripeCredentials_BothFromGSM(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "chora-stripe-api-key")
	t.Setenv("STRIPE_WEBHOOK_SECRET_SECRET_ID", "chora-stripe-webhook-secret")

	stub := cgcsecrets.NewStubClient(map[string]string{
		"chora-stripe-api-key":        "sk_test_a",
		"chora-stripe-webhook-secret": "whsec_b",
	})
	creds, err := resolveStripeCredentials(context.Background(), stub)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if creds.APIKey != "sk_test_a" || creds.WebhookSecret != "whsec_b" {
		t.Errorf("creds: got %+v", creds)
	}
}

func TestResolveStripeCredentials_ErrorsOnGSMFetchFailure(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "missing-secret")
	stub := cgcsecrets.NewStubClient(map[string]string{})
	_, err := resolveStripeCredentials(context.Background(), stub)
	if err == nil {
		t.Fatal("want error from GSM fetch failure; got nil")
	}
	if !strings.Contains(err.Error(), "stripe api key") {
		t.Errorf("error should identify which secret failed; got %v", err)
	}
}

func TestResolveStripeCredentials_BothUnsetIsValidDevMode(t *testing.T) {
	t.Setenv("STRIPE_API_KEY_SECRET_ID", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET_SECRET_ID", "")
	t.Setenv("STRIPE_API_KEY", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")

	creds, err := resolveStripeCredentials(context.Background(), nil)
	if err != nil {
		t.Fatalf("dev mode (both unset) should not error; got %v", err)
	}
	if creds.APIKey != "" || creds.WebhookSecret != "" {
		t.Errorf("expected empty creds in dev mode; got %+v", creds)
	}
	if creds.APIKeySource != "unset" || creds.WebhookSource != "unset" {
		t.Errorf("expected unset sources; got %+v", creds)
	}
}
