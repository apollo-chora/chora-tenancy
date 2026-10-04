// stripe_secrets.go — resolves Stripe credentials (API key + webhook secret)
// from GCP Secret Manager with raw-env back-compat.
//
// Resolution order (per env-name pair):
//   1. {ID}_SECRET_ID is set → fetch from Secret Manager via cgcsecrets.Client
//   2. {RAW} env is set → return the literal value (dev convenience)
//   3. Both unset → return empty string (downstream gate decides)
//
// Per `feedback_no_inline_config` + .claude/skills/secrets-and-env: production
// MUST point at a Secret Manager secret via *_SECRET_ID; the raw-env path is
// retained ONLY for local dev where Secret Manager access is impractical.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	cgcsecrets "github.com/5007-Capstone/chora/libs/chora-go-common/secrets"
)

// stripeSecretResolver is the minimal Secret Manager surface needed for
// Stripe credential lookup. Production wires *cgcsecrets.Client; tests
// pass a *cgcsecrets.StubClient.
type stripeSecretResolver interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

// resolveStripeSecret resolves a single Stripe secret with back-compat
// fallback to a raw env value.
//
// Returns (value, source, err) where source describes how the value was
// resolved ("secret-manager", "raw-env", or "unset") for caller logging.
func resolveStripeSecret(ctx context.Context, resolver stripeSecretResolver, secretIDEnvKey, rawEnvKey string) (string, string, error) {
	secretID := strings.TrimSpace(os.Getenv(secretIDEnvKey))
	if secretID != "" {
		if resolver == nil {
			return "", "", fmt.Errorf("%s=%q requires Secret Manager client (none initialised)", secretIDEnvKey, secretID)
		}
		val, err := resolver.GetSecret(ctx, secretID)
		if err != nil {
			return "", "", fmt.Errorf("%s=%q: %w", secretIDEnvKey, secretID, err)
		}
		return strings.TrimSpace(val), "secret-manager", nil
	}
	if raw := strings.TrimSpace(os.Getenv(rawEnvKey)); raw != "" {
		return raw, "raw-env", nil
	}
	return "", "unset", nil
}

// bootstrapStripeSecretClient constructs a *cgcsecrets.Client when ANY
// STRIPE_*_SECRET_ID env var is set. Returns (nil, nil, nil) when no
// SECRET_ID is set (dev mode — only raw env values will be consulted).
//
// The caller MUST defer the returned shutdown when non-nil to release
// the underlying gRPC connection.
func bootstrapStripeSecretClient(ctx context.Context) (stripeSecretResolver, func(), error) {
	if !stripeNeedsSecretClient() {
		return nil, nil, nil
	}
	project := strings.TrimSpace(os.Getenv("CHORA_STRIPE_SECRETS_PROJECT"))
	if project == "" {
		project = strings.TrimSpace(os.Getenv("CHORA_DB_PROJECT"))
	}
	if project == "" {
		project = "chora-489812"
	}
	c, err := cgcsecrets.NewClient(ctx, project)
	if err != nil {
		return nil, nil, fmt.Errorf("stripe secrets: %w", err)
	}
	shutdown := func() {
		if cerr := c.Close(); cerr != nil {
			log.Printf("tenancy: stripe secret client close: %v", cerr)
		}
	}
	return c, shutdown, nil
}

// stripeNeedsSecretClient reports whether any STRIPE *_SECRET_ID env var
// is set — kept exported (lowercase, same package) for unit tests.
func stripeNeedsSecretClient() bool {
	for _, key := range []string{
		"STRIPE_API_KEY_SECRET_ID",
		"STRIPE_WEBHOOK_SECRET_SECRET_ID",
		"STRIPE_PUBLISHABLE_KEY_SECRET_ID",
	} {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

// stripeCredentials are the resolved Stripe values consumed by the
// familiar-egg wiring. All fields may be empty when CHORA_ENV != prod.
type stripeCredentials struct {
	APIKey         string
	APIKeySource   string
	WebhookSecret  string
	WebhookSource  string
}

// resolveStripeCredentials runs both resolutions and packages the result.
// Errors short-circuit on the first failed Secret Manager fetch.
func resolveStripeCredentials(ctx context.Context, resolver stripeSecretResolver) (stripeCredentials, error) {
	apiKey, apiSrc, err := resolveStripeSecret(ctx, resolver, "STRIPE_API_KEY_SECRET_ID", "STRIPE_API_KEY")
	if err != nil {
		return stripeCredentials{}, fmt.Errorf("stripe api key: %w", err)
	}
	webhook, webhookSrc, err := resolveStripeSecret(ctx, resolver, "STRIPE_WEBHOOK_SECRET_SECRET_ID", "STRIPE_WEBHOOK_SECRET")
	if err != nil {
		return stripeCredentials{}, fmt.Errorf("stripe webhook secret: %w", err)
	}
	return stripeCredentials{
		APIKey:        apiKey,
		APIKeySource:  apiSrc,
		WebhookSecret: webhook,
		WebhookSource: webhookSrc,
	}, nil
}

// errStripeSecretManagerRequired is returned by bootstrapStripeSecretClient
// when GSM init fails AND any *_SECRET_ID is set (so falling through to
// raw-env would silently bypass the operator's explicit GSM directive).
var errStripeSecretManagerRequired = errors.New(
	"STRIPE_*_SECRET_ID is set but Secret Manager client could not be initialised — fix ADC / WIF or unset the *_SECRET_ID env var",
)
