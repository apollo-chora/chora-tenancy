// Billing-webhook configuration — env-only.
//
// Ported from services/chora-billing-webhook/internal/config as part of the
// M12.2 Batch-1 consolidation. Per locked architecture (CLAUDE.md §1)
// Tenancy + Billing are a single combined supporting domain — the
// Stripe-webhook env contract joins chora-tenancy's existing config surface.
//
// Per .claude/skills/secrets-and-env: all values come from env vars. Empty
// required values surface as descriptive errors at startup so wiring
// failures are visible in Cloud Run logs immediately.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// BillingWebhookConfig is the typed runtime configuration for the
// Stripe-webhook ingress + reconciliation Job.
type BillingWebhookConfig struct {
	// Server
	Port string

	// Stripe
	StripeAPIBase       string // STRIPE_API_BASE (default https://api.stripe.com)
	StripeSecretKey     string // STRIPE_SECRET_KEY (Secret Manager)
	StripeWebhookSecret string // STRIPE_WEBHOOK_SECRET (Secret Manager)
	SignatureSkew       time.Duration

	// Pub/Sub source attribution + Cloud Trace correlation
	SourceProject string // CHORA_SOURCE_PROJECT (defaults to chora-489812)
	SourceService string

	// Reconciliation
	ToleranceBPS   int           // RECONCILIATION_TOLERANCE_BPS (default 10)
	IdempotencyTTL time.Duration // STRIPE_WEBHOOK_IDEMPOTENCY_TTL (default 168h = 7d)

	// Observability
	OTLPEndpoint string // OTEL_EXPORTER_OTLP_ENDPOINT
}

// LoadBillingWebhookServer loads config for the HTTP webhook server.
// Returns descriptive error when STRIPE_WEBHOOK_SECRET is missing.
func LoadBillingWebhookServer() (*BillingWebhookConfig, error) {
	cfg := &BillingWebhookConfig{
		Port:                envOr("PORT", "8080"),
		StripeAPIBase:       envOr("STRIPE_API_BASE", "https://api.stripe.com"),
		StripeSecretKey:     os.Getenv("STRIPE_SECRET_KEY"),
		StripeWebhookSecret: os.Getenv("STRIPE_WEBHOOK_SECRET"),
		SignatureSkew:       envDuration("STRIPE_SIGNATURE_SKEW", 5*time.Minute),
		SourceProject:       envOr("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService:       envOr("CHORA_SOURCE_SERVICE", "chora-tenancy-billing-webhook"),
		ToleranceBPS:        envInt("RECONCILIATION_TOLERANCE_BPS", 10),
		IdempotencyTTL:      envDuration("STRIPE_WEBHOOK_IDEMPOTENCY_TTL", 7*24*time.Hour),
		OTLPEndpoint:        os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
	if strings.TrimSpace(cfg.StripeWebhookSecret) == "" {
		return nil, errors.New("config: STRIPE_WEBHOOK_SECRET required (no inline config — set via Secret Manager)")
	}
	return cfg, nil
}

// LoadBillingReconciliationJob loads config for the reconciliation Cloud Run
// Job. Differs from LoadBillingWebhookServer in that the webhook secret is
// NOT required (the Job doesn't ingress webhooks) but the Stripe SECRET_KEY
// IS required (the Job calls the Stripe API).
func LoadBillingReconciliationJob() (*BillingWebhookConfig, error) {
	cfg := &BillingWebhookConfig{
		StripeAPIBase:   envOr("STRIPE_API_BASE", "https://api.stripe.com"),
		StripeSecretKey: os.Getenv("STRIPE_SECRET_KEY"),
		SourceProject:   envOr("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService:   envOr("CHORA_SOURCE_SERVICE", "chora-tenancy-billing-reconciliation"),
		ToleranceBPS:    envInt("RECONCILIATION_TOLERANCE_BPS", 10),
		OTLPEndpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
	if strings.TrimSpace(cfg.StripeSecretKey) == "" {
		return nil, errors.New("config: STRIPE_SECRET_KEY required (no inline config — set via Secret Manager)")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}
