package config_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/config"
)

func TestLoadBillingWebhookServer_RequiresWebhookSecret(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")
	if _, err := config.LoadBillingWebhookServer(); err == nil {
		t.Fatal("expected error when STRIPE_WEBHOOK_SECRET unset")
	}
}

func TestLoadBillingWebhookServer_DefaultsApplied(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
	t.Setenv("PORT", "")
	t.Setenv("STRIPE_API_BASE", "")
	t.Setenv("CHORA_SOURCE_PROJECT", "")
	t.Setenv("RECONCILIATION_TOLERANCE_BPS", "")
	t.Setenv("STRIPE_SIGNATURE_SKEW", "")
	t.Setenv("STRIPE_WEBHOOK_IDEMPOTENCY_TTL", "")
	t.Setenv("CHORA_SOURCE_SERVICE", "")

	cfg, err := config.LoadBillingWebhookServer()
	if err != nil {
		t.Fatalf("LoadBillingWebhookServer: %v", err)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port=%q want=8080", cfg.Port)
	}
	if cfg.StripeAPIBase != "https://api.stripe.com" {
		t.Errorf("StripeAPIBase=%q", cfg.StripeAPIBase)
	}
	if cfg.SourceProject != "chora-489812" {
		t.Errorf("SourceProject=%q", cfg.SourceProject)
	}
	if cfg.ToleranceBPS != 10 {
		t.Errorf("ToleranceBPS=%d want=10", cfg.ToleranceBPS)
	}
	if cfg.SignatureSkew != 5*time.Minute {
		t.Errorf("SignatureSkew=%v want=5m", cfg.SignatureSkew)
	}
	if cfg.IdempotencyTTL != 7*24*time.Hour {
		t.Errorf("IdempotencyTTL=%v", cfg.IdempotencyTTL)
	}
	if cfg.SourceService != "chora-tenancy-billing-webhook" {
		t.Errorf("SourceService=%q want=chora-tenancy-billing-webhook", cfg.SourceService)
	}
}

func TestLoadBillingWebhookServer_OverridesApplied(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
	t.Setenv("PORT", "9090")
	t.Setenv("STRIPE_API_BASE", "http://localhost:1234")
	t.Setenv("CHORA_SOURCE_PROJECT", "chora-490000")
	t.Setenv("RECONCILIATION_TOLERANCE_BPS", "25")
	t.Setenv("STRIPE_SIGNATURE_SKEW", "10m")
	t.Setenv("STRIPE_WEBHOOK_IDEMPOTENCY_TTL", "24h")

	cfg, err := config.LoadBillingWebhookServer()
	if err != nil {
		t.Fatalf("LoadBillingWebhookServer: %v", err)
	}
	if cfg.Port != "9090" {
		t.Errorf("Port=%q want=9090", cfg.Port)
	}
	if cfg.StripeAPIBase != "http://localhost:1234" {
		t.Errorf("StripeAPIBase=%q", cfg.StripeAPIBase)
	}
	if cfg.SourceProject != "chora-490000" {
		t.Errorf("SourceProject=%q", cfg.SourceProject)
	}
	if cfg.ToleranceBPS != 25 {
		t.Errorf("ToleranceBPS=%d", cfg.ToleranceBPS)
	}
	if cfg.SignatureSkew != 10*time.Minute {
		t.Errorf("SignatureSkew=%v", cfg.SignatureSkew)
	}
	if cfg.IdempotencyTTL != 24*time.Hour {
		t.Errorf("IdempotencyTTL=%v", cfg.IdempotencyTTL)
	}
}

func TestLoadBillingWebhookServer_BadDurationFallsBackToDefault(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
	t.Setenv("STRIPE_SIGNATURE_SKEW", "not-a-duration")
	cfg, err := config.LoadBillingWebhookServer()
	if err != nil {
		t.Fatalf("LoadBillingWebhookServer: %v", err)
	}
	if cfg.SignatureSkew != 5*time.Minute {
		t.Errorf("bad duration must fall back to default; got %v", cfg.SignatureSkew)
	}
}

func TestLoadBillingWebhookServer_BadIntFallsBackToDefault(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
	t.Setenv("RECONCILIATION_TOLERANCE_BPS", "not-an-int")
	cfg, err := config.LoadBillingWebhookServer()
	if err != nil {
		t.Fatalf("LoadBillingWebhookServer: %v", err)
	}
	if cfg.ToleranceBPS != 10 {
		t.Errorf("bad int must fall back to default; got %d", cfg.ToleranceBPS)
	}
}

func TestLoadBillingReconciliationJob_RequiresStripeSecretKey(t *testing.T) {
	t.Setenv("STRIPE_SECRET_KEY", "")
	if _, err := config.LoadBillingReconciliationJob(); err == nil {
		t.Fatal("expected error when STRIPE_SECRET_KEY unset")
	}
}

func TestLoadBillingReconciliationJob_HappyPath(t *testing.T) {
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_x")
	t.Setenv("STRIPE_API_BASE", "")
	t.Setenv("CHORA_SOURCE_PROJECT", "")
	t.Setenv("RECONCILIATION_TOLERANCE_BPS", "5")
	t.Setenv("CHORA_SOURCE_SERVICE", "")

	cfg, err := config.LoadBillingReconciliationJob()
	if err != nil {
		t.Fatalf("LoadBillingReconciliationJob: %v", err)
	}
	if cfg.StripeAPIBase != "https://api.stripe.com" {
		t.Errorf("StripeAPIBase=%q", cfg.StripeAPIBase)
	}
	if cfg.SourceProject != "chora-489812" {
		t.Errorf("SourceProject=%q", cfg.SourceProject)
	}
	if cfg.ToleranceBPS != 5 {
		t.Errorf("ToleranceBPS=%d", cfg.ToleranceBPS)
	}
	if cfg.SourceService != "chora-tenancy-billing-reconciliation" {
		t.Errorf("SourceService=%q", cfg.SourceService)
	}
}
