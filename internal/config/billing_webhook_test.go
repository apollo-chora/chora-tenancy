package config_test

import (
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/config"
)

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
