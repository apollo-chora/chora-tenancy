// main_test.go — unit tests for the production-mode env gate (Iter G.4
// PROD-C + Stripe finalization). Verifies validateFamiliarEggProdEnv
// loud-fails when CHORA_ENV=prod AND required Stripe values are missing,
// and is permissive (warning only) in dev / staging / empty.
//
// Stripe credentials are validated against the resolved values returned
// by resolveStripeCredentials (either GSM via *_SECRET_ID or raw env).
package main

import (
	"context"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
)

func TestValidateFamiliarEggProdEnv_DevPermissive_AllMissing(t *testing.T) {
	t.Setenv("CHORA_ENV", "dev")
	clearStripeURLs(t)
	if err := validateFamiliarEggProdEnv(stripeCredentials{}); err != nil {
		t.Fatalf("dev mode should be permissive; got error: %v", err)
	}
}

func TestValidateFamiliarEggProdEnv_DevPermissive_EmptyEnv(t *testing.T) {
	t.Setenv("CHORA_ENV", "")
	clearStripeURLs(t)
	if err := validateFamiliarEggProdEnv(stripeCredentials{}); err != nil {
		t.Fatalf("empty CHORA_ENV should be permissive; got error: %v", err)
	}
}

func TestValidateFamiliarEggProdEnv_StagingPermissive(t *testing.T) {
	t.Setenv("CHORA_ENV", "staging")
	clearStripeURLs(t)
	if err := validateFamiliarEggProdEnv(stripeCredentials{}); err != nil {
		t.Fatalf("staging should be permissive; got error: %v", err)
	}
}

func TestValidateFamiliarEggProdEnv_ProdAllSet(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	t.Setenv("STRIPE_SUCCESS_URL", "https://example/success")
	t.Setenv("STRIPE_CANCEL_URL", "https://example/cancel")
	creds := stripeCredentials{
		APIKey:        "sk_live_x",
		WebhookSecret: "whsec_x",
	}
	if err := validateFamiliarEggProdEnv(creds); err != nil {
		t.Fatalf("prod with all vars set should pass; got error: %v", err)
	}
}

func TestValidateFamiliarEggProdEnv_ProdMissingApiKey(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	t.Setenv("STRIPE_SUCCESS_URL", "https://example/success")
	t.Setenv("STRIPE_CANCEL_URL", "https://example/cancel")
	creds := stripeCredentials{
		APIKey:        "",
		WebhookSecret: "whsec_x",
	}
	err := validateFamiliarEggProdEnv(creds)
	if err == nil {
		t.Fatalf("prod missing API key should fail")
	}
	if !strings.Contains(err.Error(), "STRIPE_API_KEY") {
		t.Errorf("error should mention the missing credential family; got %q", err.Error())
	}
}

func TestValidateFamiliarEggProdEnv_ProdMissingWebhookSecret(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	t.Setenv("STRIPE_SUCCESS_URL", "https://example/success")
	t.Setenv("STRIPE_CANCEL_URL", "https://example/cancel")
	creds := stripeCredentials{
		APIKey:        "sk_live_x",
		WebhookSecret: "",
	}
	err := validateFamiliarEggProdEnv(creds)
	if err == nil {
		t.Fatalf("prod missing webhook secret should fail")
	}
	if !strings.Contains(err.Error(), "STRIPE_WEBHOOK_SECRET") {
		t.Errorf("error should mention the missing credential family; got %q", err.Error())
	}
}

func TestValidateFamiliarEggProdEnv_ProdMissingURLs(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	clearStripeURLs(t)
	creds := stripeCredentials{
		APIKey:        "sk_live_x",
		WebhookSecret: "whsec_x",
	}
	err := validateFamiliarEggProdEnv(creds)
	if err == nil {
		t.Fatalf("prod missing URL envs should fail")
	}
	for _, want := range []string{"STRIPE_SUCCESS_URL", "STRIPE_CANCEL_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got %q", want, err.Error())
		}
	}
}

func TestValidateFamiliarEggProdEnv_ProdMissingMultiple(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	clearStripeURLs(t)
	err := validateFamiliarEggProdEnv(stripeCredentials{})
	if err == nil {
		t.Fatalf("prod with no vars set should fail")
	}
	for _, want := range []string{"STRIPE_API_KEY", "STRIPE_WEBHOOK_SECRET", "STRIPE_SUCCESS_URL", "STRIPE_CANCEL_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name missing var %q; got %q", want, err.Error())
		}
	}
}

func TestValidateFamiliarEggProdEnv_ProductionAliasAccepted(t *testing.T) {
	// "production" should be treated as prod.
	t.Setenv("CHORA_ENV", "production")
	clearStripeURLs(t)
	if err := validateFamiliarEggProdEnv(stripeCredentials{}); err == nil {
		t.Fatalf("CHORA_ENV=production should be strict")
	}
}

func TestValidateFamiliarEggProdEnv_WhitespaceTreatedAsUnset(t *testing.T) {
	t.Setenv("CHORA_ENV", "prod")
	t.Setenv("STRIPE_SUCCESS_URL", "https://example/success")
	t.Setenv("STRIPE_CANCEL_URL", "https://example/cancel")
	creds := stripeCredentials{
		APIKey:        "   ",
		WebhookSecret: "whsec_x",
	}
	err := validateFamiliarEggProdEnv(creds)
	if err == nil {
		t.Fatalf("whitespace-only value should be treated as missing")
	}
}

func TestValidateFamiliarEggProdEnv_ProdEitherSourceSatisfies(t *testing.T) {
	// The resolved value is what counts — whether it came from GSM (APIKeySource
	// = "secret-manager") or raw env doesn't matter for the prod gate.
	t.Setenv("CHORA_ENV", "prod")
	t.Setenv("STRIPE_SUCCESS_URL", "https://example/success")
	t.Setenv("STRIPE_CANCEL_URL", "https://example/cancel")
	cases := []stripeCredentials{
		{APIKey: "sk_live_gsm", APIKeySource: "secret-manager", WebhookSecret: "whsec_gsm", WebhookSource: "secret-manager"},
		{APIKey: "sk_live_raw", APIKeySource: "raw-env", WebhookSecret: "whsec_raw", WebhookSource: "raw-env"},
		{APIKey: "sk_live_a", APIKeySource: "secret-manager", WebhookSecret: "whsec_b", WebhookSource: "raw-env"},
	}
	for _, c := range cases {
		if err := validateFamiliarEggProdEnv(c); err != nil {
			t.Errorf("creds %+v should satisfy prod gate; got %v", c, err)
		}
	}
}

func TestEnvOrDefault_ReturnsDefaultWhenUnset(t *testing.T) {
	t.Setenv("TEST_KEY_UNSET", "")
	if got := envOrDefault("TEST_KEY_UNSET", "fallback"); got != "fallback" {
		t.Errorf("got %q; want %q", got, "fallback")
	}
}

func TestEnvOrDefault_ReturnsValueWhenSet(t *testing.T) {
	t.Setenv("TEST_KEY_SET", "value")
	if got := envOrDefault("TEST_KEY_SET", "fallback"); got != "value" {
		t.Errorf("got %q; want %q", got, "value")
	}
}

// clearStripeURLs zeros the URL envs validated by requiredFamiliarEggProdURLEnv.
func clearStripeURLs(t *testing.T) {
	t.Helper()
	for _, key := range requiredFamiliarEggProdURLEnv {
		t.Setenv(key, "")
	}
}

// --- debt #50: tenancy events recorder wiring gap ---
//
// Pre-fix, cmd/server/main.go at the v2Deps wireup left
//
//	v2Deps.Events = events.NewRecorder() (the NewDefaultV2Deps default)
//
// which is an in-process recorder — every tenant.created /
// addon.activated / addon.deactivated / addon.upgraded /
// addon.downgraded / addon.usage_recorded HTTP-handler emission landed
// in an in-memory ring buffer and NEVER reached the OutboxPublisher,
// so they never reached Pub/Sub.
//
// These tests pin the composition-root contract:
//
//  1. *outbox.Publisher satisfies httpapi.Publisher (the narrow port
//     used by the v2 + v1 HTTP handlers).
//  2. wireV2DepsEvents(v2Deps, outboxPublisher) binds v2Deps.Events to
//     the outbox publisher (NOT to a fresh in-memory recorder).
//
// Sister to the familiar_egg wiring (which already used the outbox
// publisher pre-fix). Per `feedback_d6_resilience_first_class` Pillar 2.
func TestOutboxPublisher_SatisfiesV2DepsPublisherPort(t *testing.T) {
	store := tenancyoutbox.NewInMemoryStore()
	pub := tenancyoutbox.NewPublisher(tenancyoutbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: serviceName,
	})

	// Compile-time check: *outbox.Publisher implements httpapi.Publisher.
	// This is the load-bearing port that the v2/v1 HTTP handlers use to
	// emit tenant.* / addon.* events.
	var _ httpapi.Publisher = pub
}

func TestWireV2DepsEvents_BindsOutboxPublisher(t *testing.T) {
	store := tenancyoutbox.NewInMemoryStore()
	pub := tenancyoutbox.NewPublisher(tenancyoutbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: serviceName,
	})

	deps := httpapi.NewDefaultV2Deps()
	wireV2DepsEvents(&deps, pub)

	// deps.Events must now be the outbox publisher (NOT the in-memory
	// recorder that NewDefaultV2Deps returns by default).
	if deps.Events != pub {
		t.Fatalf("expected deps.Events to point at the outbox publisher; got %T", deps.Events)
	}

	// Sanity: a handler-style Publish call enqueues a row in the outbox
	// store. This is the load-bearing path that was missing pre-fix.
	hdr := events.Header{TenantID: "tenant-debt50", GCID: "gcid-debt50"}
	rec := deps.Events.Publish("chora.tenancy.tenant.created.v1", hdr, map[string]interface{}{"id": "t-1"})
	if rec.EventID == "" {
		t.Fatalf("expected publish to succeed and return event_id")
	}

	rows, err := store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row from Publish; got %d", len(rows))
	}
	if rows[0].Topic != "chora.tenancy.tenant.created.v1" {
		t.Errorf("expected canonical topic; got %q", rows[0].Topic)
	}
}

func TestWireV2DepsEvents_NilPublisherKeepsDefault(t *testing.T) {
	// Dev-mode safety net: if the outbox publisher couldn't be wired
	// (e.g., no pgx pool, no DSN), keep the in-memory recorder so the
	// binary stays runnable. Production env-gating happens in main()
	// upstream — this helper just doesn't crash on a nil arg.
	deps := httpapi.NewDefaultV2Deps()
	defaultEvents := deps.Events
	wireV2DepsEvents(&deps, nil)
	if deps.Events != defaultEvents {
		t.Fatalf("nil publisher should leave the default in-memory recorder in place")
	}
}
