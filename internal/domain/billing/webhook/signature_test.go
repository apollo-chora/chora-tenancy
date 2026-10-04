// Package webhook_test — TDD RED phase for the Stripe webhook signature
// verifier. Ported from services/chora-billing-webhook/internal/domain/webhook
// as part of the M12.2 Batch-1 consolidation (chora-billing-webhook ->
// chora-tenancy per locked architecture "Tenancy + Billing combined").
package webhook_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/billing/webhook"
)

// makeStripeSignatureHeader produces a header in the same form Stripe sends.
//
//	Stripe-Signature: t=<timestamp>,v1=<hex hmac sha256>
func makeStripeSignatureHeader(secret, payload string, ts time.Time) string {
	signedPayload := fmt.Sprintf("%d.%s", ts.Unix(), payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signedPayload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%d,v1=%s", ts.Unix(), sig)
}

func TestVerifySignature_AcceptsValidSignature(t *testing.T) {
	t.Parallel()
	secret := "whsec_test_aaaa"
	payload := `{"id":"evt_sig_1","type":"payment_intent.succeeded"}`
	ts := time.Now().UTC()
	header := makeStripeSignatureHeader(secret, payload, ts)

	if err := webhook.VerifySignature(secret, header, []byte(payload), ts, 5*time.Minute); err != nil {
		t.Fatalf("VerifySignature on valid signature: unexpected error: %v", err)
	}
}

func TestVerifySignature_RejectsTamperedPayload(t *testing.T) {
	t.Parallel()
	secret := "whsec_test_bbbb"
	payload := `{"id":"evt_sig_2","type":"payment_intent.succeeded"}`
	ts := time.Now().UTC()
	header := makeStripeSignatureHeader(secret, payload, ts)

	tampered := []byte(strings.Replace(payload, "succeeded", "failed", 1))

	if err := webhook.VerifySignature(secret, header, tampered, ts, 5*time.Minute); err == nil {
		t.Fatalf("VerifySignature must reject tampered payload")
	}
}

func TestVerifySignature_RejectsWrongSecret(t *testing.T) {
	t.Parallel()
	signSecret := "whsec_real"
	verifySecret := "whsec_attacker_guess"
	payload := `{"id":"evt_sig_3"}`
	ts := time.Now().UTC()
	header := makeStripeSignatureHeader(signSecret, payload, ts)

	if err := webhook.VerifySignature(verifySecret, header, []byte(payload), ts, 5*time.Minute); err == nil {
		t.Fatalf("VerifySignature must reject wrong secret")
	}
}

func TestVerifySignature_RejectsExpiredTimestamp(t *testing.T) {
	t.Parallel()
	secret := "whsec_test_expiry"
	payload := `{"id":"evt_sig_4"}`
	ts := time.Now().UTC().Add(-10 * time.Minute) // 10 min ago > 5 min tolerance
	header := makeStripeSignatureHeader(secret, payload, ts)

	if err := webhook.VerifySignature(secret, header, []byte(payload), time.Now().UTC(), 5*time.Minute); err == nil {
		t.Fatalf("VerifySignature must reject signatures older than tolerance")
	}
}

func TestVerifySignature_RejectsMalformedHeader(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"v1=abc",
		"t=123",
		"t=notnumeric,v1=abc",
		"t=123,v1=",
		"t=,v1=deadbeef",
		"X-Y-Z",
		"t=123456789,v1=ZZZ",
	}
	for _, header := range cases {
		header := header
		t.Run(header, func(t *testing.T) {
			t.Parallel()
			err := webhook.VerifySignature("whsec_x", header, []byte(`{}`), time.Now().UTC(), 5*time.Minute)
			if err == nil {
				t.Fatalf("VerifySignature on malformed header %q must return error", header)
			}
		})
	}
}

func TestVerifySignature_RejectsEmptySecret(t *testing.T) {
	t.Parallel()
	if err := webhook.VerifySignature("", "t=123,v1=deadbeef", []byte(`{}`), time.Now().UTC(), 5*time.Minute); err == nil {
		t.Fatalf("VerifySignature must reject empty secret")
	}
}

func TestVerifySignature_RejectsEmptyHeader(t *testing.T) {
	t.Parallel()
	if err := webhook.VerifySignature("whsec_x", "", []byte(`{}`), time.Now().UTC(), 5*time.Minute); err == nil {
		t.Fatalf("VerifySignature must reject empty header")
	}
}

func TestVerifySignature_AcceptsFutureTimestampWithinTolerance(t *testing.T) {
	t.Parallel()
	secret := "whsec_test_clock_skew"
	payload := `{"id":"evt_skew"}`
	stripeTs := time.Now().UTC().Add(30 * time.Second)
	header := makeStripeSignatureHeader(secret, payload, stripeTs)
	if err := webhook.VerifySignature(secret, header, []byte(payload), time.Now().UTC(), 5*time.Minute); err != nil {
		t.Fatalf("future-skewed timestamp within tolerance must pass: %v", err)
	}
}

func TestVerifySignature_AcceptsMultipleV1Schemes(t *testing.T) {
	t.Parallel()
	secret := "whsec_rotation"
	payload := `{"id":"evt_rot"}`
	ts := time.Now().UTC()
	good := makeStripeSignatureHeader(secret, payload, ts)
	header := good + ",v1=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	if err := webhook.VerifySignature(secret, header, []byte(payload), ts, 5*time.Minute); err != nil {
		t.Fatalf("VerifySignature should accept when at least one v1 sig matches: %v", err)
	}
}
