// Package protomarshal_test verifies binary protobuf wire-format encoding
// for chora-tenancy's outbox event payloads.
//
// Per CLAUDE.md §development-execution + feedback_strict_tdd, tests are
// written BEFORE the encoder lands. Each test pins a specific Schema
// Registry-attached topic's wire layout per
// chora-contracts/proto/events-flat/tenancy/* — those flat protos ARE the
// registered schemas (see `gcloud pubsub schemas list`).
//
// Gap closed: chora-tenancy's OutboxPublisher persisted JSON-marshalled
// payload bytes that Pub/Sub Schema Registry (BINARY encoding) rejected at
// publish time with "Invalid binary proto message". Fix is producer-side
// inside the outbox path; the dispatcher passes bytes through unchanged.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events/protomarshal"
)

// fixedEnvelope returns an envelope with deterministic values for byte-level
// assertions.
func fixedEnvelope() protomarshal.Envelope {
	t := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01971a90-0000-7000-8000-000000000001",
		IdempotencyKey: "idemp-tenancy-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t,
		PublishedAt:    t.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "",
		SourceProject:  "chora-489812",
		SourceService:  "chora-tenancy",
		SchemaVersion:  1,
	}
}

// walkTopLevelTags walks all top-level wire records and returns the set of
// observed field numbers. It also asserts each TLV is well-formed.
func walkTopLevelTags(t *testing.T, bz []byte) map[protowire.Number]bool {
	t.Helper()
	seen := map[protowire.Number]bool{}
	rem := bz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			t.Fatalf("invalid tag at offset %d", len(bz)-len(rem))
		}
		rem = rem[n:]
		seen[num] = true
		switch typ {
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				t.Fatalf("invalid bytes for field %d", num)
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				t.Fatalf("invalid varint for field %d", num)
			}
			rem = rem[m:]
		default:
			t.Fatalf("unexpected wire type %d for field %d", typ, num)
		}
	}
	return seen
}

// envelopeBytes extracts the field-1 envelope submessage from a top-level
// payload.
func envelopeBytes(t *testing.T, bz []byte) []byte {
	t.Helper()
	num, typ, n := protowire.ConsumeTag(bz)
	if num != 1 || typ != protowire.BytesType || n < 0 {
		t.Fatalf("expected envelope tag (1, bytes), got num=%d typ=%d", num, typ)
	}
	envBz, m := protowire.ConsumeBytes(bz[n:])
	if m < 0 {
		t.Fatal("invalid envelope bytes")
	}
	if len(envBz) == 0 {
		t.Fatal("empty envelope bytes")
	}
	return envBz
}

// -----------------------------------------------------------------------------
// TenantCreated (chora.tenancy.tenant.created.v1)
// -----------------------------------------------------------------------------

func TestMarshal_TenantCreated_TopLevelWireShape(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":            env.TenantID,
		"parent_tenant_id":     "tenant-parent-1",
		"display_name":         "Acme Pte Ltd",
		"hosting_mode":         "platform_hosted",
		"owner_gcid":           env.GCID,
		"created_at":           env.OccurredAt,
		"chora_imda_dimension": "accountability",
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("empty bytes")
	}
	seen := walkTopLevelTags(t, bz)
	required := []protowire.Number{1, 2, 3, 4, 5, 6, 7}
	for _, want := range required {
		if !seen[want] {
			t.Fatalf("TenantCreated: missing required field %d", want)
		}
	}
}

func TestMarshal_TenantCreated_EnvelopeFields(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":            env.TenantID,
		"display_name":         "Acme Pte Ltd",
		"owner_gcid":           env.GCID,
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	envBz := envelopeBytes(t, bz)
	// chora.common.v1.EventEnvelope nested fields: event_id=1, idempotency_key=2,
	// tenant_id=3, gcid=4, occurred_at=5, published_at=6, traceparent=7,
	// source_project=9, source_service=10, schema_version=11,
	// chora_imda_dimension=14, imda_lifecycle_stage=15.
	seen := map[protowire.Number]bool{}
	rem := envBz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			t.Fatalf("invalid envelope inner tag")
		}
		rem = rem[n:]
		seen[num] = true
		switch typ {
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				t.Fatalf("invalid envelope inner bytes for field %d", num)
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				t.Fatalf("invalid envelope inner varint for field %d", num)
			}
			rem = rem[m:]
		}
	}
	required := []protowire.Number{1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 14, 15}
	for _, want := range required {
		if !seen[want] {
			t.Fatalf("envelope: missing field %d (seen=%v)", want, seen)
		}
	}
}

// -----------------------------------------------------------------------------
// Addon lifecycle events
// -----------------------------------------------------------------------------

func TestMarshal_AddonActivated_FieldsInRange(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":         env.TenantID,
		"addon_plan_id":     "plan-ai-assist-001",
		"activated_by_gcid": env.GCID,
		"activated_at":      env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.activated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	// 1=envelope, 2=tenant_id, 3=addon_plan_id, 4=activated_by_gcid, 5=activated_at.
	for _, want := range []protowire.Number{1, 2, 3, 4, 5} {
		if !seen[want] {
			t.Fatalf("AddonActivated: missing field %d", want)
		}
	}
}

func TestMarshal_AddonDeactivated_EnumAndTimestamp(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":           env.TenantID,
		"addon_plan_id":       "plan-ai-assist-001",
		"reason":              "no_longer_needed",
		"deactivated_by_gcid": env.GCID,
		"deactivated_at":      env.OccurredAt,
		"reason_code":         "no_longer_needed",
		"reason_text":         "consolidating to a different plan",
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.deactivated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8} {
		if !seen[want] {
			t.Fatalf("AddonDeactivated: missing field %d", want)
		}
	}
}

func TestMarshal_AddonUpgraded_Int64Delta(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":           env.TenantID,
		"addon_plan_id":       "plan-ai-assist-001",
		"from_tier":           "standard",
		"to_tier":             "premium",
		"effective_at":        env.OccurredAt,
		"billing_delta_cents": int64(1500),
		"schedule_id":         "sched_test_001",
		"requested_by_gcid":   env.GCID,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.upgraded.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		if !seen[want] {
			t.Fatalf("AddonUpgraded: missing field %d", want)
		}
	}
}

func TestMarshal_AddonDowngraded_SharesUpgradeLayout(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":           env.TenantID,
		"addon_plan_id":       "plan-ai-assist-001",
		"from_tier":           "premium",
		"to_tier":             "standard",
		"effective_at":        env.OccurredAt,
		"billing_delta_cents": int64(-1500),
		"requested_by_gcid":   env.GCID,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.downgraded.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	// schedule_id is empty → skipped per proto3 default-elision; the rest stays.
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 9} {
		if !seen[want] {
			t.Fatalf("AddonDowngraded: missing field %d", want)
		}
	}
}

func TestMarshal_AddonUsageRecorded_WireShape(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":       env.TenantID,
		"addon_plan_id":   "plan-ai-assist-001",
		"usage_dimension": "api_calls",
		"usage_value":     int64(420),
		"usage_unit":      "calls",
		"recorded_at":     env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.usage_recorded.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7} {
		if !seen[want] {
			t.Fatalf("AddonUsageRecorded: missing field %d", want)
		}
	}
}

func TestMarshal_AddonDeactivationRequested_FieldRange(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":         env.TenantID,
		"addon_plan_id":     "plan-ai-assist-001",
		"reason":            "cost",
		"reason_text":       "budget cycle",
		"requested_by_gcid": env.GCID,
		"effective_at":      env.OccurredAt.Add(7 * 24 * time.Hour),
		"requested_at":      env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.deactivation_requested.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8} {
		if !seen[want] {
			t.Fatalf("AddonDeactivationRequested: missing field %d", want)
		}
	}
}

// -----------------------------------------------------------------------------
// FamiliarEgg lifecycle events
// -----------------------------------------------------------------------------

func TestMarshal_FamiliarEggCheckoutStarted_StripeAndAmount(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"purchase_id":             "01971a90-1111-7000-8000-000000000001",
		"purchaser_gcid":          env.GCID,
		"target_tenant_id":        env.TenantID,
		"egg_sku":                 "egg.standard.v1",
		"stripe_session_id":       "cs_test_001",
		"stripe_checkout_url":     "https://checkout.stripe.com/c/pay/cs_test_001",
		"amount_cents":            int64(999),
		"currency":                "SGD",
		"suggested_focal_atom_id": "atom-fractions-001",
		"checkout_started_at":     env.OccurredAt,
		"chora_imda_dimension":    "accountability",
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.checkout_started.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		if !seen[want] {
			t.Fatalf("FamiliarEggCheckoutStarted: missing field %d", want)
		}
	}
}

func TestMarshal_FamiliarEggPaymentSucceeded_PaidAndRefunded(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"purchase_id":              "01971a90-1111-7000-8000-000000000001",
		"purchaser_gcid":           env.GCID,
		"target_tenant_id":         env.TenantID,
		"egg_sku":                  "egg.standard.v1",
		"stripe_session_id":        "cs_test_001",
		"stripe_payment_intent_id": "pi_test_001",
		"stripe_charge_id":         "ch_test_001",
		"amount_cents_paid":        int64(999),
		"amount_cents_refunded":    int64(0),
		"currency":                 "SGD",
		"suggested_focal_atom_id":  "atom-fractions-001",
		"paid_at":                  env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.payment_succeeded.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	// amount_cents_refunded is 0 → skipped per proto3 default-elision; the
	// rest of the schema-mandatory fields are present.
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13} {
		if !seen[want] {
			t.Fatalf("FamiliarEggPaymentSucceeded: missing field %d", want)
		}
	}
}

func TestMarshal_FamiliarEggPaymentFailed_StripeFailureCode(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"purchase_id":            "01971a90-1111-7000-8000-000000000001",
		"purchaser_gcid":         env.GCID,
		"egg_sku":                "egg.standard.v1",
		"stripe_session_id":      "cs_test_001",
		"stripe_failure_code":    "card_declined",
		"stripe_failure_message": "Your card was declined.",
		"failed_at":              env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.payment_failed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8} {
		if !seen[want] {
			t.Fatalf("FamiliarEggPaymentFailed: missing field %d", want)
		}
	}
}

func TestMarshal_FamiliarEggRefunded_CreditOnlyBool(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"purchase_id":           "01971a90-1111-7000-8000-000000000001",
		"purchaser_gcid":        env.GCID,
		"egg_sku":               "egg.standard.v1",
		"stripe_charge_id":      "ch_test_001",
		"stripe_refund_id":      "re_test_001",
		"amount_cents_refunded": int64(999),
		"currency":              "SGD",
		"reason":                "hard_expiry_unhatched",
		"credit_only":           true,
		"refunded_at":           env.OccurredAt,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.refunded.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		if !seen[want] {
			t.Fatalf("FamiliarEggRefunded: missing field %d", want)
		}
	}
}

// -----------------------------------------------------------------------------
// Edge cases
// -----------------------------------------------------------------------------

func TestMarshal_UnknownTopic_FailsLoud(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.tenancy.some.unwired.v1", env, nil)
	if err == nil {
		t.Fatal("expected ErrUnsupportedTopic, got nil")
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		t.Fatalf("expected IsUnsupportedTopic(true), got: %v", err)
	}
}

func TestMarshal_NilPayloadProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, nil)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("nil payload should still produce envelope bytes")
	}
	// First tag must be envelope (field 1).
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 {
		t.Fatal("invalid leading tag")
	}
	if num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

func TestMarshal_RejectsStringForInt64Slot(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"purchase_id":  "01971a90-1111-7000-8000-000000000001",
		"amount_cents": "not-a-number", // schema demands int64
	}
	_, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.checkout_started.v1", env, payload)
	if err == nil {
		t.Fatal("expected type error for amount_cents=string")
	}
}

func TestMarshal_RejectsIntForStringSlot(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id": 12345, // schema demands string
	}
	_, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err == nil {
		t.Fatal("expected type error for tenant_id=int")
	}
}

func TestMarshal_RejectsStringForBoolSlot(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"credit_only": "true", // schema demands bool
	}
	_, err := protomarshal.MarshalPayload("chora.tenancy.familiar_egg.refunded.v1", env, payload)
	if err == nil {
		t.Fatal("expected type error for credit_only=string")
	}
}

func TestMarshal_AcceptsRFC3339TimestampString(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":  env.TenantID,
		"created_at": env.OccurredAt.Format(time.RFC3339Nano),
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("empty bytes")
	}
	// field 7 (created_at) MUST be present after RFC3339 parse.
	seen := walkTopLevelTags(t, bz)
	if !seen[7] {
		t.Fatal("created_at (field 7) missing — RFC3339 string not consumed")
	}
}

func TestMarshal_TimePointer_Accepted(t *testing.T) {
	env := fixedEnvelope()
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	payload := map[string]any{
		"tenant_id":  env.TenantID,
		"created_at": &now,
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("empty bytes")
	}
}

func TestMarshal_HostingModeEnumNumericInput(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"tenant_id":    env.TenantID,
		"hosting_mode": 2, // HOSTING_MODE_WHITE_LABEL
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevelTags(t, bz)
	if !seen[5] {
		t.Fatal("hosting_mode (field 5) missing for numeric enum input")
	}
}
