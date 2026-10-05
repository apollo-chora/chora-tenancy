// wire_compat_test verifies the binary bytes emitted by MarshalPayload parse
// cleanly into the generated proto types from chora-contracts/gen/go/chora/
// tenancy/v1. This is the load-bearing assertion — if these tests pass, the
// topic's proto contract WILL accept the bytes.
//
// The generated types use the imported chora.common.v1.EventEnvelope rather
// than the inlined Envelope from the events-flat schemas, but both share
// identical field numbers, so wire bytes round-trip both ways. Schema
// Registry validates against the inlined-form schemas; protobuf decode here
// validates that the wire layout matches the canonical generated structs.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events/protomarshal"
)

func canonicalEnvelope() protomarshal.Envelope {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01971a90-0000-7000-8000-000000000abc",
		IdempotencyKey: "idemp-wire-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t0,
		PublishedAt:    t0.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "vendor=value",
		SourceProject:  "chora-489812",
		SourceService:  "chora-tenancy",
		SchemaVersion:  1,
	}
}

func TestWireCompat_TenantCreated_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
	payload := map[string]any{
		"tenant_id":            env.TenantID,
		"parent_tenant_id":     "tenant-parent-1",
		"display_name":         "Acme Pte Ltd",
		"hosting_mode":         "platform_hosted",
		"owner_gcid":           env.GCID,
		"created_at":           env.OccurredAt,
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "post_deploy",
	}

	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg tenancyv1.TenantCreated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if msg.GetEnvelope() == nil {
		t.Fatal("envelope not decoded")
	}
	if got := msg.GetEnvelope().GetEventId(); got != env.EventID {
		t.Fatalf("envelope.event_id: got %q want %q", got, env.EventID)
	}
	if got := msg.GetEnvelope().GetTenantId(); got != env.TenantID {
		t.Fatalf("envelope.tenant_id: got %q want %q", got, env.TenantID)
	}
	if got := msg.GetEnvelope().GetGcid(); got != env.GCID {
		t.Fatalf("envelope.gcid: got %q want %q", got, env.GCID)
	}
	if got := msg.GetEnvelope().GetTraceparent(); got != env.Traceparent {
		t.Fatalf("envelope.traceparent: got %q", got)
	}
	if got := msg.GetEnvelope().GetSchemaVersion(); got != env.SchemaVersion {
		t.Fatalf("envelope.schema_version: got %d want %d", got, env.SchemaVersion)
	}
	if got := msg.GetEnvelope().GetSourceService(); got != env.SourceService {
		t.Fatalf("envelope.source_service: got %q", got)
	}
	if got := msg.GetEnvelope().GetChoraImdaDimension(); got != "accountability" {
		t.Fatalf("envelope.chora_imda_dimension: got %q", got)
	}
	if got := msg.GetEnvelope().GetImdaLifecycleStage(); got != "post_deploy" {
		t.Fatalf("envelope.imda_lifecycle_stage: got %q", got)
	}

	if got := msg.GetTenantId(); got != env.TenantID {
		t.Fatalf("tenant_id: got %q", got)
	}
	if got := msg.GetParentTenantId(); got != "tenant-parent-1" {
		t.Fatalf("parent_tenant_id: got %q", got)
	}
	if got := msg.GetDisplayName(); got != "Acme Pte Ltd" {
		t.Fatalf("display_name: got %q", got)
	}
	if got := msg.GetHostingMode(); got != tenancyv1.HostingMode_HOSTING_MODE_PLATFORM_HOSTED {
		t.Fatalf("hosting_mode: got %v want PLATFORM_HOSTED", got)
	}
	if got := msg.GetOwnerGcid(); got != env.GCID {
		t.Fatalf("owner_gcid: got %q", got)
	}
	if got := msg.GetCreatedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("created_at: got %+v", got)
	}
}

func TestWireCompat_AddonActivated_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
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
	var msg tenancyv1.AddonActivated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetTenantId(); got != env.TenantID {
		t.Fatalf("tenant_id: got %q", got)
	}
	if got := msg.GetAddonPlanId(); got != "plan-ai-assist-001" {
		t.Fatalf("addon_plan_id: got %q", got)
	}
	if got := msg.GetActivatedByGcid(); got != env.GCID {
		t.Fatalf("activated_by_gcid: got %q", got)
	}
	if got := msg.GetActivatedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("activated_at: got %+v", got)
	}
}

func TestWireCompat_AddonDeactivated_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
	payload := map[string]any{
		"tenant_id":           env.TenantID,
		"addon_plan_id":       "plan-ai-assist-001",
		"reason":              "no_longer_needed",
		"deactivated_by_gcid": env.GCID,
		"deactivated_at":      env.OccurredAt,
		"reason_code":         "no_longer_needed",
		"reason_text":         "consolidating",
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.addon.deactivated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var msg tenancyv1.AddonDeactivated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetTenantId(); got != env.TenantID {
		t.Fatalf("tenant_id: got %q", got)
	}
	if got := msg.GetAddonPlanId(); got != "plan-ai-assist-001" {
		t.Fatalf("addon_plan_id: got %q", got)
	}
	if got := msg.GetReason(); got != "no_longer_needed" {
		t.Fatalf("reason: got %q", got)
	}
	if got := msg.GetReasonCode(); got != tenancyv1.AddonDeactivationReason_ADDON_DEACTIVATION_REASON_NO_LONGER_NEEDED {
		t.Fatalf("reason_code: got %v", got)
	}
	if got := msg.GetReasonText(); got != "consolidating" {
		t.Fatalf("reason_text: got %q", got)
	}
}

func TestWireCompat_AddonUpgraded_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
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
	var msg tenancyv1.AddonUpgraded
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetFromTier(); got != "standard" {
		t.Fatalf("from_tier: got %q", got)
	}
	if got := msg.GetToTier(); got != "premium" {
		t.Fatalf("to_tier: got %q", got)
	}
	if got := msg.GetBillingDeltaCents(); got != 1500 {
		t.Fatalf("billing_delta_cents: got %d", got)
	}
	if got := msg.GetScheduleId(); got != "sched_test_001" {
		t.Fatalf("schedule_id: got %q", got)
	}
}

func TestWireCompat_AddonUsageRecorded_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
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
	var msg tenancyv1.AddonUsageRecorded
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetUsageDimension(); got != "api_calls" {
		t.Fatalf("usage_dimension: got %q", got)
	}
	if got := msg.GetUsageValue(); got != 420 {
		t.Fatalf("usage_value: got %d", got)
	}
	if got := msg.GetUsageUnit(); got != "calls" {
		t.Fatalf("usage_unit: got %q", got)
	}
}

func TestWireCompat_FamiliarEggCheckoutStarted_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
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
	var msg tenancyv1.CompanionEggCheckoutStarted
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetPurchaseId(); got != "01971a90-1111-7000-8000-000000000001" {
		t.Fatalf("purchase_id: got %q", got)
	}
	if got := msg.GetEggSku(); got != "egg.standard.v1" {
		t.Fatalf("egg_sku: got %q", got)
	}
	if got := msg.GetStripeSessionId(); got != "cs_test_001" {
		t.Fatalf("stripe_session_id: got %q", got)
	}
	if got := msg.GetAmountCents(); got != 999 {
		t.Fatalf("amount_cents: got %d", got)
	}
	if got := msg.GetCurrency(); got != "SGD" {
		t.Fatalf("currency: got %q", got)
	}
	if got := msg.GetSuggestedFocalAtomId(); got != "atom-fractions-001" {
		t.Fatalf("suggested_focal_atom_id: got %q", got)
	}
	if got := msg.GetCheckoutStartedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("checkout_started_at: got %+v", got)
	}
	if got := msg.GetEnvelope().GetChoraImdaDimension(); got != "accountability" {
		t.Fatalf("envelope.chora_imda_dimension: got %q", got)
	}
}

func TestWireCompat_FamiliarEggRefunded_DecodesIntoGeneratedType(t *testing.T) {
	env := canonicalEnvelope()
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
	var msg tenancyv1.CompanionEggRefunded
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetCreditOnly(); !got {
		t.Fatalf("credit_only: got false; want true")
	}
	if got := msg.GetAmountCentsRefunded(); got != 999 {
		t.Fatalf("amount_cents_refunded: got %d", got)
	}
	if got := msg.GetReason(); got != "hard_expiry_unhatched" {
		t.Fatalf("reason: got %q", got)
	}
}
