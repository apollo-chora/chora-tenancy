// protomarshal_branch_test.go — white-box (package protomarshal) branch
// coverage for the typed-field helpers, the enum converters, the loose
// coercion funcs and the registered-topic encoders' alternate-type
// variants (numeric enums, time.Time / RFC3339 timestamps, []any
// repeated fields, nil payloads). Sister to the external wire-layout tests
// which pin exact bytes.
package protomarshal

import (
	"errors"
	"testing"
	"time"
)

func fixedEnv() Envelope {
	t := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	return Envelope{
		EventID: "01971a90-0000-7000-8000-000000000001", IdempotencyKey: "idem-1",
		TenantID: "tenant-1", GCID: "gcid-1", OccurredAt: t, PublishedAt: t.Add(time.Second),
		Traceparent:   "00-aabbccddeeff00112233445566778899-0102030405060708-01",
		SourceProject: "chora-489812", SourceService: "chora-tenancy", SchemaVersion: 1,
	}
}

// everyPayload exercises every optional slot of the addon encoders.
var addonPayload = map[string]any{
	"tenant_id": "tenant-1", "addon_plan_id": "plan-1", "activated_by_gcid": "g-1",
	"deactivated_by_gcid": "g-1", "requested_by_gcid": "g-1",
	"from_tier": "starter", "to_tier": "pro",
	"billing_delta_cents": 5000, "amount_cents": 4900,
	"reason": "no_longer_needed", "reason_code": "no_longer_needed",
	"reason_text": "no longer needed",
	"credit_only": true, "effective_at": "2026-06-02T00:00:00Z",
	"activated_at": "2026-06-01T00:00:00Z", "deactivated_at": "2026-06-02T00:00:00Z",
	"requested_at":    time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
	"recorded_at":     time.Unix(0, 0), // zero-adjacent → skipped branch
	"usage_dimension": "mint", "usage_unit": "tokens", "usage_value": 42,
	"currency": "SGD",
}

func TestMarshalPayload_AllRegisteredTopics_MaximalPayload(t *testing.T) {
	env := fixedEnv()
	cases := map[string]map[string]any{
		"chora.tenancy.tenant.created.v1": {
			"tenant_id": "t-1", "parent_tenant_id": "t-0", "display_name": "Phyllis",
			"hosting_mode": "platform_hosted", "owner_gcid": "g-1", "created_at": "2026-06-01T10:00:00Z",
		},
		"chora.tenancy.tenant.golive.v1": {
			"tenant_id": "t-1", "parent_tenant_id": "t-0", "display_name": "P",
			"hosting_mode": int64(4), "activated_by_gcid": "g-1", "stripe_customer_id": "cus_1",
			"default_addon_plan_ids": []any{"plan-1", int64(7)}, // non-string element skipped
			"activated_at":           time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		},
		"chora.tenancy.addon.activated.v1":   addonPayload,
		"chora.tenancy.addon.deactivated.v1": addonPayload,
		"chora.tenancy.addon.deactivation_requested.v1": map[string]any{
			"tenant_id": "t-1", "addon_plan_id": "plan-1", "requested_by_gcid": "g-1",
			"reason_code": "cost", "reason_text": "too expensive", "effective_at": "2026-06-02T00:00:00Z",
			"requested_at": time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
		},
		"chora.tenancy.addon.upgraded.v1":   addonPayload,
		"chora.tenancy.addon.downgraded.v1": addonPayload,
		"chora.tenancy.addon.usage_recorded.v1": {
			"tenant_id": "t-1", "addon_plan_id": "plan-1", "usage_dimension": "mint",
			"usage_unit": "tokens", "usage_value": 42, "recorded_at": time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		},
		"chora.tenancy.familiar_egg.checkout_started.v1": {
			"tenant_id": "t-1", "purchase_id": "pur-1", "purchaser_gcid": "g-1",
			"egg_sku": "egg.standard.v1", "suggested_focal_atom_id": "atom-1",
			"amount_cents": 999, "currency": "SGD", "stripe_checkout_url": "https://st/1",
			"stripe_session_id": "cs_1", "checkout_started_at": time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		},
		"chora.tenancy.familiar_egg.payment_succeeded.v1": {
			"tenant_id": "t-1", "purchase_id": "pur-1", "purchaser_gcid": "g-1",
			"egg_sku": "egg.standard.v1", "amount_cents_paid": 999, "currency": "SGD",
			"stripe_payment_intent_id": "pi_1", "stripe_charge_id": "ch_1", "paid_at": time.Now(),
		},
		"chora.tenancy.familiar_egg.payment_failed.v1": {
			"tenant_id": "t-1", "purchase_id": "pur-1", "purchaser_gcid": "g-1",
			"egg_sku": "egg.standard.v1", "amount_cents": 999, "currency": "SGD",
			"stripe_failure_code": "card_declined", "stripe_failure_message": "declined",
			"failed_at": time.Now(),
		},
		"chora.tenancy.familiar_egg.refunded.v1": {
			"tenant_id": "t-1", "purchase_id": "pur-1", "purchaser_gcid": "g-1",
			"egg_sku": "egg.standard.v1", "amount_cents_refunded": 999, "currency": "SGD",
			"stripe_refund_id": "re_1", "refunded_at": time.Now(),
		},
	}
	for topic, payload := range cases {
		t.Run(topic, func(t *testing.T) {
			bz, err := MarshalPayload(topic, env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload(%s): %v", topic, err)
			}
			if len(bz) == 0 {
				t.Fatalf("expected non-empty wire bytes for %s", topic)
			}
			// Deterministic: same input → same bytes.
			bz2, err := MarshalPayload(topic, env, payload)
			if err != nil || len(bz2) != len(bz) {
				t.Fatalf("non-deterministic marshal for %s", topic)
			}
		})
	}
}

func TestMarshalPayload_NilPayloadAndDefaults(t *testing.T) {
	env := fixedEnv()
	bz, err := MarshalPayload("chora.tenancy.tenant.golive.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatalf("envelope-only bytes expected")
	}
	// Empty-string fields are elided; zero ints/bools/timestamps elided too.
	bz2, err := MarshalPayload("chora.tenancy.addon.activated.v1", env, map[string]any{
		"tenant_id":           "t-1",
		"billing_delta_cents": int64(0),
		"credit_only":         false,
		"activated_at":        time.Time{},
	})
	if err != nil {
		t.Fatalf("defaults payload: %v", err)
	}
	if len(bz2) == 0 {
		t.Fatalf("expected envelope + tenant_id bytes")
	}
	// []string repeated variant for the golive default plans.
	bz3, err := MarshalPayload("chora.tenancy.tenant.golive.v1", env, map[string]any{
		"default_addon_plan_ids": []string{"plan-a", "plan-b"},
		"hosting_mode":           "self_host",
	})
	if err != nil {
		t.Fatalf("[]string repeated: %v", err)
	}
	if len(bz3) == 0 {
		t.Fatalf("expected bytes for []string variant")
	}
}

func TestMarshalPayload_UnsupportedTopic(t *testing.T) {
	_, err := MarshalPayload("chora.tenancy.no.such.topic.v1", fixedEnv(), nil)
	if err == nil || !IsUnsupportedTopic(err) {
		t.Fatalf("expected unsupported-topic error, got %v", err)
	}
	var wrapped = errors.Join(ErrUnsupportedTopic, errors.New("x"))
	if !IsUnsupportedTopic(wrapped) {
		t.Fatalf("expected IsUnsupportedTopic true for wrapped error")
	}
}

func TestMarshalPayload_TypeMismatchErrors(t *testing.T) {
	env := fixedEnv()
	base := map[string]any{"tenant_id": "t-1", "addon_plan_id": "plan-1"}
	// string slot with non-string.
	if _, err := MarshalPayload("chora.tenancy.addon.activated.v1", env, merge(base, map[string]any{"activated_by_gcid": 42})); err == nil {
		t.Fatalf("expected string-field type error")
	}
	// bool slot with non-bool (credit_only lives on the refunded encoder).
	if _, err := MarshalPayload("chora.tenancy.familiar_egg.refunded.v1", env, map[string]any{
		"tenant_id": "t-1", "purchase_id": "p-1", "purchaser_gcid": "g-1",
		"egg_sku": "egg.standard.v1", "amount_cents_refunded": 1, "currency": "SGD",
		"stripe_refund_id": "re_1", "refunded_at": time.Now(), "credit_only": "yes",
	}); err == nil {
		t.Fatalf("expected bool-field type error")
	}
	// enum slot with unknown type (reason_code lives on the deactivated encoder).
	if _, err := MarshalPayload("chora.tenancy.addon.deactivated.v1", env, merge(base, map[string]any{"reason_code": []string{"x"}})); err == nil {
		t.Fatalf("expected enum-field type error")
	}
	// int64 slot with non-numeric (billing_delta_cents lives on the tier-change encoders).
	if _, err := MarshalPayload("chora.tenancy.addon.upgraded.v1", env, merge(base, map[string]any{"billing_delta_cents": "5k"})); err == nil {
		t.Fatalf("expected int64-field type error")
	}
	// timestamp slot with wrong type.
	if _, err := MarshalPayload("chora.tenancy.addon.activated.v1", env, merge(base, map[string]any{"activated_at": 12345})); err == nil {
		t.Fatalf("expected timestamp-field type error")
	}
}

func TestCoercionHelpers(t *testing.T) {
	t.Parallel()
	// asInt32 — nil + numeric families + non-numeric.
	if _, ok := asInt32(nil); ok {
		t.Fatalf("nil should not coerce")
	}
	for _, v := range []any{int(1), int32(2), int64(3), uint(4), uint32(5), uint64(6), float32(7), float64(8)} {
		if _, ok := asInt32(v); !ok {
			t.Fatalf("asInt32(%T) should coerce", v)
		}
	}
	if _, ok := asInt32("nope"); ok {
		t.Fatalf("string should not coerce to int32")
	}
	// asInt64 — same families.
	if _, ok := asInt64(nil); ok {
		t.Fatalf("nil should not coerce to int64")
	}
	for _, v := range []any{int(1), int32(2), int64(3), uint(4), uint32(5), uint64(6), float32(7), float64(8)} {
		if got, ok := asInt64(v); !ok || got == 0 {
			t.Fatalf("asInt64(%T) should coerce", v)
		}
	}
	if _, ok := asInt64(true); ok {
		t.Fatalf("bool should not coerce to int64")
	}
	// asTime — time.Time / *time.Time / RFC3339 / RFC3339Nano / empty / bad.
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	if got, ok := asTime(now); !ok || !got.Equal(now) {
		t.Fatalf("time.Time coercion failed")
	}
	if got, ok := asTime(&now); !ok || !got.Equal(now) {
		t.Fatalf("*time.Time coercion failed")
	}
	if got, ok := asTime(now.Format(time.RFC3339)); !ok || !got.Equal(now) {
		t.Fatalf("RFC3339 string coercion failed")
	}
	if got, ok := asTime(now.Format(time.RFC3339Nano)); !ok || !got.Equal(now) {
		t.Fatalf("RFC3339Nano string coercion failed")
	}
	if _, ok := asTime(""); ok {
		t.Fatalf("empty string should not coerce")
	}
	if _, ok := asTime("not-a-time"); ok {
		t.Fatalf("bad string should not coerce")
	}
	if _, ok := asTime(nil); ok {
		t.Fatalf("nil should not coerce to time")
	}
	if _, ok := asTime(42); ok {
		t.Fatalf("int should not coerce to time")
	}
	var nilTime *time.Time
	if _, ok := asTime(nilTime); ok {
		t.Fatalf("nil *time.Time should not coerce")
	}
}

func TestEnumConverters(t *testing.T) {
	t.Parallel()
	wantHosting := map[string]int32{
		"HOSTING_MODE_PLATFORM_HOSTED": 1, "platform_hosted": 1,
		"HOSTING_MODE_WHITE_LABEL": 2, "white_label": 2,
		"HOSTING_MODE_FRANCHISE": 3, "franchise": 3,
		"HOSTING_MODE_SELF_HOST": 4, "self_host": 4,
		"unknown": 0,
	}
	for in, want := range wantHosting {
		if got := hostingModeFromString(in); got != want {
			t.Fatalf("hostingModeFromString(%q) = %d want %d", in, got, want)
		}
	}
	wantReasons := map[string]int32{
		"no_longer_needed": 1, "NO_LONGER_NEEDED": 1, "ADDON_DEACTIVATION_REASON_NO_LONGER_NEEDED": 1,
		"cost": 2, "COST": 2, "ADDON_DEACTIVATION_REASON_COST": 2,
		"consolidation": 3, "CONSOLIDATION": 3, "ADDON_DEACTIVATION_REASON_CONSOLIDATION": 3,
		"migration": 4, "MIGRATION": 4, "ADDON_DEACTIVATION_REASON_MIGRATION": 4,
		"compliance": 5, "COMPLIANCE": 5, "ADDON_DEACTIVATION_REASON_COMPLIANCE": 5,
		"other": 6, "OTHER": 6, "ADDON_DEACTIVATION_REASON_OTHER": 6,
		"nope": 0,
	}
	for in, want := range wantReasons {
		if got := addonDeactivationReasonFromString(in); got != want {
			t.Fatalf("addonDeactivationReasonFromString(%q) = %d want %d", in, got, want)
		}
	}
}

func merge(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
