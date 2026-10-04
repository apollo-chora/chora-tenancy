// Package protomarshal encodes chora-tenancy outbox event payloads to
// canonical binary protobuf wire format so GCP Pub/Sub Schema Registry
// validation (BINARY encoding) passes at publish time.
//
// Why hand-rolled
// ---------------
// Generated Go bindings exist in
// chora-contracts/gen/go/chora/tenancy/v1 but the producer-side outbox
// publisher receives a loose `map[string]any` payload that the handlers
// build inline (see services/chora-tenancy/internal/adapter/http/*). Rather
// than retro-fit the handlers to construct typed proto messages everywhere
// (a much larger refactor that crosses the events.Recorder contract), we
// emit canonical wire bytes directly via
// google.golang.org/protobuf/encoding/protowire.
//
// Field numbers + wire types are pinned to
// chora-contracts/proto/events-flat/tenancy/* — those flat protos ARE the
// schemas registered with Pub/Sub (see
// `gcloud pubsub schemas list --filter='name ~ chora-tenancy'`).
//
// Invariants per the registered Schema Registry schemas:
//
//   - Field 1 = envelope (length-delimited nested message)
//   - Envelope nested fields 1..15 follow chora.common.v1.EventEnvelope
//     layout (the flat schemas inline the envelope with identical field
//     numbers, so bytes round-trip both ways).
//   - Timestamps are nested messages: int64 seconds (field 1) + int32 nanos
//     (field 2)
//   - Enums encode as varint (proto3 int32)
//   - Unknown topics return ErrUnsupportedTopic so the publisher can fall
//     back to JSON with a WARN log; rows on schema-attached topics will
//     dead-letter at the dispatcher, which is the correct loud-failure mode.
//
// Per CLAUDE.md §6 — wire format MUST be binary protobuf for Pub/Sub-attached
// topics. JSON encoding is rejected at publish time with "Invalid binary proto
// message".
package protomarshal

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Envelope is the producer-side flat shape of chora.common.v1.EventEnvelope
// (== the inlined Envelope in every chora-contracts events-flat schema). It
// mirrors chora-common/envelope.Envelope; defined locally to keep
// this package import-cycle-free with the outbox publisher.
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
}

// ErrUnsupportedTopic is returned by MarshalPayload when the topic has no
// registered binary encoder. The publisher logs a one-shot WARN and falls
// back to JSON, where the dispatcher will dead-letter the row (Schema
// Registry binary validation rejects JSON bytes). Adding an encoder for a
// new topic is a one-line case in MarshalPayload + an encode{X} helper.
var ErrUnsupportedTopic = errors.New("protomarshal: topic has no binary encoder; outbox row will JSON-fallback and dispatcher will dead-letter")

// IsUnsupportedTopic reports whether err is (or wraps) ErrUnsupportedTopic.
func IsUnsupportedTopic(err error) bool { return errors.Is(err, ErrUnsupportedTopic) }

// MarshalPayload converts a topic + envelope + loose payload map into the
// canonical binary protobuf wire bytes for that topic's Schema Registry
// schema. Returns ErrUnsupportedTopic if no encoder is registered for the
// supplied topic.
//
// Topics with registered encoders (each matches a registered Pub/Sub schema
// per `gcloud pubsub topics list --filter='name ~ chora.tenancy'`):
//
//   - chora.tenancy.tenant.created.v1
//   - chora.tenancy.addon.activated.v1
//   - chora.tenancy.addon.deactivated.v1
//   - chora.tenancy.addon.deactivation_requested.v1
//   - chora.tenancy.addon.upgraded.v1
//   - chora.tenancy.addon.downgraded.v1
//   - chora.tenancy.addon.usage_recorded.v1
//   - chora.tenancy.familiar_egg.checkout_started.v1
//   - chora.tenancy.familiar_egg.payment_succeeded.v1
//   - chora.tenancy.familiar_egg.payment_failed.v1
//   - chora.tenancy.familiar_egg.refunded.v1
//
// Adding more topics: append a case to the switch + implement
// encode{X}(env, payload) returning the wire bytes.
func MarshalPayload(topic string, env Envelope, payload map[string]any) ([]byte, error) {
	switch topic {
	case "chora.tenancy.tenant.created.v1":
		return encodeTenantCreated(env, payload)
	case "chora.tenancy.tenant.golive.v1":
		return encodeTenantGoLive(env, payload)
	case "chora.tenancy.addon.activated.v1":
		return encodeAddonActivated(env, payload)
	case "chora.tenancy.addon.deactivated.v1":
		return encodeAddonDeactivated(env, payload)
	case "chora.tenancy.addon.deactivation_requested.v1":
		return encodeAddonDeactivationRequested(env, payload)
	case "chora.tenancy.addon.upgraded.v1":
		return encodeAddonUpgraded(env, payload)
	case "chora.tenancy.addon.downgraded.v1":
		return encodeAddonDowngraded(env, payload)
	case "chora.tenancy.addon.usage_recorded.v1":
		return encodeAddonUsageRecorded(env, payload)
	case "chora.tenancy.familiar_egg.checkout_started.v1":
		return encodeFamiliarEggCheckoutStarted(env, payload)
	case "chora.tenancy.familiar_egg.payment_succeeded.v1":
		return encodeFamiliarEggPaymentSucceeded(env, payload)
	case "chora.tenancy.familiar_egg.payment_failed.v1":
		return encodeFamiliarEggPaymentFailed(env, payload)
	case "chora.tenancy.familiar_egg.refunded.v1":
		return encodeFamiliarEggRefunded(env, payload)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedTopic, topic)
	}
}

// -----------------------------------------------------------------------------
// TenantGoLive (chora.tenancy.tenant.golive.v1)
// Field layout — chora-contracts/proto/events-flat/tenancy/tenant/golive.proto:
//
//	1 envelope, 2 tenant_id, 3 parent_tenant_id, 4 display_name,
//	5 hosting_mode(enum), 6 activated_by_gcid, 7 stripe_customer_id,
//	8 default_addon_plan_ids(repeated string), 9 activated_at(Timestamp)
//
// -----------------------------------------------------------------------------
//
// Fabric-repair 2026-07-01: the Flag-1 guardrail found this topic is PRODUCED
// (v1_handlers golive handler → deps.Events.Publish) and bound to a BINARY
// Schema Registry schema, but protomarshal had NO encoder case — so the outbox
// JSON-fell-back and the binary schema rejected every publish → deadletter
// (kg_hexagon_fog class, latent until the first tenant went live). Guarded by
// TestTenantGoLive_CanonicalRoundTrip (fabric_golive_test.go).
func encodeTenantGoLive(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "parent_tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "display_name", payload); err != nil {
		return nil, err
	}
	if err := appendEnumField(&out, 5, "hosting_mode", payload, hostingModeFromString); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "activated_by_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 7, "stripe_customer_id", payload); err != nil {
		return nil, err
	}
	// field 8 default_addon_plan_ids — repeated string (accepts []string or []any).
	if raw, ok := payload["default_addon_plan_ids"]; ok && raw != nil {
		switch v := raw.(type) {
		case []string:
			for _, s := range v {
				out = appendString(out, 8, s)
			}
		case []any:
			for _, e := range v {
				if s, ok := e.(string); ok {
					out = appendString(out, 8, s)
				}
			}
		}
	}
	if err := appendTimestampField(&out, 9, "activated_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// TenantCreated (chora.tenancy.tenant.created.v1)
// Field layout — chora-contracts/proto/events-flat/tenancy/tenant/created.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string parent_tenant_id
//	4  string display_name
//	5  varint HostingMode hosting_mode (enum)
//	6  string owner_gcid
//	7  bytes  Timestamp created_at
func encodeTenantCreated(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "parent_tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "display_name", payload); err != nil {
		return nil, err
	}
	if err := appendEnumField(&out, 5, "hosting_mode", payload, hostingModeFromString); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "owner_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 7, "created_at", payload); err != nil {
		return nil, err
	}

	return out, nil
}

// hostingModeFromString maps the HostingMode enum string values emitted by
// chora-tenancy handlers (or the on-wire snake_case style) to the proto
// int32 enum values per
// chora-contracts/proto/events-flat/tenancy/tenant/created.proto.
//
//	HOSTING_MODE_UNSPECIFIED     = 0
//	HOSTING_MODE_PLATFORM_HOSTED = 1
//	HOSTING_MODE_WHITE_LABEL     = 2
//	HOSTING_MODE_FRANCHISE       = 3
//	HOSTING_MODE_SELF_HOST       = 4
//
// Returns 0 (UNSPECIFIED) for unknown strings — proto3 enums tolerate 0
// silently so this is safe at the wire level.
func hostingModeFromString(s string) int32 {
	switch s {
	case "HOSTING_MODE_PLATFORM_HOSTED", "platform_hosted":
		return 1
	case "HOSTING_MODE_WHITE_LABEL", "white_label":
		return 2
	case "HOSTING_MODE_FRANCHISE", "franchise":
		return 3
	case "HOSTING_MODE_SELF_HOST", "self_host":
		return 4
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// AddonActivated (chora.tenancy.addon.activated.v1)
// chora-contracts/proto/events-flat/tenancy/addon/activated.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string addon_plan_id
//	4  string activated_by_gcid
//	5  bytes  Timestamp activated_at
func encodeAddonActivated(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "addon_plan_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "activated_by_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 5, "activated_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AddonDeactivated (chora.tenancy.addon.deactivated.v1)
// chora-contracts/proto/events-flat/tenancy/addon/deactivated.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string addon_plan_id
//	4  string reason            (legacy free-form)
//	5  string deactivated_by_gcid
//	6  bytes  Timestamp deactivated_at
//	7  varint AddonDeactivationReason reason_code (enum)
//	8  string reason_text
func encodeAddonDeactivated(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "addon_plan_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "reason", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "deactivated_by_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 6, "deactivated_at", payload); err != nil {
		return nil, err
	}
	if err := appendEnumField(&out, 7, "reason_code", payload, addonDeactivationReasonFromString); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 8, "reason_text", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AddonDeactivationRequested (chora.tenancy.addon.deactivation_requested.v1)
// chora-contracts/proto/events-flat/tenancy/addon/deactivation_requested.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string addon_plan_id
//	4  varint AddonDeactivationReason reason
//	5  string reason_text
//	6  string requested_by_gcid
//	7  bytes  Timestamp effective_at
//	8  bytes  Timestamp requested_at
func encodeAddonDeactivationRequested(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "addon_plan_id", payload); err != nil {
		return nil, err
	}
	if err := appendEnumField(&out, 4, "reason", payload, addonDeactivationReasonFromString); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "reason_text", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "requested_by_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 7, "effective_at", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 8, "requested_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// addonDeactivationReasonFromString maps the AddonDeactivationReason enum
// string values to int32 per
// chora-contracts/proto/events-flat/tenancy/addon/deactivated.proto.
//
//	ADDON_DEACTIVATION_REASON_UNSPECIFIED    = 0
//	ADDON_DEACTIVATION_REASON_NO_LONGER_NEEDED = 1
//	ADDON_DEACTIVATION_REASON_COST             = 2
//	ADDON_DEACTIVATION_REASON_CONSOLIDATION    = 3
//	ADDON_DEACTIVATION_REASON_MIGRATION        = 4
//	ADDON_DEACTIVATION_REASON_COMPLIANCE       = 5
//	ADDON_DEACTIVATION_REASON_OTHER            = 6
func addonDeactivationReasonFromString(s string) int32 {
	switch s {
	case "no_longer_needed", "NO_LONGER_NEEDED", "ADDON_DEACTIVATION_REASON_NO_LONGER_NEEDED":
		return 1
	case "cost", "COST", "ADDON_DEACTIVATION_REASON_COST":
		return 2
	case "consolidation", "CONSOLIDATION", "ADDON_DEACTIVATION_REASON_CONSOLIDATION":
		return 3
	case "migration", "MIGRATION", "ADDON_DEACTIVATION_REASON_MIGRATION":
		return 4
	case "compliance", "COMPLIANCE", "ADDON_DEACTIVATION_REASON_COMPLIANCE":
		return 5
	case "other", "OTHER", "ADDON_DEACTIVATION_REASON_OTHER":
		return 6
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// AddonUpgraded (chora.tenancy.addon.upgraded.v1) +
// AddonDowngraded (chora.tenancy.addon.downgraded.v1) share an identical
// field layout — same encoder body, different field 1 envelope provenance.
// chora-contracts/proto/events-flat/tenancy/addon/{upgraded,downgraded}.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string addon_plan_id
//	4  string from_tier
//	5  string to_tier
//	6  bytes  Timestamp effective_at
//	7  varint int64 billing_delta_cents
//	8  string schedule_id
//	9  string requested_by_gcid
func encodeAddonUpgraded(env Envelope, payload map[string]any) ([]byte, error) {
	return encodeAddonTierChange(env, payload)
}
func encodeAddonDowngraded(env Envelope, payload map[string]any) ([]byte, error) {
	return encodeAddonTierChange(env, payload)
}
func encodeAddonTierChange(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "addon_plan_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "from_tier", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "to_tier", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 6, "effective_at", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 7, "billing_delta_cents", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 8, "schedule_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 9, "requested_by_gcid", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AddonUsageRecorded (chora.tenancy.addon.usage_recorded.v1)
// chora-contracts/proto/events-flat/tenancy/addon/usage_recorded.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string tenant_id
//	3  string addon_plan_id
//	4  string usage_dimension
//	5  varint int64 usage_value
//	6  string usage_unit
//	7  bytes  Timestamp recorded_at
func encodeAddonUsageRecorded(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "addon_plan_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "usage_dimension", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 5, "usage_value", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "usage_unit", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 7, "recorded_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// FamiliarEggCheckoutStarted (chora.tenancy.familiar_egg.checkout_started.v1)
// chora-contracts/proto/events-flat/tenancy/familiar_egg/checkout_started.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string purchase_id
//	3  string purchaser_gcid
//	4  string target_tenant_id
//	5  string egg_sku
//	6  string stripe_session_id
//	7  string stripe_checkout_url
//	8  varint int64 amount_cents
//	9  string currency
//	10 string suggested_focal_atom_id
//	11 bytes  Timestamp checkout_started_at
func encodeFamiliarEggCheckoutStarted(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "purchase_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "purchaser_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "target_tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "egg_sku", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "stripe_session_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 7, "stripe_checkout_url", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 8, "amount_cents", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 9, "currency", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 10, "suggested_focal_atom_id", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 11, "checkout_started_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// FamiliarEggPaymentSucceeded (chora.tenancy.familiar_egg.payment_succeeded.v1)
// chora-contracts/proto/events/tenancy/familiar_egg.proto (no flat schema yet
// because the topic+Schema Registry registration lands with the next
// chora-contracts/m10-data-plane apply — kept here so the encoder is ready).
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string purchase_id
//	3  string purchaser_gcid
//	4  string target_tenant_id
//	5  string egg_sku
//	6  string stripe_session_id
//	7  string stripe_payment_intent_id
//	8  string stripe_charge_id
//	9  varint int64 amount_cents_paid
//	10 varint int64 amount_cents_refunded
//	11 string currency
//	12 string suggested_focal_atom_id
//	13 bytes  Timestamp paid_at
func encodeFamiliarEggPaymentSucceeded(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "purchase_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "purchaser_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "target_tenant_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "egg_sku", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "stripe_session_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 7, "stripe_payment_intent_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 8, "stripe_charge_id", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 9, "amount_cents_paid", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 10, "amount_cents_refunded", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 11, "currency", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 12, "suggested_focal_atom_id", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 13, "paid_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// FamiliarEggPaymentFailed (chora.tenancy.familiar_egg.payment_failed.v1)
// chora-contracts/proto/events-flat/tenancy/familiar_egg/payment_failed.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string purchase_id
//	3  string purchaser_gcid
//	4  string egg_sku
//	5  string stripe_session_id
//	6  string stripe_failure_code
//	7  string stripe_failure_message
//	8  bytes  Timestamp failed_at
func encodeFamiliarEggPaymentFailed(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "purchase_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "purchaser_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "egg_sku", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "stripe_session_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "stripe_failure_code", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 7, "stripe_failure_message", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 8, "failed_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// FamiliarEggRefunded (chora.tenancy.familiar_egg.refunded.v1)
// chora-contracts/proto/events-flat/tenancy/familiar_egg/refunded.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string purchase_id
//	3  string purchaser_gcid
//	4  string egg_sku
//	5  string stripe_charge_id
//	6  string stripe_refund_id
//	7  varint int64 amount_cents_refunded
//	8  string currency
//	9  string reason
//	10 varint bool credit_only
//	11 bytes  Timestamp refunded_at
func encodeFamiliarEggRefunded(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)
	if payload == nil {
		return out, nil
	}
	if err := appendStringField(&out, 2, "purchase_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 3, "purchaser_gcid", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 4, "egg_sku", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 5, "stripe_charge_id", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 6, "stripe_refund_id", payload); err != nil {
		return nil, err
	}
	if err := appendInt64Field(&out, 7, "amount_cents_refunded", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 8, "currency", payload); err != nil {
		return nil, err
	}
	if err := appendStringField(&out, 9, "reason", payload); err != nil {
		return nil, err
	}
	if err := appendBoolField(&out, 10, "credit_only", payload); err != nil {
		return nil, err
	}
	if err := appendTimestampField(&out, 11, "refunded_at", payload); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Envelope (nested in every event message; field layout matches
// chora.common.v1.EventEnvelope as inlined in every chora-contracts
// events-flat schema).
// -----------------------------------------------------------------------------
//
//	1  string event_id
//	2  string idempotency_key
//	3  string tenant_id
//	4  string gcid
//	5  bytes  Timestamp occurred_at
//	6  bytes  Timestamp published_at
//	7  string traceparent
//	8  string tracestate
//	9  string source_project
//	10 string source_service
//	11 varint int32 schema_version
//	12 string correlation_id
//	13 string causation_id
//	14 string chora_imda_dimension
//	15 string imda_lifecycle_stage
func encodeEnvelope(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)

	if env.EventID != "" {
		out = appendString(out, 1, env.EventID)
	}
	if env.IdempotencyKey != "" {
		out = appendString(out, 2, env.IdempotencyKey)
	}
	if env.TenantID != "" {
		out = appendString(out, 3, env.TenantID)
	}
	if env.GCID != "" {
		out = appendString(out, 4, env.GCID)
	}
	if !env.OccurredAt.IsZero() {
		out = appendLengthDelimited(out, 5, encodeTimestamp(env.OccurredAt))
	}
	if !env.PublishedAt.IsZero() {
		out = appendLengthDelimited(out, 6, encodeTimestamp(env.PublishedAt))
	}
	if env.Traceparent != "" {
		out = appendString(out, 7, env.Traceparent)
	}
	if env.Tracestate != "" {
		out = appendString(out, 8, env.Tracestate)
	}
	if env.SourceProject != "" {
		out = appendString(out, 9, env.SourceProject)
	}
	if env.SourceService != "" {
		out = appendString(out, 10, env.SourceService)
	}
	if env.SchemaVersion > 0 {
		out = appendVarint(out, 11, uint64(uint32(env.SchemaVersion)))
	}

	// Optional IMDA evidence fields sourced from the payload — chora-tenancy
	// handlers set chora_imda_dimension="accountability" on most billing /
	// lifecycle events for IMDA D1 audit trails (see addon_screens_handlers
	// + familiar_egg_publisher).
	if payload != nil {
		if v, ok := payload["chora_imda_dimension"].(string); ok && v != "" {
			out = appendString(out, 14, v)
		}
		if v, ok := payload["imda_lifecycle_stage"].(string); ok && v != "" {
			out = appendString(out, 15, v)
		}
	}

	return out, nil
}

// encodeTimestamp emits the nested google.protobuf.Timestamp wire shape:
//
//	1 varint int64  seconds
//	2 varint int32  nanos
func encodeTimestamp(t time.Time) []byte {
	out := make([]byte, 0, 16)
	t = t.UTC()
	secs := t.Unix()
	nanos := int32(t.Nanosecond())
	if secs != 0 {
		out = appendVarint(out, 1, uint64(secs))
	}
	if nanos != 0 {
		out = appendVarint(out, 2, uint64(uint32(nanos)))
	}
	return out
}

// -----------------------------------------------------------------------------
// Wire-format helpers (thin protowire wrappers; reuse keeps callers tidy).
// -----------------------------------------------------------------------------

func appendString(b []byte, field protowire.Number, v string) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendString(b, v)
	return b
}

func appendVarint(b []byte, field protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, field, protowire.VarintType)
	b = protowire.AppendVarint(b, v)
	return b
}

func appendLengthDelimited(b []byte, field protowire.Number, payload []byte) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendBytes(b, payload)
	return b
}

// -----------------------------------------------------------------------------
// Typed field helpers — fail loud on schema-type mismatches so the publisher
// can surface a proper error instead of emitting garbage bytes.
// -----------------------------------------------------------------------------

// appendStringField appends a proto string field if payload[key] is a
// non-empty string. Missing keys are skipped (proto3 default behaviour);
// non-string values for the slot return a typed error.
func appendStringField(b *[]byte, field protowire.Number, key string, payload map[string]any) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	s, ok := raw.(string)
	if !ok {
		return fmt.Errorf("field %s: expected string, got %T", key, raw)
	}
	if s == "" {
		return nil
	}
	*b = appendString(*b, field, s)
	return nil
}

// appendInt64Field appends a proto int64 field if payload[key] is numeric.
// Missing keys are skipped; non-numeric values for the slot return a typed
// error.
func appendInt64Field(b *[]byte, field protowire.Number, key string, payload map[string]any) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	v, ok := asInt64(raw)
	if !ok {
		return fmt.Errorf("field %s: expected int64-convertible, got %T", key, raw)
	}
	if v == 0 {
		return nil
	}
	*b = appendVarint(*b, field, uint64(v))
	return nil
}

// appendBoolField appends a proto bool field if payload[key] is bool-typed
// and true. False values are skipped (proto3 default). Non-bool types for
// the slot return a typed error.
func appendBoolField(b *[]byte, field protowire.Number, key string, payload map[string]any) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	v, ok := raw.(bool)
	if !ok {
		return fmt.Errorf("field %s: expected bool, got %T", key, raw)
	}
	if !v {
		return nil
	}
	*b = appendVarint(*b, field, 1)
	return nil
}

// appendEnumField appends a proto enum (encoded as varint int32) if
// payload[key] is present. Accepts either an int (the proto value directly)
// or a string (looked up via the converter). Unknown strings encode as 0
// (UNSPECIFIED) and are skipped per proto3 default-elision semantics.
func appendEnumField(b *[]byte, field protowire.Number, key string, payload map[string]any, conv func(string) int32) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	var v int32
	switch n := raw.(type) {
	case string:
		v = conv(n)
	case int, int32, int64, uint, uint32, uint64, float32, float64:
		ni, ok := asInt32(raw)
		if !ok {
			return fmt.Errorf("field %s: expected int32-convertible enum, got %T", key, raw)
		}
		v = ni
	default:
		return fmt.Errorf("field %s: expected enum (string or numeric), got %T", key, raw)
	}
	if v == 0 {
		return nil
	}
	*b = appendVarint(*b, field, uint64(uint32(v)))
	return nil
}

// appendTimestampField appends a nested google.protobuf.Timestamp if
// payload[key] is time.Time or *time.Time. Accepts RFC 3339 strings too
// (common when handlers format the time before stuffing the payload map).
// Wrong types fail loud.
func appendTimestampField(b *[]byte, field protowire.Number, key string, payload map[string]any) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	t, ok := asTime(raw)
	if !ok {
		return fmt.Errorf("field %s: expected time.Time / *time.Time / RFC3339 string, got %T", key, raw)
	}
	if t.IsZero() {
		return nil
	}
	*b = appendLengthDelimited(*b, field, encodeTimestamp(t))
	return nil
}

// -----------------------------------------------------------------------------
// Loose-typed payload coercion (in/out: map[string]any).
// -----------------------------------------------------------------------------

// asInt32 converts the value to int32. Returns false (not 0) if v is nil OR
// is a non-numeric type — caller decides whether to fail or skip.
func asInt32(v any) (int32, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	case float32:
		return int32(n), true
	case float64:
		return int32(n), true
	case uint:
		return int32(n), true
	case uint32:
		return int32(n), true
	case uint64:
		return int32(n), true
	default:
		return 0, false
	}
}

// asInt64 converts the value to int64. Returns false (not 0) on nil OR a
// non-numeric type.
func asInt64(v any) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	case uint:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	default:
		return 0, false
	}
}

// asTime coerces a value to time.Time. Accepts time.Time, *time.Time, and
// RFC 3339 / RFC 3339 nano strings (the chora-tenancy HTTP handlers format
// the time before putting it in the payload map).
func asTime(v any) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	case string:
		if t == "" {
			return time.Time{}, false
		}
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return parsed, true
		}
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			return parsed, true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}
