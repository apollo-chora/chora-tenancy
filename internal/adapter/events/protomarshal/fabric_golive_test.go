// fabric_golive_test — golden guardrail for the tenant.golive encoder the
// 2026-07-01 Flag-1 guardrail discovered was MISSING: chora-tenancy PRODUCES
// chora.tenancy.tenant.golive.v1 (v1_handlers golive handler) to a BINARY
// Schema Registry topic, but protomarshal had no encoder case → JSON fallback →
// schema-reject → deadletter (latent until the first tenant went live). This
// asserts the new encoder's bytes decode into the canonical gen struct.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/tenancy/v1"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events/protomarshal"
)

func TestTenantGoLive_CanonicalRoundTrip(t *testing.T) {
	env := fixedEnvelope()
	activatedAt := time.Date(2026, 6, 30, 9, 0, 0, 0, time.UTC)
	// Mirrors the producer payload (v1_handlers golive handler) plus the
	// optional defensive fields the encoder also maps.
	payload := map[string]any{
		"tenant_id":              "tenant-gl1",
		"stripe_customer_id":     "cus_ABC123",
		"display_name":           "Mighty Mind Tuition",
		"activated_by_gcid":      "gcid-owner",
		"default_addon_plan_ids": []string{"plan-a", "plan-b"},
		"activated_at":           activatedAt.Format(time.RFC3339Nano),
	}
	bz, err := protomarshal.MarshalPayload("chora.tenancy.tenant.golive.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var m tenancyv1.TenantGoLive
	if err := proto.Unmarshal(bz, &m); err != nil {
		t.Fatalf("Unmarshal into gen TenantGoLive: %v", err)
	}
	if m.GetTenantId() != "tenant-gl1" {
		t.Errorf("tenant_id (2) = %q", m.GetTenantId())
	}
	if m.GetDisplayName() != "Mighty Mind Tuition" {
		t.Errorf("display_name (4) = %q", m.GetDisplayName())
	}
	if m.GetActivatedByGcid() != "gcid-owner" {
		t.Errorf("activated_by_gcid (6) = %q", m.GetActivatedByGcid())
	}
	if m.GetStripeCustomerId() != "cus_ABC123" {
		t.Errorf("stripe_customer_id (7) = %q", m.GetStripeCustomerId())
	}
	if got := m.GetDefaultAddonPlanIds(); len(got) != 2 || got[0] != "plan-a" || got[1] != "plan-b" {
		t.Errorf("default_addon_plan_ids (8) = %v", got)
	}
	if m.GetActivatedAt() == nil || !m.GetActivatedAt().AsTime().Equal(activatedAt) {
		t.Errorf("activated_at (9) = %v want %v", m.GetActivatedAt(), activatedAt)
	}
	if m.GetEnvelope() == nil || m.GetEnvelope().GetTenantId() == "" {
		t.Errorf("envelope (1) missing/empty")
	}
}
