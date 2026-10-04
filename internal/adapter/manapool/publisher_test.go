package manapool_test

import (
	"strings"
	"testing"
	"time"

	manapool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	allocation "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

func TestRecorderPublisher_Publishes7TopicTypes(t *testing.T) {
	t.Parallel()
	rec := manapool.NewRecorderPublisher()

	hdr := manapool.EnvelopeHeader{TenantID: tenantA, GCID: gcidAdmin, Traceparent: "00-aabb-ccdd-01"}

	// 1. created
	rec.PublishPoolCreated(hdr, pool.PoolCreated{
		PoolID: "p1", TenantID: tenantA, AutoAllocationPolicy: pool.PolicyKindManual,
		CreatedByGCID: gcidAdmin, CreatedAt: time.Now().UTC(),
	})
	// 2. topped_up
	rec.PublishPoolToppedUp(hdr, pool.PoolToppedUp{
		PoolID: "p1", TenantID: tenantA, UnitsCredited: 500, ChargedCents: 250,
		Currency: "USD", StripePaymentIntentID: "pi_x", BalanceAfterUnits: 500,
		RecordedAt: time.Now().UTC(),
	})
	// 3. depleted
	rec.PublishPoolDepleted(hdr, pool.PoolDepleted{
		PoolID: "p1", TenantID: tenantA, BalanceUnits: 0, FullyDepleted: true,
		DetectedAt: time.Now().UTC(),
	})
	// 4. allocation granted
	rec.PublishAllocationGranted(hdr, allocation.AllocationGranted{
		AllocationID: "a1", TenantID: tenantA, GCID: gcidL1, SourcePoolID: "p1",
		Units: 100, Reason: allocation.ReasonManual, AllocatedByGCID: gcidAdmin,
		AllocatedAt: time.Now().UTC(),
	})
	// 5. allocation claimed
	rec.PublishAllocationClaimed(hdr, allocation.AllocationClaimed{
		AllocationID: "a1", TenantID: tenantA, GCID: gcidL1,
		FirstDebitEntryID: "e1", RemainingUnits: 50, ClaimedAt: time.Now().UTC(),
	})
	// 6. allocation revoked
	rec.PublishAllocationRevoked(hdr, allocation.AllocationRevoked{
		AllocationID: "a1", TenantID: tenantA, GCID: gcidL1,
		UnitsReturnedToPool: true, ReturnedUnits: 100, Reason: "test",
		RevokedByGCID: gcidAdmin, RevokedAt: time.Now().UTC(),
	})
	// 7. allocation expired
	rec.PublishAllocationExpired(hdr, allocation.AllocationExpired{
		AllocationID: "a1", TenantID: tenantA, GCID: gcidL1,
		UnitsReturnedToPool: false, ReturnedUnits: 0,
		ExpiresAt: time.Now().UTC(), ExpiredAt: time.Now().UTC(),
	})

	out := rec.Recorded()
	if len(out) != 7 {
		t.Fatalf("expected 7 published events, got %d", len(out))
	}

	wantTopics := []string{
		"chora.tenancy.tenant_mana_pool.created.v1",
		"chora.tenancy.tenant_mana_pool.topped_up.v1",
		"chora.tenancy.tenant_mana_pool.depleted.v1",
		"chora.tenancy.tenant_mana_allocation.granted.v1",
		"chora.tenancy.tenant_mana_allocation.claimed.v1",
		"chora.tenancy.tenant_mana_allocation.revoked.v1",
		"chora.tenancy.tenant_mana_allocation.expired.v1",
	}
	for i, want := range wantTopics {
		if out[i].Topic != want {
			t.Fatalf("topic[%d] = %q, want %q", i, out[i].Topic, want)
		}
		if out[i].Envelope.EventID == "" {
			t.Fatalf("event_id missing on %q", out[i].Topic)
		}
		if out[i].Envelope.IdempotencyKey == "" {
			t.Fatalf("idempotency_key missing on %q", out[i].Topic)
		}
		if out[i].Envelope.TenantID != tenantA {
			t.Fatalf("envelope tenant_id mismatch on %q", out[i].Topic)
		}
		if out[i].Envelope.SourceService != manapool.ServiceName {
			t.Fatalf("envelope source_service mismatch on %q", out[i].Topic)
		}
		if !strings.Contains(out[i].Envelope.Traceparent, "00-aabb-ccdd-01") {
			t.Fatalf("traceparent should be propagated on %q (got %q)", out[i].Topic, out[i].Envelope.Traceparent)
		}
	}
}

// EnvelopeHeader without traceparent should still produce a valid envelope
// (envelope generates a synthetic traceparent when none provided).
func TestRecorderPublisher_GeneratesTraceparentWhenAbsent(t *testing.T) {
	t.Parallel()
	rec := manapool.NewRecorderPublisher()
	rec.PublishPoolCreated(manapool.EnvelopeHeader{TenantID: tenantA, GCID: gcidAdmin}, pool.PoolCreated{
		PoolID: "p1", TenantID: tenantA, AutoAllocationPolicy: pool.PolicyKindManual,
		CreatedByGCID: gcidAdmin, CreatedAt: time.Now().UTC(),
	})
	out := rec.Recorded()[0].Envelope
	if out.Traceparent == "" {
		t.Fatalf("expected synthetic traceparent")
	}
}
