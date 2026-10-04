// Tests for the chora-payments Pub/Sub subscribers in chora-tenancy.
//
// chora-tenancy is the originating service for two Purchase aggregates
// extracted to chora-payments (ADR-164):
//
//   - FamiliarEggPurchase  → on payment_captured emit
//     chora.consumption.familiar.egg_purchased.v1 for chora-consumption
//     to provision the Stage-0 familiar_instances row.
//     On refunded with credit_only=true → credit the buyer's tenant via
//     a tenant_mana_allocation (ADR-149 §"Unhatched egg expiry").
//   - TenantManaTopUp → on payment_captured credit
//     tenant_mana_pool.balance_units += mana_units. On refunded debit by
//     mana_units_to_debit.
//
// All subscribers are idempotent on (event_id) via the shared
// libs/chora-go-common/idempotent.Store. Replays are no-ops.
package events_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
)

// -----------------------------------------------------------------------------
// fake pool credit/debit applier — minimal port for the tenant_mana_topup tests
// -----------------------------------------------------------------------------

type fakePoolApplier struct {
	credits []poolDelta
	debits  []poolDelta
	failNext error
}

type poolDelta struct {
	TenantID string
	Units    int64
	Reason   string
}

func (f *fakePoolApplier) Credit(_ context.Context, tenantID string, units int64, reason string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.credits = append(f.credits, poolDelta{TenantID: tenantID, Units: units, Reason: reason})
	return nil
}

func (f *fakePoolApplier) Debit(_ context.Context, tenantID string, units int64, reason string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.debits = append(f.debits, poolDelta{TenantID: tenantID, Units: units, Reason: reason})
	return nil
}

// -----------------------------------------------------------------------------
// FamiliarEgg payment_captured
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_FamiliarEgg_PaymentCaptured_EmitsConsumptionEvent(t *testing.T) {
	rec := events.NewRecorder()
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})

	in := events.FamiliarEggPaymentCaptured{
		EventID:               "evt_1",
		PurchaseID:            "pur_1",
		LearnerGCID:           "gcid_1",
		TargetTenantID:        "tenant_1",
		EggSKU:                "egg.standard.v1",
		StripeSessionID:       "cs_test_1",
		StripePaymentIntentID: "pi_1",
		StripeChargeID:        "ch_1",
		AmountCentsPaid:       999,
		Currency:              "SGD",
		SuggestedFocalAtomID:  "atom_1",
		PaidAt:                time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC),
		Traceparent:           "00-trace-id-span-01",
	}
	if err := sub.HandleFamiliarEggPaymentCaptured(context.Background(), in); err != nil {
		t.Fatalf("HandleFamiliarEggPaymentCaptured: %v", err)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.familiar_egg.payment_succeeded.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(emitted))
	}
	ev := emitted[0]
	if ev.TenantID != "tenant_1" || ev.GCID != "gcid_1" {
		t.Fatalf("envelope mismatch: %+v", ev)
	}
	if ev.Traceparent != "00-trace-id-span-01" {
		t.Fatalf("traceparent not propagated: %q", ev.Traceparent)
	}
	if ev.Payload["purchase_id"] != "pur_1" || ev.Payload["egg_sku"] != "egg.standard.v1" {
		t.Fatalf("payload mismatch: %+v", ev.Payload)
	}
	if ev.Payload["source"] != "purchase" {
		t.Fatalf("source must be 'purchase' (free-egg paths excluded): %v", ev.Payload["source"])
	}
}

func TestPaymentsSubscriber_FamiliarEgg_PaymentCaptured_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.FamiliarEggPaymentCaptured{
		EventID:               "evt_dup",
		PurchaseID:            "pur_dup",
		LearnerGCID:           "gcid_dup",
		TargetTenantID:        "tenant_dup",
		EggSKU:                "egg.standard.v1",
		AmountCentsPaid:       999,
		Currency:              "SGD",
		PaidAt:                time.Now().UTC(),
	}
	ctx := context.Background()
	if err := sub.HandleFamiliarEggPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := sub.HandleFamiliarEggPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("replay returned err: %v", err)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.familiar_egg.payment_succeeded.v1")
	if len(emitted) != 1 {
		t.Fatalf("idempotency broke — expected 1 emit, got %d", len(emitted))
	}
}

func TestPaymentsSubscriber_FamiliarEgg_PaymentCaptured_ValidatesEnvelope(t *testing.T) {
	rec := events.NewRecorder()
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})
	cases := []struct {
		name string
		in   events.FamiliarEggPaymentCaptured
	}{
		{name: "missing event_id", in: events.FamiliarEggPaymentCaptured{PurchaseID: "p", LearnerGCID: "g", TargetTenantID: "t", EggSKU: "s"}},
		{name: "missing purchase_id", in: events.FamiliarEggPaymentCaptured{EventID: "e", LearnerGCID: "g", TargetTenantID: "t", EggSKU: "s"}},
		{name: "missing learner_gcid", in: events.FamiliarEggPaymentCaptured{EventID: "e", PurchaseID: "p", TargetTenantID: "t", EggSKU: "s"}},
		{name: "missing tenant", in: events.FamiliarEggPaymentCaptured{EventID: "e", PurchaseID: "p", LearnerGCID: "g", EggSKU: "s"}},
		{name: "missing sku", in: events.FamiliarEggPaymentCaptured{EventID: "e", PurchaseID: "p", LearnerGCID: "g", TargetTenantID: "t"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := sub.HandleFamiliarEggPaymentCaptured(context.Background(), tc.in)
			if err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// FamiliarEgg refunded
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_FamiliarEgg_Refunded_CreditOnly_AllocatesMana(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.FamiliarEggRefunded{
		EventID:             "evt_r1",
		PurchaseID:          "pur_r1",
		LearnerGCID:         "gcid_r1",
		EggSKU:              "egg.standard.v1",
		TargetTenantID:      "tenant_r1",
		StripeChargeID:      "ch_r1",
		StripeRefundID:      "re_r1",
		AmountCentsRefunded: 500,
		Currency:            "SGD",
		Reason:              "expired_unhatched",
		CreditOnly:          true,
		RefundedAt:          time.Now().UTC(),
		ManaUnitsCredit:     500, // 1 cent = 1 mana unit baseline
	}
	if err := sub.HandleFamiliarEggRefunded(context.Background(), in); err != nil {
		t.Fatalf("HandleFamiliarEggRefunded: %v", err)
	}
	if len(pool.credits) != 1 {
		t.Fatalf("expected 1 mana credit, got %d", len(pool.credits))
	}
	if pool.credits[0].TenantID != "tenant_r1" || pool.credits[0].Units != 500 {
		t.Fatalf("mana credit mismatch: %+v", pool.credits[0])
	}
	// Downstream FamiliarEgg revocation event emitted for chora-consumption.
	emitted := rec.RecordedByTopic("chora.consumption.familiar.egg_revoked.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected revocation event, got %d", len(emitted))
	}
}

func TestPaymentsSubscriber_FamiliarEgg_Refunded_CashRefund_NoManaAlloc(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.FamiliarEggRefunded{
		EventID:             "evt_r2",
		PurchaseID:          "pur_r2",
		LearnerGCID:         "gcid_r2",
		EggSKU:              "egg.standard.v1",
		TargetTenantID:      "tenant_r2",
		AmountCentsRefunded: 999,
		Currency:            "SGD",
		Reason:              "customer_request",
		CreditOnly:          false,
		RefundedAt:          time.Now().UTC(),
	}
	if err := sub.HandleFamiliarEggRefunded(context.Background(), in); err != nil {
		t.Fatalf("HandleFamiliarEggRefunded: %v", err)
	}
	if len(pool.credits) != 0 {
		t.Fatalf("expected no mana credit (cash refund path), got %d", len(pool.credits))
	}
	// Revocation event still fires.
	emitted := rec.RecordedByTopic("chora.consumption.familiar.egg_revoked.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected revocation event, got %d", len(emitted))
	}
}

func TestPaymentsSubscriber_FamiliarEgg_Refunded_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.FamiliarEggRefunded{
		EventID:             "evt_r3",
		PurchaseID:          "pur_r3",
		LearnerGCID:         "gcid_r3",
		EggSKU:              "egg.standard.v1",
		TargetTenantID:      "tenant_r3",
		AmountCentsRefunded: 500,
		ManaUnitsCredit:     500,
		Currency:            "SGD",
		Reason:              "expired_unhatched",
		CreditOnly:          true,
		RefundedAt:          time.Now().UTC(),
	}
	ctx := context.Background()
	if err := sub.HandleFamiliarEggRefunded(ctx, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleFamiliarEggRefunded(ctx, in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(pool.credits) != 1 {
		t.Fatalf("idempotency broke — expected 1 credit, got %d", len(pool.credits))
	}
}

// -----------------------------------------------------------------------------
// FamiliarEgg expired
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_FamiliarEgg_Expired_NoOpWithEvent(t *testing.T) {
	rec := events.NewRecorder()
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.FamiliarEggExpired{
		EventID:        "evt_x1",
		PurchaseID:     "pur_x1",
		LearnerGCID:    "gcid_x1",
		EggSKU:         "egg.standard.v1",
		TargetTenantID: "tenant_x1",
		ExpiredAt:      time.Now().UTC(),
	}
	if err := sub.HandleFamiliarEggExpired(context.Background(), in); err != nil {
		t.Fatalf("HandleFamiliarEggExpired: %v", err)
	}
	// No mana change. Egg never provisioned, so no revocation event either.
	// We still emit observability ack on consumption topic so the funnel
	// reflects abandonment + chora-consumption can clean up any speculative
	// state. Asserting at least we don't crash.
}

// -----------------------------------------------------------------------------
// TenantManaTopUp payment_captured — credits balance
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_ManaTopUp_PaymentCaptured_CreditsPool(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.TenantManaTopUpPaymentCaptured{
		EventID:               "evt_m1",
		PurchaseID:            "pur_m1",
		AdminGCID:             "gcid_admin",
		TargetTenantID:        "tenant_m1",
		SKU:                   "mana.tenant_topup.standard_v1",
		StripeSessionID:       "cs_test_mana",
		StripePaymentIntentID: "pi_m1",
		StripeChargeID:        "ch_m1",
		AmountCentsPaid:       9999,
		Currency:              "SGD",
		ManaUnits:             10_000,
		PaidAt:                time.Now().UTC(),
	}
	if err := sub.HandleManaTopUpPaymentCaptured(context.Background(), in); err != nil {
		t.Fatalf("HandleManaTopUpPaymentCaptured: %v", err)
	}
	if len(pool.credits) != 1 {
		t.Fatalf("expected 1 credit, got %d", len(pool.credits))
	}
	if pool.credits[0].TenantID != "tenant_m1" || pool.credits[0].Units != 10_000 {
		t.Fatalf("credit mismatch: %+v", pool.credits[0])
	}
	// Legacy bridge event for downstream subscribers during the rollback window.
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_mana_pool.topped_up.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected legacy bridge event, got %d", len(emitted))
	}
	if emitted[0].Payload["units_credited"].(int64) != 10_000 {
		t.Fatalf("bridge payload mismatch: %+v", emitted[0].Payload)
	}
}

func TestPaymentsSubscriber_ManaTopUp_PaymentCaptured_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.TenantManaTopUpPaymentCaptured{
		EventID:        "evt_m_dup",
		PurchaseID:     "pur_m_dup",
		AdminGCID:      "g",
		TargetTenantID: "tenant_m",
		SKU:            "s",
		AmountCentsPaid: 9999,
		Currency:       "SGD",
		ManaUnits:      10_000,
		PaidAt:         time.Now().UTC(),
	}
	ctx := context.Background()
	if err := sub.HandleManaTopUpPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleManaTopUpPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(pool.credits) != 1 {
		t.Fatalf("idempotency broke — credits=%d", len(pool.credits))
	}
}

func TestPaymentsSubscriber_ManaTopUp_PaymentCaptured_PropagatesPoolError(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{failNext: errors.New("pool boom")}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.TenantManaTopUpPaymentCaptured{
		EventID:        "evt_m_err",
		PurchaseID:     "p",
		AdminGCID:      "g",
		TargetTenantID: "t",
		SKU:            "s",
		AmountCentsPaid: 1,
		Currency:       "SGD",
		ManaUnits:      1,
		PaidAt:         time.Now().UTC(),
	}
	err := sub.HandleManaTopUpPaymentCaptured(context.Background(), in)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// -----------------------------------------------------------------------------
// TenantManaTopUp refunded — debits balance
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_ManaTopUp_Refunded_DebitsPool(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.TenantManaTopUpRefunded{
		EventID:             "evt_mr1",
		PurchaseID:          "pur_mr1",
		AdminGCID:           "gcid_admin",
		TargetTenantID:      "tenant_mr1",
		SKU:                 "s",
		StripeChargeID:      "ch_mr1",
		StripeRefundID:      "re_mr1",
		AmountCentsRefunded: 9999,
		Currency:            "SGD",
		ManaUnitsToDebit:    10_000,
		Reason:              "customer_request",
		RefundedAt:          time.Now().UTC(),
	}
	if err := sub.HandleManaTopUpRefunded(context.Background(), in); err != nil {
		t.Fatalf("HandleManaTopUpRefunded: %v", err)
	}
	if len(pool.debits) != 1 {
		t.Fatalf("expected 1 debit, got %d", len(pool.debits))
	}
	if pool.debits[0].TenantID != "tenant_mr1" || pool.debits[0].Units != 10_000 {
		t.Fatalf("debit mismatch: %+v", pool.debits[0])
	}
}

func TestPaymentsSubscriber_ManaTopUp_Refunded_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	pool := &fakePoolApplier{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      pool,
		Inbox:     idempotent.NewMemoryStore(),
	})
	in := events.TenantManaTopUpRefunded{
		EventID:          "evt_mr_dup",
		PurchaseID:       "p",
		AdminGCID:        "g",
		TargetTenantID:   "t",
		SKU:              "s",
		ManaUnitsToDebit: 100,
		RefundedAt:       time.Now().UTC(),
	}
	ctx := context.Background()
	if err := sub.HandleManaTopUpRefunded(ctx, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleManaTopUpRefunded(ctx, in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(pool.debits) != 1 {
		t.Fatalf("idempotency broke — debits=%d", len(pool.debits))
	}
}

// -----------------------------------------------------------------------------
// Subscription topic helpers
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_SubscriptionNames(t *testing.T) {
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: events.NewRecorder(),
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
	})
	expect := map[string]string{
		"chora.payments.familiar_egg_purchase.payment_captured.v1": "chora-tenancy-payments-familiar_egg_purchase-payment_captured",
		"chora.payments.familiar_egg_purchase.refunded.v1":         "chora-tenancy-payments-familiar_egg_purchase-refunded",
		"chora.payments.familiar_egg_purchase.expired.v1":          "chora-tenancy-payments-familiar_egg_purchase-expired",
		"chora.payments.tenant_mana_topup.payment_captured.v1":     "chora-tenancy-payments-tenant_mana_topup-payment_captured",
		"chora.payments.tenant_mana_topup.refunded.v1":             "chora-tenancy-payments-tenant_mana_topup-refunded",
	}
	for topic, want := range expect {
		if got := sub.SubscriptionNameForTopic(topic); got != want {
			t.Errorf("SubscriptionNameForTopic(%q) = %q want %q", topic, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// CHO-1740 — fakeAddOnActivator records Activate / Deactivate calls.
// -----------------------------------------------------------------------------

type fakeAddOnActivator struct {
	activates   []activateCall
	deactivates []deactivateCall
	failNext    error
}

type activateCall struct {
	TenantID  string
	AddonCode string
	AdminGCID string
	TierCode  string
}

type deactivateCall struct {
	TenantID      string
	AddonCode     string
	RequesterGCID string
	Reason        string
}

func (f *fakeAddOnActivator) Activate(_ context.Context, tenantID, addonCode, adminGCID, tierCode string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.activates = append(f.activates, activateCall{TenantID: tenantID, AddonCode: addonCode, AdminGCID: adminGCID, TierCode: tierCode})
	return nil
}

func (f *fakeAddOnActivator) Deactivate(_ context.Context, tenantID, addonCode, requesterGCID, reason string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.deactivates = append(f.deactivates, deactivateCall{TenantID: tenantID, AddonCode: addonCode, RequesterGCID: requesterGCID, Reason: reason})
	return nil
}

// --- tenant_addon_purchase.payment_captured ---------------------------------

func TestPaymentsSubscriber_TenantAddon_PaymentCaptured_ActivatesAndEmits(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonPaymentCaptured{
		EventID:               "evt_addon_1",
		PurchaseID:            "pur_addon_1",
		AdminGCID:             "gcid_admin",
		TargetTenantID:        "tenant_a1",
		AddonPlanID:           "019e0000-0000-7000-8000-aaaaaaaaaaaa",
		AddonCode:             "knowledge_graph",
		TierCode:              "pro",
		StripeSessionID:       "cs_test_addon",
		StripePaymentIntentID: "pi_addon",
		StripeChargeID:        "ch_addon",
		AmountCentsPaid:       4900,
		Currency:              "SGD",
		PaidAt:                time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonPaymentCaptured(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonPaymentCaptured: %v", err)
	}
	if len(act.activates) != 1 {
		t.Fatalf("expected 1 activate call, got %d", len(act.activates))
	}
	if act.activates[0].TenantID != "tenant_a1" {
		t.Errorf("TenantID = %q", act.activates[0].TenantID)
	}
	if act.activates[0].AddonCode != "knowledge_graph" {
		t.Errorf("AddonCode = %q", act.activates[0].AddonCode)
	}
	if act.activates[0].TierCode != "pro" {
		t.Errorf("TierCode = %q", act.activates[0].TierCode)
	}
	if act.activates[0].AdminGCID != "gcid_admin" {
		t.Errorf("AdminGCID = %q", act.activates[0].AdminGCID)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.activated.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected activated outbound event, got %d", len(emitted))
	}
	if emitted[0].Payload["addon_code"] != "knowledge_graph" {
		t.Errorf("emitted payload addon_code = %v", emitted[0].Payload["addon_code"])
	}
}

func TestPaymentsSubscriber_TenantAddon_PaymentCaptured_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonPaymentCaptured{
		EventID:        "evt_addon_dup",
		PurchaseID:     "pur_addon_dup",
		AdminGCID:      "g",
		TargetTenantID: "tenant_a",
		AddonPlanID:    "plan_a",
		AddonCode:      "knowledge_graph",
		TierCode:       "pro",
		AmountCentsPaid: 4900,
		Currency:       "SGD",
		PaidAt:         time.Now().UTC(),
	}
	ctx := context.Background()
	if err := sub.HandleTenantAddonPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleTenantAddonPaymentCaptured(ctx, in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(act.activates) != 1 {
		t.Fatalf("idempotency broke — activates=%d", len(act.activates))
	}
}

func TestPaymentsSubscriber_TenantAddon_PaymentCaptured_FailsLoudWithoutActivator(t *testing.T) {
	rec := events.NewRecorder()
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher: rec,
		Pool:      &fakePoolApplier{},
		Inbox:     idempotent.NewMemoryStore(),
		// AddOnActivator deliberately nil.
	})
	err := sub.HandleTenantAddonPaymentCaptured(context.Background(), events.TenantAddonPaymentCaptured{
		EventID:        "evt_a1",
		PurchaseID:     "p1",
		AdminGCID:      "g",
		TargetTenantID: "t",
		AddonPlanID:    "plan",
		AddonCode:      "knowledge_graph",
		TierCode:       "pro",
	})
	if err == nil {
		t.Fatal("expected error when activator nil")
	}
}

func TestPaymentsSubscriber_TenantAddon_PaymentCaptured_PropagatesActivatorError(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{failNext: errors.New("registry-down")}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	err := sub.HandleTenantAddonPaymentCaptured(context.Background(), events.TenantAddonPaymentCaptured{
		EventID:        "evt_a2",
		PurchaseID:     "p2",
		AdminGCID:      "g",
		TargetTenantID: "t",
		AddonPlanID:    "plan",
		AddonCode:      "knowledge_graph",
		TierCode:       "pro",
		AmountCentsPaid: 4900,
		Currency:       "SGD",
	})
	if err == nil {
		t.Fatal("expected propagated activator error")
	}
}

// --- tenant_addon_purchase.refunded -----------------------------------------

func TestPaymentsSubscriber_TenantAddon_Refunded_DeactivatesAndEmits(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonRefunded{
		EventID:             "evt_addon_ref",
		PurchaseID:          "pur_r1",
		AdminGCID:           "gcid_admin",
		TargetTenantID:      "tenant_r1",
		AddonPlanID:         "knowledge_graph",
		StripeChargeID:      "ch_r1",
		StripeRefundID:      "re_r1",
		AmountCentsRefunded: 4900,
		Currency:            "SGD",
		Reason:              "customer_request",
		RefundedAt:          time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonRefunded(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonRefunded: %v", err)
	}
	if len(act.deactivates) != 1 {
		t.Fatalf("expected 1 deactivate call, got %d", len(act.deactivates))
	}
	if act.deactivates[0].TenantID != "tenant_r1" {
		t.Errorf("TenantID = %q", act.deactivates[0].TenantID)
	}
	if act.deactivates[0].Reason != "customer_request" {
		t.Errorf("Reason = %q", act.deactivates[0].Reason)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.deactivated.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected deactivated outbound event, got %d", len(emitted))
	}
}

func TestPaymentsSubscriber_TenantAddon_Refunded_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonRefunded{
		EventID:        "evt_addon_ref_dup",
		PurchaseID:     "pur_dup",
		AdminGCID:      "g",
		TargetTenantID: "t",
		AddonPlanID:    "kg",
		Reason:         "customer_request",
	}
	ctx := context.Background()
	if err := sub.HandleTenantAddonRefunded(ctx, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleTenantAddonRefunded(ctx, in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(act.deactivates) != 1 {
		t.Fatalf("idempotency broke — deactivates=%d", len(act.deactivates))
	}
}

// -----------------------------------------------------------------------------
// CHO-1775 Phase 2a — Stripe Subscription lifecycle (cancelled / payment_failed /
// payment_recovered). Mirrors the CHO-1740 refunded test shape.
// -----------------------------------------------------------------------------

func TestPaymentsSubscriber_TenantAddon_SubscriptionCancelled_DeactivatesAndEmits(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonSubscriptionCancelled{
		EventID:              "evt_sub_cancelled_1",
		PurchaseID:           "pur_c1",
		AdminGCID:            "gcid_admin",
		TargetTenantID:       "tenant_c1",
		AddonPlanID:          "019e0000-0000-7000-8000-cccccccccccc",
		AddonCode:            "tms",
		TierCode:             "starter",
		StripeSubscriptionID: "sub_test_c1",
		StripeCustomerID:     "cus_test_c1",
		CancelledAt:          time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonSubscriptionCancelled(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonSubscriptionCancelled: %v", err)
	}
	if len(act.deactivates) != 1 {
		t.Fatalf("expected 1 deactivate call, got %d", len(act.deactivates))
	}
	if act.deactivates[0].TenantID != "tenant_c1" {
		t.Errorf("TenantID = %q", act.deactivates[0].TenantID)
	}
	if act.deactivates[0].Reason != "stripe_subscription_cancelled" {
		t.Errorf("Reason = %q; want stripe_subscription_cancelled", act.deactivates[0].Reason)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.deactivated.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected deactivated outbound event, got %d", len(emitted))
	}
	if emitted[0].Payload["stripe_subscription_id"] != "sub_test_c1" {
		t.Errorf("stripe_subscription_id missing from payload: %+v", emitted[0].Payload)
	}
}

func TestPaymentsSubscriber_TenantAddon_SubscriptionCancelled_Idempotent(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonSubscriptionCancelled{
		EventID: "evt_sub_cancelled_dup", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "t", AddonPlanID: "tms",
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := sub.HandleTenantAddonSubscriptionCancelled(ctx, in); err != nil {
			t.Fatalf("Dispatch #%d: %v", i, err)
		}
	}
	if len(act.deactivates) != 1 {
		t.Errorf("idempotency broke — deactivates=%d", len(act.deactivates))
	}
}

func TestPaymentsSubscriber_TenantAddon_SubscriptionPaymentFailed_EmitsDownstream(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonSubscriptionPaymentFailed{
		EventID:              "evt_sub_pmtfail_1",
		PurchaseID:           "pur_f1",
		AdminGCID:            "gcid_admin",
		TargetTenantID:       "tenant_f1",
		AddonPlanID:          "tms",
		AddonCode:            "tms",
		TierCode:             "starter",
		StripeSubscriptionID: "sub_test_f1",
		StripeCustomerID:     "cus_test_f1",
		Currency:             "USD",
		FailedAt:             time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonSubscriptionPaymentFailed(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonSubscriptionPaymentFailed: %v", err)
	}
	// Phase 2a: NO activator mutation (registry past_due flag deferred).
	if len(act.deactivates) != 0 {
		t.Errorf("Phase 2a payment_failed MUST NOT deactivate (got %d)", len(act.deactivates))
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.payment_failed.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected payment_failed outbound event, got %d", len(emitted))
	}
	if emitted[0].Payload["addon_code"] != "tms" {
		t.Errorf("addon_code missing from payload: %+v", emitted[0].Payload)
	}
}

func TestPaymentsSubscriber_TenantAddon_PaymentRecovered_EmitsDownstream(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonPaymentRecovered{
		EventID:              "evt_sub_recovered_1",
		PurchaseID:           "pur_r1",
		AdminGCID:            "gcid_admin",
		TargetTenantID:       "tenant_r1",
		AddonPlanID:          "tms",
		AddonCode:            "tms",
		TierCode:             "starter",
		StripeSubscriptionID: "sub_test_r1",
		StripeCustomerID:     "cus_test_r1",
		Currency:             "USD",
		RecoveredAt:          time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonPaymentRecovered(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonPaymentRecovered: %v", err)
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.payment_recovered.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected payment_recovered outbound event, got %d", len(emitted))
	}
}

// CHO-1776 — fakePastDueRegistry records MarkPastDue / MarkRecovered.
type fakePastDueRegistry struct {
	pastDue   []pastDueCall
	recovered []recoveredCall
	failNext  error
}

type pastDueCall struct {
	TenantID  string
	AddonCode string
}

type recoveredCall struct {
	TenantID  string
	AddonCode string
}

func (f *fakePastDueRegistry) MarkPastDue(tenantID, addonCode string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.pastDue = append(f.pastDue, pastDueCall{TenantID: tenantID, AddonCode: addonCode})
	return nil
}

func (f *fakePastDueRegistry) MarkRecovered(tenantID, addonCode string) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.recovered = append(f.recovered, recoveredCall{TenantID: tenantID, AddonCode: addonCode})
	return nil
}

func TestPaymentsSubscriber_TenantAddon_SubscriptionPaymentFailed_FlipsRegistryPastDue(t *testing.T) {
	rec := events.NewRecorder()
	reg := &fakePastDueRegistry{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:       rec,
		Pool:            &fakePoolApplier{},
		Inbox:           idempotent.NewMemoryStore(),
		AddOnActivator:  &fakeAddOnActivator{},
		PastDueRegistry: reg,
	})
	in := events.TenantAddonSubscriptionPaymentFailed{
		EventID: "evt_pf_reg", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "tenant_pd", AddonPlanID: "tms", AddonCode: "tms",
	}
	if err := sub.HandleTenantAddonSubscriptionPaymentFailed(context.Background(), in); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(reg.pastDue) != 1 {
		t.Fatalf("MarkPastDue calls = %d; want 1", len(reg.pastDue))
	}
	if reg.pastDue[0].TenantID != "tenant_pd" || reg.pastDue[0].AddonCode != "tms" {
		t.Errorf("MarkPastDue arg = %+v", reg.pastDue[0])
	}
	if len(rec.RecordedByTopic("chora.tenancy.tenant_addon.payment_failed.v1")) != 1 {
		t.Errorf("downstream payment_failed must still be emitted")
	}
}

func TestPaymentsSubscriber_TenantAddon_PaymentRecovered_ClearsRegistryPastDue(t *testing.T) {
	rec := events.NewRecorder()
	reg := &fakePastDueRegistry{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:       rec,
		Pool:            &fakePoolApplier{},
		Inbox:           idempotent.NewMemoryStore(),
		AddOnActivator:  &fakeAddOnActivator{},
		PastDueRegistry: reg,
	})
	in := events.TenantAddonPaymentRecovered{
		EventID: "evt_rec_reg", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "tenant_pd", AddonPlanID: "tms", AddonCode: "tms",
	}
	if err := sub.HandleTenantAddonPaymentRecovered(context.Background(), in); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(reg.recovered) != 1 {
		t.Fatalf("MarkRecovered calls = %d; want 1", len(reg.recovered))
	}
	if reg.recovered[0].TenantID != "tenant_pd" || reg.recovered[0].AddonCode != "tms" {
		t.Errorf("MarkRecovered arg = %+v", reg.recovered[0])
	}
}

// Registry mutation is best-effort — when the registry returns an
// error (e.g. tenant unknown) the handler still emits + acks so
// Pub/Sub doesn't loop.
func TestPaymentsSubscriber_TenantAddon_SubscriptionPaymentFailed_RegistryErr_StillEmits(t *testing.T) {
	rec := events.NewRecorder()
	reg := &fakePastDueRegistry{failNext: errors.New("subscription not found")}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:       rec,
		Pool:            &fakePoolApplier{},
		Inbox:           idempotent.NewMemoryStore(),
		AddOnActivator:  &fakeAddOnActivator{},
		PastDueRegistry: reg,
	})
	in := events.TenantAddonSubscriptionPaymentFailed{
		EventID: "evt_pf_err", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "tenant_pd", AddonPlanID: "tms", AddonCode: "tms",
	}
	if err := sub.HandleTenantAddonSubscriptionPaymentFailed(context.Background(), in); err != nil {
		t.Fatalf("Handle should ack despite registry err: %v", err)
	}
	if len(rec.RecordedByTopic("chora.tenancy.tenant_addon.payment_failed.v1")) != 1 {
		t.Errorf("downstream emit MUST still fire on best-effort registry failure")
	}
}

// -----------------------------------------------------------------------------
// CHO-1779 — SubscriptionScheduleReleased handler tests.
// -----------------------------------------------------------------------------

// fakeScheduledTierPromoter records PromoteScheduledTier calls and
// returns whatever kind/from/to the test stages.
type fakeScheduledTierPromoter struct {
	calls    []promoteCall
	kind     string
	fromTier string
	toTier   string
	failNext error
}

type promoteCall struct {
	TenantID  string
	AddonCode string
}

func (f *fakeScheduledTierPromoter) PromoteScheduledTier(tenantID, addonCode string) (string, string, string, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return "", "", "", err
	}
	f.calls = append(f.calls, promoteCall{TenantID: tenantID, AddonCode: addonCode})
	return f.kind, f.fromTier, f.toTier, nil
}

func TestPaymentsSubscriber_TenantAddon_ScheduleReleased_EmitsUpgradedAndPromotes(t *testing.T) {
	rec := events.NewRecorder()
	promoter := &fakeScheduledTierPromoter{kind: "upgraded", fromTier: "starter", toTier: "pro"}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:             rec,
		Pool:                  &fakePoolApplier{},
		Inbox:                 idempotent.NewMemoryStore(),
		AddOnActivator:        &fakeAddOnActivator{},
		ScheduledTierPromoter: promoter,
	})
	in := events.TenantAddonSubscriptionScheduleReleased{
		EventID:              "evt_sched_rel_1",
		PurchaseID:           "pur_sr1",
		AdminGCID:            "gcid_admin",
		TargetTenantID:       "tenant_sr1",
		AddonPlanID:          "tms",
		AddonCode:            "tms",
		ToTier:               "pro",
		StripeSubscriptionID: "sub_test_sr1",
		ReleasedAt:           time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonSubscriptionScheduleReleased(context.Background(), in); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(promoter.calls) != 1 {
		t.Fatalf("PromoteScheduledTier calls = %d; want 1", len(promoter.calls))
	}
	if promoter.calls[0].TenantID != "tenant_sr1" || promoter.calls[0].AddonCode != "tms" {
		t.Errorf("Promote arg = %+v", promoter.calls[0])
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.upgraded.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected upgraded outbound event, got %d", len(emitted))
	}
	payload := emitted[0].Payload
	if payload["from_tier"] != "starter" {
		t.Errorf("payload.from_tier = %v, want starter", payload["from_tier"])
	}
	if payload["to_tier"] != "pro" {
		t.Errorf("payload.to_tier = %v, want pro", payload["to_tier"])
	}
}

func TestPaymentsSubscriber_TenantAddon_ScheduleReleased_DowngradePath(t *testing.T) {
	rec := events.NewRecorder()
	promoter := &fakeScheduledTierPromoter{kind: "downgraded", fromTier: "pro", toTier: "starter"}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:             rec,
		Pool:                  &fakePoolApplier{},
		Inbox:                 idempotent.NewMemoryStore(),
		AddOnActivator:        &fakeAddOnActivator{},
		ScheduledTierPromoter: promoter,
	})
	in := events.TenantAddonSubscriptionScheduleReleased{
		EventID: "evt_sched_rel_dn", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "t", AddonPlanID: "tms", AddonCode: "tms", ToTier: "starter",
	}
	if err := sub.HandleTenantAddonSubscriptionScheduleReleased(context.Background(), in); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(rec.RecordedByTopic("chora.tenancy.tenant_addon.downgraded.v1")) != 1 {
		t.Errorf("expected downgraded topic for downgrade path")
	}
	if len(rec.RecordedByTopic("chora.tenancy.tenant_addon.upgraded.v1")) != 0 {
		t.Errorf("upgraded topic must NOT fire on downgrade path")
	}
}

func TestPaymentsSubscriber_TenantAddon_ScheduleReleased_Idempotent(t *testing.T) {
	// Duplicate Pub/Sub delivery → inbox dedup makes the second call a
	// no-op; PromoteScheduledTier is only invoked once even though the
	// registry's own PromoteScheduledTier is also idempotent.
	rec := events.NewRecorder()
	promoter := &fakeScheduledTierPromoter{kind: "upgraded", fromTier: "starter", toTier: "pro"}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:             rec,
		Pool:                  &fakePoolApplier{},
		Inbox:                 idempotent.NewMemoryStore(),
		AddOnActivator:        &fakeAddOnActivator{},
		ScheduledTierPromoter: promoter,
	})
	in := events.TenantAddonSubscriptionScheduleReleased{
		EventID: "evt_sched_rel_dup", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "t", AddonPlanID: "tms", AddonCode: "tms", ToTier: "pro",
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := sub.HandleTenantAddonSubscriptionScheduleReleased(ctx, in); err != nil {
			t.Fatalf("Dispatch #%d: %v", i, err)
		}
	}
	if len(promoter.calls) != 1 {
		t.Errorf("PromoteScheduledTier calls = %d; want 1 (inbox dedup)", len(promoter.calls))
	}
}

func TestPaymentsSubscriber_TenantAddon_ScheduleReleased_RegistryErr_StillEmits(t *testing.T) {
	// Best-effort: registry failure (sub not found in the in-memory
	// registry — e.g. tenancy restarted mid-flight) must not block the
	// downstream emit. The handler falls back to the event's ToTier for
	// the topic + payload defaults.
	rec := events.NewRecorder()
	promoter := &fakeScheduledTierPromoter{failNext: errors.New("subscription not found")}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:             rec,
		Pool:                  &fakePoolApplier{},
		Inbox:                 idempotent.NewMemoryStore(),
		AddOnActivator:        &fakeAddOnActivator{},
		ScheduledTierPromoter: promoter,
	})
	in := events.TenantAddonSubscriptionScheduleReleased{
		EventID: "evt_sched_rel_err", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "t", AddonPlanID: "tms", AddonCode: "tms", ToTier: "pro",
	}
	if err := sub.HandleTenantAddonSubscriptionScheduleReleased(context.Background(), in); err != nil {
		t.Fatalf("Handle should ack despite registry err: %v", err)
	}
	// Default fallback: upgraded topic when registry can't tell us
	// upgraded vs downgraded.
	if len(rec.RecordedByTopic("chora.tenancy.tenant_addon.upgraded.v1")) != 1 {
		t.Errorf("downstream upgraded emit MUST still fire on registry failure")
	}
}

// -----------------------------------------------------------------------------
// CHO-1783 — when a DeactivationAnchorer is wired, the cycle-anchor cancellation
// handler flips the registry directly to DEACTIVATED via the addon_code (no
// 30-day grace stack-up) and skips the legacy AddOnActivator.Deactivate path.
// -----------------------------------------------------------------------------

type anchorCall struct {
	TenantID  string
	AddonCode string
}

type fakeDeactivationAnchorer struct {
	calls    []anchorCall
	ok       bool
	failNext error
}

func (f *fakeDeactivationAnchorer) AnchorDeactivation(tenantID, addonCode string) (bool, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return false, err
	}
	f.calls = append(f.calls, anchorCall{TenantID: tenantID, AddonCode: addonCode})
	return f.ok, nil
}

func TestPaymentsSubscriber_TenantAddon_SubscriptionCancelled_UsesAnchorerWhenWired(t *testing.T) {
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	anchorer := &fakeDeactivationAnchorer{ok: true}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:            rec,
		Pool:                 &fakePoolApplier{},
		Inbox:                idempotent.NewMemoryStore(),
		AddOnActivator:       act,
		DeactivationAnchorer: anchorer,
	})
	in := events.TenantAddonSubscriptionCancelled{
		EventID:              "evt_sub_cancelled_anchor_1",
		PurchaseID:           "pur_a1",
		AdminGCID:            "gcid_admin",
		TargetTenantID:       "tenant_a1",
		AddonPlanID:          "019e0000-0000-7000-8000-aaaaaaaaaaaa",
		AddonCode:            "tms",
		TierCode:             "starter",
		StripeSubscriptionID: "sub_test_a1",
		StripeCustomerID:     "cus_test_a1",
		CancelledAt:          time.Now().UTC(),
	}
	if err := sub.HandleTenantAddonSubscriptionCancelled(context.Background(), in); err != nil {
		t.Fatalf("HandleTenantAddonSubscriptionCancelled: %v", err)
	}
	// AnchorDeactivation was the path taken — not the legacy Deactivate.
	if len(anchorer.calls) != 1 {
		t.Fatalf("AnchorDeactivation calls = %d; want 1", len(anchorer.calls))
	}
	if anchorer.calls[0].TenantID != "tenant_a1" || anchorer.calls[0].AddonCode != "tms" {
		t.Errorf("anchor arg = %+v; want {tenant_a1, tms}", anchorer.calls[0])
	}
	if len(act.deactivates) != 0 {
		t.Errorf("legacy Deactivate must NOT fire when anchorer wired; got %d calls", len(act.deactivates))
	}
	emitted := rec.RecordedByTopic("chora.tenancy.tenant_addon.deactivated.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected deactivated outbound event, got %d", len(emitted))
	}
}

func TestPaymentsSubscriber_TenantAddon_SubscriptionCancelled_FallsBackToActivatorWhenAnchorerNil(t *testing.T) {
	// Backwards-compat sanity: with no DeactivationAnchorer, the legacy
	// path is preserved so the test suite + existing wiring keep
	// working until the cmd/server wiring is also flipped.
	rec := events.NewRecorder()
	act := &fakeAddOnActivator{}
	sub := events.NewPaymentsSubscriber(events.PaymentsSubscriberDeps{
		Publisher:      rec,
		Pool:           &fakePoolApplier{},
		Inbox:          idempotent.NewMemoryStore(),
		AddOnActivator: act,
	})
	in := events.TenantAddonSubscriptionCancelled{
		EventID: "evt_sub_cancelled_fallback", PurchaseID: "p", AdminGCID: "g",
		TargetTenantID: "t", AddonPlanID: "tms", AddonCode: "tms",
	}
	if err := sub.HandleTenantAddonSubscriptionCancelled(context.Background(), in); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(act.deactivates) != 1 {
		t.Errorf("legacy Deactivate MUST fire when anchorer nil; got %d", len(act.deactivates))
	}
}
