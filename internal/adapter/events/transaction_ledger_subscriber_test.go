package events

import (
	"context"
	"fmt"
	"testing"
	"time"

	cgcenv "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/idempotent"
	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	"github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/identity/v1"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type spend struct {
	e transactionledger.Entry
	d transactionledger.Detail
}

type fakeRepo struct {
	purchases []transactionledger.Entry
	spends    []spend
}

func (f *fakeRepo) UpsertPurchase(_ context.Context, e transactionledger.Entry) error {
	f.purchases = append(f.purchases, e)
	return nil
}

func (f *fakeRepo) AccumulateSpend(_ context.Context, e transactionledger.Entry, d transactionledger.Detail) error {
	f.spends = append(f.spends, spend{e, d})
	return nil
}

func newSub() (*TransactionLedgerSubscriber, *fakeRepo) {
	r := &fakeRepo{}
	return NewTransactionLedgerSubscriber(r, idempotent.NewMemoryStore()), r
}

func msgFor(t *testing.T, topic, eventID, tenant, gcid string, m proto.Message) *cgcpubsub.Message {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &cgcpubsub.Message{
		Topic:    topic,
		Envelope: cgcenv.Envelope{EventID: eventID, TenantID: tenant, GCID: gcid, OccurredAt: time.Now().UTC()},
		Payload:  b,
	}
}

var ctx = context.Background()
var ts = timestamppb.New(time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC))

func TestTxLedger_CoursePurchaseCaptured(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicCoursePurchaseCaptured, "e1", "ten1", "g1",
		&paymentsv1.CoursePurchasePaymentCaptured{
			PurchaseId: "pur1", LearnerGcid: "g1", AmountCentsPaid: 4999, Currency: "sgd", PaidAt: ts,
		})
	if err := sub.Handle(ctx, topicCoursePurchaseCaptured, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(repo.purchases) != 1 {
		t.Fatalf("expected 1 purchase, got %d", len(repo.purchases))
	}
	e := repo.purchases[0]
	if e.Kind != transactionledger.KindPurchase || e.SourceDomain != transactionledger.SourcePayments ||
		e.SourceRefID != "pur1" || e.LearnerGCID != "g1" || e.AmountMinor != 4999 ||
		e.Currency != "sgd" || e.Status != transactionledger.StatusCaptured {
		t.Fatalf("unexpected entry: %+v", e)
	}
}

func TestTxLedger_Refund_FlipsStatus(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicCoursePurchaseRefunded, "e2", "ten1", "g1",
		&paymentsv1.CoursePurchaseRefunded{
			PurchaseId: "pur1", LearnerGcid: "g1", AmountCentsRefunded: 4999, Currency: "sgd", RefundedAt: ts,
		})
	if err := sub.Handle(ctx, topicCoursePurchaseRefunded, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if repo.purchases[0].Status != transactionledger.StatusRefunded {
		t.Fatalf("expected refunded, got %q", repo.purchases[0].Status)
	}
}

func TestTxLedger_UserManaTopUp_FiatPlusMana(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicUserManaTopUpCaptured, "e3", "ten1", "g1",
		&paymentsv1.UserManaTopUpPaymentCaptured{
			PurchaseId: "top1", LearnerGcid: "g1", AmountCentsPaid: 1000, Currency: "usd", ManaUnits: 5000, PaidAt: ts,
		})
	if err := sub.Handle(ctx, topicUserManaTopUpCaptured, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	e := repo.purchases[0]
	if e.Kind != transactionledger.KindManaTopup || e.AmountMinor != 1000 || e.ManaUnits != 5000 {
		t.Fatalf("mana topup should carry fiat + mana: %+v", e)
	}
}

func TestTxLedger_TenantAddon_TenantLevel(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicTenantAddonCaptured, "e4", "ten1", "",
		&paymentsv1.TenantAddonPurchasePaymentCaptured{
			PurchaseId: "ad1", AdminGcid: "admin1", AddonCode: "knowledge_graph",
			AmountCentsPaid: 9900, Currency: "sgd", PaidAt: ts,
		})
	if err := sub.Handle(ctx, topicTenantAddonCaptured, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	e := repo.purchases[0]
	if e.LearnerGCID != "" {
		t.Fatalf("tenant-level row must have empty learner_gcid, got %q", e.LearnerGCID)
	}
	if e.Metadata["admin_gcid"] != "admin1" {
		t.Fatalf("expected admin_gcid in metadata, got %+v", e.Metadata)
	}
}

func TestTxLedger_IdentityGrant_ProjectedButTopupSkipped(t *testing.T) {
	// reason=topup is the payments dup → skipped.
	sub, repo := newSub()
	skip := msgFor(t, topicUserManaCredited, "e5", "ten1", "g1",
		&identityv1.UserManaCredited{
			EntryId: "ent1", Gcid: "g1", Units: 5000,
			Reason: identityv1.ManaReason_MANA_REASON_TOPUP, RecordedAt: ts,
		})
	if err := sub.Handle(ctx, topicUserManaCredited, skip); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(repo.purchases) != 0 {
		t.Fatalf("reason=topup must be skipped (payments dup), got %d", len(repo.purchases))
	}
	// reason=subscription_grant → projected as a mana-only grant.
	grant := msgFor(t, topicUserManaCredited, "e6", "ten1", "g1",
		&identityv1.UserManaCredited{
			EntryId: "ent2", Gcid: "g1", Units: 1200,
			Reason: identityv1.ManaReason_MANA_REASON_SUBSCRIPTION_GRANT, RecordedAt: ts,
		})
	if err := sub.Handle(ctx, topicUserManaCredited, grant); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(repo.purchases) != 1 {
		t.Fatalf("grant should project 1 row, got %d", len(repo.purchases))
	}
	e := repo.purchases[0]
	if e.Kind != transactionledger.KindManaTopup || e.SourceDomain != transactionledger.SourceIdentity ||
		e.ManaUnits != 1200 || e.SourceRefID != "ent2" || e.AmountMinor != 0 {
		t.Fatalf("unexpected grant entry: %+v", e)
	}
}

func TestTxLedger_TokenUsage_AccumulateSpendAndDetail(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicTokenUsageRecorded, "e7", "ten1", "g1",
		&observabilityv1.TokenUsageRecorded{
			TenantId: "ten1", Gcid: "g1", ModelId: "gemini-2.5",
			ManaUnits: 40, ActionCode: "question_generation", InvocationId: "inv1", RecordedAt: ts,
		})
	if err := sub.Handle(ctx, topicTokenUsageRecorded, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(repo.spends) != 1 {
		t.Fatalf("expected 1 spend, got %d", len(repo.spends))
	}
	sp := repo.spends[0]
	if sp.e.Kind != transactionledger.KindManaSpendDaily || sp.e.ManaUnits != -40 ||
		sp.e.SourceRefID != "2026-06-29" {
		t.Fatalf("spend entry wrong: %+v", sp.e)
	}
	if sp.d.ManaUnits != -40 || sp.d.ActionCode != "question_generation" || sp.d.Model != "gemini-2.5" {
		t.Fatalf("spend detail wrong: %+v", sp.d)
	}
}

func TestTxLedger_TokenUsage_ZeroManaSkipped(t *testing.T) {
	sub, repo := newSub()
	m := msgFor(t, topicTokenUsageRecorded, "e8", "ten1", "g1",
		&observabilityv1.TokenUsageRecorded{TenantId: "ten1", Gcid: "g1", ManaUnits: 0, RecordedAt: ts})
	if err := sub.Handle(ctx, topicTokenUsageRecorded, m); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(repo.spends) != 0 {
		t.Fatalf("zero-mana usage must be skipped, got %d", len(repo.spends))
	}
}

func TestTxLedger_Idempotent_SameEventID(t *testing.T) {
	sub, repo := newSub()
	build := func() *cgcpubsub.Message {
		return msgFor(t, topicCoursePurchaseCaptured, "dup", "ten1", "g1",
			&paymentsv1.CoursePurchasePaymentCaptured{PurchaseId: "p", LearnerGcid: "g1", AmountCentsPaid: 1, Currency: "sgd", PaidAt: ts})
	}
	for i := 0; i < 2; i++ {
		if err := sub.Handle(ctx, topicCoursePurchaseCaptured, build()); err != nil {
			t.Fatalf("handle #%d: %v", i, err)
		}
	}
	if len(repo.purchases) != 1 {
		t.Fatalf("inbox should dedup same event_id, got %d", len(repo.purchases))
	}
}

func TestTxLedger_Guards(t *testing.T) {
	sub, _ := newSub()
	// empty tenant_id
	if err := sub.Handle(ctx, topicCoursePurchaseCaptured,
		msgFor(t, topicCoursePurchaseCaptured, "e", "", "g1", &paymentsv1.CoursePurchasePaymentCaptured{PurchaseId: "p"})); err == nil {
		t.Fatal("empty tenant_id should error")
	}
	// empty event_id
	if err := sub.Handle(ctx, topicCoursePurchaseCaptured,
		msgFor(t, topicCoursePurchaseCaptured, "", "ten1", "g1", &paymentsv1.CoursePurchasePaymentCaptured{PurchaseId: "p"})); err == nil {
		t.Fatal("empty event_id should error")
	}
	// unhandled topic
	if err := sub.Handle(ctx, "chora.unknown.x.y.v1",
		&cgcpubsub.Message{Topic: "chora.unknown.x.y.v1", Envelope: cgcenv.Envelope{EventID: "e", TenantID: "ten1"}}); err == nil {
		t.Fatal("unhandled topic should error")
	}
	// nil message
	if err := sub.Handle(ctx, topicCoursePurchaseCaptured, nil); err == nil {
		t.Fatal("nil message should error")
	}
}

// TestTxLedger_AllPaymentsAggregates exercises every remaining payments
// capture/refund case + validates each maps to the right kind / ref / status /
// scope (tenant-level rows carry no learner_gcid).
func TestTxLedger_AllPaymentsAggregates(t *testing.T) {
	cases := []struct {
		topic       string
		msg         proto.Message
		wantKind    string
		wantRef     string
		wantStatus  string
		tenantLevel bool
	}{
		{topicApplicationPaymentCaptured, &paymentsv1.ApplicationPaymentPaymentCaptured{PurchaseId: "ap1", LearnerGcid: "g", AmountCentsPaid: 100, Currency: "sgd", PaidAt: ts}, transactionledger.KindPurchase, "ap1", transactionledger.StatusCaptured, false},
		{topicApplicationPaymentRefunded, &paymentsv1.ApplicationPaymentRefunded{PurchaseId: "ap1", LearnerGcid: "g", AmountCentsRefunded: 100, Currency: "sgd", RefundedAt: ts}, transactionledger.KindPurchase, "ap1", transactionledger.StatusRefunded, false},
		{topicFamiliarEggCaptured, &paymentsv1.CompanionEggPurchasePaymentCaptured{PurchaseId: "eg1", LearnerGcid: "g", AmountCentsPaid: 500, Currency: "sgd", PaidAt: ts}, transactionledger.KindPurchase, "eg1", transactionledger.StatusCaptured, false},
		{topicFamiliarEggRefunded, &paymentsv1.CompanionEggPurchaseRefunded{PurchaseId: "eg1", LearnerGcid: "g", AmountCentsRefunded: 500, Currency: "sgd", RefundedAt: ts}, transactionledger.KindPurchase, "eg1", transactionledger.StatusRefunded, false},
		{topicIdentityKycFeeCaptured, &paymentsv1.IdentityKycFeePaymentCaptured{PurchaseId: "ky1", LearnerGcid: "g", AmountCentsPaid: 300, Currency: "sgd", PaidAt: ts}, transactionledger.KindPurchase, "ky1", transactionledger.StatusCaptured, false},
		{topicIdentityKycFeeRefunded, &paymentsv1.IdentityKycFeeRefunded{PurchaseId: "ky1", LearnerGcid: "g", AmountCentsRefunded: 300, Currency: "sgd", RefundedAt: ts}, transactionledger.KindPurchase, "ky1", transactionledger.StatusRefunded, false},
		{topicUserSubscriptionCaptured, &paymentsv1.UserSubscriptionPaymentCaptured{PurchaseId: "su1", LearnerGcid: "g", AmountCentsPaid: 1500, Currency: "sgd", PaidAt: ts}, transactionledger.KindPurchase, "su1", transactionledger.StatusCaptured, false},
		{topicUserSubscriptionRefunded, &paymentsv1.UserSubscriptionRefunded{PurchaseId: "su1", LearnerGcid: "g", AmountCentsRefunded: 1500, Currency: "sgd", RefundedAt: ts}, transactionledger.KindPurchase, "su1", transactionledger.StatusRefunded, false},
		{topicTenantManaTopUpCaptured, &paymentsv1.TenantManaTopUpPaymentCaptured{PurchaseId: "tm1", AdminGcid: "ad", AmountCentsPaid: 2000, Currency: "sgd", ManaUnits: 9000, PaidAt: ts}, transactionledger.KindManaTopup, "tm1", transactionledger.StatusCaptured, true},
		{topicTenantManaTopUpRefunded, &paymentsv1.TenantManaTopUpRefunded{PurchaseId: "tm1", AdminGcid: "ad", AmountCentsRefunded: 2000, Currency: "sgd", RefundedAt: ts}, transactionledger.KindManaTopup, "tm1", transactionledger.StatusRefunded, true},
		{topicUserManaTopUpRefunded, &paymentsv1.UserManaTopUpRefunded{PurchaseId: "um1", LearnerGcid: "g", AmountCentsRefunded: 1000, Currency: "usd", RefundedAt: ts}, transactionledger.KindManaTopup, "um1", transactionledger.StatusRefunded, false},
	}
	for i, c := range cases {
		sub, repo := newSub()
		m := msgFor(t, c.topic, fmt.Sprintf("ev%d", i), "ten1", "g", c.msg)
		if err := sub.Handle(ctx, c.topic, m); err != nil {
			t.Fatalf("[%s] handle: %v", c.topic, err)
		}
		if len(repo.purchases) != 1 {
			t.Fatalf("[%s] expected 1 purchase, got %d", c.topic, len(repo.purchases))
		}
		e := repo.purchases[0]
		if e.Kind != c.wantKind || e.SourceRefID != c.wantRef || e.Status != c.wantStatus {
			t.Fatalf("[%s] got kind=%s ref=%s status=%s", c.topic, e.Kind, e.SourceRefID, e.Status)
		}
		if c.tenantLevel && e.LearnerGCID != "" {
			t.Fatalf("[%s] tenant-level row must have empty learner_gcid, got %q", c.topic, e.LearnerGCID)
		}
		if !c.tenantLevel && e.LearnerGCID == "" {
			t.Fatalf("[%s] learner-scoped row must carry learner_gcid", c.topic)
		}
	}
}

func TestTxLedger_SubscriptionName(t *testing.T) {
	sub, _ := newSub()
	if got := sub.SubscriptionNameForTopic(topicCoursePurchaseCaptured); got != "chora-tenancy-txledger-course_purchase-payment_captured" {
		t.Fatalf("subscription name: %q", got)
	}
	if got := sub.SubscriptionNameForTopic(topicTokenUsageRecorded); got != "chora-tenancy-txledger-token_usage-recorded" {
		t.Fatalf("subscription name: %q", got)
	}
	if len(sub.Topics()) != 18 {
		t.Fatalf("expected 18 topics, got %d", len(sub.Topics()))
	}
}
