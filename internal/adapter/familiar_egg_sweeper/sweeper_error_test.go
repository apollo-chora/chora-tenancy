package familiar_egg_sweeper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/familiar_egg_sweeper"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

type failingPublisher struct{}

func (failingPublisher) Publish(string, events.Header, map[string]interface{}) events.PublishedEvent {
	return events.PublishedEvent{}
}
func (failingPublisher) PublishWithError(string, events.Header, map[string]interface{}) (events.PublishedEvent, error) {
	return events.PublishedEvent{}, errors.New("publish boom")
}

func TestSweeper_SweepOnce_ManaCreditErrorContinues(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-mc", TenantID: "t", PurchaserGCID: "g", EggSKU: "egg.standard.v1",
		AmountCents: 999, Currency: "SGD",
		StripeSessionID: "cs_mc", StripeCheckoutURL: "https://x",
		Now: now.AddDate(0, 0, -90), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	_ = p.MarkPaid(now.AddDate(0, 0, -89), "", "", 999)
	store := &stubStore{candidates: []*familiareag.Purchase{p}}
	mana := &stubMana{err: errors.New("mana boom")}
	rec := events.NewRecorder()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec, ManaCredits: mana,
		Now: func() time.Time { return now },
	})
	n, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 0 {
		t.Errorf("processed: got %d (want 0)", n)
	}
}

func TestSweeper_SweepOnce_PublishErrorContinues(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-pub", TenantID: "t", PurchaserGCID: "g", EggSKU: "egg.standard.v1",
		AmountCents: 999, Currency: "SGD",
		StripeSessionID: "cs_pub", StripeCheckoutURL: "https://x",
		Now: now.AddDate(0, 0, -90), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	_ = p.MarkPaid(now.AddDate(0, 0, -89), "", "", 999)
	store := &stubStore{candidates: []*familiareag.Purchase{p}}
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: failingPublisher{},
		Now: func() time.Time { return now },
	})
	n, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 0 {
		t.Errorf("processed: got %d (want 0 since publish failed)", n)
	}
}
