package familiar_egg_sweeper_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/familiar_egg_sweeper"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

func TestSweeper_Run_EnabledExitsOnContextCancel(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-r", TenantID: "t-r", PurchaserGCID: "g-r", EggSKU: "egg.standard.v1",
		AmountCents: 999, Currency: "SGD",
		StripeSessionID: "cs_r", StripeCheckoutURL: "https://x",
		Now: now.AddDate(0, 0, -90), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	_ = p.MarkPaid(now.AddDate(0, 0, -89), "", "", 999)
	store := &stubStore{candidates: []*familiareag.Purchase{p}}
	rec := events.NewRecorder()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec,
		PollInterval: 50 * time.Millisecond,
		Now:          func() time.Time { return now },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := sw.Run(ctx); err != nil {
		t.Errorf("Run: %v", err)
	}
	// Initial sweep should have processed the candidate.
	if len(store.updateCalls) < 1 {
		t.Errorf("expected at least 1 update; got %d", len(store.updateCalls))
	}
}

func TestSweeper_Run_RequiresPublisher(t *testing.T) {
	t.Parallel()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: &stubStore{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sw.Run(ctx); err == nil {
		t.Errorf("expected publisher-required error")
	}
}
