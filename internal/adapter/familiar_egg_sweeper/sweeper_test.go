package familiar_egg_sweeper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiar_egg_sweeper"
	familiareggpub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pubsub"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

type stubStore struct {
	candidates  []*familiareag.Purchase
	candErr     error
	updateCalls []*familiareag.Purchase
	updateErr   error
}

func (s *stubStore) FindHardExpiryCandidates(_ context.Context, _ time.Time, _ int) ([]*familiareag.Purchase, error) {
	if s.candErr != nil {
		return nil, s.candErr
	}
	return s.candidates, nil
}
func (s *stubStore) UpdateState(_ context.Context, p *familiareag.Purchase) error {
	s.updateCalls = append(s.updateCalls, p)
	return s.updateErr
}

type stubMana struct {
	calls []string
	err   error
}

func (s *stubMana) IssueRefundCredit(_ context.Context, _, _ string, _ int64, purchaseID string) error {
	s.calls = append(s.calls, purchaseID)
	return s.err
}

func TestSweeper_Disabled_ReturnsImmediatelyOnCancel(t *testing.T) {
	t.Parallel()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sw.Run(ctx); err != nil {
		t.Errorf("Run: %v", err)
	}
}

func TestSweeper_Enabled_RequiresStoreAndPublisher(t *testing.T) {
	t.Parallel()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{Enabled: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sw.Run(ctx); err == nil {
		t.Errorf("expected Store-required error")
	}
}

func TestSweeper_SweepOnce_TransitionsAndPublishes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID:        "01970000-0000-7000-8000-aaaaaaaaaaaa",
		TenantID:          "tenant-1",
		PurchaserGCID:     "gcid-1",
		EggSKU:            "egg.standard.v1",
		AmountCents:       999,
		Currency:          "SGD",
		StripeSessionID:   "cs_x",
		StripeCheckoutURL: "https://stripe.example/cs_x",
		Now:               now.AddDate(0, 0, -90),
		SoftExpiryDays:    30,
		HardExpiryDays:    60,
	})
	_ = p.MarkPaid(now.AddDate(0, 0, -89), "pi_x", "ch_x", 999)
	store := &stubStore{candidates: []*familiareag.Purchase{p}}
	rec := events.NewRecorder()
	mana := &stubMana{}
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec, ManaCredits: mana,
		Now: func() time.Time { return now },
	})
	n, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("processed: got %d, want 1", n)
	}
	if len(store.updateCalls) != 1 {
		t.Errorf("update calls: got %d", len(store.updateCalls))
	}
	if store.updateCalls[0].State != familiareag.StateExpired {
		t.Errorf("state: got %q", store.updateCalls[0].State)
	}
	if len(mana.calls) != 1 {
		t.Errorf("mana credit calls: got %d", len(mana.calls))
	}
	if len(rec.RecordedByTopic(familiareggpub.TopicRefunded)) != 1 {
		t.Errorf("refunded event missing")
	}
}

func TestSweeper_SweepOnce_NoCandidatesReturnsZero(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	rec := events.NewRecorder()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec,
	})
	n, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("processed: got %d", n)
	}
}

func TestSweeper_SweepOnce_StoreErrorBubblesUp(t *testing.T) {
	t.Parallel()
	store := &stubStore{candErr: errors.New("db down")}
	rec := events.NewRecorder()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec,
	})
	if _, err := sw.SweepOnce(context.Background()); err == nil {
		t.Errorf("expected error")
	}
}

func TestSweeper_SweepOnce_RequiresConfig(t *testing.T) {
	t.Parallel()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{Enabled: true})
	if _, err := sw.SweepOnce(context.Background()); err == nil {
		t.Errorf("expected config error")
	}
}

func TestSweeper_SweepOnce_UpdateErrorContinues(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	p := familiareag.NewPurchase(familiareag.NewPurchaseInput{
		PurchaseID: "p-1", TenantID: "t-1", PurchaserGCID: "g-1", EggSKU: "egg.standard.v1",
		AmountCents: 999, Currency: "SGD",
		StripeSessionID: "cs_y", StripeCheckoutURL: "https://x",
		Now: now.AddDate(0, 0, -90), SoftExpiryDays: 30, HardExpiryDays: 60,
	})
	_ = p.MarkPaid(now.AddDate(0, 0, -89), "", "", 999)
	store := &stubStore{candidates: []*familiareag.Purchase{p}, updateErr: errors.New("update fail")}
	rec := events.NewRecorder()
	sw := familiar_egg_sweeper.New(familiar_egg_sweeper.Config{
		Enabled: true, Store: store, Publisher: rec,
		Now: func() time.Time { return now },
	})
	n, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 0 {
		t.Errorf("processed: %d (want 0 because UpdateState fails)", n)
	}
}

func TestNoopManaCreditIssuer_AlwaysSucceeds(t *testing.T) {
	t.Parallel()
	n := familiar_egg_sweeper.NoopManaCreditIssuer{}
	if err := n.IssueRefundCredit(context.Background(), "t", "g", 100, "p"); err != nil {
		t.Errorf("err: %v", err)
	}
}
