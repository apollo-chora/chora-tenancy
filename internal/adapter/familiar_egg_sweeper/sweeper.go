// Package familiar_egg_sweeper is the Iter G.3 stub of the hard-expiry
// sweeper (ADR-149 §"Unhatched egg expiry").
//
// Final M14 wiring will:
//
//   - Run every 6h as a goroutine
//   - SELECT rows in state IN ('paid','provisioned') AND hard_expiry_at < now()
//     AND provisioned_at IS NULL (no Familiar hatched)
//   - Transition each row to 'expired' via Purchase.MarkExpired
//   - Issue a mana_credit refund via the TenantManaPool plumbing
//     (handler-side; reuses adapter/manapool.Stripe + Publisher)
//   - Publish chora.tenancy.familiar_egg.refunded.v1 with
//     credit_only=true + reason=hard_expiry_unhatched
//
// Iter G.3 ships the function + feature-flag wiring (off by default) +
// the SQL-level pg.FindHardExpiryCandidates port. Full mana-credit
// crediting + 6h goroutine loop is Iter G.6.
//
// TODO(iter_g.6): wire the Run() goroutine + mana-credit refund + 6h
// poll interval per ADR-149 expiry-cycle.
package familiar_egg_sweeper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	familiareggpub "github.com/apollo-chora/chora-tenancy/internal/adapter/pubsub"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

// PurchaseStore is the persistence port the sweeper depends on.
type PurchaseStore interface {
	FindHardExpiryCandidates(ctx context.Context, before time.Time, limit int) ([]*familiareag.Purchase, error)
	UpdateState(ctx context.Context, p *familiareag.Purchase) error
}

// ManaCreditIssuer is the port the sweeper uses to issue mana-credit
// refunds. Iter G.3 ships a no-op stub; Iter G.6 wires the real
// TenantManaPool plumbing via internal/adapter/manapool.
type ManaCreditIssuer interface {
	IssueRefundCredit(ctx context.Context, tenantID, gcid string, amountCents int64, purchaseID string) error
}

// NoopManaCreditIssuer is the Iter G.3 stub.
type NoopManaCreditIssuer struct{}

// IssueRefundCredit satisfies ManaCreditIssuer; logs + returns nil.
func (NoopManaCreditIssuer) IssueRefundCredit(_ context.Context, tenantID, gcid string, amountCents int64, purchaseID string) error {
	log.Printf("familiar_egg_sweeper: NOOP IssueRefundCredit tenant=%s gcid=%s amount=%d purchase=%s (Iter G.6 will wire)", tenantID, gcid, amountCents, purchaseID)
	return nil
}

// Config wires the sweeper.
type Config struct {
	Enabled      bool
	Store        PurchaseStore
	Publisher    familiareggpub.EggPublisher
	ManaCredits  ManaCreditIssuer
	PollInterval time.Duration
	BatchSize    int
	Now          func() time.Time
}

// Sweeper expires unhatched egg purchases past hard_expiry_at.
type Sweeper struct {
	cfg Config
}

// New constructs a Sweeper. Defaults: PollInterval=6h, BatchSize=100,
// Now=time.Now().UTC, ManaCredits=NoopManaCreditIssuer.
func New(cfg Config) *Sweeper {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 6 * time.Hour
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.ManaCredits == nil {
		cfg.ManaCredits = NoopManaCreditIssuer{}
	}
	return &Sweeper{cfg: cfg}
}

// Run is the long-running loop. Returns when ctx is canceled.
//
// Iter G.3: if Enabled=false this is a fast no-op (skip the ticker; just
// wait for ctx.Done). The feature-flag-off default keeps the binary's
// concurrency footprint identical to pre-Iter G.3.
//
// TODO(iter_g.6): batch retries + backoff + Cloud Trace span per sweep.
func (s *Sweeper) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		log.Printf("familiar_egg_sweeper: disabled (ENABLE_EGG_EXPIRY_SWEEPER=false); skipping loop")
		<-ctx.Done()
		return nil
	}
	if s.cfg.Store == nil {
		return errors.New("familiar_egg_sweeper: Store required")
	}
	if s.cfg.Publisher == nil {
		return errors.New("familiar_egg_sweeper: Publisher required")
	}
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	// Sweep once immediately so test environments don't wait 6h.
	if _, err := s.SweepOnce(ctx); err != nil {
		log.Printf("familiar_egg_sweeper: initial sweep error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.SweepOnce(ctx); err != nil {
				log.Printf("familiar_egg_sweeper: sweep error: %v", err)
			}
		}
	}
}

// SweepOnce runs a single sweep — exported for tests + final-drain calls
// on shutdown.
func (s *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	if s.cfg.Store == nil || s.cfg.Publisher == nil {
		return 0, fmt.Errorf("familiar_egg_sweeper: not configured")
	}
	rows, err := s.cfg.Store.FindHardExpiryCandidates(ctx, s.cfg.Now(), s.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("familiar_egg_sweeper: find candidates: %w", err)
	}
	processed := 0
	for _, p := range rows {
		if err := p.MarkExpired(s.cfg.Now(), p.AmountCentsPaid); err != nil {
			log.Printf("familiar_egg_sweeper: MarkExpired %s: %v", p.PurchaseID, err)
			continue
		}
		if err := s.cfg.Store.UpdateState(ctx, p); err != nil {
			log.Printf("familiar_egg_sweeper: UpdateState %s: %v", p.PurchaseID, err)
			continue
		}
		if err := s.cfg.ManaCredits.IssueRefundCredit(ctx, p.TenantID, p.PurchaserGCID, p.AmountCentsPaid, p.PurchaseID); err != nil {
			log.Printf("familiar_egg_sweeper: IssueRefundCredit %s: %v", p.PurchaseID, err)
			continue
		}
		hdr := events.Header{TenantID: p.TenantID, GCID: p.PurchaserGCID}
		if _, err := familiareggpub.EmitRefunded(s.cfg.Publisher, hdr, p); err != nil {
			log.Printf("familiar_egg_sweeper: EmitRefunded %s: %v", p.PurchaseID, err)
			continue
		}
		processed++
	}
	return processed, nil
}
