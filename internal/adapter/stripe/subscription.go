// Package stripestub — Stripe Subscription Schedule adapter.
//
// Models the Stripe Subscription Schedule API surface (phases + phase
// transitions). The H+ tier-change preview + change-tier flow consumes
// this adapter:
//
//   - Upgrades — current phase replaced immediately (1 phase total).
//   - Downgrades — current phase + next phase (defer to cycle end). Stripe
//     Subscription Schedule with `phases[1].start_date = cycle_end` is the
//     production wire; the stub mirrors the same shape with mock IDs.
//   - Preview-only — returns the same payload but never persists; the
//     ScheduleID is left empty so the caller can detect preview mode.
//
// Per CLAUDE.md §6 (no inline config) Stripe URL + key flow from env vars.
// Real wiring (M14) hits
//
//	POST /v1/subscription_schedules { phases: [{...}, {...}] }
//
// with `Idempotency-Key: chora.tenancy.schedule.<tenant>.<subscription>`.
package stripestub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SubscriptionScheduleInput captures the fields Stripe's
// subscription_schedules.create endpoint consumes.
type SubscriptionScheduleInput struct {
	TenantID          string    // chora tenant_id (idempotency key prefix)
	CustomerID        string    // cus_*
	SubscriptionID    string    // sub_*
	FromPriceID       string    // price_*
	ToPriceID         string    // price_*
	ProrationBehavior string    // "create_prorations" | "none"
	DeferToCycleEnd   bool      // true = phase 1 starts at CycleEndAt
	CycleEndAt        time.Time // ignored unless DeferToCycleEnd=true
}

// SchedulePhase mirrors the Stripe phase object.
type SchedulePhase struct {
	PriceID   string
	StartDate time.Time
	EndDate   *time.Time
}

// SubscriptionScheduleResult carries the Stripe Schedule object.
type SubscriptionScheduleResult struct {
	ScheduleID string // empty in preview-only mode
	Phases     []SchedulePhase
	Status     string // "active" | "released" | "canceled"
}

// stripe schedule store — keyed by (tenant, subscription) for idempotency.
type scheduleStore struct {
	mu  sync.Mutex
	by  map[string]*SubscriptionScheduleResult // key = tenant|subscription
	ids map[string]bool                        // schedule_id → exists
}

func (c *Client) initScheduleStore() *scheduleStore {
	c.muSched.Lock()
	defer c.muSched.Unlock()
	if c.schedules == nil {
		c.schedules = &scheduleStore{
			by:  make(map[string]*SubscriptionScheduleResult),
			ids: make(map[string]bool),
		}
	}
	return c.schedules
}

// CreateSubscriptionSchedule creates (or returns the cached) Stripe
// Subscription Schedule for the supplied tenant + subscription. Idempotent on
// (tenant, subscription).
func (c *Client) CreateSubscriptionSchedule(ctx context.Context, in SubscriptionScheduleInput) (*SubscriptionScheduleResult, error) {
	if err := validateScheduleInput(c, in); err != nil {
		return nil, err
	}
	store := c.initScheduleStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	key := in.TenantID + "|" + in.SubscriptionID
	if cached, ok := store.by[key]; ok {
		return cached, nil
	}
	now := time.Now().UTC()
	phases := buildPhases(in, now)
	n := c.seq.Add(1)
	id := fmt.Sprintf("sched_%d", n)
	res := &SubscriptionScheduleResult{
		ScheduleID: id,
		Phases:     phases,
		Status:     "active",
	}
	store.by[key] = res
	store.ids[id] = true
	return res, nil
}

// PreviewSubscriptionScheduleChange returns the schedule shape WITHOUT
// persisting — the H+ preview-tier-change endpoint uses this to render
// the proration / next-invoice preview.
func (c *Client) PreviewSubscriptionScheduleChange(ctx context.Context, in SubscriptionScheduleInput) (*SubscriptionScheduleResult, error) {
	if err := validateScheduleInput(c, in); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	phases := buildPhases(in, now)
	return &SubscriptionScheduleResult{
		ScheduleID: "", // preview-only; no persisted id
		Phases:     phases,
		Status:     "preview",
	}, nil
}

// CancelSubscriptionSchedule removes a stored schedule. Returns true if it
// was actually cancelled, false if unknown / already cancelled.
func (c *Client) CancelSubscriptionSchedule(ctx context.Context, scheduleID string) (bool, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return false, fmt.Errorf("%w: schedule_id required", ErrInvalidArgument)
	}
	store := c.initScheduleStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.ids[scheduleID] {
		return false, nil
	}
	delete(store.ids, scheduleID)
	for k, v := range store.by {
		if v != nil && v.ScheduleID == scheduleID {
			delete(store.by, k)
		}
	}
	return true, nil
}

// validateScheduleInput enforces the field rules.
func validateScheduleInput(c *Client, in SubscriptionScheduleInput) error {
	if strings.TrimSpace(c.cfg.APIURL) == "" {
		return ErrAPIURLMissing
	}
	if strings.TrimSpace(in.TenantID) == "" {
		return fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.CustomerID) == "" {
		return fmt.Errorf("%w: customer_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.SubscriptionID) == "" {
		return fmt.Errorf("%w: subscription_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.FromPriceID) == "" {
		return fmt.Errorf("%w: from_price_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.ToPriceID) == "" {
		return fmt.Errorf("%w: to_price_id required", ErrInvalidArgument)
	}
	// CHO-1786 — accept always_invoice so the SubscriptionSchedule path
	// can pass through the FE's "Immediately" intent without 422.
	if in.ProrationBehavior != "" &&
		in.ProrationBehavior != "create_prorations" &&
		in.ProrationBehavior != "none" &&
		in.ProrationBehavior != "always_invoice" {
		return fmt.Errorf("%w: unknown proration_behavior %q", ErrInvalidArgument, in.ProrationBehavior)
	}
	return nil
}

func buildPhases(in SubscriptionScheduleInput, now time.Time) []SchedulePhase {
	if !in.DeferToCycleEnd {
		// Immediate transition: 1 phase = the new tier.
		return []SchedulePhase{
			{PriceID: in.ToPriceID, StartDate: now},
		}
	}
	// Deferred: 2 phases, current + next (next starts at CycleEndAt).
	cycleEnd := in.CycleEndAt
	if cycleEnd.IsZero() {
		cycleEnd = now.AddDate(0, 0, 30)
	}
	return []SchedulePhase{
		{PriceID: in.FromPriceID, StartDate: now, EndDate: &cycleEnd},
		{PriceID: in.ToPriceID, StartDate: cycleEnd},
	}
}

// ErrScheduleNotFound is returned for misses on Cancel.
var ErrScheduleNotFound = errors.New("subscription schedule not found")
