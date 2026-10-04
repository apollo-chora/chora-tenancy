// stripe_webhook_event_repository.go — pgx-backed implementation of the
// Stripe webhook event idempotency repo (Iter G.4 PROD-C, ADR-149).
//
// SQL contract (per migrations/0008_stripe_webhook_events.sql):
//
//   - stripe_webhook_events has event_id as TEXT PRIMARY KEY. UNIQUE
//     constraint surfaces 23505 SQLSTATE on duplicate insert; this repo
//     swallows that as the "already-seen" signal.
//   - RLS is INTENTIONALLY DISABLED on this table — it is a global
//     operational table. Tenant context is in the event payload routed
//     to FamiliarEgg purchase rows AFTER de-dup.
//
// This file SATISFIES the http.WebhookIdemStore port at
// internal/adapter/http/familiar_egg_handlers.go via the MarkSeen
// method — the production wire-up in cmd/server/main.go injects this
// repo in place of the in-memory store.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StripeWebhookEventRepository is the pgx-backed stripe_webhook_events repo.
type StripeWebhookEventRepository struct {
	q QueryRunner
}

// NewStripeWebhookEventRepository constructs the production-wired repository.
func NewStripeWebhookEventRepository(q QueryRunner) *StripeWebhookEventRepository {
	return &StripeWebhookEventRepository{q: q}
}

// Insert attempts to record the (event_id, event_type) row. Returns:
//
//   - newRow=true  : first time the event_id is seen (row inserted).
//   - newRow=false : event_id already seen (duplicate; 23505 caught + swallowed).
//
// On any other SQL error, Insert returns the error verbatim.
//
// Implementation: strict INSERT (no ON CONFLICT) so that the 23505
// SQLSTATE on the event_id PRIMARY KEY is the de-dup signal. The
// approach matches tenant_repository.go's isUniqueViolation seam.
func (r *StripeWebhookEventRepository) Insert(ctx context.Context, eventID, eventType string) (bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return false, errors.New("pg.StripeWebhookEventRepository.Insert: event_id required")
	}
	const q = `
        INSERT INTO stripe_webhook_events (event_id, event_type, received_at)
        VALUES ($1, $2, now())
    `
	if err := r.q.Exec(ctx, q, eventID, eventType); err != nil {
		if isUniqueViolation(err, "stripe_webhook_events_pkey") ||
			isUniqueViolation(err, "event_id") ||
			isUniqueViolation(err, "stripe_webhook_events") {
			return false, nil
		}
		return false, fmt.Errorf("pg.StripeWebhookEventRepository.Insert: %w", err)
	}
	return true, nil
}

// MarkSeen satisfies the http.WebhookIdemStore port. Wraps Insert.
// Returns alreadySeen=true if the event_id was previously recorded;
// false if this call inserted the row.
//
// The seenAt parameter is accepted for port compatibility but the
// stripe_webhook_events table uses now() under the hood — the
// difference is < clock-skew and not relevant to dedup correctness.
func (r *StripeWebhookEventRepository) MarkSeen(ctx context.Context, eventID string, seenAt time.Time) (bool, error) {
	_ = seenAt
	newRow, err := r.Insert(ctx, eventID, "")
	if err != nil {
		return false, err
	}
	return !newRow, nil
}

// MarkProcessed sets processed_at = now() on the row. Idempotent — calling
// twice is harmless (second write is a no-op overwrite). Returns error
// only on driver/SQL failure (NOT for "row does not exist" — we accept
// the silent path so a delayed-arrival processed-ack can't bring the
// service down).
func (r *StripeWebhookEventRepository) MarkProcessed(ctx context.Context, eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("pg.StripeWebhookEventRepository.MarkProcessed: event_id required")
	}
	const q = `UPDATE stripe_webhook_events SET processed_at = now(), processing_error = NULL WHERE event_id = $1`
	if err := r.q.Exec(ctx, q, eventID); err != nil {
		return fmt.Errorf("pg.StripeWebhookEventRepository.MarkProcessed: %w", err)
	}
	return nil
}

// MarkFailed records processing_error on the row. Caller invokes this
// when downstream work (purchase update / outbox publish) fails so ops
// can spot stuck rows via the idx_stripe_webhook_events_unprocessed
// partial index.
func (r *StripeWebhookEventRepository) MarkFailed(ctx context.Context, eventID, errMsg string) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("pg.StripeWebhookEventRepository.MarkFailed: event_id required")
	}
	const q = `UPDATE stripe_webhook_events SET processing_error = $2 WHERE event_id = $1`
	if err := r.q.Exec(ctx, q, eventID, errMsg); err != nil {
		return fmt.Errorf("pg.StripeWebhookEventRepository.MarkFailed: %w", err)
	}
	return nil
}
