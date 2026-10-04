// stripe_webhook_event_repository_test.go — unit tests for the pgx
// stripe_webhook_events repo (Iter G.4 PROD-C, ADR-149).
//
// Stubs QueryRunner to avoid a live Cloud SQL dependency. Verifies the
// SQL emit + duplicate-key detection (the UNIQUE constraint on event_id
// is the heart of the de-dup contract).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

func TestStripeWebhookEventRepository_Insert_NewRow(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	newRow, err := r.Insert(context.Background(), "evt_test_1", "checkout.session.completed")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !newRow {
		t.Errorf("expected newRow=true for unique insert")
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(q.execCalls))
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "INSERT INTO stripe_webhook_events") {
		t.Errorf("expected INSERT INTO stripe_webhook_events; got %q", got)
	}
}

func TestStripeWebhookEventRepository_Insert_Duplicate_ReturnsFalse(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		execErr: errors.New(`ERROR: duplicate key value violates unique constraint "stripe_webhook_events_pkey" (SQLSTATE 23505)`),
	}
	r := pg.NewStripeWebhookEventRepository(q)
	newRow, err := r.Insert(context.Background(), "evt_dup_1", "charge.failed")
	if err != nil {
		t.Fatalf("Insert: unexpected error: %v", err)
	}
	if newRow {
		t.Errorf("expected newRow=false on duplicate event_id")
	}
}

func TestStripeWebhookEventRepository_Insert_OtherErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{execErr: errors.New("connection refused")}
	r := pg.NewStripeWebhookEventRepository(q)
	_, err := r.Insert(context.Background(), "evt_x", "charge.refunded")
	if err == nil {
		t.Fatalf("expected error propagation for non-23505 failure")
	}
}

func TestStripeWebhookEventRepository_Insert_EmptyEventID(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	_, err := r.Insert(context.Background(), "", "checkout.session.completed")
	if err == nil {
		t.Fatalf("expected error for empty event_id")
	}
}

func TestStripeWebhookEventRepository_MarkSeen_FirstSeenReturnsFalse(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	// MarkSeen wraps Insert + always reports back the alreadySeen bool.
	alreadySeen, err := r.MarkSeen(context.Background(), "evt_seen_1", time.Now())
	if err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}
	if alreadySeen {
		t.Errorf("expected alreadySeen=false on first sighting")
	}
}

func TestStripeWebhookEventRepository_MarkSeen_DuplicateReportsAlreadySeen(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		execErr: errors.New(`ERROR: duplicate key value violates unique constraint "stripe_webhook_events_pkey" (SQLSTATE 23505)`),
	}
	r := pg.NewStripeWebhookEventRepository(q)
	alreadySeen, err := r.MarkSeen(context.Background(), "evt_dup_2", time.Now())
	if err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}
	if !alreadySeen {
		t.Errorf("expected alreadySeen=true on duplicate")
	}
}

func TestStripeWebhookEventRepository_MarkProcessed_EmitsUpdate(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkProcessed(context.Background(), "evt_done_1"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "UPDATE stripe_webhook_events") {
		t.Errorf("expected UPDATE stripe_webhook_events; got %q", got)
	}
	if !strings.Contains(got, "processed_at") {
		t.Errorf("expected processed_at touched; got %q", got)
	}
}

func TestStripeWebhookEventRepository_MarkFailed_EmitsUpdate(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkFailed(context.Background(), "evt_fail_1", "downstream timeout"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	got := q.execCalls[0].sql
	if !strings.Contains(got, "UPDATE stripe_webhook_events") {
		t.Errorf("expected UPDATE stripe_webhook_events; got %q", got)
	}
	if !strings.Contains(got, "processing_error") {
		t.Errorf("expected processing_error touched; got %q", got)
	}
}

func TestStripeWebhookEventRepository_MarkProcessed_EmptyEventID(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkProcessed(context.Background(), ""); err == nil {
		t.Fatalf("expected error for empty event_id")
	}
}

func TestStripeWebhookEventRepository_MarkFailed_EmptyEventID(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkFailed(context.Background(), "", "x"); err == nil {
		t.Fatalf("expected error for empty event_id")
	}
}

func TestStripeWebhookEventRepository_MarkProcessed_PropagatesError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{execErr: errors.New("conn closed")}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkProcessed(context.Background(), "evt_x"); err == nil {
		t.Fatalf("expected error propagation")
	}
}

func TestStripeWebhookEventRepository_MarkFailed_PropagatesError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{execErr: errors.New("conn closed")}
	r := pg.NewStripeWebhookEventRepository(q)
	if err := r.MarkFailed(context.Background(), "evt_x", "downstream"); err == nil {
		t.Fatalf("expected error propagation")
	}
}

func TestStripeWebhookEventRepository_MarkSeen_PropagatesNonUniqueError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{execErr: errors.New("connection refused")}
	r := pg.NewStripeWebhookEventRepository(q)
	_, err := r.MarkSeen(context.Background(), "evt_x", time.Now())
	if err == nil {
		t.Fatalf("expected error propagation for non-23505 failure")
	}
}

func TestStripeWebhookEventRepository_MarkSeen_EmptyEventID(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewStripeWebhookEventRepository(q)
	_, err := r.MarkSeen(context.Background(), "", time.Now())
	if err == nil {
		t.Fatalf("expected error for empty event_id")
	}
}
