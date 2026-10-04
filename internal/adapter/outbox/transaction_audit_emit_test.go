package outbox_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	governancev1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/governance/v1"

	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// captureStore records Insert'd rows for assertion; other Store methods no-op.
type captureStore struct {
	rows []outbox.Row
	err  error
}

func (c *captureStore) Insert(_ context.Context, r outbox.Row) error {
	if c.err != nil {
		return c.err
	}
	c.rows = append(c.rows, r)
	return nil
}
func (c *captureStore) FetchPending(context.Context, int) ([]outbox.Row, error) { return nil, nil }
func (c *captureStore) MarkPublished(context.Context, string) error             { return nil }
func (c *captureStore) MarkFailed(context.Context, string, string) error        { return nil }
func (c *captureStore) Deadletter(context.Context, string, string, int) error   { return nil }

func TestEmitCrossTenantViewed(t *testing.T) {
	store := &captureStore{}
	at := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	emitter := outbox.NewTransactionAuditEmitter(store).WithClock(func() time.Time { return at })

	from := at.Add(-90 * 24 * time.Hour)
	if err := emitter.EmitCrossTenantViewed(
		context.Background(),
		"gcid-operator",
		tl.AuditFilter{Kind: tl.KindPurchase, Status: tl.StatusCaptured, From: &from},
		[]string{"tenant-b"},
		at,
	); err != nil {
		t.Fatalf("EmitCrossTenantViewed: %v", err)
	}

	if len(store.rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(store.rows))
	}
	row := store.rows[0]
	if row.Topic != "chora.governance.audit.cross_tenant_payments_viewed.v1" {
		t.Errorf("topic = %q", row.Topic)
	}
	// The outbox ROW tenant_id column is a uuid type → cross-tenant rows use
	// the nil UUID for storage scope (the "platform" sentinel only rides the
	// PUBLISHED envelope, asserted below). Using "platform" on the column was
	// the 22P02 bug that aborted master span-all reads.
	if row.TenantID != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("tenant_id column = %q; want nil-UUID storage scope", row.TenantID)
	}
	if row.GCID != "gcid-operator" {
		t.Errorf("gcid = %q", row.GCID)
	}
	if row.IdempotencyKey == "" || row.IdempotencyKey != row.ID {
		t.Errorf("idempotency_key/id mismatch: %q / %q", row.IdempotencyKey, row.ID)
	}
	if row.Envelope["tenant_id"] != "platform" || row.Envelope["event_id"] != row.ID {
		t.Errorf("envelope wrong: %v", row.Envelope)
	}

	var msg governancev1.CrossTenantPaymentsViewed
	if err := proto.Unmarshal(row.Payload, &msg); err != nil {
		t.Fatalf("payload not valid CrossTenantPaymentsViewed proto: %v", err)
	}
	if msg.GetActorGcid() != "gcid-operator" || msg.GetActorRole() != "PLATFORM_OPERATOR" {
		t.Errorf("actor = %q/%q", msg.GetActorGcid(), msg.GetActorRole())
	}
	if got := msg.GetFilterContext().GetAggregateType(); got != tl.KindPurchase {
		t.Errorf("filter kind echo = %q; want %q", got, tl.KindPurchase)
	}
	if got := msg.GetFilterContext().GetState(); got != tl.StatusCaptured {
		t.Errorf("filter status echo = %q; want %q", got, tl.StatusCaptured)
	}
	if len(msg.GetTenantIdsInView()) != 1 || msg.GetTenantIdsInView()[0] != "tenant-b" {
		t.Errorf("tenant_ids_in_view = %v", msg.GetTenantIdsInView())
	}
	if !msg.GetOccurredAt().AsTime().Equal(at) {
		t.Errorf("occurred_at = %v; want %v", msg.GetOccurredAt().AsTime(), at)
	}
}

// TestEmitCrossTenantViewed_StampsTraceparent_WhenContextHasNone is the
// event-fabric (2026-07-01) Class-A regression guard for the ADR-165 audit.
// The audit is emitted BEFORE the cross-tenant span-all SQL from a gRPC
// context that may carry no W3C trace context. The envelope's traceparent is
// mandatory — without it the shared envelope Validate() rejects the row
// pre-publish ("envelope: traceparent is required") and the compliance audit
// deadletters instead of reaching Pub/Sub. The emitter mints a synthetic-root
// traceparent when the context has none.
func TestEmitCrossTenantViewed_StampsTraceparent_WhenContextHasNone(t *testing.T) {
	store := &captureStore{}
	at := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	emitter := outbox.NewTransactionAuditEmitter(store).WithClock(func() time.Time { return at })

	if err := emitter.EmitCrossTenantViewed(
		context.Background(), // no W3C trace context
		"gcid-operator",
		tl.AuditFilter{Kind: tl.KindPurchase, Status: tl.StatusCaptured},
		nil,
		at,
	); err != nil {
		t.Fatalf("EmitCrossTenantViewed: %v", err)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(store.rows))
	}
	if store.rows[0].Envelope["traceparent"] == "" {
		t.Error("envelope.traceparent empty; want a minted W3C traceparent (else the shared envelope Validate() rejects it and the audit deadletters)")
	}
}

// TestEmitCrossTenantViewed_PropagatesContextTraceparent guards the
// only-mint-when-empty rule: an inbound trace context is propagated unchanged
// onto the audit envelope (trace correlation of the operator read).
func TestEmitCrossTenantViewed_PropagatesContextTraceparent(t *testing.T) {
	store := &captureStore{}
	at := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	emitter := outbox.NewTransactionAuditEmitter(store).WithClock(func() time.Time { return at })

	const tp = "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01"
	ctx := tracing.WithTraceparent(context.Background(), tp)
	if err := emitter.EmitCrossTenantViewed(
		ctx, "gcid-operator",
		tl.AuditFilter{Kind: tl.KindPurchase, Status: tl.StatusCaptured},
		nil, at,
	); err != nil {
		t.Fatalf("EmitCrossTenantViewed: %v", err)
	}
	if got := store.rows[0].Envelope["traceparent"]; got != tp {
		t.Errorf("envelope.traceparent = %q; want propagated %q", got, tp)
	}
}
