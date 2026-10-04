// transaction_audit_emit.go — emits the IMDA-D1 cross-tenant access audit for
// the ADR-205 Transaction History MASTER (operator) span-all read path
// (B3 / CHO-1939). Mirrors services/chora-payments .../outbox/audit_emit.go:
// the same canonical governance event + topic is REUSED (ADR-205 D1 locks
// `chora.governance.audit.cross_tenant_payments_viewed.v1`), written as a
// pending row into chora_tenancy's outbox_events so the existing Dispatcher
// drains it to Pub/Sub. The chora-governance subscriber (B6 / CHO-1942) is
// already the consumer.
//
// The emit MUST run BEFORE the span-all SQL (fail-closed: if the durable
// outbox write fails, the gRPC layer aborts the read — no un-audited
// cross-tenant access). The outbox row's tenant_id slot carries the
// "platform" sentinel (envelope.proto convention for cross-tenant
// platform-level events), matching the payments precedent.
//
// The payload is binary protobuf (proto.Marshal) — identical wire shape to the
// payments emission, so the topic's Pub/Sub Schema Registry binary schema
// validates and B6 decodes it unchanged. We build the outbox.Row directly
// (NOT via Publisher) because Publisher enforces the chora.tenancy.* topic
// namespace and would reject a chora.governance.* topic.
package outbox

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	governancev1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/governance/v1"

	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"

	"github.com/5007-Capstone/chora/libs/chora-go-common/env"
)

// crossTenantViewedTopic is the canonical ADR-165 governance audit topic the
// transaction-history operator span-all reuses (ADR-205 D1).
const crossTenantViewedTopic = "chora.governance.audit.cross_tenant_payments_viewed.v1"

// operatorRole is the role string stamped on the audit (always the operator).
const operatorRole = "PLATFORM_OPERATOR"

// TransactionAuditEmitter writes cross-tenant-view audits via the outbox Store.
type TransactionAuditEmitter struct {
	store         Store
	now           func() time.Time
	sourceProject string
	sourceService string
}

// NewTransactionAuditEmitter wires the emitter to the shared outbox Store.
func NewTransactionAuditEmitter(store Store) *TransactionAuditEmitter {
	return &TransactionAuditEmitter{
		store:         store,
		now:           func() time.Time { return time.Now().UTC() },
		sourceProject: env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		sourceService: "chora-tenancy",
	}
}

// WithClock overrides the time source (tests).
func (e *TransactionAuditEmitter) WithClock(now func() time.Time) *TransactionAuditEmitter {
	e.now = now
	return e
}

// EmitCrossTenantViewed durably records that the operator (actorGCID) performed
// a cross-tenant transaction-history read under the given filter. tenantIDsInView
// is the franchisee set the read targeted: a single-element slice for a
// franchisee-narrowed read, or empty for an open span-all (the touched set is
// not yet known pre-SQL — the proto permits empty, and emitting BEFORE the SQL
// is the fail-closed contract). Satisfies the gRPC server's audit port
// structurally.
func (e *TransactionAuditEmitter) EmitCrossTenantViewed(
	ctx context.Context,
	actorGCID string,
	filter tl.AuditFilter,
	tenantIDsInView []string,
	occurredAt time.Time,
) error {
	if e == nil || e.store == nil {
		return fmt.Errorf("outbox: TransactionAuditEmitter not wired")
	}
	eventID := newUUIDv7()
	msg := &governancev1.CrossTenantPaymentsViewed{
		EventId:         eventID,
		ActorGcid:       actorGCID,
		ActorRole:       operatorRole,
		OccurredAt:      timestamppb.New(occurredAt.UTC()),
		FilterContext:   auditFilterToProto(filter),
		TenantIdsInView: tenantIDsInView,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal CrossTenantPaymentsViewed: %w", err)
	}

	now := e.now()

	// Event-fabric (2026-07-01) Class-A fix: this ADR-165 compliance audit is
	// emitted BEFORE the cross-tenant span-all SQL from a gRPC context that may
	// carry no W3C trace context. The envelope traceparent is mandatory —
	// without it the shared envelope Validate() rejects the row pre-publish
	// ("envelope: traceparent is required") and the audit deadletters instead
	// of reaching Pub/Sub. Mirror envelope.Build(): propagate an inbound
	// traceparent, mint a synthetic root only when absent. The audit content,
	// the emit ORDERING, and the before-SQL fail-closed semantics are unchanged
	// — this only populates the previously-missing envelope attribute.
	traceparent := tracing.TraceparentFromContext(ctx)
	if traceparent == "" {
		traceparent = tracing.EnsureTraceparent("")
	}

	row := Row{
		ID: eventID,
		// The outbox `tenant_id` COLUMN is a uuid type, so a cross-tenant
		// platform-level row must carry a valid UUID for its storage scope —
		// the nil UUID (the span-all sentinel, matching the export-jobs
		// convention). The PUBLISHED envelope tenant_id below keeps the
		// "platform" string sentinel (platformSentinel) that the governance
		// consumer expects, mirroring the payments cross-tenant audit. Using
		// "platform" here directly is the 22P02 bug that aborted master reads.
		TenantID:      platformTenantRowID,
		GCID:          actorGCID,
		AggregateType: "audit",
		AggregateID:   eventID,
		EventType:     "governance.audit.cross_tenant_payments_viewed",
		Topic:         crossTenantViewedTopic,
		Payload:       payload,
		Envelope: map[string]string{
			"event_id":        eventID,
			"idempotency_key": eventID,
			"tenant_id":       platformSentinel,
			"gcid":            actorGCID,
			"traceparent":     traceparent,
			"occurred_at":     now.Format(time.RFC3339Nano),
			"published_at":    now.Format(time.RFC3339Nano),
			"source_project":  e.sourceProject,
			"source_service":  e.sourceService,
			"schema_version":  "1",
		},
		IdempotencyKey: eventID,
		OccurredAt:     now,
	}
	if err := e.store.Insert(ctx, row); err != nil {
		return fmt.Errorf("outbox: insert cross-tenant audit: %w", err)
	}
	return nil
}

// platformSentinel is the cross-tenant platform-level events tenant_id slot
// for the PUBLISHED envelope (envelope.proto convention; matches the payments
// audit emission the governance consumer expects).
const platformSentinel = "platform"

// platformTenantRowID is the nil UUID used for the outbox ROW's tenant_id
// column (a uuid type) on cross-tenant platform-level events. The row needs a
// valid UUID for its storage scope; the published envelope keeps the
// "platform" string sentinel above. (Mirrors the span-all export-jobs nil-UUID
// convention; fixes the 22P02 invalid-uuid-"platform" insert that aborted the
// master span-all read.)
const platformTenantRowID = "00000000-0000-0000-0000-000000000000"

// auditFilterToProto echoes the read filter onto the reused governance
// PurchaseFilterContext. The transaction Kind maps onto AggregateType and the
// Status onto State (the reused event predates the unified ledger; the field
// names are payments-era but carry the transaction-history filter here).
func auditFilterToProto(f tl.AuditFilter) *governancev1.PurchaseFilterContext {
	pf := &governancev1.PurchaseFilterContext{
		AggregateType: f.Kind,
		State:         f.Status,
	}
	if f.From != nil {
		pf.From = timestamppb.New(f.From.UTC())
	}
	if f.To != nil {
		pf.To = timestamppb.New(f.To.UTC())
	}
	return pf
}
