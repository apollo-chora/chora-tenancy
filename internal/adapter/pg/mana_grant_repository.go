// mana_grant_repository.go: the tenant-to-member mana grant, persisted
// atomically (CHO-2414 follow-up).
//
// WHY IT EXISTS
// The grant handler used to do three things in sequence, none of them
// together:
//
//	deps.PoolRepo.Save(p)            // durable, its own transaction
//	deps.ManaAllocations.put(entry)  // an in-process map
//	deps.Events.Publish(...)         // return value discarded
//	writeAllocationAccepted(...)     // 202 regardless
//
// So the debit was durable and the record of WHO it was granted to was not.
// Two live grants of 200 units each moved the pool from 41000 to 40600 and
// reached nobody; at the next pod restart the allocation entries would have
// vanished entirely, leaving a smaller pool and no record of where the units
// went. The publish error was discarded on top of that, so a failed outbox
// write would also have returned 202 over an already-debited pool.
//
// ⚠ Note what was NOT the cause, because the obvious fix does not fix it:
// both outbox rows were written durably and it was the DISPATCHER that later
// failed (the topic does not exist, CHO-2415). Making the debit atomic with
// the outbox write alone would have changed nothing. The record that has to
// survive is the ALLOCATION; the outbox row is the notification about it.
//
// WHAT IT DOES
// One RunInTenantTx containing all three writes, following the in-house
// pattern already used by ExternalEgressRepository.Save (row upsert plus
// INSERT INTO outbox_events, OCC-guarded on version) and
// BootstrapRepository.Persist. Nothing here touches the shared
// outbox.Publisher, which keeps its own transaction for every other caller.
//
// ⚠ RLS: tenant_mana_allocations is RLS-enabled and chora_tenancy_app_rw is
// NOBYPASSRLS, so the insert only succeeds inside SET LOCAL chora.tenant_id.
// This is not theoretical. The pre-existing pg.AllocationRepository takes the
// plain Querier and never opens a tenant transaction, and the table holds
// zero rows, so its inserts have been failing the policy since it was
// written. Measured 2026-08-24 as app_rw in rolled-back transactions:
// without the GUC, "new row violates row-level security policy for table
// tenant_mana_allocations"; with it, INSERT 0 1.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/outbox"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// ErrGrantAlreadyRecorded means this allocation_id is already on record, so
// the caller is retrying a grant that already succeeded. NOTHING is written,
// and critically the pool is NOT debited again.
//
// This sentinel closes a double-debit that the obvious design walks straight
// into: if the idempotency check lives in an in-process map (as it did), then
// after a pod restart a client retry finds no cached entry, debits the pool a
// second time, and the allocation INSERT quietly no-ops on ON CONFLICT. The
// check therefore has to be inside the same transaction as the debit.
var ErrGrantAlreadyRecorded = errors.New("pg.ManaGrantRepository: allocation already recorded, grant not re-applied")

// GrantEvent describes the notification enqueued alongside the grant. The
// payload is the same map the handler used to hand to Events.Publish, so the
// wire shape is unchanged by this refactor.
type GrantEvent struct {
	Topic       string
	GCID        string
	Traceparent string
	Payload     map[string]any
}

// ManaGrantRepository persists a tenant-to-member mana grant.
type ManaGrantRepository struct {
	tq TxQuerier
}

// NewManaGrantRepository constructs the production-wired repository.
func NewManaGrantRepository(tq TxQuerier) *ManaGrantRepository {
	return &ManaGrantRepository{tq: tq}
}

// insertGrantAllocationSQL records the allocation. Idempotent on the caller's
// allocation_id, which is the request's own key, so a client retry after a
// timeout re-presents the same id and lands no second row.
const insertGrantAllocationSQL = `
        INSERT INTO tenant_mana_allocations (
            allocation_id, tenant_id, source_pool_id, gcid, units,
            status, allocation_reason, allocated_by_gcid, allocated_at, updated_at
        ) VALUES (
            $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5,
            $6::tenant_mana_allocation_status,
            $7::tenant_mana_allocation_reason,
            NULLIF($8, '')::uuid, $9, $10
        )
        ON CONFLICT (allocation_id) DO NOTHING
        RETURNING allocation_id::text
    `

// probeGrantAllocationSQL is the durable idempotency check. It runs FIRST,
// inside the transaction, so a retry cannot debit before discovering that the
// grant is already recorded.
const probeGrantAllocationSQL = `
        SELECT allocation_id::text
        FROM tenant_mana_allocations
        WHERE allocation_id = $1::uuid
    `

const insertGrantOutboxSQL = `
    INSERT INTO outbox_events (
        id, tenant_id, gcid, aggregate_type, aggregate_id,
        event_type, topic, payload, envelope, idempotency_key,
        occurred_at, status
    )
    VALUES ($1, $2, NULLIF($3, '')::uuid, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending')
`

// SaveGrant persists the debited pool, the allocation row and the grant event
// in ONE transaction. Either all three land or none do, so a grant whose
// record cannot be written does not move the balance.
//
// The pool UPDATE is the same version-guarded statement Save uses, so a
// concurrent writer loses loudly with ErrVersionConflict rather than
// silently overwriting, and the caller's Nack or 5xx is retryable.
func (r *ManaGrantRepository) SaveGrant(
	ctx context.Context,
	p *pool.TenantManaPool,
	a *allocation.TenantManaAllocation,
	ev GrantEvent,
) error {
	if p == nil {
		return errors.New("pg.ManaGrantRepository.SaveGrant: nil pool")
	}
	if a == nil {
		return errors.New("pg.ManaGrantRepository.SaveGrant: nil allocation")
	}
	if p.TenantID == "" || a.TenantID == "" {
		return errors.New("pg.ManaGrantRepository.SaveGrant: both pool and allocation need a tenant_id")
	}
	if p.TenantID != a.TenantID {
		return fmt.Errorf("pg.ManaGrantRepository.SaveGrant: pool tenant %s does not match allocation tenant %s",
			p.TenantID, a.TenantID)
	}

	policyKind, units, tiersJSON, err := encodePolicy(p.AutoAllocationPolicy)
	if err != nil {
		return err
	}

	// Mint a synthetic root when the caller supplied none. Without this the
	// envelope carries an empty traceparent and the DISPATCHER rejects the
	// row with "envelope: traceparent is required" AFTER the pool has already
	// moved. Mirrors buildExternalEgressOutboxRow.
	traceparent := ev.Traceparent
	if traceparent == "" {
		traceparent = tracing.EnsureTraceparent("")
	}

	row, err := buildGrantOutboxRow(ev, a, traceparent, time.Now().UTC())
	if err != nil {
		return err
	}
	envBytes, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("pg.ManaGrantRepository.SaveGrant: marshal outbox envelope: %w", err)
	}

	return r.tq.RunInTenantTx(ctx, p.TenantID, func(ctx context.Context, tx Tx) error {
		// 0. Durable idempotency, BEFORE the debit. See ErrGrantAlreadyRecorded.
		var existing string
		probeErr := tx.QueryRow(ctx, probeGrantAllocationSQL, a.AllocationID).Scan(&existing)
		if probeErr == nil {
			return ErrGrantAlreadyRecorded
		}
		if !errors.Is(probeErr, ErrNoRows) {
			return fmt.Errorf("pg.ManaGrantRepository.SaveGrant: probe allocation: %w", probeErr)
		}

		// 1. The debit, version-guarded exactly as Save does it.
		var storedVersion int64
		updErr := tx.QueryRow(ctx, updatePool,
			p.TenantID, p.PoolID,
			p.BalanceUnits, p.LifetimeToppedUpUnits, p.LifetimeAllocatedUnits,
			p.MonthlyTopupUnits, policyKind, units, tiersJSON,
			p.Version, p.UpdatedAt, p.LastToppedUpAt, p.DeletedAt,
		).Scan(&storedVersion)
		if errors.Is(updErr, ErrNoRows) {
			return fmt.Errorf("%w (tenant=%s pool=%s attempted_version=%d)",
				ErrVersionConflict, p.TenantID, p.PoolID, p.Version)
		}
		if updErr != nil {
			return fmt.Errorf("pg.ManaGrantRepository.SaveGrant: debit pool: %w", updErr)
		}

		// 2. The allocation record. This is the write whose absence let 400
		// units leave the pool with nothing naming their recipient.
		var insertedID string
		allocErr := tx.QueryRow(ctx, insertGrantAllocationSQL,
			a.AllocationID, a.TenantID, a.SourcePoolID, a.GCID, a.Units,
			string(a.Status), string(a.Reason), a.AllocatedByGCID,
			a.AllocatedAt, a.UpdatedAt,
		).Scan(&insertedID)
		if allocErr != nil && !errors.Is(allocErr, ErrNoRows) {
			return fmt.Errorf("pg.ManaGrantRepository.SaveGrant: insert allocation: %w", allocErr)
		}
		// ErrNoRows here means ON CONFLICT DO NOTHING fired despite the step-0
		// probe finding nothing, which is the concurrent-duplicate race. It is
		// belt-and-braces: step 0 is the primary guard. Treat it the same way,
		// refusing rather than leaving a debit with no new record.
		if errors.Is(allocErr, ErrNoRows) {
			return ErrGrantAlreadyRecorded
		}

		// 3. The notification, atomically with both writes above.
		return tx.Exec(ctx, insertGrantOutboxSQL,
			row.ID, row.TenantID, row.GCID, row.AggregateType, row.AggregateID,
			row.EventType, row.Topic, row.Payload, string(envBytes),
			row.IdempotencyKey, row.OccurredAt,
		)
	})
}

// buildGrantOutboxRow assembles the outbox row for a grant.
//
// It deliberately calls the SAME protomarshal.MarshalPayload the shared
// outbox.Publisher calls, with the same IsUnsupportedTopic JSON fallback, so
// this builder cannot drift from the publisher's wire format. When CHO-2415
// adds a protobuf encoder for the grant topic, this path picks it up with no
// change here. Duplicating the encoding instead would have created exactly
// the silent divergence this codebase keeps finding.
//
// The idempotency key is the allocation id, so a retried grant reproduces the
// same key and the dispatcher de-duplicates rather than notifying twice.
func buildGrantOutboxRow(
	ev GrantEvent,
	a *allocation.TenantManaAllocation,
	traceparent string,
	now time.Time,
) (outbox.Row, error) {
	if strings.TrimSpace(ev.Topic) == "" {
		return outbox.Row{}, errors.New("buildGrantOutboxRow: empty topic")
	}
	if a == nil || a.AllocationID == "" {
		return outbox.Row{}, errors.New("buildGrantOutboxRow: allocation must carry an id, it is the idempotency key")
	}

	eventID := newOutboxUUIDv7()
	idemKey := "tenant-mana-grant:" + a.AllocationID

	env := protomarshal.Envelope{
		EventID:        eventID,
		IdempotencyKey: idemKey,
		TenantID:       a.TenantID,
		GCID:           ev.GCID,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    traceparent,
		SourceProject:  outboxSourceProject,
		SourceService:  outboxSourceService,
		SchemaVersion:  outboxSchemaVersion,
	}

	payloadBytes, err := protomarshal.MarshalPayload(ev.Topic, env, ev.Payload)
	if err != nil {
		if !protomarshal.IsUnsupportedTopic(err) {
			return outbox.Row{}, fmt.Errorf("buildGrantOutboxRow: marshal payload: %w", err)
		}
		// No protobuf encoder for this topic yet, same fallback the shared
		// publisher takes. CHO-2415 owns closing that.
		payloadBytes, err = json.Marshal(ev.Payload)
		if err != nil {
			return outbox.Row{}, fmt.Errorf("buildGrantOutboxRow: json fallback: %w", err)
		}
	}

	return outbox.Row{
		ID:            eventID,
		TenantID:      a.TenantID,
		GCID:          ev.GCID,
		AggregateType: "tenant_mana_allocation",
		AggregateID:   a.AllocationID,
		EventType:     "tenancy.mana_pool.adjusted",
		Topic:         ev.Topic,
		Payload:       payloadBytes,
		Envelope: map[string]string{
			"event_id":             eventID,
			"idempotency_key":      idemKey,
			"tenant_id":            a.TenantID,
			"gcid":                 ev.GCID,
			"occurred_at":          now.Format(time.RFC3339Nano),
			"published_at":         now.Format(time.RFC3339Nano),
			"traceparent":          traceparent,
			"source_project":       outboxSourceProject,
			"source_service":       outboxSourceService,
			"schema_version":       "1",
			"chora_imda_dimension": "accountability",
		},
		IdempotencyKey: idemKey,
		OccurredAt:     now,
	}, nil
}
