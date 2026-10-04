// restore-tenant-mana-topup is a ONE-SHOT operator tool (CHO-2414).
//
// WHY IT EXISTS
// Until 2026-08-24 the tenant mana pool was bound to an in-process map
// (manapool.InmemPoolRepo), so `tenant_mana_pools` held zero rows across the
// whole estate and every credit a payment produced was lost at the next pod
// restart. Tenant 11111111-1111-7111-8111-111111111111 has two
// `payment_captured` rows in chora_payments.tenant_mana_topups, SGD 24.99
// each, 20000 mana units each, nothing refunded, and no surviving balance.
//
// Owner ruling 2026-08-24: restore the credit VIA THE PoolApplier, so the
// correction takes the same path a live capture takes and leaves the same
// event trail. A hand-written UPDATE was rejected by name, on the grounds
// that a direct write leaves no trail and reads as a hand-edit to a later
// auditor.
//
// WHAT IT DOES
// Re-emits chora.payments.tenant_mana_topup.payment_captured.v1 for the
// named purchases. The live chora-tenancy subscriber decodes it, calls
// PaymentsPoolApplier.Credit, which writes through pg.ManaPoolRepository,
// and then emits chora.tenancy.tenant_mana_pool.topped_up.v1. Nothing here
// touches a database.
//
// WHY THE ORIGINALS COULD NOT SIMPLY BE REDRIVEN
// The DLQ chora.dlq.payments.tenant_mana_topup.payment_captured.v1 retains
// for 604800s (7 days) and the events are 37 days old. A peek pull returns
// nothing. The originals are gone, so the event has to be reconstructed.
//
// IDEMPOTENCY, AND A CORRECTION TO WHAT THIS COMMENT FIRST CLAIMED
//  1. event_id is a deterministic UUIDv5 over the purchase id, so a re-run
//     inside the subscriber's 24h inbox TTL (events.PaymentsInboxTTL) is a
//     no-op rather than a second credit. Use --id-salt to mint a fresh id for
//     a deliberate SECOND corrective pass.
//  2. ⚠ The durable guard is `balance_units`, NOT `lifetime_topped_up_units`.
//     This file originally named the latter and that was wrong: the ADR-164
//     PoolApplier calls pool.Credit(), which by domain design touches only
//     BalanceUnits and LifetimeAllocatedUnits. Only pool.TopUp() writes
//     LifetimeToppedUpUnits and LastToppedUpAt, and nothing on this path calls
//     it. The first run proved it, leaving lifetime_topped_up_units at 0 with
//     a real credit in the pool, so the guard could never have fired.
//
// ⚠ THE FIRST RUN ALSO LOST HALF THE CREDIT. Both events were handled 3ms
// apart, both read version 3, both wrote version 4, and the pool ended at
// 20000 instead of 40000. pg.ManaPoolRepository.Save now carries a version
// guard (ErrVersionConflict) that turns the loser into a Nack, which the
// subscriber inbox does not Mark, so redelivery re-applies on top. Publishing
// both events again would now be safe from the race, but it would also
// double-count what already landed: use --only plus --id-salt to apply
// exactly the shortfall.
//
// ⚠ `mana_units_debited` is deliberately NOT written. Despite the name it is
// NOT an "applied to pool" flag: the aggregate documents it as "tracks
// proportional debits on refund" and its only reader feeds it into the
// refunded event as ManaUnitsToDebit. Setting it here would tell the refund
// path that units had already been clawed back, so a later genuine refund
// would debit the wrong amount.
//
// PROVENANCE
// source_service is stamped as this tool, NOT as chora-payments. The event
// is a reconstruction and the envelope says so; claiming the payments
// service emitted it would make the provenance false and indistinguishable
// from a real capture.
//
// Usage (dry run is the default; --apply is required to publish):
//
//	go run ./cmd/restore-tenant-mana-topup --spec=spec.json
//	go run ./cmd/restore-tenant-mana-topup --spec=spec.json --apply
package main

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	envelopepkg "github.com/apollo-chora/chora-common/envelope"
	pubsublib "github.com/apollo-chora/chora-common/pubsub"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/env"
)

const (
	topic         = "chora.payments.tenant_mana_topup.payment_captured.v1"
	sourceService = "chora-tenancy-restore-tenant-mana-topup"
	// restoreNamespace seeds the deterministic event_id. Any fixed string
	// works; it is fixed so two runs derive the SAME id.
	restoreNamespace = "chora.cho-2414.restore-tenant-mana-topup"
)

// purchaseSpec is one captured top-up to re-emit. Every field is copied from
// the chora_payments.tenant_mana_topups row; the tool invents nothing.
type purchaseSpec struct {
	PurchaseID            string `json:"purchase_id"`
	AdminGCID             string `json:"admin_gcid"`
	SKU                   string `json:"sku"`
	StripeSessionID       string `json:"stripe_session_id"`
	StripePaymentIntentID string `json:"stripe_payment_intent_id"`
	StripeChargeID        string `json:"stripe_charge_id"`
	AmountCentsPaid       int64  `json:"amount_cents_paid"`
	AmountCentsRefunded   int64  `json:"amount_cents_refunded"`
	Currency              string `json:"currency"`
	ManaUnits             int64  `json:"mana_units"`
	PaidAt                string `json:"paid_at"` // RFC3339Nano
}

type spec struct {
	Project   string         `json:"project"`
	TenantID  string         `json:"tenant_id"`
	Purchases []purchaseSpec `json:"purchases"`
}

func main() {
	specPath := flag.String("spec", "", "path to the JSON spec describing the purchases to re-emit (required)")
	apply := flag.Bool("apply", false, "actually publish; without this the tool prints what it would send and exits")
	only := flag.String("only", "", "publish only this purchase_id from the spec (default: all)")
	idSalt := flag.String("id-salt", "", "mix a tag into the deterministic event_id, for a SECOND corrective pass "+
		"whose first pass was already consumed by the subscriber inbox. Empty reproduces the original ids exactly.")
	flag.Parse()

	if err := run(context.Background(), *specPath, *apply, *only, *idSalt); err != nil {
		log.Fatalf("restore-tenant-mana-topup: %v", err)
	}
}

func run(ctx context.Context, specPath string, apply bool, only, idSalt string) error {
	if specPath == "" {
		return errors.New("--spec is required")
	}
	raw, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	var s spec
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("parse spec: %w", err)
	}
	if s.Project == "" || s.TenantID == "" || len(s.Purchases) == 0 {
		return errors.New("spec needs project, tenant_id and at least one purchase")
	}

	if only != "" {
		kept := s.Purchases[:0]
		for _, p := range s.Purchases {
			if p.PurchaseID == only {
				kept = append(kept, p)
			}
		}
		if len(kept) == 0 {
			return fmt.Errorf("--only %q matched no purchase in the spec", only)
		}
		s.Purchases = kept
	}

	var totalUnits int64
	for _, p := range s.Purchases {
		if p.PurchaseID == "" || p.ManaUnits <= 0 {
			return fmt.Errorf("purchase %q: purchase_id and a positive mana_units are required", p.PurchaseID)
		}
		totalUnits += p.ManaUnits
	}

	log.Printf("tenant=%s purchases=%d total_mana_units=%d topic=%s apply=%v only=%q id_salt=%q",
		s.TenantID, len(s.Purchases), totalUnits, topic, apply, only, idSalt)

	// Publish through the raw client, exactly as chora-payments' own outbox
	// dispatcher does (internal/adapter/outbox/dispatcher.go publishOne).
	// pubsublib.CloudPublisher.Publish CANNOT be used here: its
	// ValidateTopicName rejects the entire `payments` domain because
	// knownDomains has no "payments" entry, which is also why the real
	// producer never routes through it. Measured, not assumed: the first
	// run of this tool failed with `unknown domain "payments"`.
	var client *pubsublib.GCPClient
	if apply {
		c, cErr := pubsublib.NewGCPClient(ctx, s.Project)
		if cErr != nil {
			return fmt.Errorf("pubsub client: %w", cErr)
		}
		client = c
	}

	for i, p := range s.Purchases {
		env, payload, bErr := build(s.TenantID, p, idSalt)
		if bErr != nil {
			return fmt.Errorf("purchase %s: %w", p.PurchaseID, bErr)
		}
		if vErr := envelopepkg.Validate(env); vErr != nil {
			return fmt.Errorf("purchase %s: envelope: %w", p.PurchaseID, vErr)
		}
		log.Printf("[%d/%d] purchase=%s mana_units=%d event_id=%s idempotency_key=%s payload_bytes=%d",
			i+1, len(s.Purchases), p.PurchaseID, p.ManaUnits, env.EventID, env.IdempotencyKey, len(payload))
		if !apply {
			continue
		}
		if _, err := client.PublishMessage(ctx, topic, payload, attrs(env, p)); err != nil {
			return fmt.Errorf("publish purchase %s: %w", p.PurchaseID, err)
		}
		log.Printf("[%d/%d] PUBLISHED purchase=%s", i+1, len(s.Purchases), p.PurchaseID)
	}

	if !apply {
		log.Printf("DRY RUN. Nothing published. Re-run with --apply to publish.")
		return nil
	}
	log.Printf("DONE. Published %d event(s), %d mana units total. Now verify "+
		"tenant_mana_pools.lifetime_topped_up_units reads exactly %d.", len(s.Purchases), totalUnits, totalUnits)
	return nil
}

// build assembles the envelope and the marshalled protobuf payload for one
// purchase. occurred_at is the ORIGINAL paid_at, because that is when the
// capture happened; published_at is now, because that is when this
// reconstruction was put on the wire. Keeping them distinct is what makes
// the record honest about being a replay.
func build(tenantID string, p purchaseSpec, idSalt string) (envelopepkg.Envelope, []byte, error) {
	paidAt, err := time.Parse(time.RFC3339Nano, p.PaidAt)
	if err != nil {
		return envelopepkg.Envelope{}, nil, fmt.Errorf("paid_at %q: %w", p.PaidAt, err)
	}
	// An empty salt reproduces the ids of the first pass exactly, so those
	// events stay identifiable. A non-empty salt is how a SECOND corrective
	// pass gets past the subscriber inbox, which has already Marked the
	// first pass's ids.
	seed := restoreNamespace + ":" + p.PurchaseID
	idemKey := "cho2414-restore:" + p.PurchaseID
	if idSalt != "" {
		seed = restoreNamespace + ":" + idSalt + ":" + p.PurchaseID
		idemKey = "cho2414-restore:" + idSalt + ":" + p.PurchaseID
	}
	eventID := deterministicUUID(seed)
	now := time.Now().UTC()

	env := envelopepkg.Envelope{
		EventID:            eventID,
		IdempotencyKey:     idemKey,
		TenantID:           tenantID,
		GCID:               p.AdminGCID,
		OccurredAt:         paidAt,
		PublishedAt:        now,
		Traceparent:        deterministicTraceparent(eventID),
		SourceProject:      env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService:      sourceService,
		SchemaVersion:      1,
		ChoraImdaDimension: "accountability",
	}

	msg := &paymentsv1.TenantManaTopUpPaymentCaptured{
		Envelope: &commonv1.EventEnvelope{
			EventId:        env.EventID,
			IdempotencyKey: env.IdempotencyKey,
			TenantId:       env.TenantID,
			Gcid:           env.GCID,
			OccurredAt:     timestamppb.New(env.OccurredAt),
			PublishedAt:    timestamppb.New(env.PublishedAt),
			Traceparent:    env.Traceparent,
		},
		PurchaseId:            p.PurchaseID,
		AdminGcid:             p.AdminGCID,
		Sku:                   p.SKU,
		StripeSessionId:       p.StripeSessionID,
		StripePaymentIntentId: p.StripePaymentIntentID,
		StripeChargeId:        p.StripeChargeID,
		AmountCentsPaid:       p.AmountCentsPaid,
		AmountCentsRefunded:   p.AmountCentsRefunded,
		Currency:              p.Currency,
		ManaUnits:             p.ManaUnits,
		PaidAt:                timestamppb.New(paidAt),
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return envelopepkg.Envelope{}, nil, fmt.Errorf("marshal: %w", err)
	}
	return env, payload, nil
}

// deterministicUUID returns an RFC 9562 version-5 (SHA-1, name-based) UUID
// over `name`. Deterministic is the whole point: two runs derive the same
// event_id, so the subscriber's inbox dedupes the second one inside its TTL.
func deterministicUUID(name string) string {
	sum := sha1.Sum([]byte(name))
	b := sum[:16]
	b[6] = (b[6] & 0x0F) | 0x50 // version 5
	b[8] = (b[8] & 0x3F) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// deterministicTraceparent derives a W3C traceparent from the event id so a
// replay carries the same trace context rather than a fresh random one.
func deterministicTraceparent(eventID string) string {
	sum := sha1.Sum([]byte("traceparent:" + eventID))
	return fmt.Sprintf("00-%016x%016x-%016x-01",
		bytesToUint64(sum[0:8]), bytesToUint64(sum[8:16]), bytesToUint64(sum[12:20]))
}

func bytesToUint64(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}

// attrs mirrors chora-payments' outbox dispatcher pubsubAttributes so the
// message is indistinguishable on the wire from a real capture in every
// field EXCEPT source_service, which honestly names this tool. Nothing
// routes on source_service; claiming chora-payments emitted a
// reconstruction would make the provenance false.
//
// occurred_at and published_at MUST parse as RFC3339Nano or the subscriber's
// envelopeFromAttributes nacks the message before the handler ever sees it.
func attrs(env envelopepkg.Envelope, p purchaseSpec) map[string]string {
	a := map[string]string{
		"event_id":             env.EventID,
		"idempotency_key":      env.IdempotencyKey,
		"tenant_id":            env.TenantID,
		"aggregate_type":       "tenant_mana_topup",
		"aggregate_id":         p.PurchaseID,
		"source_project":       env.SourceProject,
		"source_service":       env.SourceService,
		"occurred_at":          env.OccurredAt.UTC().Format(time.RFC3339Nano),
		"published_at":         env.PublishedAt.UTC().Format(time.RFC3339Nano),
		"schema_version":       "1",
		"topic":                topic,
		"traceparent":          env.Traceparent,
		"chora_imda_dimension": env.ChoraImdaDimension,
	}
	if env.GCID != "" {
		a["gcid"] = env.GCID
	}
	return a
}
