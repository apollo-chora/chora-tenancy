// subscription_repository.go: the DURABLE write side for add-on subscriptions
// (UX Track U, row E4 slice 2).
//
// # WHY THIS EXISTS
//
// Before row E4 every runtime entitlement mutation (the v2 handlers, the
// payments subscriber activator, the wizard's SubscribePending) mutated the
// per-pod in-memory *add_on.SubscriptionRegistry and nothing else. The registry
// is hydrated from PG at boot, so a pod restart re-read the old row and
// resurrected an add-on the admin had deactivated. That is CHO-1811, which
// `cmd/server/registry_entitlement_store.go` calls breakage 4/5 in its own
// header, and it is also why a multi-pod deployment diverges: each pod holds a
// different truth.
//
// This repository is the destination that makes the write-through possible.
// Migration 0039 gave the table the six domain statuses and the seven columns
// it was missing; migration 0040 FORCEd RLS on it and NULLIF-guarded the
// policy. This file is the seam between the two.
//
// # EVERY ACCESS GOES THROUGH RunInTenantTx
//
// `add_on_subscriptions` carries the `tenant_isolation` policy and, since 0040,
// FORCE ROW LEVEL SECURITY. `chora_tenancy_app_rw` is NOBYPASSRLS. So a bare
// SELECT returns zero rows and a bare INSERT fails the WITH CHECK, silently in
// the first case. There is no correct bare query against this table: the
// TxQuerier seam sets `SET LOCAL chora.tenant_id` and is the only access path.
//
// # WHAT THIS FILE DOES NOT DECIDE
//
// It does not decide ordering. Whether the registry is mutated before or after
// this Upsert, and what happens to memory when the Upsert fails, is the call
// site's business (row E4 slice 2's eviction rule). This layer is deliberately
// ignorant of the registry so that it stays a plain durable store.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// ErrSubscriptionNotFound is returned by Get when the tenant holds no
// subscription for the code. It is a normal answer, not a fault: a tenant that
// never subscribed has no row.
var ErrSubscriptionNotFound = errors.New("pg: subscription not found")

// SubscriptionRepository persists add_on.Subscription to the durable
// `add_on_subscriptions` table.
type SubscriptionRepository struct {
	q TxQuerier
}

// NewSubscriptionRepository binds the repository to a tenant-transaction
// runner. The parameter is the TxQuerier seam and not a raw pool, because
// every statement in this file MUST run with the tenant GUC applied.
func NewSubscriptionRepository(q TxQuerier) *SubscriptionRepository {
	return &SubscriptionRepository{q: q}
}

// Upsert writes the subscription, creating the row or replacing it in place.
//
// IDEMPOTENT on (tenant_id, add_on_id), which the table enforces with a UNIQUE
// constraint. That is not a nicety: the ordinary lifecycle is subscribe then
// deactivate, so the second mutation of the same subscription is the common
// case and a blind INSERT would violate the constraint there.
//
// `subscription_id` is written on insert and deliberately NOT updated on
// conflict: the durable row keeps the identity it was created with, so a
// caller that re-mints an id in memory cannot fork the row's history.
func (r *SubscriptionRepository) Upsert(ctx context.Context, sub *addon.Subscription) error {
	if sub == nil {
		return errors.New("pg.SubscriptionRepository.Upsert: nil subscription")
	}
	if strings.TrimSpace(sub.TenantID) == "" {
		return errors.New("pg.SubscriptionRepository.Upsert: tenant_id required")
	}
	if strings.TrimSpace(sub.AddOnID) == "" {
		return errors.New("pg.SubscriptionRepository.Upsert: add_on_id required")
	}

	const q = `
        INSERT INTO add_on_subscriptions (
            subscription_id, tenant_id, add_on_id, status,
            monthly_price_cents_snap, started_at, created_at, updated_at,
            past_due, current_tier, plan_family,
            owner_gcid, version, grace_ends_at,
            scheduled_tier, scheduled_effective_at, next_renewal_at,
            deactivated_at, cancelled_at,
            deactivation_reason, deactivation_reason_text,
            deactivation_requested_by, deactivation_requested_at,
            deactivation_effective_at
        ) VALUES (
            $1, $2, $3, $4::entitlement_status,
            $5, $6, now(), $7,
            $8, $9, $10,
            $11, $12, $13,
            $14, $15, $16,
            $17, $18,
            NULLIF($19,'')::addon_deactivation_reason, NULLIF($20,''),
            $21, $22,
            $23
        )
        ON CONFLICT (tenant_id, add_on_id) DO UPDATE SET
            status                    = EXCLUDED.status,
            monthly_price_cents_snap  = EXCLUDED.monthly_price_cents_snap,
            started_at                = EXCLUDED.started_at,
            updated_at                = EXCLUDED.updated_at,
            past_due                  = EXCLUDED.past_due,
            current_tier              = EXCLUDED.current_tier,
            plan_family               = EXCLUDED.plan_family,
            owner_gcid                = EXCLUDED.owner_gcid,
            version                   = EXCLUDED.version,
            grace_ends_at             = EXCLUDED.grace_ends_at,
            scheduled_tier            = EXCLUDED.scheduled_tier,
            scheduled_effective_at    = EXCLUDED.scheduled_effective_at,
            next_renewal_at           = EXCLUDED.next_renewal_at,
            deactivated_at            = EXCLUDED.deactivated_at,
            cancelled_at              = EXCLUDED.cancelled_at,
            deactivation_reason       = EXCLUDED.deactivation_reason,
            deactivation_reason_text  = EXCLUDED.deactivation_reason_text,
            deactivation_requested_by = EXCLUDED.deactivation_requested_by,
            deactivation_requested_at = EXCLUDED.deactivation_requested_at,
            deactivation_effective_at = EXCLUDED.deactivation_effective_at`

	return r.q.RunInTenantTx(ctx, sub.TenantID, func(ctx context.Context, tx Tx) error {
		err := tx.Exec(ctx, q,
			nullIfEmpty(sub.ID), sub.TenantID, sub.AddOnID, string(sub.Status),
			sub.MonthlyPriceCentsSnapshot, sub.ActivatedAt, sub.UpdatedAt,
			sub.PastDue, nullIfEmpty(sub.CurrentTier), nullIfEmpty(sub.PlanFamily),
			nullIfEmpty(sub.OwnerGCID), sub.Version, sub.GraceEndsAt,
			nullIfEmpty(sub.ScheduledTier), sub.ScheduledEffectiveAt, sub.NextRenewalAt,
			sub.DeactivatedAt, sub.CancelledAt,
			string(sub.DeactivationReason), sub.DeactivationReasonText,
			nullIfEmpty(sub.DeactivationRequestedBy), sub.DeactivationRequestedAt,
			sub.DeactivationEffectiveAt,
		)
		if err != nil {
			return fmt.Errorf("pg.SubscriptionRepository.Upsert %s/%s: %w", sub.TenantID, sub.AddOnCode, err)
		}
		return nil
	})
}

// Get reads one subscription by tenant and add-on CODE, joining the catalogue
// because the durable row keys on add_on_id while the domain keys on the code.
//
// Returns ErrSubscriptionNotFound when the tenant holds no such row. Note that
// under RLS a wrong tenant is indistinguishable from a missing row, which is
// the correct posture: isolation reads as absence, never as a permission error
// that would confirm the row exists.
func (r *SubscriptionRepository) Get(ctx context.Context, tenantID, addOnCode string) (*addon.Subscription, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, errors.New("pg.SubscriptionRepository.Get: tenant_id required")
	}

	const q = `
        SELECT s.subscription_id::text, s.tenant_id::text, a.code, s.add_on_id::text,
               COALESCE(s.plan_family,''), COALESCE(s.current_tier,''),
               s.monthly_price_cents_snap, COALESCE(s.owner_gcid::text,''),
               s.status::text, COALESCE(s.version, 0),
               s.started_at, s.cancelled_at, s.grace_ends_at, s.deactivated_at,
               COALESCE(s.deactivation_reason::text,''), COALESCE(s.deactivation_reason_text,''),
               COALESCE(s.deactivation_requested_by::text,''), s.deactivation_requested_at,
               s.deactivation_effective_at, s.updated_at, s.past_due,
               COALESCE(s.scheduled_tier,''), s.scheduled_effective_at, s.next_renewal_at
          FROM add_on_subscriptions s
          JOIN add_ons a ON a.add_on_id = s.add_on_id
         WHERE s.tenant_id = $1 AND a.code = $2`

	var out *addon.Subscription
	err := r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		var (
			sub                                               addon.Subscription
			cancelledAt, graceEndsAt, deactivatedAt           *time.Time
			deactRequestedAt, deactEffectiveAt, schedEffectAt *time.Time
			nextRenewalAt                                     *time.Time
			reason                                            string
		)
		row := tx.QueryRow(ctx, q, tenantID, addOnCode)
		if err := row.Scan(
			&sub.ID, &sub.TenantID, &sub.AddOnCode, &sub.AddOnID,
			&sub.PlanFamily, &sub.CurrentTier,
			&sub.MonthlyPriceCentsSnapshot, &sub.OwnerGCID,
			&sub.Status, &sub.Version,
			&sub.ActivatedAt, &cancelledAt, &graceEndsAt, &deactivatedAt,
			&reason, &sub.DeactivationReasonText,
			&sub.DeactivationRequestedBy, &deactRequestedAt,
			&deactEffectiveAt, &sub.UpdatedAt, &sub.PastDue,
			&sub.ScheduledTier, &schedEffectAt, &nextRenewalAt,
		); err != nil {
			if errors.Is(err, ErrNoRows) {
				return ErrSubscriptionNotFound
			}
			return fmt.Errorf("pg.SubscriptionRepository.Get %s/%s: %w", tenantID, addOnCode, err)
		}
		sub.CancelledAt = cancelledAt
		sub.GraceEndsAt = graceEndsAt
		sub.DeactivatedAt = deactivatedAt
		sub.DeactivationRequestedAt = deactRequestedAt
		sub.DeactivationEffectiveAt = deactEffectiveAt
		sub.ScheduledEffectiveAt = schedEffectAt
		sub.NextRenewalAt = nextRenewalAt
		sub.DeactivationReason = addon.DeactivationReason(reason)
		out = &sub
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// nullIfEmpty maps "" to nil so an empty domain string lands as SQL NULL rather
// than as an empty string in a uuid or enum column, which would raise 22P02.
func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
