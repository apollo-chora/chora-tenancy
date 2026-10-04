// familiar_egg_repository.go — pgx-backed implementation of the Iter G.3
// FamiliarEgg purchase + catalog repositories (ADR-149).
//
// SQL contract (per migrations/0007_familiar_egg_purchases.sql):
//
//   - familiar_egg_catalog SELECT policy allows tenant_id IS NULL (platform
//     SKUs) OR tenant_id = current_setting('chora.tenant_id'). CatalogRepository
//     opens the SET LOCAL transaction itself via the TxQuerier seam, so every
//     method takes the caller's tenant. See the CatalogRepository doc comment.
//   - familiar_egg_purchases had RLS via current_setting('chora.tenant_id'),
//     but the chora_tenancy copy was DROPPED by migration 0017 (ADR-164 moved
//     it to chora_payments). PurchaseRepository below still targets it and is
//     wired in cmd/server; it runs on the bare pool with no RLS session.
//   - stripe_session_id has a UNIQUE constraint; duplicate-session inserts
//     surface as ErrDuplicateStripeSession.
//
// Querier+ extension: this file introduces multi-row Query() on the local
// Querier seam (existing pg.Querier was Exec + QueryRow only) so we can
// list catalog SKUs.
package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

// ErrDuplicateStripeSession is returned when Insert collides on the
// familiar_egg_purchases.stripe_session_id UNIQUE constraint. Callers MAY
// treat this as a no-op (the prior /checkout call already persisted the
// row + emitted the outbox event).
var ErrDuplicateStripeSession = errors.New("pg: duplicate stripe_session_id")

// QueryRunner is the extended Querier seam used by the FamiliarEgg repos.
// Adds a multi-row Query() method on top of pg.Querier.
type QueryRunner interface {
	Querier
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Rows is the minimal database/sql Rows surface used by the FamiliarEgg
// catalog list query.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// PgxPoolQuerier compile-time check that the production wrapper satisfies
// QueryRunner once Query() is added below (in pgx_query_adapter.go).
var _ QueryRunner = (*PgxPoolQuerier)(nil)

// -----------------------------------------------------------------------------
// PurchaseRepository
// -----------------------------------------------------------------------------

// PurchaseRepository is the pgx-backed familiar_egg_purchases repo.
type PurchaseRepository struct {
	q QueryRunner
}

// NewPurchaseRepository constructs the production-wired repository.
func NewPurchaseRepository(q QueryRunner) *PurchaseRepository {
	return &PurchaseRepository{q: q}
}

// Insert persists a new Purchase row in state 'checkout_started'.
func (r *PurchaseRepository) Insert(ctx context.Context, p *familiareag.Purchase) error {
	if p == nil {
		return errors.New("pg.PurchaseRepository.Insert: nil purchase")
	}
	const q = `
        INSERT INTO familiar_egg_purchases (
            purchase_id, tenant_id, purchaser_gcid, egg_sku,
            suggested_focal_atom_id, state, amount_cents,
            amount_cents_paid, amount_cents_refunded, currency,
            stripe_session_id, stripe_checkout_url,
            checkout_started_at, soft_expiry_at, hard_expiry_at,
            created_at, updated_at
        ) VALUES (
            $1::uuid, $2::uuid, $3::uuid, $4,
            NULLIF($5, '')::uuid, $6, $7,
            $8, $9, $10,
            $11, $12,
            $13, $14, $15,
            $16, $17
        )
    `
	err := r.q.Exec(ctx, q,
		p.PurchaseID, p.TenantID, p.PurchaserGCID, p.EggSKU,
		p.SuggestedFocalAtomID, string(p.State), p.AmountCents,
		p.AmountCentsPaid, p.AmountCentsRefunded, p.Currency,
		p.StripeSessionID, p.StripeCheckoutURL,
		p.CheckoutStartedAt, p.SoftExpiryAt, p.HardExpiryAt,
		p.CreatedAt, p.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err, "familiar_egg_purchases_stripe_session_id_key") ||
			isUniqueViolation(err, "stripe_session_id") {
			return fmt.Errorf("%w: %s", ErrDuplicateStripeSession, p.StripeSessionID)
		}
		return fmt.Errorf("pg.PurchaseRepository.Insert: %w", err)
	}
	return nil
}

// UpdateState persists a state transition on an existing Purchase row.
//
// The supplied Purchase is the post-transition snapshot. Repo writes the
// full mutable column set rather than computing a per-state UPDATE — keeps
// the call sites small + the SQL pattern uniform across paid/failed/refunded/expired.
func (r *PurchaseRepository) UpdateState(ctx context.Context, p *familiareag.Purchase) error {
	if p == nil {
		return errors.New("pg.PurchaseRepository.UpdateState: nil purchase")
	}
	const q = `
        UPDATE familiar_egg_purchases SET
            state = $2,
            amount_cents_paid = $3,
            amount_cents_refunded = $4,
            stripe_payment_intent_id = NULLIF($5, ''),
            stripe_charge_id = NULLIF($6, ''),
            stripe_refund_id = NULLIF($7, ''),
            stripe_failure_code = NULLIF($8, ''),
            stripe_failure_message = NULLIF($9, ''),
            refund_reason = NULLIF($10, ''),
            refund_credit_only = $11,
            paid_at = $12,
            failed_at = $13,
            provisioned_at = $14,
            refunded_at = $15,
            expired_at = $16,
            updated_at = now()
        WHERE purchase_id = $1::uuid
    `
	return r.q.Exec(ctx, q,
		p.PurchaseID,
		string(p.State),
		p.AmountCentsPaid,
		p.AmountCentsRefunded,
		p.StripePaymentIntentID,
		p.StripeChargeID,
		p.StripeRefundID,
		p.StripeFailureCode,
		p.StripeFailureMessage,
		p.RefundReason,
		p.RefundCreditOnly,
		p.PaidAt,
		p.FailedAt,
		p.ProvisionedAt,
		p.RefundedAt,
		p.ExpiredAt,
	)
}

// GetByStripeSessionID returns the (single) purchase row by Stripe session
// id. Used by the webhook handler to look up the row pre-transition.
func (r *PurchaseRepository) GetByStripeSessionID(ctx context.Context, sessionID string) (*familiareag.Purchase, bool, error) {
	const q = `
        SELECT purchase_id::text, tenant_id::text, purchaser_gcid::text,
               egg_sku, COALESCE(suggested_focal_atom_id::text, ''),
               state, amount_cents, amount_cents_paid, amount_cents_refunded, currency,
               stripe_session_id, COALESCE(stripe_payment_intent_id, ''),
               COALESCE(stripe_charge_id, ''), COALESCE(stripe_refund_id, ''),
               COALESCE(stripe_checkout_url, ''),
               COALESCE(stripe_failure_code, ''), COALESCE(stripe_failure_message, ''),
               COALESCE(refund_reason, ''), refund_credit_only,
               checkout_started_at, paid_at, failed_at, provisioned_at, refunded_at, expired_at,
               soft_expiry_at, hard_expiry_at,
               created_at, updated_at
        FROM familiar_egg_purchases
        WHERE stripe_session_id = $1
        LIMIT 1
    `
	row := r.q.QueryRow(ctx, q, sessionID)
	p, err := scanPurchase(row)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("pg.PurchaseRepository.GetByStripeSessionID: %w", err)
	}
	return p, true, nil
}

// FindHardExpiryCandidates returns rows in state='paid' AND provisioned_at IS NULL
// AND hard_expiry_at < $1. Used by the Iter G.6 sweeper.
func (r *PurchaseRepository) FindHardExpiryCandidates(ctx context.Context, before time.Time, limit int) ([]*familiareag.Purchase, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `
        SELECT purchase_id::text, tenant_id::text, purchaser_gcid::text,
               egg_sku, COALESCE(suggested_focal_atom_id::text, ''),
               state, amount_cents, amount_cents_paid, amount_cents_refunded, currency,
               stripe_session_id, COALESCE(stripe_payment_intent_id, ''),
               COALESCE(stripe_charge_id, ''), COALESCE(stripe_refund_id, ''),
               COALESCE(stripe_checkout_url, ''),
               COALESCE(stripe_failure_code, ''), COALESCE(stripe_failure_message, ''),
               COALESCE(refund_reason, ''), refund_credit_only,
               checkout_started_at, paid_at, failed_at, provisioned_at, refunded_at, expired_at,
               soft_expiry_at, hard_expiry_at,
               created_at, updated_at
        FROM familiar_egg_purchases
        WHERE state = 'paid'
          AND provisioned_at IS NULL
          AND hard_expiry_at < $1
        ORDER BY hard_expiry_at ASC
        LIMIT $2
    `
	rows, err := r.q.Query(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("pg.PurchaseRepository.FindHardExpiryCandidates: %w", err)
	}
	defer rows.Close()
	var out []*familiareag.Purchase
	for rows.Next() {
		p, scanErr := scanPurchase(&rowsScanner{rows: rows})
		if scanErr != nil {
			return nil, fmt.Errorf("pg.PurchaseRepository.FindHardExpiryCandidates scan: %w", scanErr)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.PurchaseRepository.FindHardExpiryCandidates iter: %w", err)
	}
	return out, nil
}

// rowsScanner adapts a Rows iteration into the Row Scan interface.
type rowsScanner struct {
	rows Rows
}

func (s *rowsScanner) Scan(dest ...any) error { return s.rows.Scan(dest...) }

func scanPurchase(row Row) (*familiareag.Purchase, error) {
	var (
		p             familiareag.Purchase
		paidAt        sql.NullTime
		failedAt      sql.NullTime
		provisionedAt sql.NullTime
		refundedAt    sql.NullTime
		expiredAt     sql.NullTime
	)
	err := row.Scan(
		&p.PurchaseID, &p.TenantID, &p.PurchaserGCID,
		&p.EggSKU, &p.SuggestedFocalAtomID,
		(*string)(&p.State), &p.AmountCents, &p.AmountCentsPaid, &p.AmountCentsRefunded, &p.Currency,
		&p.StripeSessionID, &p.StripePaymentIntentID,
		&p.StripeChargeID, &p.StripeRefundID,
		&p.StripeCheckoutURL,
		&p.StripeFailureCode, &p.StripeFailureMessage,
		&p.RefundReason, &p.RefundCreditOnly,
		&p.CheckoutStartedAt, &paidAt, &failedAt, &provisionedAt, &refundedAt, &expiredAt,
		&p.SoftExpiryAt, &p.HardExpiryAt,
		&p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if paidAt.Valid {
		t := paidAt.Time.UTC()
		p.PaidAt = &t
	}
	if failedAt.Valid {
		t := failedAt.Time.UTC()
		p.FailedAt = &t
	}
	if provisionedAt.Valid {
		t := provisionedAt.Time.UTC()
		p.ProvisionedAt = &t
	}
	if refundedAt.Valid {
		t := refundedAt.Time.UTC()
		p.RefundedAt = &t
	}
	if expiredAt.Valid {
		t := expiredAt.Time.UTC()
		p.ExpiredAt = &t
	}
	return &p, nil
}

// -----------------------------------------------------------------------------
// CatalogRepository
// -----------------------------------------------------------------------------

// CatalogRepository is the pgx-backed familiar_egg_catalog repo.
//
// RLS contract: familiar_egg_catalog is ENABLE + FORCE ROW LEVEL SECURITY
// (migration 0007) and chora_tenancy_app_rw is NOBYPASSRLS (migration 0013),
// so EVERY statement here runs inside a `SET LOCAL chora.tenant_id`
// transaction via the TxQuerier seam. The visibility policy is:
//
//	tenant_or_platform_visibility  FOR SELECT USING
//	  (tenant_id IS NULL)
//	  OR (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid)
//
// A bare-pool query leaves the GUC empty, migration 0028's NULLIF maps that
// to NULL, and the policy collapses to "platform rows only": every
// tenant-scoped SKU becomes structurally unreachable (the reveal path 500s
// on a lookup that can never succeed). Hence the TxQuerier, not a plain
// QueryRunner: there is no non-RLS table in this repository's reach.
//
// Every method also carries its own tenant predicate in SQL. That is
// deliberate redundancy, not belt-and-braces theatre: it keeps each
// statement correct when read in isolation and holds if the policy is ever
// widened.
type CatalogRepository struct {
	tx TxQuerier
}

// NewCatalogRepository constructs the production-wired repository.
func NewCatalogRepository(tx TxQuerier) *CatalogRepository {
	return &CatalogRepository{tx: tx}
}

// GetBySKU returns a single (non-deleted) catalog row by SKU, scoped to the
// caller's tenant: the tenant's own SKUs plus every platform-global SKU
// (tenant_id IS NULL). A SKU owned by a different tenant reports not-found,
// which is the RLS answer and leaks no existence.
//
// tenantID is REQUIRED. An empty tenant is a caller bug, and passing it
// through would silently degrade the lookup to platform-only results, so it
// fails loud instead.
func (r *CatalogRepository) GetBySKU(ctx context.Context, tenantID, sku string) (*familiareag.CatalogEntry, bool, error) {
	const q = `
        SELECT sku, COALESCE(tenant_id::text, ''), display_name, COALESCE(description, ''),
               price_cents, currency, COALESCE(suggested_focal_atom_id::text, ''),
               breed_distribution::text, breed_distribution_updated_at,
               purchasable, is_trial, soft_expiry_days, hard_expiry_days,
               available_from, available_until,
               created_at, updated_at, deleted_at
        FROM familiar_egg_catalog
        WHERE sku = $1
          AND deleted_at IS NULL
          AND (tenant_id IS NULL OR tenant_id = $2::uuid)
        LIMIT 1
    `
	var (
		out   *familiareag.CatalogEntry
		found bool
	)
	err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		e, scanErr := scanCatalog(tx.QueryRow(ctx, q, sku, tenantID))
		if scanErr != nil {
			if errors.Is(scanErr, ErrNoRows) {
				return nil
			}
			return scanErr
		}
		out, found = e, true
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("pg.CatalogRepository.GetBySKU: %w", err)
	}
	return out, found, nil
}

// ListForTenant returns rows visible to the supplied tenant: tenant_id
// match OR tenant_id IS NULL (platform). Filters by available window
// + deleted_at IS NULL. Sorted by (platform-last, price_cents ASC).
//
// Runs under the tenant's RLS session. Without it the SELECT policy hides
// every tenant-scoped row and the marketplace quietly lists platform SKUs
// only, with no error: just missing inventory.
func (r *CatalogRepository) ListForTenant(ctx context.Context, tenantID string, at time.Time) ([]*familiareag.CatalogEntry, error) {
	const q = `
        SELECT sku, COALESCE(tenant_id::text, ''), display_name, COALESCE(description, ''),
               price_cents, currency, COALESCE(suggested_focal_atom_id::text, ''),
               breed_distribution::text, breed_distribution_updated_at,
               purchasable, is_trial, soft_expiry_days, hard_expiry_days,
               available_from, available_until,
               created_at, updated_at, deleted_at
        FROM familiar_egg_catalog
        WHERE deleted_at IS NULL
          AND (tenant_id IS NULL OR tenant_id = $1::uuid)
          AND (available_from IS NULL OR available_from <= $2)
          AND (available_until IS NULL OR available_until >= $2)
        ORDER BY (tenant_id IS NULL), price_cents ASC, sku ASC
    `
	var out []*familiareag.CatalogEntry
	err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		rows, qErr := tx.Query(ctx, q, tenantID, at)
		if qErr != nil {
			return qErr
		}
		defer rows.Close()
		for rows.Next() {
			e, scanErr := scanCatalog(&rowsScanner{rows: rows})
			if scanErr != nil {
				return fmt.Errorf("scan: %w", scanErr)
			}
			out = append(out, e)
		}
		if iterErr := rows.Err(); iterErr != nil {
			return fmt.Errorf("iter: %w", iterErr)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pg.CatalogRepository.ListForTenant: %w", err)
	}
	return out, nil
}

// Upsert inserts or updates a catalog row under the entry's own tenant
// session.
//
// e.TenantID is REQUIRED. The tenant_admin_mutation policy is
// `tenant_id = NULLIF(current_setting('chora.tenant_id', true), ”)::uuid`
// and declares no explicit WITH CHECK, so Postgres applies that same
// expression as the WITH CHECK on INSERT: a platform-global row (tenant_id
// IS NULL) can never satisfy it under a NOBYPASSRLS app role. Platform SKUs
// are seeded by migration (0007 / 0030), by the migrate role. Refusing here
// beats emitting a statement the database will reject.
func (r *CatalogRepository) Upsert(ctx context.Context, e *familiareag.CatalogEntry) error {
	if e == nil {
		return errors.New("pg.CatalogRepository.Upsert: nil entry")
	}
	if strings.TrimSpace(e.TenantID) == "" {
		return errors.New("pg.CatalogRepository.Upsert: tenant_id required (platform-global SKUs are seeded by migration, not writable by the app role under FORCE RLS)")
	}
	breedJSON, err := json.Marshal(e.BreedDistribution)
	if err != nil {
		return fmt.Errorf("pg.CatalogRepository.Upsert: marshal breed: %w", err)
	}
	const q = `
        INSERT INTO familiar_egg_catalog (
            sku, tenant_id, display_name, description, price_cents, currency,
            suggested_focal_atom_id, breed_distribution,
            purchasable, is_trial, soft_expiry_days, hard_expiry_days,
            available_from, available_until
        ) VALUES (
            $1, NULLIF($2, '')::uuid, $3, NULLIF($4, ''), $5, $6,
            NULLIF($7, '')::uuid, $8::jsonb,
            $9, $10, $11, $12,
            $13, $14
        )
        ON CONFLICT (sku) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            price_cents = EXCLUDED.price_cents,
            currency = EXCLUDED.currency,
            suggested_focal_atom_id = EXCLUDED.suggested_focal_atom_id,
            breed_distribution = EXCLUDED.breed_distribution,
            purchasable = EXCLUDED.purchasable,
            is_trial = EXCLUDED.is_trial,
            soft_expiry_days = EXCLUDED.soft_expiry_days,
            hard_expiry_days = EXCLUDED.hard_expiry_days,
            available_from = EXCLUDED.available_from,
            available_until = EXCLUDED.available_until,
            updated_at = now()
    `
	if err := r.tx.RunInTenantTx(ctx, e.TenantID, func(ctx context.Context, tx Tx) error {
		return tx.Exec(ctx, q,
			e.SKU, e.TenantID, e.DisplayName, e.Description, e.PriceCents, e.Currency,
			e.SuggestedFocalAtomID, string(breedJSON),
			e.Purchasable, e.IsTrial, e.SoftExpiryDays, e.HardExpiryDays,
			e.AvailableFrom, e.AvailableUntil,
		)
	}); err != nil {
		return fmt.Errorf("pg.CatalogRepository.Upsert: %w", err)
	}
	return nil
}

// SoftDelete marks a catalog row deleted (deleted_at = now()) under the
// owning tenant's RLS session.
//
// NOTE: a SKU the tenant does not own matches zero rows and still returns
// nil. The Exec seam discards the command tag, so this cannot distinguish
// "deleted" from "no such row for you". See the caller for the response
// it produces.
func (r *CatalogRepository) SoftDelete(ctx context.Context, tenantID, sku string) error {
	const q = `
        UPDATE familiar_egg_catalog
        SET    deleted_at = now()
        WHERE  sku = $1
          AND  tenant_id = $2::uuid
          AND  deleted_at IS NULL
    `
	if err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		return tx.Exec(ctx, q, sku, tenantID)
	}); err != nil {
		return fmt.Errorf("pg.CatalogRepository.SoftDelete: %w", err)
	}
	return nil
}

func scanCatalog(row Row) (*familiareag.CatalogEntry, error) {
	var (
		e              familiareag.CatalogEntry
		breedJSON      string
		availableFrom  sql.NullTime
		availableUntil sql.NullTime
		deletedAt      sql.NullTime
	)
	err := row.Scan(
		&e.SKU, &e.TenantID, &e.DisplayName, &e.Description,
		&e.PriceCents, &e.Currency, &e.SuggestedFocalAtomID,
		&breedJSON, &e.BreedDistributionUpdated,
		&e.Purchasable, &e.IsTrial, &e.SoftExpiryDays, &e.HardExpiryDays,
		&availableFrom, &availableUntil,
		&e.CreatedAt, &e.UpdatedAt, &deletedAt,
	)
	if err != nil {
		return nil, err
	}
	dist := familiareag.BreedDistribution{}
	if strings.TrimSpace(breedJSON) != "" {
		if jerr := json.Unmarshal([]byte(breedJSON), &dist); jerr != nil {
			return nil, fmt.Errorf("scanCatalog: unmarshal breed_distribution: %w", jerr)
		}
	}
	e.BreedDistribution = dist
	if availableFrom.Valid {
		t := availableFrom.Time.UTC()
		e.AvailableFrom = &t
	}
	if availableUntil.Valid {
		t := availableUntil.Time.UTC()
		e.AvailableUntil = &t
	}
	if deletedAt.Valid {
		t := deletedAt.Time.UTC()
		e.DeletedAt = &t
	}
	return &e, nil
}
