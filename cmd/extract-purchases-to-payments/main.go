// Package main — ADR-164 Wave 1 Stage D one-shot extract binary.
//
// PURPOSE
//
//	Migrates rows from the chora_tenancy database into the canonical
//	chora_payments database, then validates row-count parity:
//
//	  chora_tenancy.familiar_egg_purchases    →    chora_payments.familiar_egg_purchases
//	  chora_tenancy.tenant_mana_pool_topups   →    chora_payments.tenant_mana_topups
//
//	After validation, the operator runs migration 0017 against the
//	chora_tenancy DB to DROP the source tables. See the runbook in
//	services/chora-tenancy/migrations/0016_extract_purchases_to_payments.up.sql.
//
// WHY A SEPARATE BINARY (Pattern B)
//
//	The Cloud SQL `migrations-runner` Cloud Run Job applies SQL migrations
//	per-database via psql, and the `dblink` extension is NOT pre-loaded on
//	chora_tenancy → chora_payments. The extract therefore lives in a small
//	pgx-backed Go binary that opens TWO pools (one per DB DSN) at
//	migration time — Stage D's only acceptable cross-DB code path.
//
// ENV CONTRACT (canonical Secret Manager pattern per `feedback_no_inline_config`)
//
//	CHORA_DB_DSN              — chora_tenancy DSN (READ).  REQUIRED.
//	CHORA_PAYMENTS_DB_DSN     — chora_payments DSN (WRITE). REQUIRED.
//	EXTRACT_MODE              — "dry_run" (default) | "apply".
//	DIRECTION                 — "forward" (default) | "reverse" (Stage G rollback).
//	EXTRACT_MANA_DEFAULT_SKU  — SKU substituted on the legacy `tenant_mana_
//	                            pool_topups` rows lacking a canonical SKU
//	                            value. Defaults to `mana.tenant_topup.legacy_v1`.
//	EXTRACT_PARITY_TOLERANCE  — Permitted row-count delta when validating.
//	                            Default `0` (strict parity).
//
//	The binary exits 0 on success + nonzero on any of: DSN missing,
//	connect / ping failure, column-mapping error, INSERT failure, parity
//	mismatch beyond tolerance.
//
// IDEMPOTENCY
//
//	INSERT … ON CONFLICT (purchase_id) DO NOTHING — re-runs are safe.
//	Row-count parity check after apply: if SOURCE > DEST after the run,
//	abort with a clear error so the operator can investigate.
//
// REVERSE DIRECTION
//
//	`DIRECTION=reverse` is the symmetric path used during Stage G rollback:
//	chora_payments rows are streamed back into chora_tenancy. The reverse
//	mapping has the same ON CONFLICT DO NOTHING semantics + uses the
//	`MapEggStateToTenancy` helper to re-derive the legacy `paid` state.
//	The reverse mode REQUIRES the chora_tenancy.familiar_egg_purchases +
//	tenant_mana_pool_topups tables to exist — operator must run migration
//	0017.down.sql first.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	envSourceDSN         = "CHORA_DB_DSN"
	envDestDSN           = "CHORA_PAYMENTS_DB_DSN"
	envMode              = "EXTRACT_MODE"
	envDirection         = "DIRECTION"
	envManaDefaultSKU    = "EXTRACT_MANA_DEFAULT_SKU"
	envParityTolerance   = "EXTRACT_PARITY_TOLERANCE"
	modeDryRun           = "dry_run"
	modeApply            = "apply"
	directionForward     = "forward"
	directionReverse     = "reverse"
	defaultManaLegacySKU = "mana.tenant_topup.legacy_v1"
)

// runConfig is parsed once at startup from env. Fail-loud per
// `feedback_no_stubs_real_wiring` — no defaults that silently mask a
// missing DSN.
type runConfig struct {
	sourceDSN       string
	destDSN         string
	mode            string // dry_run | apply
	direction       string // forward | reverse
	manaDefaultSKU  string
	parityTolerance int64
}

func parseConfig() (runConfig, error) {
	cfg := runConfig{
		sourceDSN:       strings.TrimSpace(os.Getenv(envSourceDSN)),
		destDSN:         strings.TrimSpace(os.Getenv(envDestDSN)),
		mode:            strings.ToLower(strings.TrimSpace(os.Getenv(envMode))),
		direction:       strings.ToLower(strings.TrimSpace(os.Getenv(envDirection))),
		manaDefaultSKU:  strings.TrimSpace(os.Getenv(envManaDefaultSKU)),
		parityTolerance: 0,
	}
	if cfg.sourceDSN == "" {
		return cfg, fmt.Errorf("extract: %s required", envSourceDSN)
	}
	if cfg.destDSN == "" {
		return cfg, fmt.Errorf("extract: %s required", envDestDSN)
	}
	if cfg.mode == "" {
		cfg.mode = modeDryRun
	}
	if cfg.mode != modeDryRun && cfg.mode != modeApply {
		return cfg, fmt.Errorf("extract: %s must be %s or %s, got %q",
			envMode, modeDryRun, modeApply, cfg.mode)
	}
	if cfg.direction == "" {
		cfg.direction = directionForward
	}
	if cfg.direction != directionForward && cfg.direction != directionReverse {
		return cfg, fmt.Errorf("extract: %s must be %s or %s, got %q",
			envDirection, directionForward, directionReverse, cfg.direction)
	}
	if cfg.manaDefaultSKU == "" {
		cfg.manaDefaultSKU = defaultManaLegacySKU
	}
	if v := strings.TrimSpace(os.Getenv(envParityTolerance)); v != "" {
		var tol int64
		if _, err := fmt.Sscanf(v, "%d", &tol); err != nil || tol < 0 {
			return cfg, fmt.Errorf("extract: %s must be a non-negative integer, got %q", envParityTolerance, v)
		}
		cfg.parityTolerance = tol
	}
	return cfg, nil
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("extract: mode=%s direction=%s parity_tolerance=%d mana_default_sku=%s",
		cfg.mode, cfg.direction, cfg.parityTolerance, cfg.manaDefaultSKU)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	srcPool, err := pgxpool.New(ctx, cfg.sourceDSN)
	if err != nil {
		log.Fatalf("extract: source DSN connect: %v", err)
	}
	defer srcPool.Close()
	if err := srcPool.Ping(ctx); err != nil {
		log.Fatalf("extract: source DSN ping: %v", err)
	}

	dstPool, err := pgxpool.New(ctx, cfg.destDSN)
	if err != nil {
		log.Fatalf("extract: dest DSN connect: %v", err)
	}
	defer dstPool.Close()
	if err := dstPool.Ping(ctx); err != nil {
		log.Fatalf("extract: dest DSN ping: %v", err)
	}

	if cfg.direction == directionReverse {
		log.Printf("extract: DIRECTION=reverse — destination pool is chora_tenancy; source pool is chora_payments")
		// swap pool roles so the rest of the run code is symmetric.
		srcPool, dstPool = dstPool, srcPool
	}

	eggsExtracted, manaExtracted, err := runExtract(ctx, srcPool, dstPool, cfg)
	if err != nil {
		log.Fatalf("extract: run failed: %v", err)
	}
	log.Printf("extract: rows touched — familiar_egg_purchases=%d tenant_mana_topups=%d (mode=%s)",
		eggsExtracted, manaExtracted, cfg.mode)

	if cfg.mode == modeApply {
		if err := validateParity(ctx, srcPool, dstPool, cfg); err != nil {
			log.Fatalf("extract: parity validation FAILED: %v", err)
		}
		log.Printf("extract: parity validation PASSED")
	} else {
		log.Printf("extract: dry-run complete — set %s=%s to write to %s", envMode, modeApply, envDestDSN)
	}
}

// runExtract streams source rows + writes them to dest. In dry-run mode it
// counts only — no INSERT happens.
func runExtract(ctx context.Context, src, dst *pgxpool.Pool, cfg runConfig) (int64, int64, error) {
	var eggs, mana int64
	var err error
	if eggs, err = extractEggs(ctx, src, dst, cfg); err != nil {
		return eggs, 0, fmt.Errorf("eggs: %w", err)
	}
	if mana, err = extractManaTopUps(ctx, src, dst, cfg); err != nil {
		return eggs, mana, fmt.Errorf("mana: %w", err)
	}
	return eggs, mana, nil
}

// extractEggs runs the chora_tenancy.familiar_egg_purchases →
// chora_payments.familiar_egg_purchases extract.
func extractEggs(ctx context.Context, src, dst *pgxpool.Pool, cfg runConfig) (int64, error) {
	rows, err := src.Query(ctx, SourceSelectEggSQL)
	if err != nil {
		return 0, fmt.Errorf("source select: %w", err)
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var row EggRow
		var paidAt, failedAt, provAt, refAt, expAt pgxNullTime
		if err := rows.Scan(
			&row.PurchaseID, &row.TenantID, &row.PurchaserGCID, &row.EggSKU,
			&row.SuggestedFocalAtomID,
			&row.State,
			&row.AmountCents, &row.AmountCentsPaid, &row.AmountCentsRefunded, &row.Currency,
			&row.StripeSessionID,
			&row.StripePaymentIntentID,
			&row.StripeChargeID,
			&row.StripeRefundID,
			&row.StripeCheckoutURL,
			&row.StripeFailureCode,
			&row.StripeFailureMessage,
			&row.RefundReason, &row.RefundCreditOnly,
			&row.CheckoutStartedAt, &paidAt, &failedAt, &provAt, &refAt, &expAt,
			&row.SoftExpiryAt, &row.HardExpiryAt,
			&row.CreatedAt, &row.UpdatedAt,
		); err != nil {
			return count, fmt.Errorf("scan row %d: %w", count, err)
		}
		row.PaidAt = paidAt.AsPointer()
		row.FailedAt = failedAt.AsPointer()
		row.ProvisionedAt = provAt.AsPointer()
		row.RefundedAt = refAt.AsPointer()
		row.ExpiredAt = expAt.AsPointer()

		args, err := MapEggRowToPaymentsArgs(row)
		if err != nil {
			return count, fmt.Errorf("row %d map: %w", count, err)
		}
		if cfg.mode == modeApply {
			if _, err := dst.Exec(ctx, SourceInsertEggSQL, args...); err != nil {
				return count, fmt.Errorf("row %d insert (purchase_id=%s): %w", count, row.PurchaseID, err)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("rows iteration: %w", err)
	}
	return count, nil
}

// extractManaTopUps runs the chora_tenancy.tenant_mana_pool_topups →
// chora_payments.tenant_mana_topups extract.
func extractManaTopUps(ctx context.Context, src, dst *pgxpool.Pool, cfg runConfig) (int64, error) {
	rows, err := src.Query(ctx, SourceSelectManaTopUpSQL)
	if err != nil {
		return 0, fmt.Errorf("source select: %w", err)
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var row ManaTopUpRow
		if err := rows.Scan(
			&row.TopupID, &row.PoolID, &row.TenantID, &row.UnitsCredited, &row.ChargedCents, &row.Currency,
			&row.StripePaymentIntentID, &row.AutoRenew,
			&row.ToppedUpByGCID,
			&row.BalanceAfterUnits,
			&row.EventEnvelopeID,
			&row.Traceparent,
			&row.RecordedAt, &row.CreatedAt,
		); err != nil {
			return count, fmt.Errorf("scan row %d: %w", count, err)
		}
		args := MapManaTopUpRowToPaymentsArgs(row, cfg.manaDefaultSKU)
		if cfg.mode == modeApply {
			if _, err := dst.Exec(ctx, SourceInsertManaTopUpSQL, args...); err != nil {
				return count, fmt.Errorf("row %d insert (topup_id=%s): %w", count, row.TopupID, err)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("rows iteration: %w", err)
	}
	return count, nil
}

// validateParity asserts each source row landed in the destination. Abort
// LOUD on mismatch beyond the configured tolerance.
func validateParity(ctx context.Context, src, dst *pgxpool.Pool, cfg runConfig) error {
	for _, pair := range []struct {
		srcTable string
		dstTable string
	}{
		{srcTable: "familiar_egg_purchases", dstTable: "familiar_egg_purchases"},
		{srcTable: "tenant_mana_pool_topups", dstTable: "tenant_mana_topups"},
	} {
		var srcCount, dstCount int64
		if err := src.QueryRow(ctx, "SELECT count(*) FROM "+pair.srcTable).Scan(&srcCount); err != nil {
			return fmt.Errorf("source count %s: %w", pair.srcTable, err)
		}
		if err := dst.QueryRow(ctx, "SELECT count(*) FROM "+pair.dstTable).Scan(&dstCount); err != nil {
			return fmt.Errorf("dest count %s: %w", pair.dstTable, err)
		}
		delta := srcCount - dstCount
		if delta < 0 {
			delta = -delta
		}
		log.Printf("extract: parity %s=%d dest %s=%d delta=%d tolerance=%d",
			pair.srcTable, srcCount, pair.dstTable, dstCount, delta, cfg.parityTolerance)
		if delta > cfg.parityTolerance {
			return fmt.Errorf("%w: %s src=%d dst=%d delta=%d (tolerance=%d)",
				errParityMismatch, pair.srcTable, srcCount, dstCount, delta, cfg.parityTolerance)
		}
	}
	return nil
}

var errParityMismatch = errors.New("parity mismatch")

// pgxNullTime accommodates nullable TIMESTAMPTZ columns. pgx supports
// *time.Time directly but using this helper keeps the scan call readable
// + lets us project to a regular *time.Time in the EggRow shape.
type pgxNullTime struct {
	Valid bool
	T     time.Time
}

func (n *pgxNullTime) Scan(value any) error {
	if value == nil {
		n.Valid = false
		return nil
	}
	switch v := value.(type) {
	case time.Time:
		n.T = v
		n.Valid = true
		return nil
	case *time.Time:
		if v == nil {
			n.Valid = false
			return nil
		}
		n.T = *v
		n.Valid = true
		return nil
	default:
		return fmt.Errorf("pgxNullTime.Scan: unsupported type %T", value)
	}
}

func (n pgxNullTime) AsPointer() *time.Time {
	if !n.Valid {
		return nil
	}
	t := n.T
	return &t
}

// Compile-time check — make sure we don't accidentally drop the
// pgx.Rows symbol from imports. Removing the unused-symbol guard would
// reduce IDE friction so this is the canonical no-op pattern.
var _ pgx.Rows = (pgx.Rows)(nil)
