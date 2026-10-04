//go:build integration

// transaction_ledger_write_repository_integration_test.go — live SQL behaviour
// of the B2 (CHO-1938) projection write repo. Reuses liveDB(t).
//
//	export CHORA_TEST_DSN=postgres://...              # ephemeral or app_rw DSN
//	go test -tags integration -run TestTransactionLedgerWriteRepo ./internal/adapter/pg/...
//
// Asserts the SQL contract: idempotent purchase upsert, sticky refund,
// accumulate-spend day rollup + per-call detail. Scopes everything to a unique
// tenant and cleans up so the live DB is left untouched.
package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

func TestTransactionLedgerWriteRepo(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := pg.NewTransactionLedgerWriteRepo(pg.NewPgxPoolQuerier(pool))
	tenant := uuid.NewString()
	gcid := uuid.NewString()
	now := time.Now().UTC()

	defer cleanupTenant(t, pool, tenant)

	// scalar reads back the value under the tenant's RLS context.
	scalar := func(dest any, sql string, args ...any) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin read: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tenant)); err != nil {
			t.Fatalf("set tenant: %v", err)
		}
		if err := tx.QueryRow(ctx, sql, args...).Scan(dest); err != nil {
			t.Fatalf("read %q: %v", sql, err)
		}
	}

	// ── Purchase: idempotent upsert ──────────────────────────────────
	purchase := transactionledger.Entry{
		OccurredAt: now, TenantID: tenant, LearnerGCID: gcid,
		Kind: transactionledger.KindPurchase, SourceDomain: transactionledger.SourcePayments,
		SourceRefID: "pur_idem", Label: "Course X", Currency: "sgd",
		AmountMinor: 4999, Status: transactionledger.StatusCaptured,
	}
	for i := 0; i < 2; i++ { // re-emit ⇒ still one row
		if err := repo.UpsertPurchase(ctx, purchase); err != nil {
			t.Fatalf("UpsertPurchase #%d: %v", i, err)
		}
	}
	var n int
	scalar(&n, `SELECT count(*) FROM transaction_ledger WHERE source_ref_id=$1`, "pur_idem")
	if n != 1 {
		t.Fatalf("idempotent upsert: expected 1 row, got %d", n)
	}

	// Refund flips status; re-emitting captured must NOT revert (sticky).
	refunded := purchase
	refunded.Status = transactionledger.StatusRefunded
	if err := repo.UpsertPurchase(ctx, refunded); err != nil {
		t.Fatalf("UpsertPurchase refund: %v", err)
	}
	if err := repo.UpsertPurchase(ctx, purchase); err != nil { // late captured redelivery
		t.Fatalf("UpsertPurchase late-captured: %v", err)
	}
	var status string
	scalar(&status, `SELECT status FROM transaction_ledger WHERE source_ref_id=$1`, "pur_idem")
	if status != transactionledger.StatusRefunded {
		t.Fatalf("refund stickiness: expected refunded, got %q", status)
	}

	// ── Spend: accumulate day rollup + append detail ─────────────────
	day := "2026-06-29"
	spend := transactionledger.Entry{
		OccurredAt: now, TenantID: tenant, LearnerGCID: gcid,
		Kind: transactionledger.KindManaSpendDaily, SourceDomain: transactionledger.SourceObservability,
		SourceRefID: day, Label: "Mana " + day, ManaUnits: -40,
		Status: transactionledger.StatusPosted,
	}
	det := transactionledger.Detail{
		OccurredAt: now, TenantID: tenant, LearnerGCID: gcid,
		ActionCode: "question_generation", ManaUnits: -40, Model: "gemini-2.5",
	}
	if err := repo.AccumulateSpend(ctx, spend, det); err != nil {
		t.Fatalf("AccumulateSpend #1: %v", err)
	}
	spend.ManaUnits, det.ManaUnits = -60, -60
	det.ActionCode = "oe_grading"
	if err := repo.AccumulateSpend(ctx, spend, det); err != nil {
		t.Fatalf("AccumulateSpend #2: %v", err)
	}

	scalar(&n, `SELECT count(*) FROM transaction_ledger WHERE kind='mana_spend_daily' AND source_ref_id=$1`, day)
	if n != 1 {
		t.Fatalf("spend rollup: expected 1 day row, got %d", n)
	}
	var mana int64
	scalar(&mana, `SELECT mana_units FROM transaction_ledger WHERE kind='mana_spend_daily' AND source_ref_id=$1`, day)
	if mana != -100 {
		t.Fatalf("spend accumulate: expected -100, got %d", mana)
	}
	scalar(&n, `SELECT count(*) FROM transaction_ledger_detail d JOIN transaction_ledger l ON l.ledger_id=d.ledger_id WHERE l.source_ref_id=$1`, day)
	if n != 2 {
		t.Fatalf("detail append: expected 2 detail rows, got %d", n)
	}
}

func cleanupTenant(t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Logf("cleanup begin: %v", err)
		return
	}
	defer func() { _ = tx.Commit(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tenant)); err != nil {
		t.Logf("cleanup set tenant: %v", err)
		return
	}
	// detail cascades from the ledger delete (FK ON DELETE CASCADE).
	if _, err := tx.Exec(ctx, `DELETE FROM transaction_ledger WHERE tenant_id = $1::uuid`, tenant); err != nil {
		t.Logf("cleanup delete: %v", err)
	}
}
