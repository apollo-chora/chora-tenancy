//go:build integration

// transaction_ledger_rls_integration_test.go — live RLS verification for the
// CHO-1937 (Wave B1) unified transaction-ledger read projection (ADR-205).
//
// Reuses the liveDB(t) harness in integration_test.go (same pg_test package):
//
//	export CHORA_TEST_DSN_SECRET_ID=chora-dev-cloudsql-chora_tenancy-app_rw-dsn
//	export CHORA_TEST_DB_PROJECT=chora-489812
//	go test -tags integration ./internal/adapter/pg/...
//
// The app_rw DSN role is NOBYPASSRLS, so this proves the policy itself. The
// whole test runs inside ONE transaction that is ROLLED BACK — it never
// persists a row to the live DB. RLS is re-evaluated per query against the
// current `chora.tenant_id` GUC, so switching the GUC mid-transaction
// exercises cross-tenant isolation + the 'platform' span-all sentinel without
// needing committed fixtures.
package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTransactionLedger_RLS_Isolation_And_PlatformSpanAll(t *testing.T) {
	pool := liveDB(t) // t.Skip()s when no DSN configured
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := uuid.NewString()
	tenantB := uuid.NewString()
	gcid := uuid.NewString()
	ref := "pur_b1_rls_" + tenantA // unique tag so 'platform' counts only our row

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // non-destructive — never commits

	setTenant := func(tid string) {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tid)); err != nil {
			t.Fatalf("SET LOCAL chora.tenant_id=%s: %v", tid, err)
		}
	}
	countMine := func() int {
		var n int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM transaction_ledger WHERE source_ref_id = $1`, ref,
		).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Insert one row under tenant A (WITH CHECK requires GUC == row tenant).
	setTenant(tenantA)
	if _, err := tx.Exec(ctx,
		`INSERT INTO transaction_ledger
		   (occurred_at, tenant_id, learner_gcid, kind, source_domain, source_ref_id, status)
		 VALUES (now(), $1, $2, 'purchase', 'payments', $3, 'captured')`,
		tenantA, gcid, ref,
	); err != nil {
		t.Fatalf("insert under tenant A: %v", err)
	}

	// Tenant A sees it.
	setTenant(tenantA)
	if got := countMine(); got != 1 {
		t.Fatalf("tenant A: expected 1, got %d", got)
	}
	// Tenant B must NOT (cross-tenant isolation).
	setTenant(tenantB)
	if got := countMine(); got != 0 {
		t.Fatalf("tenant B leakage: expected 0, got %d", got)
	}
	// Platform sentinel spans all (operator span-all — the CHO-1931 fix).
	setTenant("platform")
	if got := countMine(); got != 1 {
		t.Fatalf("platform span-all: expected 1, got %d", got)
	}

	// WITH CHECK: under tenant A, a write for tenant B must be rejected.
	setTenant(tenantA)
	if _, err := tx.Exec(ctx,
		`INSERT INTO transaction_ledger
		   (occurred_at, tenant_id, kind, source_domain, source_ref_id, status)
		 VALUES (now(), $1, 'purchase', 'payments', $2, 'captured')`,
		tenantB, ref+"_evil",
	); err == nil {
		t.Fatalf("WITH CHECK: cross-tenant write was allowed (expected rejection)")
	}
}
