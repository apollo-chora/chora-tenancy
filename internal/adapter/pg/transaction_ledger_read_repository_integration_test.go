//go:build integration

// transaction_ledger_read_repository_integration_test.go — live SQL behaviour
// of the B3 (CHO-1939) projection read repo. Seeds via the B2 write repo (real
// write→read round-trip) across two tenants + two learners, then asserts:
//
//   - tenant scope isolation (RLS pins the caller tenant)
//
//   - learner scope (own learner_gcid only)
//
//   - master span-all ("platform" sentinel) returns every tenant
//
//   - master single-franchisee narrows to one tenant
//
//   - keyset cursor pagination (occurred_at desc) is complete + dup-free
//
//   - amount sort orders by the unified magnitude
//
//   - summary KPIs (counts by kind/status, per-currency net, mana up/spent)
//
//   - mana_spend_daily drill-down (parent + per-call detail)
//
//     export CHORA_TEST_DSN=postgres://chora_tenancy_app_rw:...@.../chora_tenancy
//     go test -tags integration -run TestTransactionLedgerReadRepo ./internal/adapter/pg/...
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

func TestTransactionLedgerReadRepo(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	querier := pg.NewPgxPoolQuerier(pool)
	write := pg.NewTransactionLedgerWriteRepo(querier)
	read := pg.NewTransactionLedgerReadRepo(querier)

	tA := uuid.NewString()
	tB := uuid.NewString()
	g1 := uuid.NewString()
	g2 := uuid.NewString()
	gB := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)
	defer cleanupTenant(t, pool, tA)
	defer cleanupTenant(t, pool, tB)

	// ── seed tenant A: 2 purchases, 1 mana top-up, 1 spend-rollup ────────
	mustUpsert(t, ctx, write, tl.Entry{
		OccurredAt: now.Add(-72 * time.Hour), TenantID: tA, LearnerGCID: g1,
		Kind: tl.KindPurchase, SourceDomain: tl.SourcePayments, SourceRefID: "pA1",
		Label: "Course A1", Currency: "sgd", AmountMinor: 4999, Status: tl.StatusCaptured,
	})
	mustUpsert(t, ctx, write, tl.Entry{
		OccurredAt: now.Add(-48 * time.Hour), TenantID: tA, LearnerGCID: g1,
		Kind: tl.KindManaTopup, SourceDomain: tl.SourcePayments, SourceRefID: "mtA1",
		Label: "Mana top-up", Currency: "sgd", AmountMinor: 1000, ManaUnits: 500, Status: tl.StatusCaptured,
	})
	mustUpsert(t, ctx, write, tl.Entry{
		OccurredAt: now.Add(-24 * time.Hour), TenantID: tA, LearnerGCID: g2,
		Kind: tl.KindPurchase, SourceDomain: tl.SourcePayments, SourceRefID: "pA2",
		Label: "Course A2", Currency: "sgd", AmountMinor: 2999, Status: tl.StatusCaptured,
	})
	if err := write.AccumulateSpend(ctx,
		tl.Entry{
			OccurredAt: now.Add(-12 * time.Hour), TenantID: tA, LearnerGCID: g1,
			SourceDomain: tl.SourceObservability, SourceRefID: "2026-06-20",
			Label: "Mana spent 2026-06-20", ManaUnits: -30, Status: tl.StatusPosted,
		},
		tl.Detail{
			OccurredAt: now.Add(-12 * time.Hour), TenantID: tA, LearnerGCID: g1,
			ActionCode: "question_generation", ManaUnits: -30, Model: "gemini-2.5-flash",
		}); err != nil {
		t.Fatalf("AccumulateSpend: %v", err)
	}

	// ── seed tenant B: 1 purchase ────────────────────────────────────────
	mustUpsert(t, ctx, write, tl.Entry{
		OccurredAt: now.Add(-24 * time.Hour), TenantID: tB, LearnerGCID: gB,
		Kind: tl.KindPurchase, SourceDomain: tl.SourcePayments, SourceRefID: "pB1",
		Label: "Course B1", Currency: "usd", AmountMinor: 999, Status: tl.StatusCaptured,
	})

	win := tl.ReadFilters{From: now.Add(-365 * 24 * time.Hour), To: now.Add(time.Hour), Sort: tl.ParseSort("")}

	// ── tenant scope isolation ───────────────────────────────────────────
	tenantPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tA, Filters: win, Limit: 100})
	if err != nil {
		t.Fatalf("List tenant A: %v", err)
	}
	if len(tenantPage.Items) != 4 {
		t.Fatalf("tenant A scope: expected 4 rows, got %d", len(tenantPage.Items))
	}
	for _, it := range tenantPage.Items {
		if it.TenantID != tA {
			t.Fatalf("tenant A scope leaked tenant %s", it.TenantID)
		}
	}
	// newest-first
	if tenantPage.Items[0].SourceRefID != "2026-06-20" || tenantPage.Items[3].SourceRefID != "pA1" {
		t.Fatalf("occurred_at:desc order wrong: %s..%s", tenantPage.Items[0].SourceRefID, tenantPage.Items[3].SourceRefID)
	}

	// ── learner scope (g1 only) ──────────────────────────────────────────
	learnerFilters := win
	learnerFilters.LearnerGCIDs = []string{g1}
	learnerPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tA, Filters: learnerFilters, Limit: 100})
	if err != nil {
		t.Fatalf("List learner g1: %v", err)
	}
	if len(learnerPage.Items) != 3 { // pA1, mtA1, spend (NOT pA2 which is g2)
		t.Fatalf("learner g1 scope: expected 3 rows, got %d", len(learnerPage.Items))
	}
	for _, it := range learnerPage.Items {
		if it.LearnerGCID != g1 {
			t.Fatalf("learner scope leaked gcid %s", it.LearnerGCID)
		}
	}

	// ── multi-learner drill (g1 + g2) — OR over the set (ADR-205/CHO-1930) ──
	multiFilters := win
	multiFilters.LearnerGCIDs = []string{g1, g2}
	multiPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tA, Filters: multiFilters, Limit: 100})
	if err != nil {
		t.Fatalf("List learner [g1,g2]: %v", err)
	}
	// g1 has 3 rows, g2 has 1 (pA2) → the union is all 4 tenant-A rows.
	if len(multiPage.Items) != 4 {
		t.Fatalf("multi-learner [g1,g2]: expected 4 rows, got %d", len(multiPage.Items))
	}
	for _, it := range multiPage.Items {
		if it.LearnerGCID != g1 && it.LearnerGCID != g2 {
			t.Fatalf("multi-learner scope leaked gcid %s", it.LearnerGCID)
		}
	}

	// ── master span-all ("platform") sees both tenants ───────────────────
	allPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tl.PlatformScope, Filters: win, Limit: 100})
	if err != nil {
		t.Fatalf("List span-all: %v", err)
	}
	if !containsTenant(allPage.Items, tA) || !containsTenant(allPage.Items, tB) {
		t.Fatalf("span-all missing a tenant: A=%v B=%v", containsTenant(allPage.Items, tA), containsTenant(allPage.Items, tB))
	}

	// ── master single-franchisee narrows to tB ───────────────────────────
	bPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tB, Filters: win, Limit: 100})
	if err != nil {
		t.Fatalf("List franchisee tB: %v", err)
	}
	if len(bPage.Items) != 1 || bPage.Items[0].SourceRefID != "pB1" {
		t.Fatalf("franchisee tB: expected [pB1], got %d rows", len(bPage.Items))
	}

	// ── cursor pagination (tenant A, page size 2) is complete + dup-free ──
	// The repo honours any positive limit; the gRPC layer normalises page
	// size to the allowed {10,20,50,100} set upstream.
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		p, perr := read.List(ctx, tl.ListQuery{GUCTenantID: tA, Filters: win, Cursor: cursor, Limit: 2})
		if perr != nil {
			t.Fatalf("paged List: %v", perr)
		}
		for _, it := range p.Items {
			if seen[it.LedgerID] {
				t.Fatalf("cursor returned duplicate ledger_id %s", it.LedgerID)
			}
			seen[it.LedgerID] = true
		}
		pages++
		if p.NextPageToken == "" {
			break
		}
		cursor = p.NextPageToken
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 4 {
		t.Fatalf("cursor pagination saw %d distinct rows; want 4", len(seen))
	}

	// ── amount sort (desc): 4999, 2999, 1000, 30 ─────────────────────────
	amtFilters := win
	amtFilters.Sort = tl.ParseSort("amount:desc")
	amtPage, err := read.List(ctx, tl.ListQuery{GUCTenantID: tA, Filters: amtFilters, Limit: 100})
	if err != nil {
		t.Fatalf("List amount:desc: %v", err)
	}
	wantOrder := []string{"pA1", "pA2", "mtA1", "2026-06-20"}
	for i, ref := range wantOrder {
		if amtPage.Items[i].SourceRefID != ref {
			t.Fatalf("amount:desc[%d] = %s; want %s", i, amtPage.Items[i].SourceRefID, ref)
		}
	}

	// ── summary (tenant A) ───────────────────────────────────────────────
	sum, err := read.GetSummary(ctx, tl.SummaryQuery{GUCTenantID: tA, Filters: win})
	if err != nil {
		t.Fatalf("GetSummary: %v", err)
	}
	if sum.TotalCount != 4 {
		t.Fatalf("summary total_count = %d; want 4", sum.TotalCount)
	}
	if sum.CountByKind[tl.KindPurchase] != 2 || sum.CountByKind[tl.KindManaTopup] != 1 || sum.CountByKind[tl.KindManaSpendDaily] != 1 {
		t.Fatalf("summary count_by_kind = %v", sum.CountByKind)
	}
	if sum.AmountMinorByCurrency["sgd"] != 8998 { // 4999 + 1000 + 2999
		t.Fatalf("summary sgd net = %d; want 8998", sum.AmountMinorByCurrency["sgd"])
	}
	if sum.TotalManaToppedUp != 500 {
		t.Fatalf("summary mana topped up = %d; want 500", sum.TotalManaToppedUp)
	}
	if sum.TotalManaSpent != 30 {
		t.Fatalf("summary mana spent = %d; want 30", sum.TotalManaSpent)
	}

	// ── drill-down: mana_spend_daily parent + per-call detail ────────────
	var spendLedgerID string
	for _, it := range tenantPage.Items {
		if it.Kind == tl.KindManaSpendDaily {
			spendLedgerID = it.LedgerID
			if !it.HasDetail {
				t.Fatalf("mana_spend_daily row has_detail=false; want true")
			}
		}
	}
	if spendLedgerID == "" {
		t.Fatal("no mana_spend_daily row found to drill")
	}
	detail, err := read.GetDetail(ctx, tl.DetailQuery{GUCTenantID: tA, LedgerID: spendLedgerID, Limit: 50})
	if err != nil {
		t.Fatalf("GetDetail: %v", err)
	}
	if !detail.Found {
		t.Fatal("GetDetail parent not found")
	}
	if len(detail.Details) != 1 || detail.Details[0].ActionCode != "question_generation" {
		t.Fatalf("GetDetail details = %+v; want 1 question_generation row", detail.Details)
	}

	// non-existent ledger id → Found=false (no error).
	missing, err := read.GetDetail(ctx, tl.DetailQuery{GUCTenantID: tA, LedgerID: uuid.NewString(), Limit: 50})
	if err != nil {
		t.Fatalf("GetDetail missing: %v", err)
	}
	if missing.Found {
		t.Fatal("GetDetail on missing ledger_id reported Found=true")
	}
}

func mustUpsert(t *testing.T, ctx context.Context, w *pg.TransactionLedgerWriteRepo, e tl.Entry) {
	t.Helper()
	if err := w.UpsertPurchase(ctx, e); err != nil {
		t.Fatalf("UpsertPurchase(%s): %v", e.SourceRefID, err)
	}
}

func containsTenant(rows []tl.LedgerRow, tenant string) bool {
	for _, r := range rows {
		if r.TenantID == tenant {
			return true
		}
	}
	return false
}
