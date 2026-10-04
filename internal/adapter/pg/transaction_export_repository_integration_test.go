//go:build integration

// transaction_export_repository_integration_test.go — live SQL behaviour of the
// B5 (CHO-1941) export-job store. Asserts create→get, the claim flip
// (pending→building, dup-free), mark-ready, RLS tenant isolation, and the
// span-all (nil-uuid → 'platform') job visibility.
//
//	export CHORA_TEST_DSN=postgres://chora_tenancy_app_rw:...@.../chora_tenancy
//	go test -tags integration -run TestTransactionExportRepo ./internal/adapter/pg/...
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

func cleanupExportJobs(t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Commit(ctx) }()
	// 'platform' sees all rows under the sentinel policy → covers nil-uuid jobs.
	if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = 'platform'"); err != nil {
		return
	}
	_, _ = tx.Exec(ctx, `DELETE FROM transaction_export_jobs WHERE owner_gcid = $1::uuid`, tenant)
}

func TestTransactionExportRepo(t *testing.T) {
	pool := liveDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	repo := pg.NewTransactionExportRepo(pg.NewPgxPoolQuerier(pool))

	tA := uuid.NewString()
	tB := uuid.NewString()
	owner := uuid.NewString() // also the cleanup key
	defer cleanupExportJobs(t, pool, owner)

	win := tl.ReadFilters{From: time.Now().Add(-24 * time.Hour).UTC(), To: time.Now().UTC(), Sort: tl.ParseSort("amount:desc")}

	// ── tenant-scoped job: create → get ──────────────────────────────────
	created, err := repo.Create(ctx, tl.NewExportJob{
		TenantID: tA, OwnerGCID: owner, LearnerGCID: owner, Scope: "learner",
		Format: tl.FormatCSV, Filters: win,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.JobID == "" || created.Status != tl.ExportPending {
		t.Fatalf("created job wrong: %+v", created)
	}

	got, found, err := repo.Get(ctx, tA, created.JobID, owner)
	if err != nil || !found {
		t.Fatalf("Get: err=%v found=%v", err, found)
	}
	if got.Format != tl.FormatCSV || got.Scope != "learner" || got.Filters.Sort.Field != tl.SortByAmount {
		t.Fatalf("get round-trip wrong: %+v / sort=%+v", got, got.Filters.Sort)
	}

	// RLS: tenant tB cannot see tA's job.
	_, foundB, err := repo.Get(ctx, tB, created.JobID, "")
	if err != nil {
		t.Fatalf("Get under tB: %v", err)
	}
	if foundB {
		t.Fatal("RLS leak: tB saw tA's export job")
	}

	// Wrong-learner constraint: another learner in tA cannot fetch it.
	_, foundOther, err := repo.Get(ctx, tA, created.JobID, uuid.NewString())
	if err != nil {
		t.Fatalf("Get wrong-learner: %v", err)
	}
	if foundOther {
		t.Fatal("learner constraint leak: another learner saw the job")
	}

	// ── span-all (master) job: nil-uuid tenant, visible under 'platform' ──
	spanAll, err := repo.Create(ctx, tl.NewExportJob{
		TenantID: tl.NilTenantUUID, OwnerGCID: owner, Scope: "master",
		Format: tl.FormatJSON, Filters: win,
	})
	if err != nil {
		t.Fatalf("Create span-all: %v", err)
	}
	_, foundSA, err := repo.Get(ctx, tl.PlatformScope, spanAll.JobID, "")
	if err != nil || !foundSA {
		t.Fatalf("Get span-all under platform: err=%v found=%v", err, foundSA)
	}

	// ── claim flips pending→building, dup-free ───────────────────────────
	claimed, err := repo.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	seen := map[string]bool{}
	gotMine := 0
	for _, j := range claimed {
		if seen[j.JobID] {
			t.Fatalf("claim returned duplicate %s", j.JobID)
		}
		seen[j.JobID] = true
		if j.Status != tl.ExportBuilding {
			t.Errorf("claimed job %s status = %s; want building", j.JobID, j.Status)
		}
		if j.OwnerGCID == owner {
			gotMine++
		}
	}
	if gotMine < 2 { // our 2 pending jobs must have been claimed
		t.Fatalf("expected >=2 of our jobs claimed, got %d", gotMine)
	}

	// Re-claim immediately returns neither (both now 'building' within the
	// reservation window).
	again, err := repo.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending #2: %v", err)
	}
	for _, j := range again {
		if j.JobID == created.JobID || j.JobID == spanAll.JobID {
			t.Fatalf("job %s re-claimed inside the reservation window", j.JobID)
		}
	}

	// ── mark ready → get reflects it ─────────────────────────────────────
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	if err := repo.MarkReady(ctx, created.JobID, "exports/x.csv", "https://signed/x", exp); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	ready, _, err := repo.Get(ctx, tA, created.JobID, owner)
	if err != nil {
		t.Fatalf("Get after ready: %v", err)
	}
	if ready.Status != tl.ExportReady || ready.DownloadURL != "https://signed/x" {
		t.Fatalf("ready job wrong: %+v", ready)
	}
	if !ready.ExpiresAt.Equal(exp) {
		t.Errorf("expires_at = %v; want %v", ready.ExpiresAt, exp)
	}

	// ── mark failed ──────────────────────────────────────────────────────
	if err := repo.MarkFailed(ctx, spanAll.JobID, "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	failed, _, err := repo.Get(ctx, tl.PlatformScope, spanAll.JobID, "")
	if err != nil {
		t.Fatalf("Get after failed: %v", err)
	}
	if failed.Status != tl.ExportFailed || failed.Error != "boom" {
		t.Fatalf("failed job wrong: %+v", failed)
	}
}
