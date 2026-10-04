// mana_grant_repository_test.go: the assertion the fix exists for.
//
// Two live grants of 200 units each moved the pool from 41000 to 40600 and
// reached nobody, because the debit was durable and the allocation record was
// an in-process map. The whole point of SaveGrant is that the balance must
// not move unless the record of WHERE it went moves with it.
//
// So these tests are not about the happy path. They are about proving that a
// failure at the allocation insert or at the outbox insert aborts INSIDE the
// transaction callback, which is what makes runInSessionTx's deferred
// Rollback discard the debit. Stated narrowly: these prove the abort and the
// single-transaction seam. The rollback itself is Postgres's, exercised by
// the existing RunInTenantTx machinery, and is not re-proved here.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	poolpkg "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// grantTx records every statement and can be told to fail a chosen one.
type grantTx struct {
	stmts   []string
	execSQL []string
	// failOn: a substring; the matching statement returns failWith.
	failOn   string
	failWith error
	// allocConflict makes the allocation insert behave as ON CONFLICT DO
	// NOTHING (no row returned).
	allocConflict bool
	// allocExists makes the step-0 durable idempotency probe find a row.
	allocExists bool
}

func (t *grantTx) record(sql string) { t.stmts = append(t.stmts, sql) }

func (t *grantTx) Exec(_ context.Context, sql string, _ ...any) error {
	t.record(sql)
	t.execSQL = append(t.execSQL, sql)
	if t.failOn != "" && strings.Contains(sql, t.failOn) {
		return t.failWith
	}
	return nil
}

func (t *grantTx) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	t.record(sql)
	return nil, errors.New("grantTx.Query: not used by SaveGrant")
}

func (t *grantTx) QueryRow(_ context.Context, sql string, _ ...any) pg.Row {
	t.record(sql)
	if t.failOn != "" && strings.Contains(sql, t.failOn) {
		return &mpRow{fn: func(...any) error { return t.failWith }}
	}
	switch {
	case strings.Contains(sql, "SELECT allocation_id::text"):
		return &mpRow{fn: func(dest ...any) error {
			if t.allocExists {
				*(dest[0].(*string)) = gAlloc
				return nil
			}
			return pg.ErrNoRows
		}}
	case strings.Contains(sql, "UPDATE tenant_mana_pools"):
		return &mpRow{fn: func(dest ...any) error {
			*(dest[0].(*int64)) = 9
			return nil
		}}
	case strings.Contains(sql, "INSERT INTO tenant_mana_allocations"):
		return &mpRow{fn: func(dest ...any) error {
			if t.allocConflict {
				return pg.ErrNoRows
			}
			*(dest[0].(*string)) = "01991111-0000-7000-8000-0000000aaaa1"
			return nil
		}}
	}
	return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
}

type grantQuerier struct {
	tx      *grantTx
	tenants []string
}

func (q *grantQuerier) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	q.tenants = append(q.tenants, tenantID)
	if q.tx == nil {
		q.tx = &grantTx{}
	}
	return fn(ctx, q.tx)
}

const (
	gTenant = "11111111-1111-7111-8111-111111111111"
	gPool   = "01a0322b-b131-7aca-ae07-b1e112af8289"
	gActor  = "00000000-0000-7000-8000-000000001999"
	gAlloc  = "01991111-0000-7000-8000-0000000aaaa1"
)

func grantFixtures(t *testing.T) (*poolpkg.TenantManaPool, *allocation.TenantManaAllocation, pg.GrantEvent) {
	t.Helper()
	p, err := poolpkg.NewPool(gTenant, gActor, poolpkg.ManualPolicy{})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	p.PoolID = gPool
	p.BalanceUnits = 41000
	if err := p.Debit(200); err != nil {
		t.Fatalf("Debit: %v", err)
	}
	a := &allocation.TenantManaAllocation{
		AllocationID:    gAlloc,
		TenantID:        gTenant,
		SourcePoolID:    gPool,
		GCID:            gActor,
		Units:           200,
		Status:          allocation.Status("pending"),
		Reason:          allocation.Reason("manual"),
		AllocatedByGCID: gActor,
		AllocatedAt:     time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC),
	}
	ev := pg.GrantEvent{
		Topic:   "chora.tenancy.mana_pool.adjusted.v1",
		GCID:    gActor,
		Payload: map[string]any{"tenant_id": gTenant, "units": int64(200)},
	}
	return p, a, ev
}

// ---------------------------------------------------------------------------
// The assertions the fix exists for
// ---------------------------------------------------------------------------

func TestSaveGrant_AllocationInsertFailureAbortsBeforeTheOutbox(t *testing.T) {
	t.Parallel()
	boom := errors.New("allocation insert exploded")
	q := &grantQuerier{tx: &grantTx{failOn: "INSERT INTO tenant_mana_allocations", failWith: boom}}
	p, a, ev := grantFixtures(t)

	err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev)
	if !errors.Is(err, boom) {
		t.Fatalf("want the allocation failure surfaced so the tx rolls back and the debit is discarded, got %v", err)
	}
	for _, s := range q.tx.stmts {
		if strings.Contains(s, "INSERT INTO outbox_events") {
			t.Fatal("the outbox must not be written after the allocation record failed")
		}
	}
}

func TestSaveGrant_OutboxFailureAbortsTheWholeTransaction(t *testing.T) {
	t.Parallel()
	boom := errors.New("outbox insert exploded")
	q := &grantQuerier{tx: &grantTx{failOn: "INSERT INTO outbox_events", failWith: boom}}
	p, a, ev := grantFixtures(t)

	err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev)
	if !errors.Is(err, boom) {
		t.Fatalf("an outbox failure must abort the tx so the debit is discarded, got %v", err)
	}
}

// The single most important structural property: all three writes share ONE
// transaction. If they ever split, the debit can commit while the record does
// not, which is the original defect.
func TestSaveGrant_AllThreeWritesShareOneTenantScopedTransaction(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{}
	p, a, ev := grantFixtures(t)

	if err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev); err != nil {
		t.Fatalf("SaveGrant: %v", err)
	}
	if len(q.tenants) != 1 || q.tenants[0] != gTenant {
		t.Fatalf("want exactly one tenant-scoped tx for %s, got %v", gTenant, q.tenants)
	}
	var sawPool, sawAlloc, sawOutbox bool
	for _, s := range q.tx.stmts {
		switch {
		case strings.Contains(s, "UPDATE tenant_mana_pools"):
			sawPool = true
		case strings.Contains(s, "INSERT INTO tenant_mana_allocations"):
			sawAlloc = true
		case strings.Contains(s, "INSERT INTO outbox_events"):
			sawOutbox = true
		}
	}
	if !sawPool || !sawAlloc || !sawOutbox {
		t.Fatalf("all three writes must be in the tx: pool=%v alloc=%v outbox=%v (%v)",
			sawPool, sawAlloc, sawOutbox, q.tx.stmts)
	}
}

func TestSaveGrant_DebitIsVersionGuarded(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{tx: &grantTx{failOn: "UPDATE tenant_mana_pools", failWith: pg.ErrNoRows}}
	p, a, ev := grantFixtures(t)

	err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev)
	if !errors.Is(err, pg.ErrVersionConflict) {
		t.Fatalf("a concurrent writer must lose loudly, got %v", err)
	}
	// Match WRITES only. The step-0 idempotency probe is a SELECT against
	// tenant_mana_allocations and legitimately runs before the debit, so a
	// bare table-name match here would fail on a read.
	for _, s := range q.tx.stmts {
		if strings.Contains(s, "INSERT INTO tenant_mana_allocations") ||
			strings.Contains(s, "INSERT INTO outbox_events") {
			t.Fatalf("nothing may be written after the debit lost its version guard, wrote %q", s)
		}
	}
}

func TestSaveGrant_RejectsMismatchedTenants(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{}
	p, a, ev := grantFixtures(t)
	a.TenantID = "22222222-2222-7222-8222-222222222222"

	if err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev); err == nil {
		t.Fatal("granting one tenant's pool to another tenant's allocation must fail loud")
	}
	if len(q.tenants) != 0 {
		t.Fatalf("must reject before opening a tx, opened %v", q.tenants)
	}
}

// The dispatcher rejects an empty traceparent AFTER the pool has moved, which
// is one of the three causes of the live incident. The row must carry one.
func TestSaveGrant_MintsATraceparentWhenTheCallerSuppliesNone(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{}
	p, a, ev := grantFixtures(t)
	ev.Traceparent = ""

	if err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev); err != nil {
		t.Fatalf("SaveGrant: %v", err)
	}
	var sawOutbox bool
	for _, s := range q.tx.execSQL {
		if strings.Contains(s, "INSERT INTO outbox_events") {
			sawOutbox = true
		}
	}
	if !sawOutbox {
		t.Fatal("expected the outbox insert to run with a minted traceparent rather than be skipped")
	}
}

// ⚠ The double-debit this design walks into if the idempotency check is not
// inside the transaction. A retry after a pod restart finds no cached entry,
// debits again, and the allocation INSERT quietly no-ops on ON CONFLICT.
func TestSaveGrant_AlreadyRecordedGrantDoesNotDebitAgain(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{tx: &grantTx{allocExists: true}}
	p, a, ev := grantFixtures(t)

	err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev)
	if !errors.Is(err, pg.ErrGrantAlreadyRecorded) {
		t.Fatalf("want ErrGrantAlreadyRecorded, got %v", err)
	}
	for _, s := range q.tx.stmts {
		if strings.Contains(s, "UPDATE tenant_mana_pools") {
			t.Fatal("a retry of an already-recorded grant must NOT debit the pool again")
		}
		if strings.Contains(s, "INSERT INTO") {
			t.Fatalf("a retry must write nothing, wrote %q", s)
		}
	}
}

// The concurrent-duplicate race: step 0 saw nothing, the INSERT then lost to
// ON CONFLICT. That must refuse too, not leave a debit with no new record.
func TestSaveGrant_ConcurrentDuplicateRefusesRatherThanLeavingABareDebit(t *testing.T) {
	t.Parallel()
	q := &grantQuerier{tx: &grantTx{allocConflict: true}}
	p, a, ev := grantFixtures(t)

	err := pg.NewManaGrantRepository(q).SaveGrant(context.Background(), p, a, ev)
	if !errors.Is(err, pg.ErrGrantAlreadyRecorded) {
		t.Fatalf("want ErrGrantAlreadyRecorded so the tx rolls the debit back, got %v", err)
	}
	for _, s := range q.tx.stmts {
		if strings.Contains(s, "INSERT INTO outbox_events") {
			t.Fatal("must not notify about a grant that was not recorded")
		}
	}
}
