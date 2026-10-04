package entitlement_test

// writethrough_test.go: the ordering rule every entitlement mutation obeys
// (UX Track U, row E4 slice 2b).
//
// # THE RULE, AND WHY IT IS SHAPED THIS WAY
//
// PostgreSQL is the source of truth for entitlements. Memory must never be
// ahead of it. The registry cannot honour that by persisting first, because
// every mutation method takes the mutex, applies the state machine and returns
// the mutated subscription in one step. So the sequence is: mutate, persist,
// and on a persist FAILURE evict the tenant, which leaves memory ABSENT rather
// than ahead and sends the next read through to the durable table.
//
// Two conditions this pins, both of which are the difference between a fix and
// a fresh lie:
//
//  1. A failed persist returns the ERROR and never the mutated state. A caller
//     must not receive a 200 describing something PostgreSQL does not hold.
//  2. A missing durable store is a REFUSAL, not a silent in-memory-only write.
//     That is the whole CHO-1811 failure mode: a mutation that lives only in a
//     per-pod map and evaporates on restart. The v2 GrantStore already
//     established this shape, refusing with 503 rather than debiting a pool it
//     cannot record against.
//
// ⚠ WHAT IT DOES NOT CLOSE: between the mutation and the commit a same-pod
// reader can observe a state that is not yet durable, for one write's
// duration. Closing that needs the lock or the transaction to span both, which
// is the load-modify-save end state slice 3 points at.

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/entitlement"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

type fakeWriter struct {
	err    error
	calls  int
	lastID string
}

func (f *fakeWriter) Upsert(_ context.Context, sub *addon.Subscription) error {
	f.calls++
	if sub != nil {
		f.lastID = sub.TenantID
	}
	return f.err
}

type fakeEvicter struct {
	evicted []string
}

func (f *fakeEvicter) Evict(tenantID string) { f.evicted = append(f.evicted, tenantID) }

const tenant = "01990000-0000-7000-8000-00000000000a"

func sub() *addon.Subscription {
	return &addon.Subscription{TenantID: tenant, AddOnCode: "tms", AddOnID: "a-1", Status: addon.StatusActive}
}

func TestPersist_SuccessDoesNotEvict(t *testing.T) {
	w := &fakeWriter{}
	e := &fakeEvicter{}

	if err := entitlement.Persist(context.Background(), w, e, sub()); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if w.calls != 1 {
		t.Fatalf("writer called %d times, want 1", w.calls)
	}
	if len(e.evicted) != 0 {
		t.Fatalf("evicted %v on a SUCCESSFUL persist; the cache would be dropped on every write", e.evicted)
	}
}

// TestPersist_FailureEvictsAndReturnsTheError is the core arm. Both halves
// matter: without the evict, memory stays ahead of PG, which is CHO-1811;
// without the error, the caller answers 200 for a state PG does not hold.
func TestPersist_FailureEvictsAndReturnsTheError(t *testing.T) {
	boom := errors.New("connection refused")
	w := &fakeWriter{err: boom}
	e := &fakeEvicter{}

	err := entitlement.Persist(context.Background(), w, e, sub())
	if err == nil {
		t.Fatal("Persist returned nil on a failed write; the caller would answer success for a state PostgreSQL does not hold")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v so the call site can classify it", err, boom)
	}
	if len(e.evicted) != 1 || e.evicted[0] != tenant {
		t.Fatalf("evicted %v, want exactly [%s]; without the eviction memory stays ahead of the database, which is the bug this row exists to close", e.evicted, tenant)
	}
}

// TestPersist_NilWriterRefusesAndEvicts is the fail-loud arm. A nil durable
// store must not degrade to an in-memory-only mutation: that is precisely the
// state CHO-1811 describes, and it would evaporate on the next restart.
func TestPersist_NilWriterRefusesAndEvicts(t *testing.T) {
	e := &fakeEvicter{}

	err := entitlement.Persist(context.Background(), nil, e, sub())
	if err == nil {
		t.Fatal("a nil durable store was accepted; the mutation would live only in a per-pod map and evaporate on restart, which is exactly CHO-1811")
	}
	if !errors.Is(err, entitlement.ErrNoDurableStore) {
		t.Fatalf("error = %v, want ErrNoDurableStore so the handler can answer 503 rather than a generic 500", err)
	}
	if len(e.evicted) != 1 {
		t.Fatalf("evicted %v, want the tenant dropped: the in-memory mutation already happened and nothing durable recorded it", e.evicted)
	}
}

// TestPersist_NilSubscriptionIsRefusedNotIgnored stops a caller from treating
// "nothing to persist" as success. Every mutation this wraps returns a
// subscription; a nil one means the call site is wired wrong.
func TestPersist_NilSubscriptionIsRefused(t *testing.T) {
	w := &fakeWriter{}
	e := &fakeEvicter{}

	if err := entitlement.Persist(context.Background(), w, e, nil); err == nil {
		t.Fatal("a nil subscription persisted as success; a mis-wired call site would silently write nothing")
	}
	if w.calls != 0 {
		t.Fatalf("writer called %d times for a nil subscription", w.calls)
	}
}

// TestPersist_NilEvicterStillReturnsTheError guards the degenerate wiring: a
// missing evicter must not swallow the write error or panic on the failure
// path, because that path is already the unhappy one.
func TestPersist_NilEvicterStillReturnsTheError(t *testing.T) {
	boom := errors.New("connection refused")
	w := &fakeWriter{err: boom}

	err := entitlement.Persist(context.Background(), w, nil, sub())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the write error even with no evicter wired", err)
	}
}
