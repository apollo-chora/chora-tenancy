// Package entitlement holds the write-through seam every runtime entitlement
// mutation goes through (UX Track U, row E4).
//
// # THE PROBLEM IT CLOSES
//
// Before row E4 the v2 handlers, the payments subscriber activator and the
// wizard's SubscribePending each mutated the per-pod in-memory
// *add_on.SubscriptionRegistry and nothing else. The registry is hydrated from
// PostgreSQL at boot, so a pod restart re-read the old row and resurrected an
// add-on the admin had deactivated. `cmd/server/registry_entitlement_store.go`
// calls that breakage 4/5 in its own header; it is CHO-1811. Multi-pod makes it
// worse, because each pod then holds a different truth.
//
// # THE ORDERING RULE
//
// PostgreSQL is the source of truth. Memory must never be ahead of it.
//
// The registry cannot honour that by persisting first: every mutation method
// takes the mutex, applies the state machine and returns the mutated
// subscription in one step, so there is no way to ask what the result WOULD be
// without having already applied it, and the only insert path is the domain
// Subscribe itself. So the sequence is mutate, persist, and on a persist
// FAILURE evict the tenant. After eviction memory is not ahead, it is ABSENT,
// `KnowsTenant` reports false, and the entitlement read falls through to the
// durable table. A failed write degrades to reading the source of truth rather
// than serving a state PostgreSQL does not hold.
//
// ⚠ WHAT IT DOES NOT CLOSE. Between the in-memory mutation and the commit, a
// reader on the SAME POD can observe a state that is not yet durable. That
// window is one write long. Closing it requires the lock or the transaction to
// span both, which is the load-modify-save shape the read-side retirement
// points at, and is deliberately not done here.
package entitlement

import (
	"context"
	"errors"

	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
)

// ErrNoDurableStore is returned when no durable writer is wired.
//
// It is a REFUSAL, not a fallback. Continuing without a durable store would
// leave the mutation in a per-pod map that evaporates on the next restart,
// which is exactly the CHO-1811 failure mode this package exists to end. The
// v2 GrantStore set the precedent: refuse with 503 rather than debit a pool
// the service cannot record against.
var ErrNoDurableStore = errors.New("entitlement: no durable subscription store wired")

// Writer persists a subscription to the durable table.
//
// A port rather than the concrete *pg.SubscriptionRepository, so call sites
// stay testable without a database and so the nil case above is expressible.
type Writer interface {
	Upsert(ctx context.Context, sub *addon.Subscription) error
}

// Evicter forgets a tenant's cached subscriptions.
//
// *add_on.SubscriptionRegistry satisfies it. Narrowed to the one method this
// package calls so a test does not have to build a whole registry.
type Evicter interface {
	Evict(tenantID string)
}
