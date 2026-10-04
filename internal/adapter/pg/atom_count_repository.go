// atom_count_repository.go — pg adapter for the ADR-217 Debt 3 (CHO-2011)
// atomcount.WriteRepository. Upserts the per-tenant atom counter under tenant
// RLS (SET LOCAL chora.tenant_id via RunInTenantTx). The chora_tenancy_app_rw
// role is NOBYPASSRLS, so the write MUST run inside a tenant-scoped tx.
//
// Idempotency lives in the subscriber inbox (event_id); this write is a
// commutative fold, so the counter is redelivery-safe and order-independent.
// The stored value MAY go transiently negative (an archive whose atom.created
// predates the projection); the hierarchy read clamps with GREATEST(...,0).
package pg

import (
	"context"

	"github.com/apollo-chora/chora-tenancy/internal/domain/atomcount"
)

// AtomCountWriteRepo folds per-tenant atom-count deltas under tenant RLS.
type AtomCountWriteRepo struct {
	tx TxQuerier
}

// NewAtomCountWriteRepo wires the projection write repo to a tenant-scoped
// transaction runner (*PgxPoolQuerier satisfies TxQuerier).
func NewAtomCountWriteRepo(tx TxQuerier) *AtomCountWriteRepo {
	return &AtomCountWriteRepo{tx: tx}
}

var _ atomcount.WriteRepository = (*AtomCountWriteRepo)(nil)

// ApplyDelta upserts tenant_atom_counts by delta (+1 on atom.created, -1 on
// atom.archived). A first insert seeds the counter at delta; a conflict folds
// the delta into the existing count.
func (r *AtomCountWriteRepo) ApplyDelta(ctx context.Context, tenantID string, delta int64) error {
	return r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		return tx.Exec(ctx, `
INSERT INTO tenant_atom_counts (tenant_id, atom_count)
VALUES ($1::uuid, $2)
ON CONFLICT (tenant_id) DO UPDATE SET
  atom_count = tenant_atom_counts.atom_count + EXCLUDED.atom_count,
  updated_at = now()`, tenantID, delta)
	})
}
