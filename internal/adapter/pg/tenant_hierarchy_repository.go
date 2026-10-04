// tenant_hierarchy_repository.go — pg adapter for the ADR-217 Phase 2.3b
// (CHO-2010) HierarchyReader port. Returns the DIRECT children of a tenant
// (Model β franchise hierarchy) with a per-child live-member count.
//
// RLS split:
//   - The `tenants` registry is RLS-FREE (migration 0001 — Hub+ reads children
//     cross-tenant), so the children query runs under the current tenant's own
//     scope and the parent_tenant_id predicate bounds the result.
//   - The `members` table IS tenant-pinned RLS (migration 0012: tenant_id =
//     current_setting('chora.tenant_id')::uuid — NOT platform-sentinel-aware,
//     unlike transaction_ledger), so each child's member count is read under
//     that child's OWN RLS scope. N = the number of DIRECT children (4 for
//     chora-master, 0 for a normal tenant) — bounded, admin-screen, not a hot
//     path. Widening the members RLS to span-all would be an ADR-scoped
//     security change, so we loop per child instead.
//
// atom_count is served from the tenant_atom_counts projection (migration 0027,
// ADR-217 Debt 3) — an event-fed SAME-DB read-model folded from
// chora.creation.atom.{created,archived}.v1 by the atom_count subscriber, read
// under each child's own RLS scope (NOT a cross-DB JOIN — atoms live in
// chora_creation, cross-DB reads forbidden). Forward-only: atoms predating the
// projection are uncounted; the read clamps a transient negative fold to 0.
package pg

import (
	"context"
	"errors"
	"fmt"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
)

// TenantHierarchyRepo reads the franchise hierarchy scope-aware under RLS.
type TenantHierarchyRepo struct {
	tx ScopeTxQuerier
}

// NewTenantHierarchyRepo wires the repo to a scope-aware tx runner
// (*PgxPoolQuerier satisfies ScopeTxQuerier).
func NewTenantHierarchyRepo(tx ScopeTxQuerier) *TenantHierarchyRepo {
	return &TenantHierarchyRepo{tx: tx}
}

var _ tenancygrpc.HierarchyReader = (*TenantHierarchyRepo)(nil)

// GetHierarchy returns the DIRECT children of currentTenantID + per-child live
// member counts. is_parent = (#children > 0). currentTenantID is a UUID-shaped
// tenant id (the gRPC adapter vetted it) — it doubles as the RLS scope for the
// RLS-free tenants read.
func (r *TenantHierarchyRepo) GetHierarchy(ctx context.Context, currentTenantID string) (tenancygrpc.TenantHierarchy, error) {
	if r == nil || r.tx == nil {
		return tenancygrpc.TenantHierarchy{}, errors.New("pg/tenant_hierarchy: repo not wired")
	}

	// 1) Direct children from the RLS-free tenants registry.
	const childSQL = `
SELECT tenant_id::text, name
FROM tenants
WHERE parent_tenant_id = $1::uuid
  AND deleted_at IS NULL
ORDER BY name, tenant_id`

	children := make([]tenancygrpc.ChildTenantSummary, 0, 4)
	err := r.tx.RunInScopeTx(ctx, currentTenantID, func(ctx context.Context, tx Tx) error {
		rs, qerr := tx.Query(ctx, childSQL, currentTenantID)
		if qerr != nil {
			return fmt.Errorf("pg/tenant_hierarchy: list children: %w", qerr)
		}
		defer rs.Close()
		for rs.Next() {
			var c tenancygrpc.ChildTenantSummary
			if scanErr := rs.Scan(&c.TenantID, &c.TenantName); scanErr != nil {
				return fmt.Errorf("pg/tenant_hierarchy: scan child: %w", scanErr)
			}
			children = append(children, c)
		}
		return rs.Err()
	})
	if err != nil {
		return tenancygrpc.TenantHierarchy{}, err
	}

	// 2) Per-child live-member count + atom_count under EACH child's own RLS
	//    scope. member count() never returns NULL (0 for an empty child); the
	//    atom_count read COALESCEs a missing projection row to 0 and clamps a
	//    transient negative fold with GREATEST(...,0), so both int64 scans are
	//    safe. The atom_count projection (tenant_atom_counts) shares this DB, so
	//    reading it here is NOT a forbidden cross-DB access.
	const memberCountSQL = `SELECT count(DISTINCT gcid) FROM members WHERE deleted_at IS NULL`
	const atomCountSQL = `SELECT COALESCE((SELECT GREATEST(atom_count, 0) FROM tenant_atom_counts), 0)`
	for i := range children {
		childID := children[i].TenantID
		cerr := r.tx.RunInScopeTx(ctx, childID, func(ctx context.Context, tx Tx) error {
			if err := tx.QueryRow(ctx, memberCountSQL).Scan(&children[i].UserCount); err != nil {
				return err
			}
			return tx.QueryRow(ctx, atomCountSQL).Scan(&children[i].AtomCount)
		})
		if cerr != nil {
			return tenancygrpc.TenantHierarchy{}, fmt.Errorf("pg/tenant_hierarchy: count members+atoms for %s: %w", childID, cerr)
		}
	}

	return tenancygrpc.TenantHierarchy{
		IsParent: len(children) > 0,
		Children: children,
	}, nil
}
