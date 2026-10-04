// Package atomcount is the per-tenant atom-count read projection (ADR-217
// Debt 3, CHO-2011).
//
// atom_count on the franchise hierarchy (ChildTenantSummary) was hard-0
// because atoms live in chora_creation and cross-DB reads are forbidden
// (ddd-enforcement). The sanctioned source is this event-fed projection: the
// atom_count subscriber folds chora.creation.atom.created.v1 (+1) /
// chora.creation.atom.archived.v1 (-1) into a per-tenant counter in
// chora_tenancy, and the tenant-hierarchy repo reads it under each child's RLS
// scope to populate atom_count (identical to the user_count loop).
package atomcount

import "context"

// WriteRepository applies a per-tenant atom-count delta: +1 on atom.created,
// -1 on atom.archived. Idempotency is provided by the subscriber inbox (keyed
// on event_id); the write is a commutative counter fold, so redelivery dedup
// and out-of-order tolerance both hold. Runs under the tenant's RLS scope.
type WriteRepository interface {
	ApplyDelta(ctx context.Context, tenantID string, delta int64) error
}
