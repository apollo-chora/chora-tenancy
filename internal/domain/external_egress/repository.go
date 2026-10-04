package external_egress

import (
	"context"
	"errors"
)

// ErrConcurrentUpdate — another writer advanced this tenant's policy version
// past ours between our read and our write. The write is REFUSED rather than
// clobbering the newer decision: a lost update here could silently re-enable
// external web egress a tenant admin had just turned off.
var ErrConcurrentUpdate = errors.New("external_egress: policy changed concurrently — write refused")

// Repository is the persistence port for the external-egress entitlement.
type Repository interface {
	// Get returns the tenant's persisted policy, or the fail-closed DEFAULT
	// (Persisted() == false, egress OFF) when the tenant has never opted in.
	//
	// It NEVER fabricates a persisted row on a read — absence IS the denial
	// (ADR-220 D4), so a read must not create the very row whose absence is
	// what keeps a franchise tenant safe.
	Get(ctx context.Context, tenantID string) (Policy, error)

	// Save persists a CHANGED policy and enqueues the
	// chora.tenancy.external_egress_policy.updated.v1 event in the SAME
	// transaction — so the entitlement and the chora-observability projection
	// it feeds can never diverge on a crash between the two writes.
	//
	// prev is the pre-change snapshot, supplying the before/after values the
	// event carries for the audit trail.
	//
	// Returns ErrConcurrentUpdate when another writer has already advanced the
	// tenant's version past next.Version.
	Save(ctx context.Context, next Policy, prev Policy) error
}
