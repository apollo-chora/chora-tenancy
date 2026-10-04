// Package pool — domain event payloads (canonical Go shape).
//
// These structs mirror chora-contracts/proto/events/tenancy/tenant_mana_pool.proto
// for the Go-side adapter that builds Pub/Sub envelopes. Field types are
// chosen for round-trip with the proto generator output: int64 for unit
// counts, string for IDs, time.Time for timestamps.
//
// The chora-contracts repo owns the proto schemas; this Go shape is
// duplicated here to keep the domain layer free of proto-generated imports
// (allowing the adapter layer to do the proto<->struct mapping).
package pool

import "time"

// PoolCreated mirrors chora.tenancy.v1.TenantManaPoolCreated.
type PoolCreated struct {
	PoolID               string
	TenantID             string
	InitialBalanceUnits  int64
	MonthlyTopupUnits    int64
	AutoAllocationPolicy PolicyKind
	CreatedByGCID        string
	CreatedAt            time.Time
}

// PoolToppedUp mirrors chora.tenancy.v1.TenantManaPoolToppedUp.
type PoolToppedUp struct {
	PoolID                string
	TenantID              string
	UnitsCredited         int64
	ChargedCents          int64
	Currency              string
	StripePaymentIntentID string
	AutoRenew             bool
	ToppedUpByGCID        string
	BalanceAfterUnits     int64
	RecordedAt            time.Time
}

// PoolDepleted mirrors chora.tenancy.v1.TenantManaPoolDepleted.
type PoolDepleted struct {
	PoolID         string
	TenantID       string
	BalanceUnits   int64
	ThresholdUnits int64
	FullyDepleted  bool
	DetectedAt     time.Time
}
