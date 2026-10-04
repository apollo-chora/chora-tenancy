// Package allocation — domain event payloads.
//
// These structs mirror chora-contracts/proto/events/tenancy/tenant_mana_allocation.proto.
// The proto schemas are owned by chora-contracts; this Go shape lives here
// to keep the domain layer free of proto-generated imports.
package allocation

import "time"

// AllocationGranted mirrors chora.tenancy.v1.TenantManaAllocationGranted.
type AllocationGranted struct {
	AllocationID    string
	TenantID        string
	GCID            string
	SourcePoolID    string
	Units           int64
	Reason          Reason
	AllocatedByGCID string
	ExpiresAt       *time.Time
	AllocatedAt     time.Time
}

// AllocationClaimed mirrors chora.tenancy.v1.TenantManaAllocationClaimed.
type AllocationClaimed struct {
	AllocationID      string
	TenantID          string
	GCID              string
	FirstDebitEntryID string
	RemainingUnits    int64
	ClaimedAt         time.Time
}

// AllocationRevoked mirrors chora.tenancy.v1.TenantManaAllocationRevoked.
type AllocationRevoked struct {
	AllocationID        string
	TenantID            string
	GCID                string
	UnitsReturnedToPool bool
	ReturnedUnits       int64
	Reason              string
	RevokedByGCID       string
	RevokedAt           time.Time
}

// AllocationExpired mirrors chora.tenancy.v1.TenantManaAllocationExpired.
type AllocationExpired struct {
	AllocationID        string
	TenantID            string
	GCID                string
	UnitsReturnedToPool bool
	ReturnedUnits       int64
	ExpiresAt           time.Time
	ExpiredAt           time.Time
}
