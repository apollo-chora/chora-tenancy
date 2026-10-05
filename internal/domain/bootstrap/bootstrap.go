// Package bootstrap is the pure-domain core of the H+ Setup-Tenant
// bootstrap orchestration (CHO-1628 Phase 1).
//
// A bootstrap operation atomically provisions THREE rows in chora_tenancy:
//
//  1. tenants            — status=active, empty branding
//  2. members            — caller's GCID as role=owner, joined_at=now
//  3. tenant_entitlements — the `core` add-on
//
// All three rows MUST land in a single Postgres transaction at the
// adapter layer (see internal/adapter/pg/bootstrap_repository.go).
//
// Caller's GCID comes from the verified Chora session JWT — the HTTP
// adapter pulls it from the request context and passes it via Input.
// It is NEVER read from the HTTP body (per ADR-165 trust boundary —
// the body is caller-controlled and untrusted).
//
// Phase 1 scope: chora-tenancy writes only. The mirror row in
// chora_identity.tenant_memberships lands via an event in Phase 2
// (out of scope per CHO-1628).
package bootstrap

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

var (
	// ErrInvalidArgument is returned when the bootstrap Input fails
	// validation (empty / too-short / too-long name, missing GCID).
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrAlreadyMember is returned by the service layer when the caller
	// already holds an active TenantMembership. Phase 1 enforces a
	// one-tenant-per-GCID self-onboard rule.
	ErrAlreadyMember = errors.New("caller already holds an active TenantMembership")

	// ErrUnknownAddOnCode is returned by the repository when a code in
	// Input.AddOnCodes resolves to no catalogue row (E1). It lives here rather
	// than in the pg adapter because the gRPC server has to map it to
	// INVALID_ARGUMENT, and the adapter cannot be imported from there without
	// inverting the dependency.
	//
	// The distinction matters to the operator: left on the generic persist arm
	// this surfaces as FAILED_PRECONDITION and reaches them as a 409 saying the
	// PARENT cannot take a new organisation, which is a true sentence about the
	// wrong field. It is a mistyped add-on code, so it is a 400.
	ErrUnknownAddOnCode = errors.New("unknown add-on code")
)

// Length bounds for Tenant.DisplayName — matches the OpenAPI
// BootstrapTenantRequest schema in chora-contracts/openapi/tenancy-admin.yaml.
const (
	MinNameLen = 3
	MaxNameLen = 256
)

// Canonical literal values written to the chora_tenancy schema. Keep in
// lockstep with services/chora-tenancy/migrations/0001_initial.sql
// (tenant_status ENUM and tenant_member_role ENUM).
const (
	TenantStatusActive = "active"
	MemberRoleOwner    = "owner"
	CoreAddOnCode      = "core"
)

// Hosting-mode classification values — mirror the tenant_hosting_mode Postgres
// enum (migration 0026) AND the proto/OpenAPI HostingMode wire set (UPPERCASE).
const (
	HostingModePlatformHosted = "PLATFORM_HOSTED"
	HostingModeWhiteLabel     = "WHITE_LABEL"
	HostingModeFranchise      = "FRANCHISE"
	HostingModeSelfHost       = "SELF_HOST"
)

// validHostingMode reports whether s is one of the four canonical modes.
func validHostingMode(s string) bool {
	switch s {
	case HostingModePlatformHosted, HostingModeWhiteLabel, HostingModeFranchise, HostingModeSelfHost:
		return true
	default:
		return false
	}
}

// Input is the bootstrap request. OwnerGCID is supplied by the HTTP
// adapter after JWT verification — never accepted from the request body.
type Input struct {
	Name      string
	OwnerGCID string

	// ParentTenantID, when non-empty, makes this a SUB-tenant (ADR-217): the
	// tenants row is written with parent_tenant_id set. Empty = a root tenant
	// (the existing self-onboard bootstrap path). The chora-tenancy parent-
	// invariant trigger (migration 0026) rejects a parent outside the
	// chora-master tree at persist time.
	ParentTenantID string

	// AddOnCodes are the add-ons chosen at creation, beyond the mandatory
	// `core` (E1 / UX refactor wave W2). They become real
	// `add_on_subscriptions` rows in the SAME transaction as the tenant, so a
	// created organisation holds its entitlements across a restart. Empty is
	// the historical behaviour: core only.
	//
	// Codes only, never display names: `add_ons` carries both, and after
	// migration 0035 the canonical codes live in `code` while `name` holds a
	// human string. Resolution happens in the repository.
	AddOnCodes []string

	// HostingMode is the tenant classification (PLATFORM_HOSTED | WHITE_LABEL |
	// FRANCHISE | SELF_HOST). Empty defaults to PLATFORM_HOSTED here; the
	// operator sub-tenant path (Service.CreateSubTenant) defaults it to
	// FRANCHISE before calling New.
	HostingMode string
}

// Output is the public response shape returned to the caller after a
// successful bootstrap. Mirrors openapi.BootstrapTenantResponse.
type Output struct {
	TenantID      string
	OwnerMemberID string
	EntitlementID string
	CreatedAt     time.Time
}

// Bundle is the trio of rows the bootstrap transaction must persist
// atomically. The repository takes a Bundle and produces three INSERTs
// inside one Postgres transaction.
type Bundle struct {
	Tenant      BootstrappedTenant
	OwnerMember BootstrappedMember
	Entitlement BootstrappedEntitlement
	// ExtraEntitlements are the add-ons chosen at creation beyond `core`
	// (E1). They are written in the same transaction, so the bundle is no
	// longer a fixed trio; `core` keeps its own field because it is mandatory
	// and its absence from the catalogue is a distinct, fatal condition
	// (ErrCoreAddOnNotFound).
	ExtraEntitlements []BootstrappedEntitlement
	CreatedAt         time.Time
}

// BootstrappedTenant is the minimal field set the adapter writes to the
// tenants table on bootstrap. Other tenant columns (parent_tenant_id,
// suspension_reason, ...) default to NULL / their schema defaults.
type BootstrappedTenant struct {
	ID        string
	Name      string
	OwnerGCID string
	// ParentTenantID is empty for a root tenant, or the parent's UUID for a
	// sub-tenant (ADR-217). The adapter writes NULL when empty.
	ParentTenantID string
	// HostingMode is one of the tenant_hosting_mode enum values (migration
	// 0026). Never empty after New (defaults applied).
	HostingMode string
	Branding    string // raw JSON literal; Phase 1 always "{}"
	SelfHosted  bool
	Status      string // TenantStatusActive
}

// BootstrappedMember is the minimal field set written to the members
// table — caller's GCID as joined owner.
type BootstrappedMember struct {
	ID       string
	TenantID string
	GCID     string
	Role     string // MemberRoleOwner
	JoinedAt time.Time
}

// BootstrappedEntitlement is the minimal field set written to the
// tenant_entitlements table. AddOnCode (rather than AddOnID) is carried
// so the adapter can resolve the catalogue row at INSERT time — the
// `core` add_on_id is not stable across environments.
type BootstrappedEntitlement struct {
	ID        string
	TenantID  string
	AddOnCode string
}

// normaliseAddOnCodes lower-cases, trims, de-duplicates and drops the codes
// that must not become a second row, preserving the caller's order for the
// rest (E1). A blank entry is an error rather than a silent drop: it means the
// wire carried something the caller believed was a choice.
//
// Two codes are folded into `core` rather than added beside it:
//
//	core  already the mandatory entitlement; a second row would violate the
//	      unique constraint and roll back a request that was, in substance,
//	      fine.
//	base  has no `add_ons` row at all. Migration 0035's reconciler maps it onto
//	      core (`OR (c.code = 'base' AND a.code = 'core')`), so passing it
//	      through verbatim resolves to nothing and fails the creation.
func normaliseAddOnCodes(codes []string) ([]string, error) {
	out := make([]string, 0, len(codes))
	seen := map[string]bool{CoreAddOnCode: true}
	for _, raw := range codes {
		code := strings.ToLower(strings.TrimSpace(raw))
		if code == "" {
			return nil, fmt.Errorf("%w: add_on_codes carries a blank entry", ErrInvalidArgument)
		}
		if code == BaseAddOnCode {
			code = CoreAddOnCode
		}
		if seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	return out, nil
}

// New validates the Input and assembles the three-row Bundle. Pure —
// no I/O, no repository access. IDs are freshly generated UUIDv7
// (matches new-tables convention per ddd-enforcement.md invariant #7).
func New(in Input) (*Bundle, error) {
	name := strings.TrimSpace(in.Name)
	gcid := strings.TrimSpace(in.OwnerGCID)

	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if len(name) < MinNameLen {
		return nil, fmt.Errorf("%w: name must be at least %d characters", ErrInvalidArgument, MinNameLen)
	}
	if len(name) > MaxNameLen {
		return nil, fmt.Errorf("%w: name must be at most %d characters", ErrInvalidArgument, MaxNameLen)
	}
	if gcid == "" {
		return nil, fmt.Errorf("%w: owner_gcid required (must come from verified session JWT)", ErrInvalidArgument)
	}

	// ADR-217: parent (empty = root) + hosting-mode classification. Empty
	// hosting_mode defaults to PLATFORM_HOSTED; any non-empty value must be
	// one of the four canonical modes.
	parentID := strings.TrimSpace(in.ParentTenantID)
	hostingMode := strings.TrimSpace(in.HostingMode)
	if hostingMode == "" {
		hostingMode = HostingModePlatformHosted
	}
	if !validHostingMode(hostingMode) {
		return nil, fmt.Errorf("%w: hosting_mode %q not one of PLATFORM_HOSTED|WHITE_LABEL|FRANCHISE|SELF_HOST", ErrInvalidArgument, hostingMode)
	}

	extraCodes, err := normaliseAddOnCodes(in.AddOnCodes)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	tenantID := tenant.NewUUIDv7()
	memberID := tenant.NewUUIDv7()
	entitlementID := tenant.NewUUIDv7()

	// One row per chosen add-on, each with its own id so the INSERTs cannot
	// collide on the primary key.
	extras := make([]BootstrappedEntitlement, 0, len(extraCodes))
	for _, code := range extraCodes {
		extras = append(extras, BootstrappedEntitlement{
			ID:        tenant.NewUUIDv7(),
			TenantID:  tenantID,
			AddOnCode: code,
		})
	}

	return &Bundle{
		CreatedAt: now,
		Tenant: BootstrappedTenant{
			ID:             tenantID,
			Name:           name,
			OwnerGCID:      gcid,
			ParentTenantID: parentID,
			HostingMode:    hostingMode,
			Branding:       "{}",
			SelfHosted:     false,
			Status:         TenantStatusActive,
		},
		OwnerMember: BootstrappedMember{
			ID:       memberID,
			TenantID: tenantID,
			GCID:     gcid,
			Role:     MemberRoleOwner,
			JoinedAt: now,
		},
		Entitlement: BootstrappedEntitlement{
			ID:        entitlementID,
			TenantID:  tenantID,
			AddOnCode: CoreAddOnCode,
		},
		ExtraEntitlements: extras,
	}, nil
}

// Output projects the Bundle to the public response shape.
func (b *Bundle) Output() Output {
	return Output{
		TenantID:      b.Tenant.ID,
		OwnerMemberID: b.OwnerMember.ID,
		EntitlementID: b.Entitlement.ID,
		CreatedAt:     b.CreatedAt,
	}
}
