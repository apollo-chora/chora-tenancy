// admin_authz.go — shared chora-tenancy H+ admin authorization
// (CHO-1801 established the pattern; CHO-2072 generalized it to every gate).
//
// Every H+ (Hub+) admin endpoint authorizes on the gateway-propagated
// `x-mesh-user-roles` mesh header (servicemesh.HeaderUserRoles), fail-CLOSED:
//   - add-on WRITES: activate / change-tier / deactivate / cancel-deactivation
//     / preview-tier-change / request-deactivation
//   - mana-pool WRITES: create / auto-renew / allocations
//   - add-on READS: list / detail / usage
//
// This REPLACES the legacy fail-OPEN `rolesAllowed(X-Role)` helper (deleted in
// CHO-2072). The gateway never stamps `X-Role` (only `x-mesh-user-roles` via
// servicemesh.MarshalToHeaders), and `rolesAllowed` returned true on an empty
// header — so every gate built on it silently no-op'd in production, letting a
// non-admin perform tenant-admin writes. All gates now read the mesh header and
// fail closed; the same header chora-identity (callerHoldsAdminRole) and this
// service's bootstrap_handler.go (callerIsPlatformOperator) already read.
//
// Two role sets, both matched case-insensitively, exact per-entry — these gate
// TENANT DATA, so platform_operator is NOT admitted (CHO-2076: ADR-165 scopes
// the operator to the audited chora-payments READ only, not tenant entitlement/
// mana/compliance data; tenant lifecycle is gated separately by
// bootstrap_handler.callerIsPlatformOperator):
//   - HPlusAdmin — the roles RolesToSurfaces (membership_server.go) maps to the
//     `hplus` surface: tenant_admin / admin / owner / super_admin. The general
//     admin gate, so the API gate and the FE surface guard (CHO-1801) agree on
//     who reaches H+.
//   - SuperAdmin — super_admin ONLY. The STRICTER gate for compliance-lock
//     overrides (deactivating a ComplianceLocked add-on). Deliberately NOT
//     flattened into HPlusAdmin: a tenant_admin must not override a lock.
package httpapi

import (
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-common/auth/servicemesh"
)

// hplusAdminRoles — the general tenant-admin accept-set (RolesToSurfaces→hplus,
// lowercase; the caller's role is lowercased before lookup). Keep in lock-step
// with roleSurfaceMap's hplus rows (membership_server.go). NB: billing_admin is
// intentionally NOT here — it is not an hplus surface role (the legacy
// rolesAllowed set that included it never actually enforced, being fail-open).
var hplusAdminRoles = map[string]struct{}{
	"tenant_admin": {},
	"admin":        {},
	"owner":        {},
	"super_admin":  {},
}

// superAdminRoles — the stricter compliance-override accept-set.
var superAdminRoles = map[string]struct{}{
	"super_admin": {},
}

// meshRolesContain reports whether the gateway-propagated x-mesh-user-roles
// header carries any role in `allowed`. Fail-CLOSED: an absent/empty header
// denies. Case-insensitive, exact per-entry match (prefix/superset names do NOT
// pass). The single fail-closed primitive every tenant-DATA admin gate composes
// — the mesh replacement for the deleted fail-open rolesAllowed.
//
// platform_operator is deliberately NOT admitted here (CHO-2076). ADR-165 scopes
// the operator to exactly ONE cross-tenant capability — the audited
// chora-payments READ — NOT a blanket god-mode. It is a non-tenant-scoped role
// (holds no TenantMembership), so it must not reach a specific tenant's
// entitlement / mana-pool / compliance data. Tenant LIFECYCLE (private-tenant
// create) is gated SEPARATELY by bootstrap_handler.callerIsPlatformOperator.
func meshRolesContain(h http.Header, allowed map[string]struct{}) bool {
	raw := strings.TrimSpace(h.Get(servicemesh.HeaderUserRoles))
	if raw == "" {
		return false
	}
	for _, part := range strings.Split(raw, ",") {
		role := strings.ToLower(strings.TrimSpace(part))
		if _, ok := allowed[role]; ok {
			return true
		}
	}
	return false
}

// callerHoldsHPlusAdminRole — the general H+ tenant-admin gate (add-on +
// mana-pool writes, add-on reads). tenant_admin / admin / owner / super_admin
// (NO operator — CHO-2076; operator is non-tenant-scoped).
func callerHoldsHPlusAdminRole(h http.Header) bool {
	return meshRolesContain(h, hplusAdminRoles)
}

// callerHoldsSuperAdminRole — the stricter gate for compliance-lock overrides
// (ComplianceLocked add-on deactivation). super_admin ONLY (NO operator —
// CHO-2076; the compliance lock is tenant data).
func callerHoldsSuperAdminRole(h http.Header) bool {
	return meshRolesContain(h, superAdminRoles)
}
