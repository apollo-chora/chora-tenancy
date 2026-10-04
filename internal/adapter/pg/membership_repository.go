// membership_repository.go — pgx-backed implementation of the
// grpc.MembershipLister port (Bucket 1, 2026-05-14 multi-tenant identity;
// D0.1 RLS-bypass rewire 2026-05-14, CHO-1538).
//
// Schema reference: services/chora-tenancy/migrations/0001_initial.sql
// (members table) + 0004_phyllis_mvp.sql (tenants.slug) +
// 0011_list_memberships_by_gcid_security_definer.sql (the function this
// repo calls) + 0012_members_reenable_rls.sql (RLS re-enabled on members).
//
// SQL contract:
//
//   - ListByGCID: SELECT * FROM list_memberships_by_gcid($1::uuid). The
//     function returns one row per (gcid, tenant_id) pair, with all roles
//     held in that tenant aggregated into a single roles[] array, joined
//     to tenants for the slug, soft-delete + suspension filtered, ordered
//     joined_at DESC.
//
// Why a SECURITY DEFINER function instead of an inline SELECT? The
// `members` table carries the `tenant_isolation` RLS policy
// (`tenant_id = current_setting('chora.tenant_id', true)::uuid`). This
// repository is called by the chora-tenancy gRPC server to satisfy the
// auth mint flow's CROSS-TENANT directory lookup ("given a GCID, what
// tenants does this user belong to?") — there is NO `chora.tenant_id` to
// set, because resolving the tenant set is the point of the call. The
// gRPC server's pg pool authenticates as `chora_tenancy_app_rw`, which
// does NOT carry BYPASSRLS. An inline `SELECT ... FROM members WHERE
// gcid=$1` is therefore filtered to ZERO rows whenever RLS is enabled —
// mint then 403s. The fix (migration 0011) is a `SECURITY DEFINER`
// function owned by the migrate role, with `SET row_security = off`, so
// the cross-tenant read inside its body is not RLS-filtered; `EXECUTE` is
// granted to app_rw only. Ordinary tenant-scoped reads of `members` from
// app_rw remain fully RLS-governed (the prior table-wide RLS DISABLE
// workaround is reverted by migration 0012).
//
// Why aggregate roles[]? The 0001_initial.sql members table has a single
// 'role' column (NOT an array), enforced by the tenant_member_role enum.
// Per the user invariant (many roles per tenant) the function ARRAY_AGGs
// the per-row roles by (gcid, tenant_id) — historic single-role rows
// aggregate to a single-element array; multi-role pre-staged seed rows
// aggregate naturally.
//
// Soft delete: the function filters `members.deleted_at IS NULL` AND
// `tenants.deleted_at IS NULL` per ddd-enforcement.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	tenancygrpc "github.com/apollo-chora/chora-tenancy/internal/adapter/grpc"
)

// MembershipRepository is the pgx-backed implementation of
// grpc.MembershipLister.
type MembershipRepository struct {
	q QueryRunner
}

// NewMembershipRepository constructs the production-wired repository.
func NewMembershipRepository(q QueryRunner) *MembershipRepository {
	return &MembershipRepository{q: q}
}

// ListByGCID returns one row per (gcid, tenant_id) pair, with all roles
// aggregated into a single roles[] array per row. Sorted by joined_at
// DESC so callers get newest-first ordering (the gRPC adapter computes
// is_default independently of caller ordering, but sorted output keeps
// tests + diagnostic logs stable).
//
// Invokes the `list_memberships_by_gcid` SECURITY DEFINER function
// (migration 0011) — NOT an inline SELECT on `members`. The function runs
// with `SET row_security = off` so this cross-tenant directory query is
// not filtered by the `tenant_isolation` RLS policy. See the package doc
// comment for the full rationale. The function's RETURNS TABLE shape is
// (tenant_id uuid, tenant_slug text, roles text[], joined_or_invited_at
// timestamptz); tenant_id is cast to text in the SELECT to keep the
// row-scan signature identical to the gRPC adapter's MembershipRow.
// includeSuspended widens the read over `members.suspended_at` and NOTHING
// else. Pass false everywhere except the closure ownership pre-flight: mint
// must keep excluding suspended members, or a suspended member authenticates
// back into the tenant that suspended them. See migration 0038 for why the
// closure path needs true (a suspended owner still holds the tenant, because
// the one-live-owner index ignores suspension).
func (r *MembershipRepository) ListByGCID(ctx context.Context, gcid string, includeSuspended bool) ([]tenancygrpc.MembershipRow, error) {
	if strings.TrimSpace(gcid) == "" {
		return nil, errors.New("pg.MembershipRepository.ListByGCID: gcid required")
	}
	// $1 is the GCID, cast to uuid for the function's p_gcid parameter.
	// $2 is the suspension flag. tenant_id::text keeps the scan target a
	// string (the function returns it as uuid).
	const q = `
        SELECT
            tenant_id::text       AS tenant_id,
            tenant_slug           AS tenant_slug,
            roles                 AS roles,
            joined_or_invited_at  AS joined_or_invited_at
        FROM list_memberships_by_gcid($1::uuid, $2::boolean)
    `
	rows, err := r.q.Query(ctx, q, gcid, includeSuspended)
	if err != nil {
		return nil, fmt.Errorf("pg.MembershipRepository.ListByGCID: %w", err)
	}
	defer rows.Close()

	out := make([]tenancygrpc.MembershipRow, 0, 4)
	for rows.Next() {
		var (
			tenantID   string
			tenantSlug string
			roles      []string
			joinedAt   time.Time
		)
		if err := rows.Scan(&tenantID, &tenantSlug, &roles, &joinedAt); err != nil {
			return nil, fmt.Errorf("pg.MembershipRepository.ListByGCID scan: %w", err)
		}
		// Defensive: ARRAY_AGG with DISTINCT can carry NULLs when the
		// underlying row has role IS NULL (shouldn't happen under the
		// current schema — role is NOT NULL — but guard anyway).
		cleaned := make([]string, 0, len(roles))
		for _, role := range roles {
			role = strings.TrimSpace(role)
			if role == "" {
				continue
			}
			cleaned = append(cleaned, role)
		}
		out = append(out, tenancygrpc.MembershipRow{
			TenantID:   tenantID,
			TenantSlug: tenantSlug,
			Roles:      cleaned,
			JoinedAt:   joinedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.MembershipRepository.ListByGCID rows.Err: %w", err)
	}
	return out, nil
}

// Compile-time check.
var _ tenancygrpc.MembershipLister = (*MembershipRepository)(nil)
