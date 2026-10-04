// membership_write_repository.go — pgx-backed implementation of the
// grpc.MembershipUpsertRepo port (ADR-182: UpsertMembership RPC).
//
// Schema reference: 0001_initial.sql (members) + 0004_phyllis_mvp.sql
// (tenants.slug) + 0021_chora_master_public_tenant.up.sql (author enum
// value + UNIQUE(tenant_id, gcid, role) + chora-master row).
//
// RLS contract: `members` carries the tenant_isolation policy and the
// app_rw role is NOBYPASSRLS — every read/write here runs inside a
// `SET LOCAL chora.tenant_id` transaction via the TxQuerier seam
// (LegacyEntitlementStore precedent). All statements are scoped to the
// SINGLE target tenant, so the tenant GUC matches every row touched.
// `tenants` is the un-RLS'd registry — the slug resolution reads it with
// the plain QueryRunner.
//
// Suspension rule: an existing suspended or soft-deleted (tenant, gcid)
// membership fails the upsert with grpc.ErrMembershipSuspended — upsert
// NEVER resurrects; un-suspending is an explicit admin action.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
)

// MembershipWriteRepository is the pgx-backed implementation of
// grpc.MembershipUpsertRepo.
type MembershipWriteRepository struct {
	q  QueryRunner // plain pool — un-RLS'd tenants registry reads
	tx TxQuerier   // SET LOCAL chora.tenant_id transactions — members writes
}

// NewMembershipWriteRepository constructs the production-wired repository.
func NewMembershipWriteRepository(q QueryRunner, tx TxQuerier) *MembershipWriteRepository {
	return &MembershipWriteRepository{q: q, tx: tx}
}

// liveOwnerGCIDs returns every GCID holding a live `owner` row in the tenant,
// taking a row lock on each so the count cannot change under us before the
// caller's write commits (S7-B1). MUST be called inside RunInTenantTx: the
// RLS policy scopes it to the tenant whose GUC is set.
//
// Two deliberate omissions in the predicate:
//
//   - It does NOT filter on gcid. The last-owner question is about the TENANT
//     ("would this leave nobody owning it"), so the scan has to see every
//     owner row, not just the target's.
//   - It does NOT filter on suspended_at. A suspended owner is still the owner
//     of record; treating them as absent would let an admin suspend an owner
//     and then remove them, which is the very strip this guard exists to stop.
func liveOwnerGCIDs(ctx context.Context, tx Tx, tenantID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
        SELECT gcid::text
        FROM   members
        WHERE  tenant_id = $1
          AND  role = 'owner'
          AND  deleted_at IS NULL
        FOR UPDATE`,
		tenantID)
	if err != nil {
		return nil, fmt.Errorf("owner guard: %w", err)
	}
	defer rows.Close()
	var owners []string
	for rows.Next() {
		var gcid string
		if err := rows.Scan(&gcid); err != nil {
			return nil, fmt.Errorf("owner guard scan: %w", err)
		}
		owners = append(owners, gcid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("owner guard rows.Err: %w", err)
	}
	return owners, nil
}

// containsString reports membership in a small unsorted slice. The owner set
// and a role set are both at most a handful of entries, so a map would cost
// more than it saves.
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// ResolveTenantIDBySlug implements grpc.MembershipUpsertRepo.
func (r *MembershipWriteRepository) ResolveTenantIDBySlug(ctx context.Context, slug string) (string, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return "", errors.New("pg.MembershipWriteRepository.ResolveTenantIDBySlug: slug required")
	}
	const q = `
        SELECT tenant_id::text
        FROM   tenants
        WHERE  slug = $1
          AND  deleted_at IS NULL
          AND  status = 'active'
    `
	rows, err := r.q.Query(ctx, q, slug)
	if err != nil {
		return "", fmt.Errorf("pg.MembershipWriteRepository.ResolveTenantIDBySlug: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("pg.MembershipWriteRepository.ResolveTenantIDBySlug rows.Err: %w", err)
		}
		return "", tenancygrpc.ErrTenantNotFound
	}
	var tenantID string
	if err := rows.Scan(&tenantID); err != nil {
		return "", fmt.Errorf("pg.MembershipWriteRepository.ResolveTenantIDBySlug scan: %w", err)
	}
	return tenantID, nil
}

// UpsertRoles implements grpc.MembershipUpsertRepo. Inserts one members
// row per missing (tenant, gcid, role) and returns the full post-upsert
// live role set + whether any row was newly created.
func (r *MembershipWriteRepository) UpsertRoles(ctx context.Context, tenantID, gcid string, roles []string) ([]string, bool, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(gcid) == "" || len(roles) == 0 {
		return nil, false, errors.New("pg.MembershipWriteRepository.UpsertRoles: tenantID, gcid and roles required")
	}

	var (
		allRoles []string
		created  bool
	)
	err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		// 1. Live-tenant guard (tenants is the un-RLS'd registry; reading
		//    it inside the tenant tx is fine).
		tRows, err := tx.Query(ctx,
			`SELECT 1 FROM tenants WHERE tenant_id = $1 AND deleted_at IS NULL AND status = 'active'`,
			tenantID)
		if err != nil {
			return fmt.Errorf("tenant guard: %w", err)
		}
		tenantLive := tRows.Next()
		tErr := tRows.Err()
		tRows.Close()
		if tErr != nil {
			return fmt.Errorf("tenant guard rows.Err: %w", tErr)
		}
		if !tenantLive {
			return tenancygrpc.ErrTenantNotFound
		}

		// 2. No-resurrection guard: any suspended or soft-deleted row for
		//    (tenant, gcid) blocks the upsert.
		sRows, err := tx.Query(ctx, `
            SELECT 1 FROM members
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  (suspended_at IS NOT NULL OR deleted_at IS NOT NULL)
            LIMIT  1`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("suspension guard: %w", err)
		}
		suspended := sRows.Next()
		sErr := sRows.Err()
		sRows.Close()
		if sErr != nil {
			return fmt.Errorf("suspension guard rows.Err: %w", sErr)
		}
		if suspended {
			return tenancygrpc.ErrMembershipSuspended
		}

		// 3. One INSERT per role — ON CONFLICT (tenant_id, gcid, role)
		//    DO NOTHING makes re-delivery a no-op; RETURNING 1 detects
		//    whether THIS call created the row (Tx.Exec exposes no
		//    RowsAffected).
		for _, role := range roles {
			iRows, err := tx.Query(ctx, `
                INSERT INTO members (
                    tenant_id, gcid, role, invited_at, joined_at,
                    created_at, updated_at
                )
                VALUES ($1, $2::uuid, $3::tenant_member_role, now(), now(), now(), now())
                ON CONFLICT (tenant_id, gcid, role) DO NOTHING
                RETURNING 1`,
				tenantID, gcid, role)
			if err != nil {
				return fmt.Errorf("insert role %q: %w", role, err)
			}
			if iRows.Next() {
				created = true
			}
			iErr := iRows.Err()
			iRows.Close()
			if iErr != nil {
				return fmt.Errorf("insert role %q rows.Err: %w", role, iErr)
			}
		}

		// 4. Post-upsert live role aggregate for the response.
		aRows, err := tx.Query(ctx, `
            SELECT DISTINCT role::text
            FROM   members
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  deleted_at IS NULL AND suspended_at IS NULL
            ORDER  BY role::text`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("role aggregate: %w", err)
		}
		defer aRows.Close()
		for aRows.Next() {
			var role string
			if err := aRows.Scan(&role); err != nil {
				return fmt.Errorf("role aggregate scan: %w", err)
			}
			allRoles = append(allRoles, role)
		}
		return aRows.Err()
	})
	if err != nil {
		// Sentinels pass through untouched for the gRPC layer's mapping.
		if errors.Is(err, tenancygrpc.ErrTenantNotFound) || errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("pg.MembershipWriteRepository.UpsertRoles: %w", err)
	}
	return allRoles, created, nil
}

// ReplaceRoles is the REPLACE flavour of UpsertRoles (CHO-1809 follow-up
// for the H+ Members page multi-role editor). Soft-deletes every existing
// live `members` row for (tenant, gcid) whose `role` is NOT in `roles`,
// then upserts the rows that ARE in `roles` (same semantics as
// UpsertRoles for the kept set). Returns the FULL post-replace role
// aggregate and whether any row was newly created. Sentinels are the
// same as UpsertRoles.
func (r *MembershipWriteRepository) ReplaceRoles(ctx context.Context, tenantID, gcid string, roles []string) ([]string, bool, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(gcid) == "" || len(roles) == 0 {
		return nil, false, errors.New("pg.MembershipWriteRepository.ReplaceRoles: tenantID, gcid and roles required")
	}

	var (
		allRoles []string
		created  bool
	)
	err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		// 1. Live-tenant guard.
		tRows, err := tx.Query(ctx,
			`SELECT 1 FROM tenants WHERE tenant_id = $1 AND deleted_at IS NULL AND status = 'active'`,
			tenantID)
		if err != nil {
			return fmt.Errorf("tenant guard: %w", err)
		}
		tenantLive := tRows.Next()
		tErr := tRows.Err()
		tRows.Close()
		if tErr != nil {
			return fmt.Errorf("tenant guard rows.Err: %w", tErr)
		}
		if !tenantLive {
			return tenancygrpc.ErrTenantNotFound
		}

		// 2. Suspension-only guard. Unlike UpsertRoles, ReplaceRoles is the
		//    explicit-admin-choice path: the admin posted the exact desired
		//    role set, so a soft-deleted row whose role is in `roles` is
		//    intentionally being resurrected (handled by the ON CONFLICT
		//    upsert below). Only `suspended_at` — admin-intent pause — still
		//    blocks; the admin must un-suspend before any role mutation.
		sRows, err := tx.Query(ctx, `
            SELECT 1 FROM members
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  suspended_at IS NOT NULL
            LIMIT  1`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("suspension guard: %w", err)
		}
		suspended := sRows.Next()
		sErr := sRows.Err()
		sRows.Close()
		if sErr != nil {
			return fmt.Errorf("suspension guard rows.Err: %w", sErr)
		}
		if suspended {
			return tenancygrpc.ErrMembershipSuspended
		}

		// 3. Owner guard (S7-B1 defect D2). Step 4 soft-deletes every live row
		//    whose role is not in the new set, and `owner` can never be in
		//    that set (grpc.upsertableRoles rejects it), so without this guard
		//    ANY role edit on the owner silently strips ownership. The H+
		//    multi-role editor's PUT /tenant-members/{gcid}/roles is exactly
		//    this call. Refuse whether or not another owner remains: ownership
		//    is the one role no admin API can grant back, so the loss is
		//    one-way and a "the tenant still has an owner" test would not make
		//    it recoverable for this person.
		if !containsString(roles, "owner") {
			owners, err := liveOwnerGCIDs(ctx, tx, tenantID)
			if err != nil {
				return err
			}
			if containsString(owners, gcid) {
				return tenancygrpc.ErrOwnerRoleProtected
			}
		}

		// 4. Soft-delete every live row whose role is NOT in the new set.
		//    Pre-filter via UNNEST so the SQL stays parameterised even with
		//    a variable-length kept-roles list. Exec — UPDATE returns no
		//    rows; Query would leave the conn busy for step 4.
		if err := tx.Exec(ctx, `
            UPDATE members
            SET    deleted_at = now(), updated_at = now()
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  deleted_at IS NULL AND suspended_at IS NULL
              AND  role::text <> ALL ($3::text[])`,
			tenantID, gcid, roles); err != nil {
			return fmt.Errorf("replace soft-delete: %w", err)
		}

		// 5. Upsert each role in the new set. The ON CONFLICT branch is
		//    deliberately DO UPDATE (not the additive DO NOTHING):
		//    ReplaceRoles also RESURRECTS rows whose role is in `roles`
		//    but were soft-deleted by step 4 (or by a prior replace).
		//    Clearing `deleted_at` is what brings the row back into the
		//    live aggregate. RETURNING (xmax=0) is the standard "was this
		//    a fresh INSERT" tell; we set `created=true` when any role in
		//    the request was a new row.
		for _, role := range roles {
			iRows, err := tx.Query(ctx, `
                INSERT INTO members (
                    tenant_id, gcid, role, invited_at, joined_at,
                    created_at, updated_at
                )
                VALUES ($1, $2::uuid, $3::tenant_member_role, now(), now(), now(), now())
                ON CONFLICT (tenant_id, gcid, role) DO UPDATE
                SET deleted_at = NULL, updated_at = now()
                RETURNING (xmax = 0)`,
				tenantID, gcid, role)
			if err != nil {
				return fmt.Errorf("insert role %q: %w", role, err)
			}
			if iRows.Next() {
				var freshInsert bool
				if err := iRows.Scan(&freshInsert); err == nil && freshInsert {
					created = true
				}
			}
			iErr := iRows.Err()
			iRows.Close()
			if iErr != nil {
				return fmt.Errorf("insert role %q rows.Err: %w", role, iErr)
			}
		}

		// 6. Post-replace live role aggregate.
		aRows, err := tx.Query(ctx, `
            SELECT DISTINCT role::text
            FROM   members
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  deleted_at IS NULL AND suspended_at IS NULL
            ORDER  BY role::text`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("role aggregate: %w", err)
		}
		defer aRows.Close()
		for aRows.Next() {
			var role string
			if err := aRows.Scan(&role); err != nil {
				return fmt.Errorf("role aggregate scan: %w", err)
			}
			allRoles = append(allRoles, role)
		}
		return aRows.Err()
	})
	if err != nil {
		if errors.Is(err, tenancygrpc.ErrTenantNotFound) || errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("pg.MembershipWriteRepository.ReplaceRoles: %w", err)
	}
	return allRoles, created, nil
}

// RemoveAllRoles SOFT-DELETES every live role row for (tenant, gcid) — the
// member-centric revoke (WS2b / CHO-1869). NEVER hard-deletes. Returns the
// count of rows removed (0 ⇒ no live membership). Mirrors ReplaceRoles'
// tenant-live + suspension guards: a suspended membership must be un-suspended
// before removal (explicit admin state).
func (r *MembershipWriteRepository) RemoveAllRoles(ctx context.Context, tenantID, gcid string) (int, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(gcid) == "" {
		return 0, errors.New("pg.MembershipWriteRepository.RemoveAllRoles: tenantID and gcid required")
	}

	var removed int
	err := r.tx.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		// 1. Live-tenant guard.
		tRows, err := tx.Query(ctx,
			`SELECT 1 FROM tenants WHERE tenant_id = $1 AND deleted_at IS NULL AND status = 'active'`,
			tenantID)
		if err != nil {
			return fmt.Errorf("tenant guard: %w", err)
		}
		tenantLive := tRows.Next()
		tErr := tRows.Err()
		tRows.Close()
		if tErr != nil {
			return fmt.Errorf("tenant guard rows.Err: %w", tErr)
		}
		if !tenantLive {
			return tenancygrpc.ErrTenantNotFound
		}

		// 2. Suspension guard — mirror ReplaceRoles: a suspended membership
		//    blocks until explicitly un-suspended.
		sRows, err := tx.Query(ctx, `
            SELECT 1 FROM members
            WHERE  tenant_id = $1 AND gcid = $2::uuid
              AND  suspended_at IS NOT NULL
            LIMIT  1`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("suspension guard: %w", err)
		}
		suspended := sRows.Next()
		sErr := sRows.Err()
		sRows.Close()
		if sErr != nil {
			return fmt.Errorf("suspension guard rows.Err: %w", sErr)
		}
		if suspended {
			return tenancygrpc.ErrMembershipSuspended
		}

		// 3. Last-owner guard (S7-B1 defect D1). Step 4 has no role filter, so
		//    removing a member takes their `owner` row with everything else.
		//    Refuse only when this GCID holds the tenant's LAST live owner
		//    row: the invariant is "no organisation is ever left without an
		//    owner", not "an owner row is untouchable". A co-owner can still
		//    be removed because the tenant stays owned. Handing over ownership
		//    (S7a) or the operator override (S7c) is how a sole owner leaves.
		owners, err := liveOwnerGCIDs(ctx, tx, tenantID)
		if err != nil {
			return err
		}
		if len(owners) == 1 && owners[0] == gcid {
			return tenancygrpc.ErrLastOwnerProtected
		}

		// 4. Soft-delete every live role row; count via RETURNING. Query (not
		//    Exec) so we can tally the removed rows for the response.
		dRows, err := tx.Query(ctx, `
            UPDATE members
            SET    deleted_at = now(), updated_at = now()
            WHERE  tenant_id = $1 AND gcid = $2::uuid AND deleted_at IS NULL
            RETURNING role`,
			tenantID, gcid)
		if err != nil {
			return fmt.Errorf("remove soft-delete: %w", err)
		}
		defer dRows.Close()
		for dRows.Next() {
			// Scan the RETURNING role (advances the cursor; the value itself
			// is unused — we only tally the soft-deleted rows).
			var role string
			if err := dRows.Scan(&role); err != nil {
				return fmt.Errorf("remove soft-delete scan: %w", err)
			}
			removed++
		}
		return dRows.Err()
	})
	if err != nil {
		if errors.Is(err, tenancygrpc.ErrTenantNotFound) || errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
			return 0, err
		}
		return 0, fmt.Errorf("pg.MembershipWriteRepository.RemoveAllRoles: %w", err)
	}
	return removed, nil
}

// Compile-time check.
var _ tenancygrpc.MembershipUpsertRepo = (*MembershipWriteRepository)(nil)
