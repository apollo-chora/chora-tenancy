// membership_owner_guard_test.go: S7-B1, the last-owner and owner-strip
// guards on the AUTHORITATIVE membership store (UX refactor R21).
//
// Two shipped defects, named D1 and D2 in
// `.handoffs/UX-REFACTOR-FIRST-LAUNCH-SPEC-2026-09-02.md` section 13.2:
//
//	D1  RemoveAllRoles soft-deletes EVERY live role row with no role filter,
//	    so DELETE /api/v1/admin/tenant-members/{gcid} on the owner leaves the
//	    tenant with no owner and no warning.
//	D2  ReplaceRoles soft-deletes every row whose role is NOT in the new set,
//	    and `owner` can never be in that set (the gRPC layer rejects it as
//	    non-upsertable), so any role edit on the owner strips ownership.
//
// The guards are deliberately ASYMMETRIC, and the asymmetry is the point:
//
//	RemoveAllRoles  refuses only when the target holds the tenant's LAST live
//	                owner row. "This person leaves the organisation" is a
//	                complete, explicit act; what must never happen is the
//	                organisation being left with nobody who owns it.
//	ReplaceRoles    refuses whenever it would strip ANY live owner row. A role
//	                edit is a partial edit, and ownership must not be its
//	                collateral damage: ownership is the one role no admin API
//	                can grant back (spec 13.3.1), so losing it here is
//	                one-way.
//
// Both refusals are sentinels the gRPC layer maps to FAILED_PRECONDITION plus
// a named errdetails reason; neither is silent, and neither is a 5xx.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
)

// mwOtherGcid is a second member of the same tenant, used to distinguish
// "the target is the last owner" from "somebody else owns this tenant".
const mwOtherGcid = "018f6f3a-0000-7000-8000-000000000def"

// ownerRow builds a stubRows row-fn for the owner-guard scan, which selects
// one text column (the owning GCID).
func ownerRow(gcid string) func(dest ...any) error {
	return func(dest ...any) error {
		if s, ok := dest[0].(*string); ok {
			*s = gcid
		}
		return nil
	}
}

// ownerGuardKey is the substring the stubTx keys the owner-guard query on.
// It is deliberately the WHERE clause rather than the table name: every other
// statement in these two methods also touches `members`.
const ownerGuardKey = "role = 'owner'"

// -----------------------------------------------------------------------------
// D1. RemoveAllRoles must refuse to remove a tenant's last owner.
// -----------------------------------------------------------------------------

func TestMembershipWriteRepository_RemoveAllRoles_RefusesLastOwner(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil},
		// The target IS the tenant's only live owner.
		ownerGuardKey:    {rows: []func(dest ...any) error{ownerRow(mwGcid)}},
		"RETURNING role": {rows: []func(dest ...any) error{roleRow("owner"), roleRow("admin")}},
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	removed, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid)
	if !errors.Is(err, tenancygrpc.ErrLastOwnerProtected) {
		t.Fatalf("err = %v, want ErrLastOwnerProtected", err)
	}
	if removed != 0 {
		t.Errorf("removed = %d, want 0 on a refusal", removed)
	}
	for _, sql := range tx.querySQL {
		if strings.Contains(sql, "deleted_at = now()") {
			t.Errorf("the last owner must be refused BEFORE any soft-delete; ran %q", sql)
		}
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_AllowsNonOwner(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil},
		// Somebody else owns this tenant; the target is an ordinary member.
		ownerGuardKey:    {rows: []func(dest ...any) error{ownerRow(mwOtherGcid)}},
		"RETURNING role": {rows: []func(dest ...any) error{roleRow("learner")}},
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	removed, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid)
	if err != nil {
		t.Fatalf("RemoveAllRoles: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d, want 1 (an ordinary member is unaffected by the guard)", removed)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_AllowsCoOwner(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil},
		// Two live owner rows: removing this one still leaves the tenant owned,
		// so the LAST-owner guard does not fire. The invariant the guard
		// defends is "never zero owners", not "never touch an owner".
		ownerGuardKey:    {rows: []func(dest ...any) error{ownerRow(mwGcid), ownerRow(mwOtherGcid)}},
		"RETURNING role": {rows: []func(dest ...any) error{roleRow("owner")}},
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	if _, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid); err != nil {
		t.Fatalf("RemoveAllRoles: %v", err)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_OwnerGuardIsTenantWideAndLocks(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil},
		ownerGuardKey:              {rows: []func(dest ...any) error{ownerRow(mwOtherGcid)}},
		"RETURNING role":           {rows: []func(dest ...any) error{roleRow("learner")}},
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})
	if _, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid); err != nil {
		t.Fatalf("RemoveAllRoles: %v", err)
	}
	assertOwnerGuardShape(t, tx.querySQL)
}

// -----------------------------------------------------------------------------
// D2. ReplaceRoles must never strip the owner role.
// -----------------------------------------------------------------------------

// replaceRolesTx wires the stubTx guard responses for a ReplaceRoles run.
func replaceRolesTx(owners []string, aggregate []string) *stubTx {
	ownerRows := make([]func(dest ...any) error, 0, len(owners))
	for _, g := range owners {
		ownerRows = append(ownerRows, ownerRow(g))
	}
	aggRows := make([]func(dest ...any) error, 0, len(aggregate))
	for _, role := range aggregate {
		aggRows = append(aggRows, roleRow(role))
	}
	return &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":               {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL":   {rows: nil},
		ownerGuardKey:                {rows: ownerRows},
		"INSERT INTO members":        {rows: []func(dest ...any) error{scanRow(true)}},
		"SELECT DISTINCT role::text": {rows: aggRows},
	}}
}

func TestMembershipWriteRepository_ReplaceRoles_RefusesOwnerStrip(t *testing.T) {
	t.Parallel()
	// The target owns the tenant and the requested set does not carry `owner`,
	// which is the only set the gRPC layer can ever produce. This is the H+
	// multi-role editor's exact call.
	tx := replaceRolesTx([]string{mwGcid}, nil)
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	roles, created, err := r.ReplaceRoles(context.Background(), mwTenant, mwGcid, []string{"instructor"})
	if !errors.Is(err, tenancygrpc.ErrOwnerRoleProtected) {
		t.Fatalf("err = %v, want ErrOwnerRoleProtected", err)
	}
	if roles != nil || created {
		t.Errorf("refusal must return no roles and created=false; got %v / %v", roles, created)
	}
	for _, sql := range tx.execSQL {
		if strings.Contains(sql, "deleted_at = now()") {
			t.Errorf("owner strip must be refused BEFORE the soft-delete; ran %q", sql)
		}
	}
	for _, sql := range tx.querySQL {
		if strings.Contains(sql, "INSERT INTO members") {
			t.Errorf("owner strip must be refused BEFORE any insert; ran %q", sql)
		}
	}
}

func TestMembershipWriteRepository_ReplaceRoles_RefusesEvenWhenAnotherOwnerExists(t *testing.T) {
	t.Parallel()
	// Unlike the last-owner guard, this one does not care whether the tenant
	// keeps an owner: the individual loses a role no API can grant back.
	tx := replaceRolesTx([]string{mwGcid, mwOtherGcid}, nil)
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	if _, _, err := r.ReplaceRoles(context.Background(), mwTenant, mwGcid, []string{"admin"}); !errors.Is(err, tenancygrpc.ErrOwnerRoleProtected) {
		t.Fatalf("err = %v, want ErrOwnerRoleProtected", err)
	}
}

func TestMembershipWriteRepository_ReplaceRoles_AllowsWhenOwnerIsKept(t *testing.T) {
	t.Parallel()
	// Defensive: the gRPC layer never sends `owner`, but the guard predicate
	// must be exact: it fires on a STRIP, not on the presence of an owner.
	tx := replaceRolesTx([]string{mwGcid}, []string{"admin", "owner"})
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	roles, _, err := r.ReplaceRoles(context.Background(), mwTenant, mwGcid, []string{"owner", "admin"})
	if err != nil {
		t.Fatalf("ReplaceRoles: %v", err)
	}
	if len(roles) != 2 {
		t.Errorf("roles = %v, want the post-replace aggregate", roles)
	}
}

func TestMembershipWriteRepository_ReplaceRoles_AllowsNonOwner(t *testing.T) {
	t.Parallel()
	tx := replaceRolesTx([]string{mwOtherGcid}, []string{"instructor"})
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	if _, _, err := r.ReplaceRoles(context.Background(), mwTenant, mwGcid, []string{"instructor"}); err != nil {
		t.Fatalf("ReplaceRoles: %v", err)
	}
	assertOwnerGuardShape(t, tx.querySQL)
}

// assertOwnerGuardShape pins the four properties the guard query must have.
// A guard that got any of these wrong would still pass the behaviour tests
// above against a stub, and would be wrong against Postgres.
func assertOwnerGuardShape(t *testing.T, queries []string) {
	t.Helper()
	var guard string
	for _, sql := range queries {
		if strings.Contains(sql, ownerGuardKey) {
			guard = sql
			break
		}
	}
	if guard == "" {
		t.Fatalf("no owner-guard query issued; queries=%v", queries)
	}
	if strings.Contains(guard, "gcid = $2") || strings.Contains(guard, "gcid = $1") {
		t.Errorf("the guard must scan the TENANT's owner rows, not just the target's, "+
			"or it can never tell a last owner from a co-owner; got %q", guard)
	}
	if !strings.Contains(guard, "deleted_at IS NULL") {
		t.Errorf("the guard must count LIVE owner rows only; got %q", guard)
	}
	if !strings.Contains(guard, "FOR UPDATE") {
		t.Errorf("the guard must lock the owner rows it asserts on, or a concurrent "+
			"remove can empty the tenant between the check and the write; got %q", guard)
	}
	if strings.Contains(guard, "suspended_at") {
		t.Errorf("a suspended owner is still the owner of record; excluding them would let "+
			"an admin suspend-then-remove to strip ownership; got %q", guard)
	}
}

// -----------------------------------------------------------------------------
// The guard's own failure modes. A guard that swallows its query error would
// read as "no owners" and wave the destructive write straight through, which
// is worse than the defect it replaces.
// -----------------------------------------------------------------------------

func TestMembershipWriteRepository_OwnerGuard_FailsLoudOnQueryError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		third func() (pg.Rows, error)
		want  string
	}{
		{
			name:  "query error",
			third: guardErr,
			want:  "owner guard",
		},
		{
			name: "rows iteration error",
			third: func() (pg.Rows, error) {
				return &stubRows{err: errors.New("owner iter boom")}, nil
			},
			want: "owner guard rows.Err",
		},
		{
			name: "scan error",
			third: func() (pg.Rows, error) {
				return &stubRows{rows: []func(dest ...any) error{
					func(dest ...any) error { return errors.New("owner scan boom") },
				}}, nil
			},
			want: "owner guard scan",
		},
	}
	for _, tc := range cases {
		t.Run("ReplaceRoles/"+tc.name, func(t *testing.T) {
			t.Parallel()
			r := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, tc.third)}
			repo := pg.NewMembershipWriteRepository(r, r)
			_, _, err := repo.ReplaceRoles(context.Background(), mwTenant, mwGcid, []string{"admin"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
		t.Run("RemoveAllRoles/"+tc.name, func(t *testing.T) {
			t.Parallel()
			r := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, tc.third)}
			repo := pg.NewMembershipWriteRepository(r, r)
			_, err := repo.RemoveAllRoles(context.Background(), mwTenant, mwGcid)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}
