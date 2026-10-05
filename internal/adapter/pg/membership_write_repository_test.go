// membership_write_repository_test.go — unit tests for the pgx
// MembershipWriteRepository (ADR-182 UpsertMembership write path).
//
// Stubs QueryRunner + TxQuerier — no live Postgres. The RLS semantics
// (SET LOCAL chora.tenant_id) are exercised by RunInTenantTx itself and
// covered by the existing pg integration tests; these tests assert the
// SQL shapes, the guard ordering, and the sentinel error mapping.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	tenancygrpc "github.com/apollo-chora/chora-tenancy/internal/adapter/grpc"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

const (
	mwTenant = "00000000-0000-7000-8000-000000000001"
	mwGcid   = "018f6f3a-0000-7000-8000-000000000abc"
)

func liveRow() func(dest ...any) error {
	return func(dest ...any) error { return nil } // guards only Next(), never Scan
}

func roleRow(role string) func(dest ...any) error {
	return func(dest ...any) error {
		if s, ok := dest[0].(*string); ok {
			*s = role
		}
		return nil
	}
}

func TestMembershipWriteRepository_UpsertRoles_HappyPath(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":               {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL":   {rows: nil},                                  // no suspended rows
		"INSERT INTO members":        {rows: []func(dest ...any) error{liveRow()}}, // first insert created, second conflicts
		"SELECT DISTINCT role::text": {rows: []func(dest ...any) error{roleRow("author"), roleRow("learner")}},
	}}
	q := &stubTxQuerier{tx: tx}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, q)

	allRoles, created, err := r.UpsertRoles(context.Background(), mwTenant, mwGcid, []string{"learner", "author"})
	if err != nil {
		t.Fatalf("UpsertRoles: %v", err)
	}
	if !created {
		t.Errorf("created = false, want true (first insert returned a row)")
	}
	if len(allRoles) != 2 || allRoles[0] != "author" || allRoles[1] != "learner" {
		t.Errorf("allRoles = %v, want [author learner]", allRoles)
	}
	if q.opened != 1 {
		t.Errorf("expected exactly 1 tenant-scoped tx, got %d", q.opened)
	}
	var inserts int
	for _, sql := range tx.querySQL {
		if strings.Contains(sql, "INSERT INTO members") {
			inserts++
			if !strings.Contains(sql, "ON CONFLICT (tenant_id, gcid, role) DO NOTHING") {
				t.Errorf("insert must be idempotent on (tenant_id, gcid, role); got %q", sql)
			}
			if !strings.Contains(sql, "RETURNING 1") {
				t.Errorf("insert must RETURNING 1 for created detection; got %q", sql)
			}
		}
	}
	if inserts != 2 {
		t.Errorf("expected 2 role inserts, got %d", inserts)
	}
}

func TestMembershipWriteRepository_UpsertRoles_SuspendedBlocked(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: []func(dest ...any) error{liveRow()}}, // suspended row present
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	_, _, err := r.UpsertRoles(context.Background(), mwTenant, mwGcid, []string{"learner"})
	if !errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
		t.Fatalf("err = %v, want ErrMembershipSuspended", err)
	}
	for _, sql := range tx.querySQL {
		if strings.Contains(sql, "INSERT INTO members") {
			t.Errorf("suspended membership must block before any INSERT; ran %q", sql)
		}
	}
}

func TestMembershipWriteRepository_UpsertRoles_DeadTenant(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants": {rows: nil}, // no live tenant
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})

	_, _, err := r.UpsertRoles(context.Background(), mwTenant, mwGcid, []string{"learner"})
	if !errors.Is(err, tenancygrpc.ErrTenantNotFound) {
		t.Fatalf("err = %v, want ErrTenantNotFound", err)
	}
}

func TestMembershipWriteRepository_UpsertRoles_InputValidation(t *testing.T) {
	t.Parallel()
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{})
	if _, _, err := r.UpsertRoles(context.Background(), "", mwGcid, []string{"learner"}); err == nil {
		t.Errorf("empty tenantID must error")
	}
	if _, _, err := r.UpsertRoles(context.Background(), mwTenant, "", []string{"learner"}); err == nil {
		t.Errorf("empty gcid must error")
	}
	if _, _, err := r.UpsertRoles(context.Background(), mwTenant, mwGcid, nil); err == nil {
		t.Errorf("empty roles must error")
	}
}

func TestMembershipWriteRepository_ResolveTenantIDBySlug(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{
			{rows: []func(dest ...any) error{roleRow(mwTenant)}}, // hit
			{rows: nil}, // miss
		},
	}
	r := pg.NewMembershipWriteRepository(q, &stubTxQuerier{})

	id, err := r.ResolveTenantIDBySlug(context.Background(), "chora-master")
	if err != nil {
		t.Fatalf("ResolveTenantIDBySlug: %v", err)
	}
	if id != mwTenant {
		t.Errorf("id = %q, want %q", id, mwTenant)
	}
	sql := q.queryCalls[0].sql
	if !strings.Contains(sql, "deleted_at IS NULL") || !strings.Contains(sql, "status = 'active'") {
		t.Errorf("slug resolution must filter live+active tenants; got %q", sql)
	}

	if _, err := r.ResolveTenantIDBySlug(context.Background(), "ghost"); !errors.Is(err, tenancygrpc.ErrTenantNotFound) {
		t.Errorf("unknown slug err = %v, want ErrTenantNotFound", err)
	}
}

// -----------------------------------------------------------------------------
// RemoveAllRoles (WS2b / CHO-1869) — member-centric soft-delete.
// -----------------------------------------------------------------------------

func TestMembershipWriteRepository_RemoveAllRoles_HappyPath(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil}, // not suspended
		"RETURNING role":           {rows: []func(dest ...any) error{roleRow("instructor"), roleRow("learner")}},
	}}
	q := &stubTxQuerier{tx: tx}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, q)

	removed, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid)
	if err != nil {
		t.Fatalf("RemoveAllRoles: %v", err)
	}
	if removed != 2 {
		t.Errorf("removed = %d, want 2", removed)
	}
	// SQL shape: an UPDATE soft-delete with RETURNING and NO per-role filter
	// (remove erases the whole membership, unlike ReplaceRoles' `<> ALL`).
	var sawSoftDelete bool
	for _, sql := range tx.querySQL {
		if strings.Contains(sql, "UPDATE") && strings.Contains(sql, "deleted_at = now()") && strings.Contains(sql, "RETURNING role") {
			sawSoftDelete = true
			if strings.Contains(sql, "<> ALL") {
				t.Errorf("RemoveAllRoles must NOT filter by a kept-role set")
			}
		}
	}
	if !sawSoftDelete {
		t.Errorf("no soft-delete UPDATE...RETURNING role issued; queries=%v", tx.querySQL)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_NoLiveRows_Zero(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: nil},
		"RETURNING role":           {rows: nil}, // nothing live to remove
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})
	removed, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid)
	if err != nil {
		t.Fatalf("RemoveAllRoles: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed = %d, want 0 (server maps this to NOT_FOUND)", removed)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_Suspended(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants":             {rows: []func(dest ...any) error{liveRow()}},
		"suspended_at IS NOT NULL": {rows: []func(dest ...any) error{liveRow()}}, // suspended
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})
	if _, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid); !errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
		t.Errorf("err = %v, want ErrMembershipSuspended", err)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_TenantNotLive(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowsByQ: map[string]*stubRows{
		"FROM tenants": {rows: nil}, // tenant not live/active
	}}
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: tx})
	if _, err := r.RemoveAllRoles(context.Background(), mwTenant, mwGcid); !errors.Is(err, tenancygrpc.ErrTenantNotFound) {
		t.Errorf("err = %v, want ErrTenantNotFound", err)
	}
}

func TestMembershipWriteRepository_RemoveAllRoles_Validation(t *testing.T) {
	t.Parallel()
	r := pg.NewMembershipWriteRepository(&stubQueryRunner{}, &stubTxQuerier{tx: &stubTx{}})
	if _, err := r.RemoveAllRoles(context.Background(), "", mwGcid); err == nil {
		t.Errorf("empty tenant must error")
	}
	if _, err := r.RemoveAllRoles(context.Background(), mwTenant, ""); err == nil {
		t.Errorf("empty gcid must error")
	}
}
