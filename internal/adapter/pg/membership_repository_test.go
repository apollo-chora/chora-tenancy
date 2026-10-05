// membership_repository_test.go — unit tests for the pgx MembershipRepository
// (Bucket 1, 2026-05-14 multi-tenant identity; D0.1 RLS-bypass rewire
// 2026-05-14, CHO-1538).
//
// Stubs QueryRunner to avoid a live Postgres dependency.
//
// D0.1 rewire: ListByGCID no longer emits an inline cross-tenant
// `SELECT ... ARRAY_AGG ... FROM members JOIN tenants`. That inline query
// is filtered to ZERO rows by the `tenant_isolation` RLS policy on
// `members` once RLS is re-enabled (the mint flow is a cross-tenant
// directory lookup with no `chora.tenant_id` to set). Instead it calls
// the `SECURITY DEFINER` PL/pgSQL function `list_memberships_by_gcid(uuid)`
// (migration 0011) which runs with `SET row_security = off` — the
// aggregation + JOIN now live INSIDE the function body. The unit tests
// assert the new function-call SQL shape; the cross-tenant vs
// tenant-scoped RLS semantics are asserted by the integration test in
// integration_test.go.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
)

// TestMembershipRepository_ListByGCID_CallsSecurityDefinerFunction asserts
// the D0.1 rewire: the repo invokes the SECURITY DEFINER function rather
// than the legacy inline cross-tenant SELECT (which RLS would refuse).
func TestMembershipRepository_ListByGCID_CallsSecurityDefinerFunction(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{rows: nil}},
	}
	r := pg.NewMembershipRepository(q)
	_, err := r.ListByGCID(context.Background(), "01970000-0000-7000-8000-0000000000aa", false)
	if err != nil {
		t.Fatalf("ListByGCID: %v", err)
	}
	if len(q.queryCalls) != 1 {
		t.Fatalf("expected 1 Query call, got %d", len(q.queryCalls))
	}
	got := q.queryCalls[0].sql
	// Must call the SECURITY DEFINER function (migration 0011) — this is
	// the RLS-bypass path. The inline `FROM members` / `ARRAY_AGG` is
	// gone: the aggregation lives inside the function body now.
	if !strings.Contains(got, "list_memberships_by_gcid") {
		t.Errorf("SQL must call list_memberships_by_gcid SECURITY DEFINER function; got %q", got)
	}
	// The function takes the gcid as $1, cast to uuid.
	if !strings.Contains(got, "$1") {
		t.Errorf("SQL must pass gcid as $1 to the function; got %q", got)
	}
	if !strings.Contains(strings.ToLower(got), "uuid") {
		t.Errorf("SQL must cast the gcid arg to uuid; got %q", got)
	}
	// Guard against a regression back to the inline cross-tenant SELECT —
	// that query is RLS-filtered to zero rows once RLS is re-enabled.
	if strings.Contains(got, "FROM   members m") || strings.Contains(got, "FROM members") {
		t.Errorf("SQL must NOT inline-SELECT FROM members (RLS would refuse it); got %q", got)
	}
}

func TestMembershipRepository_ListByGCID_ScansRow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{
			rows: []func(dest ...any) error{
				func(dest ...any) error {
					// dest = (*string tenant_id, *string slug, *[]string roles, *time.Time joined)
					*(dest[0].(*string)) = "01970000-0000-7000-8000-000000000001"
					*(dest[1].(*string)) = "academy-A"
					*(dest[2].(*[]string)) = []string{"learner", "author"}
					*(dest[3].(*time.Time)) = now
					return nil
				},
				func(dest ...any) error {
					*(dest[0].(*string)) = "01970000-0000-7000-8000-000000000002"
					*(dest[1].(*string)) = "academy-B"
					*(dest[2].(*[]string)) = []string{"instructor"}
					*(dest[3].(*time.Time)) = now.Add(-24 * time.Hour)
					return nil
				},
			},
		}},
	}
	r := pg.NewMembershipRepository(q)
	out, err := r.ListByGCID(context.Background(), "01970000-0000-7000-8000-0000000000aa", false)
	if err != nil {
		t.Fatalf("ListByGCID: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d rows, want 2", len(out))
	}
	if out[0].TenantID != "01970000-0000-7000-8000-000000000001" {
		t.Errorf("row 0 TenantID = %q", out[0].TenantID)
	}
	if got, want := out[0].Roles, []string{"learner", "author"}; !equalStringsLocal(got, want) {
		t.Errorf("row 0 Roles = %v want %v", got, want)
	}
	if out[1].TenantSlug != "academy-B" {
		t.Errorf("row 1 TenantSlug = %q", out[1].TenantSlug)
	}
}

func TestMembershipRepository_ListByGCID_EmptyGCID(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	r := pg.NewMembershipRepository(q)
	_, err := r.ListByGCID(context.Background(), "", false)
	if err == nil {
		t.Fatalf("expected error for empty gcid")
	}
}

func TestMembershipRepository_ListByGCID_ScanErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{
			rows: []func(dest ...any) error{
				func(dest ...any) error {
					return errors.New("malformed pg row")
				},
			},
		}},
	}
	r := pg.NewMembershipRepository(q)
	_, err := r.ListByGCID(context.Background(), "01970000-0000-7000-8000-0000000000aa", false)
	if err == nil {
		t.Fatalf("expected error propagation when row Scan fails")
	}
}

func TestMembershipRepository_ListByGCID_EmptyResultIsNotAnError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{rows: nil}},
	}
	r := pg.NewMembershipRepository(q)
	out, err := r.ListByGCID(context.Background(), "01970000-0000-7000-8000-0000000000aa", false)
	if err != nil {
		t.Fatalf("ListByGCID: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty result for unknown gcid, got %d rows", len(out))
	}
}

func TestMembershipRepository_ListByGCID_EmptyRolesAreCleaned(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{
		rowsResponses: []*stubRows{{
			rows: []func(dest ...any) error{
				func(dest ...any) error {
					*(dest[0].(*string)) = "01970000-0000-7000-8000-000000000001"
					*(dest[1].(*string)) = "academy-A"
					*(dest[2].(*[]string)) = []string{"  ", "learner", ""}
					*(dest[3].(*time.Time)) = time.Now()
					return nil
				},
			},
		}},
	}
	r := pg.NewMembershipRepository(q)
	out, err := r.ListByGCID(context.Background(), "01970000-0000-7000-8000-0000000000aa", false)
	if err != nil {
		t.Fatalf("ListByGCID: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d rows, want 1", len(out))
	}
	if got, want := out[0].Roles, []string{"learner"}; !equalStringsLocal(got, want) {
		t.Errorf("expected empty roles to be cleaned out, got %v want %v", got, want)
	}
}

// equalStringsLocal — local helper because the existing pg test files all
// have a sibling helper named `equal` for their domain types. Avoid name
// collisions by using a suffixed helper here.
func equalStringsLocal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
