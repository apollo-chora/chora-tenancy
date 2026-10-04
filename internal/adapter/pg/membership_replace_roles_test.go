// membership_replace_roles_test.go — unit coverage for the H+ Members-page
// REPLACE flavour (CHO-1809): ReplaceRoles' tenant-live + suspension guards,
// soft-delete of dropped roles, per-role resurrecting upsert and the
// post-replace role aggregate. Stubs the QueryRunner/TxQuerier seams the
// same way membership_write_repository_test.go does for UpsertRoles.
package pg_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
)

// replaceRolesHappy wires a stub whose Query responses model:
// tenant-guard (1 live row) → suspension-guard (0 rows) → per-role
// RETURNING(xmax=0) rows → post-replace role aggregate.
func replaceRolesHappy(t *testing.T, roleInserts []bool, aggregate []string) *stubQueryRunner {
	t.Helper()
	q := &stubQueryRunner{}
	q.rowsResponses = []*stubRows{
		{rows: []func(dest ...any) error{scanRow(int64(1))}}, // tenant guard: live
		{}, // suspension guard: not suspended
		{}, // owner guard (S7-B1): tenant holds no live owner row
	}
	for _, fresh := range roleInserts {
		q.rowsResponses = append(q.rowsResponses, &stubRows{rows: []func(dest ...any) error{
			scanRow(fresh),
		}})
	}
	agg := make([]func(dest ...any) error, 0, len(aggregate))
	for _, r := range aggregate {
		agg = append(agg, scanRow(r))
	}
	q.rowsResponses = append(q.rowsResponses, &stubRows{rows: agg})
	return q
}

func TestMembershipWriteRepository_ReplaceRoles_HappyPath(t *testing.T) {
	t.Parallel()
	q := replaceRolesHappy(t, []bool{true, false}, []string{"admin", "trainer"})
	repo := pg.NewMembershipWriteRepository(q, q)
	roles, created, err := repo.ReplaceRoles(context.Background(), "tenant-1", "gcid-1", []string{"admin", "trainer"})
	if err != nil {
		t.Fatalf("ReplaceRoles: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true (fresh INSERT on 'admin')")
	}
	if !reflect.DeepEqual(roles, []string{"admin", "trainer"}) {
		t.Fatalf("unexpected aggregate %v", roles)
	}
	if len(q.txTenants) != 1 || q.txTenants[0] != "tenant-1" {
		t.Fatalf("expected tenant-scoped tx, got %v", q.txTenants)
	}
	// 1 soft-delete Exec with the kept-set bound as text[].
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 soft-delete exec, got %d", len(q.execCalls))
	}
	del := q.execCalls[0]
	if !strings.Contains(del.sql, "role::text <> ALL ($3::text[])") {
		t.Fatalf("expected ALL($3::text[]) soft-delete, sql=%q", del.sql)
	}
	if got := del.args[2]; !reflect.DeepEqual(got, []string{"admin", "trainer"}) {
		t.Fatalf("expected kept roles bound as slice, got %#v", got)
	}
}

func TestMembershipWriteRepository_ReplaceRoles_NoFreshInsert(t *testing.T) {
	t.Parallel()
	q := replaceRolesHappy(t, []bool{false}, []string{"admin"})
	repo := pg.NewMembershipWriteRepository(q, q)
	_, created, err := repo.ReplaceRoles(context.Background(), "tenant-1", "gcid-1", []string{"admin"})
	if err != nil {
		t.Fatalf("ReplaceRoles: %v", err)
	}
	if created {
		t.Fatalf("expected created=false when every role already existed")
	}
}

func TestMembershipWriteRepository_ReplaceRoles_InputValidation(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{}
	repo := pg.NewMembershipWriteRepository(q, q)
	if _, _, err := repo.ReplaceRoles(context.Background(), "", "g", []string{"admin"}); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
	if _, _, err := repo.ReplaceRoles(context.Background(), "t", "", []string{"admin"}); err == nil {
		t.Fatalf("expected error for empty gcid")
	}
	if _, _, err := repo.ReplaceRoles(context.Background(), "t", "g", nil); err == nil {
		t.Fatalf("expected error for empty roles")
	}
	if _, _, err := repo.ReplaceRoles(context.Background(), "t", "g", []string{}); err == nil {
		t.Fatalf("expected error for empty roles slice")
	}
	if len(q.txTenants) != 0 {
		t.Fatalf("expected no tx for invalid input")
	}
}

func TestMembershipWriteRepository_ReplaceRoles_Guards(t *testing.T) {
	t.Parallel()
	// Tenant not live → ErrTenantNotFound (sentinel passthrough).
	q := &stubQueryRunner{}
	q.rowsResponses = []*stubRows{{}} // guards: 0 rows
	repo := pg.NewMembershipWriteRepository(q, q)
	if _, _, err := repo.ReplaceRoles(context.Background(), "tenant-1", "gcid-1", []string{"admin"}); !errors.Is(err, tenancygrpc.ErrTenantNotFound) {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
	// Suspended membership → ErrMembershipSuspended.
	q2 := &stubQueryRunner{}
	q2.rowsResponses = []*stubRows{
		{rows: []func(dest ...any) error{scanRow(int64(1))}}, // tenant live
		{rows: []func(dest ...any) error{scanRow(int64(1))}}, // suspended
	}
	repo2 := pg.NewMembershipWriteRepository(q2, q2)
	if _, _, err := repo2.ReplaceRoles(context.Background(), "tenant-1", "gcid-1", []string{"admin"}); !errors.Is(err, tenancygrpc.ErrMembershipSuspended) {
		t.Fatalf("expected ErrMembershipSuspended, got %v", err)
	}
}

// seqQueryRunner scripts per-Query behaviour positionally so a specific
// statement (tenant guard, suspension guard, role insert, aggregate) can be
// made to fail while the others succeed.
type seqQueryRunner struct {
	execFn  func(sql string, args ...any) error
	queryFn func(sql string, args ...any) (pg.Rows, error)
	rowFn   func(sql string, args ...any) pg.Row
}

func (s *seqQueryRunner) Exec(ctx context.Context, sql string, args ...any) error {
	if s.execFn == nil {
		return nil
	}
	return s.execFn(sql, args...)
}
func (s *seqQueryRunner) QueryRow(ctx context.Context, sql string, args ...any) pg.Row {
	if s.rowFn != nil {
		return s.rowFn(sql, args...)
	}
	return &stubRow{scanFn: func(dest ...any) error { return pg.ErrNoRows }}
}
func (s *seqQueryRunner) Query(ctx context.Context, sql string, args ...any) (pg.Rows, error) {
	if s.queryFn == nil {
		return &stubRows{}, nil
	}
	return s.queryFn(sql, args...)
}
func (s *seqQueryRunner) RunInTenantTx(ctx context.Context, _ string, fn func(context.Context, pg.Tx) error) error {
	return fn(ctx, s)
}

// querySeq is a helper that returns scripted Query responses in order.
func querySeq(seq ...func() (pg.Rows, error)) func(string, ...any) (pg.Rows, error) {
	i := 0
	return func(sql string, args ...any) (pg.Rows, error) {
		if i >= len(seq) {
			return &stubRows{}, nil
		}
		r := seq[i]
		i++
		return r()
	}
}

func guardLive() (pg.Rows, error) {
	return &stubRows{rows: []func(dest ...any) error{scanRow(int64(1))}}, nil
}
func guardEmpty() (pg.Rows, error) { return &stubRows{}, nil }
func roleInsert(fresh bool) func() (pg.Rows, error) {
	return func() (pg.Rows, error) {
		return &stubRows{rows: []func(dest ...any) error{scanRow(bool(fresh))}}, nil
	}
}
func guardErr() (pg.Rows, error) { return nil, errors.New("db down") }

func TestMembershipWriteRepository_ReplaceRoles_ErrorPaths(t *testing.T) {
	t.Parallel()
	// Tenant-guard Query fails.
	r := &seqQueryRunner{queryFn: func(sql string, args ...any) (pg.Rows, error) { return guardErr() }}
	repo := pg.NewMembershipWriteRepository(r, r)
	if _, _, err := repo.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "tenant guard") {
		t.Fatalf("expected tenant-guard error, got %v", err)
	}
	// Tenant-guard rows.Err after Next=false.
	r2 := &seqQueryRunner{queryFn: querySeq(func() (pg.Rows, error) {
		return &stubRows{err: errors.New("guard iter boom")}, nil
	})}
	repo2 := pg.NewMembershipWriteRepository(r2, r2)
	if _, _, err := repo2.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "rows.Err") {
		t.Fatalf("expected guard rows.Err error, got %v", err)
	}
	// Suspension guard Query fails.
	r3 := &seqQueryRunner{queryFn: querySeq(guardLive, guardErr)}
	repo3 := pg.NewMembershipWriteRepository(r3, r3)
	if _, _, err := repo3.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "suspension guard") {
		t.Fatalf("expected suspension-guard error, got %v", err)
	}
	// Suspension guard rows.Err.
	r3b := &seqQueryRunner{queryFn: querySeq(guardLive, func() (pg.Rows, error) {
		return &stubRows{err: errors.New("susp iter boom")}, nil
	})}
	repo3b := pg.NewMembershipWriteRepository(r3b, r3b)
	if _, _, err := repo3b.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "suspension guard") {
		t.Fatalf("expected suspension rows.Err error, got %v", err)
	}
	// Soft-delete Exec fails.
	r4 := &seqQueryRunner{
		queryFn: querySeq(guardLive, guardEmpty),
		execFn:  func(sql string, args ...any) error { return errors.New("update db down") },
	}
	repo4 := pg.NewMembershipWriteRepository(r4, r4)
	if _, _, err := repo4.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "replace soft-delete") {
		t.Fatalf("expected soft-delete error, got %v", err)
	}
	// Role upsert Query fails (guards succeed).
	r5 := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, guardEmpty, guardErr)}
	repo5 := pg.NewMembershipWriteRepository(r5, r5)
	if _, _, err := repo5.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "insert role") {
		t.Fatalf("expected role-insert error, got %v", err)
	}
	// Role-insert rows.Err.
	r6 := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, guardEmpty, func() (pg.Rows, error) {
		return &stubRows{rows: []func(dest ...any) error{scanRow(true)}, err: errors.New("insert iter boom")}, nil
	})}
	repo6 := pg.NewMembershipWriteRepository(r6, r6)
	if _, _, err := repo6.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "insert role") {
		t.Fatalf("expected role-insert rows.Err error, got %v", err)
	}
	// Role-aggregate Query fails.
	r7 := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, guardEmpty, roleInsert(true), guardErr)}
	repo7 := pg.NewMembershipWriteRepository(r7, r7)
	if _, _, err := repo7.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "role aggregate") {
		t.Fatalf("expected aggregate error, got %v", err)
	}
	// Role-aggregate rows.Err.
	r8 := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, guardEmpty, roleInsert(true), func() (pg.Rows, error) {
		return &stubRows{rows: []func(dest ...any) error{scanRow("admin")}, err: errors.New("agg iter boom")}, nil
	})}
	repo8 := pg.NewMembershipWriteRepository(r8, r8)
	if _, _, err := repo8.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "ReplaceRoles") {
		t.Fatalf("expected wrapped aggregate rows.Err error, got %v", err)
	}
	// Aggregate scan error.
	r9 := &seqQueryRunner{queryFn: querySeq(guardLive, guardEmpty, guardEmpty, roleInsert(true), func() (pg.Rows, error) {
		return &stubRows{rows: []func(dest ...any) error{func(dest ...any) error { return errors.New("agg scan boom") }}}, nil
	})}
	repo9 := pg.NewMembershipWriteRepository(r9, r9)
	if _, _, err := repo9.ReplaceRoles(context.Background(), "t", "g", []string{"admin"}); !strings.Contains(err.Error(), "role aggregate scan") {
		t.Fatalf("expected aggregate scan error, got %v", err)
	}
}
