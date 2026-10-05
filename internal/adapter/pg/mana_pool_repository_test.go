// mana_pool_repository_test.go: CHO-2414 RED tests for the pgx-backed
// TenantManaPool store that replaces manapool.InmemPoolRepo on the
// production wiring.
//
// Why these tests and not just an integration test: `tenant_mana_pools`
// has RLS ENABLED with the `tenant_isolation` policy and
// chora_tenancy_app_rw is NOBYPASSRLS (migration 0013). A repository that
// reaches the table through the plain pool Querier rather than
// RunInTenantTx reads ZERO rows and fails the INSERT's WITH CHECK, and
// the table has never held a production row so nothing would have caught
// it. The tenant-scoped-tx assertion below is the guard for that class.
//
// Live-pg proof lives in mana_pool_repository_integration_test.go
// (//go:build integration).
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	poolpkg "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

const (
	mpTenantA = "11111111-1111-7111-8111-111111111111"
	mpGCID    = "22222222-2222-7222-8222-222222222222"
	mpPoolID  = "33333333-3333-7333-8333-333333333333"
)

// ----------------------------------------------------------------------------
// stubs (named mpXxx so they do not collide with the package-wide stubs in
// legacy_tenancy_repository_test.go / tenant_repository_test.go)
// ----------------------------------------------------------------------------

type mpRow struct{ fn func(dest ...any) error }

func (r *mpRow) Scan(dest ...any) error { return r.fn(dest...) }

type mpTx struct {
	execSQL  []string
	execArgs [][]any
	querySQL []string
	rowFn    func(sql string, args ...any) pg.Row
	execErr  error
}

func (t *mpTx) Exec(_ context.Context, sql string, args ...any) error {
	t.execSQL = append(t.execSQL, sql)
	t.execArgs = append(t.execArgs, args)
	return t.execErr
}

func (t *mpTx) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	t.querySQL = append(t.querySQL, sql)
	return nil, errors.New("mpTx.Query: not used by the mana-pool repository")
}

func (t *mpTx) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	t.querySQL = append(t.querySQL, sql)
	if t.rowFn != nil {
		return t.rowFn(sql, args...)
	}
	return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
}

// mpQuerier records the tenant id every RunInTenantTx was opened with, which
// is what proves the RLS GUC would have been set for the right tenant.
type mpQuerier struct {
	tx       *mpTx
	tenants  []string
	beginErr error
}

func (q *mpQuerier) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	q.tenants = append(q.tenants, tenantID)
	if q.beginErr != nil {
		return q.beginErr
	}
	if q.tx == nil {
		q.tx = &mpTx{}
	}
	return fn(ctx, q.tx)
}

// poolRowFn returns a QueryRow fn that answers the SELECT-for-read with one
// fully populated row.
func poolRowFn(policyKind string, unitsOnEnrollment int64, tierAmountsJSON string) func(string, ...any) pg.Row {
	now := time.Date(2026, 8, 24, 4, 0, 0, 0, time.UTC)
	return func(sql string, _ ...any) pg.Row {
		return &mpRow{fn: func(dest ...any) error {
			vals := []any{
				mpPoolID, mpTenantA, int64(500), int64(900), int64(400), int64(1000),
				policyKind, unitsOnEnrollment, tierAmountsJSON, int64(7), mpGCID,
				now, now, (*time.Time)(nil),
			}
			if len(dest) != len(vals) {
				return errors.New("mpRow: scan arity mismatch")
			}
			return assignScan(dest, vals)
		}}
	}
}

func assignScan(dest []any, vals []any) error {
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = vals[i].(string)
		case *int64:
			*p = vals[i].(int64)
		case *time.Time:
			*p = vals[i].(time.Time)
		case **time.Time:
			*p = vals[i].(*time.Time)
		default:
			return errors.New("mpRow: unsupported scan dest")
		}
	}
	return nil
}

func newTestPool(t *testing.T, policy poolpkg.AllocationPolicy) *poolpkg.TenantManaPool {
	t.Helper()
	p, err := poolpkg.NewPool(mpTenantA, mpGCID, policy)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	p.PoolID = mpPoolID
	return p
}

// ----------------------------------------------------------------------------
// Interface conformance
// ----------------------------------------------------------------------------

func TestManaPoolRepository_SatisfiesPoolStore(t *testing.T) {
	t.Parallel()
	var _ poolpkg.PoolStore = pg.NewManaPoolRepository(&mpQuerier{})
}

// ----------------------------------------------------------------------------
// GetByTenant
// ----------------------------------------------------------------------------

func TestManaPoolRepository_GetByTenant_OpensTenantScopedTx(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: poolRowFn("manual", 0, "")}}
	if _, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA); err != nil {
		t.Fatalf("GetByTenant: %v", err)
	}
	if len(q.tenants) != 1 || q.tenants[0] != mpTenantA {
		t.Fatalf("expected exactly one tenant-scoped tx for %s, got %v", mpTenantA, q.tenants)
	}
}

func TestManaPoolRepository_GetByTenant_FiltersSoftDeleted(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: poolRowFn("manual", 0, "")}}
	if _, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA); err != nil {
		t.Fatalf("GetByTenant: %v", err)
	}
	if len(q.tx.querySQL) == 0 || !strings.Contains(q.tx.querySQL[0], "deleted_at IS NULL") {
		t.Fatalf("read must filter soft-deleted rows; SQL was %q", q.tx.querySQL)
	}
}

func TestManaPoolRepository_GetByTenant_NotFound(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
	}}}
	_, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA)
	if !errors.Is(err, poolpkg.ErrPoolNotFound) {
		t.Fatalf("want ErrPoolNotFound, got %v", err)
	}
}

func TestManaPoolRepository_GetByTenant_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{}
	if _, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), "  "); err == nil {
		t.Fatal("empty tenant id must fail loud, not open a tx with a blank GUC")
	}
	if len(q.tenants) != 0 {
		t.Fatalf("must not open a tx for a blank tenant, opened %v", q.tenants)
	}
}

func TestManaPoolRepository_GetByTenant_ReconstructsPolicies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		kind       string
		units      int64
		tiersJSON  string
		wantKind   poolpkg.PolicyKind
		wantUnits  int64
		wantTierNo int
	}{
		{"manual", "manual", 0, "", poolpkg.PolicyKindManual, 0, 0},
		{"on_enrollment", "on_enrollment", 25, "", poolpkg.PolicyKindOnEnrollment, 25, 0},
		{"equal_split", "equal_split", 0, "", poolpkg.PolicyKindEqualSplit, 0, 0},
		{"tier_based", "tier_based", 0, `{"power_user": 90, "standard": 30}`, poolpkg.PolicyKindTierBased, 0, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := &mpQuerier{tx: &mpTx{rowFn: poolRowFn(tc.kind, tc.units, tc.tiersJSON)}}
			got, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA)
			if err != nil {
				t.Fatalf("GetByTenant: %v", err)
			}
			if got.AutoAllocationPolicy == nil || got.AutoAllocationPolicy.Kind() != tc.wantKind {
				t.Fatalf("policy kind = %v, want %v", got.AutoAllocationPolicy, tc.wantKind)
			}
			if tc.wantKind == poolpkg.PolicyKindOnEnrollment {
				u, ok := got.AutoAllocationPolicy.UnitsForLearner(poolpkg.LearnerContext{GCID: mpGCID})
				if !ok || u != tc.wantUnits {
					t.Fatalf("on_enrollment units = (%d,%v), want (%d,true)", u, ok, tc.wantUnits)
				}
			}
			if tc.wantKind == poolpkg.PolicyKindTierBased {
				tb, ok := got.AutoAllocationPolicy.(poolpkg.TierBasedPolicy)
				if !ok || len(tb.TierToUnits) != tc.wantTierNo {
					t.Fatalf("tier map = %+v, want %d entries", got.AutoAllocationPolicy, tc.wantTierNo)
				}
				if u, ok := tb.UnitsForLearner(poolpkg.LearnerContext{Role: "power_user"}); !ok || u != 90 {
					t.Fatalf("power_user units = (%d,%v), want (90,true)", u, ok)
				}
			}
			// Scalars must survive the round trip too.
			if got.PoolID != mpPoolID || got.TenantID != mpTenantA ||
				got.BalanceUnits != 500 || got.LifetimeToppedUpUnits != 900 ||
				got.LifetimeAllocatedUnits != 400 || got.MonthlyTopupUnits != 1000 ||
				got.Version != 7 || got.CreatedByGCID != mpGCID {
				t.Fatalf("scalar round-trip lost data: %+v", got)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Save
// ----------------------------------------------------------------------------

func TestManaPoolRepository_Save_OpensTenantScopedTxAndInserts(t *testing.T) {
	t.Parallel()
	// No existing row for the tenant.
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
	}}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.tenants) != 1 || q.tenants[0] != mpTenantA {
		t.Fatalf("Save must run in the pool's own tenant tx, got %v", q.tenants)
	}
	if len(q.tx.execSQL) != 1 || !strings.Contains(q.tx.execSQL[0], "INSERT INTO tenant_mana_pools") {
		t.Fatalf("expected one INSERT, got %v", q.tx.execSQL)
	}
}

// The UPDATE moved from Exec to QueryRow when the version guard landed: Exec
// cannot report rows affected, so the losing writer in a race looked like a
// success. See the optimistic-concurrency block below.
func TestManaPoolRepository_Save_UpdatesWhenSamePoolExists(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: updateRowFn(mpPoolID, true)}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.Version = 4
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	var updates int
	for _, s := range q.tx.querySQL {
		if strings.Contains(s, "UPDATE tenant_mana_pools") {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("expected exactly one UPDATE, got %d in %v", updates, q.tx.querySQL)
	}
	if len(q.tx.execSQL) != 0 {
		t.Fatalf("the update path must not use Exec, got %v", q.tx.execSQL)
	}
}

func TestManaPoolRepository_Save_RejectsSecondPoolForSameTenant(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(dest ...any) error {
			*(dest[0].(*string)) = "99999999-9999-7999-8999-999999999999"
			return nil
		}}
	}}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if !errors.Is(err, poolpkg.ErrPoolAlreadyExists) {
		t.Fatalf("want ErrPoolAlreadyExists, got %v", err)
	}
	if len(q.tx.execSQL) != 0 {
		t.Fatalf("a rejected save must write nothing, wrote %v", q.tx.execSQL)
	}
}

func TestManaPoolRepository_Save_RejectsNilPool(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{}
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), nil); err == nil {
		t.Fatal("nil pool must fail loud")
	}
	if len(q.tenants) != 0 {
		t.Fatalf("must not open a tx for a nil pool, opened %v", q.tenants)
	}
}

// The 0003 CHECK constraint chk_pool_on_enrollment_units rejects an
// on_enrollment policy carrying units <= 0, but the domain's
// PolicyFromConfig accepts units_on_enrollment == 0. Catch the mismatch in
// the adapter with a named error rather than letting it surface as a raw
// constraint violation at runtime.
func TestManaPoolRepository_Save_RejectsOnEnrollmentZeroUnits(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
	}}}
	p := newTestPool(t, poolpkg.OnEnrollmentPolicy{DefaultUnits: 0})
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if err == nil {
		t.Fatal("on_enrollment with 0 units violates chk_pool_on_enrollment_units and must be refused")
	}
	if len(q.tx.execSQL) != 0 {
		t.Fatalf("must not attempt the write, wrote %v", q.tx.execSQL)
	}
}

func TestManaPoolRepository_Save_SerialisesTierAmountsAsJSONObject(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
	}}}
	p := newTestPool(t, poolpkg.TierBasedPolicy{TierToUnits: map[string]int64{"power_user": 90}})
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.tx.execArgs) != 1 {
		t.Fatalf("expected one write, got %d", len(q.tx.execArgs))
	}
	var found string
	for _, a := range q.tx.execArgs[0] {
		s, ok := a.(string)
		if !ok || !strings.HasPrefix(strings.TrimSpace(s), "{") {
			continue
		}
		found = s
	}
	if found == "" {
		t.Fatalf("tier_amounts_json was not passed as a JSON object, args = %#v", q.tx.execArgs[0])
	}
	var decoded map[string]int64
	if err := json.Unmarshal([]byte(found), &decoded); err != nil {
		t.Fatalf("tier_amounts_json is not valid JSON: %v (%q)", err, found)
	}
	if decoded["power_user"] != 90 {
		t.Fatalf("tier map lost data: %+v", decoded)
	}
}

func TestManaPoolRepository_Save_PropagatesTxError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("begin failed")
	q := &mpQuerier{beginErr: sentinel}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); !errors.Is(err, sentinel) {
		t.Fatalf("want the begin error surfaced, got %v", err)
	}
}

// ----------------------------------------------------------------------------
// Fail-loud policy paths
//
// These are the branches that matter most: a policy the adapter cannot
// represent, or a stored enum value it cannot read, must surface as an error.
// Silently degrading either to ManualPolicy would switch a tenant's
// auto-allocation off with no signal anywhere.
// ----------------------------------------------------------------------------

type mpUnknownPolicy struct{}

func (mpUnknownPolicy) Kind() poolpkg.PolicyKind { return poolpkg.PolicyKind("wormhole") }
func (mpUnknownPolicy) UnitsForLearner(poolpkg.LearnerContext) (int64, bool) {
	return 0, false
}

func TestManaPoolRepository_Save_RejectsNilPolicy(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.AutoAllocationPolicy = nil
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if !errors.Is(err, pg.ErrPolicyUnsupported) {
		t.Fatalf("want ErrPolicyUnsupported, got %v", err)
	}
	if len(q.tenants) != 0 {
		t.Fatalf("must reject before opening a tx, opened %v", q.tenants)
	}
}

func TestManaPoolRepository_Save_RejectsUnknownPolicyType(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.AutoAllocationPolicy = mpUnknownPolicy{}
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); !errors.Is(err, pg.ErrPolicyUnsupported) {
		t.Fatalf("want ErrPolicyUnsupported, got %v", err)
	}
}

func TestManaPoolRepository_Save_EqualSplitAndNilTierMap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy poolpkg.AllocationPolicy
		want   string
	}{
		{"equal_split", poolpkg.EqualSplitPolicy{}, "equal_split"},
		// A nil tier map must serialise as {} so it satisfies
		// chk_pool_tier_based_amounts, never as JSON null.
		{"tier_based nil map", poolpkg.TierBasedPolicy{}, "tier_based"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
				return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
			}}}
			p := newTestPool(t, tc.policy)
			if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
				t.Fatalf("Save: %v", err)
			}
			args := q.tx.execArgs[0]
			var sawKind, sawTiers bool
			for _, a := range args {
				s, ok := a.(string)
				if !ok {
					continue
				}
				if s == tc.want {
					sawKind = true
				}
				if s == "{}" {
					sawTiers = true
				}
				if s == "null" {
					t.Fatalf("tier_amounts must never serialise to JSON null (fails chk_pool_tier_based_amounts)")
				}
			}
			if !sawKind {
				t.Fatalf("policy kind %q not written, args = %#v", tc.want, args)
			}
			if tc.want == "tier_based" && !sawTiers {
				t.Fatalf("nil tier map must write an empty JSON object, args = %#v", args)
			}
		})
	}
}

func TestManaPoolRepository_GetByTenant_RejectsUnknownStoredPolicy(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: poolRowFn("wormhole", 0, "")}}
	_, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA)
	if !errors.Is(err, pg.ErrPolicyUnsupported) {
		t.Fatalf("an unreadable stored policy must fail loud, got %v", err)
	}
}

func TestManaPoolRepository_GetByTenant_RejectsMalformedTierJSON(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: poolRowFn("tier_based", 0, "not-json")}}
	_, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA)
	if !errors.Is(err, pg.ErrPolicyUnsupported) {
		t.Fatalf("malformed tier_amounts_json must fail loud, got %v", err)
	}
}

func TestManaPoolRepository_GetByTenant_PropagatesScanError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("connection reset")
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return sentinel }}
	}}}
	if _, err := pg.NewManaPoolRepository(q).GetByTenant(context.Background(), mpTenantA); !errors.Is(err, sentinel) {
		t.Fatalf("want the scan error surfaced, got %v", err)
	}
}

func TestManaPoolRepository_Save_PropagatesProbeError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("probe blew up")
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return sentinel }}
	}}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if !errors.Is(err, sentinel) {
		t.Fatalf("want the probe error surfaced, got %v", err)
	}
	if len(q.tx.execSQL) != 0 {
		t.Fatalf("must not write after a failed probe, wrote %v", q.tx.execSQL)
	}
}

func TestManaPoolRepository_Save_RejectsBlankTenantOnPool(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.TenantID = "   "
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err == nil {
		t.Fatal("a pool with no tenant must fail loud rather than open a tx with a blank GUC")
	}
	if len(q.tenants) != 0 {
		t.Fatalf("must not open a tx, opened %v", q.tenants)
	}
}

// ----------------------------------------------------------------------------
// Optimistic concurrency (CHO-2414 follow-up)
//
// The first production use of this repository lost an update. Two
// tenant_mana_topup.payment_captured events were handled 3ms apart; both
// read version 3 / balance 0, both computed 20000, both saved version 4, and
// the second overwrote the first. 20000 of 40000 mana units vanished with no
// error anywhere. Documenting last-write-wins in the package header did not
// stop it, so the write now refuses to move a row that another writer has
// already advanced.
//
// The refusal is what makes the lane self-healing: the losing handler returns
// an error, idempotent.PostgresStore.Process does NOT Mark a key whose fn
// failed, so the broker redelivers, the retry re-reads the advanced row and
// credits on top of it.
// ----------------------------------------------------------------------------

// updateRowFn answers the existing-pool probe with `existingPoolID` and the
// UPDATE ... RETURNING with either a row (won) or ErrNoRows (lost).
func updateRowFn(existingPoolID string, updateWins bool) func(string, ...any) pg.Row {
	return func(sql string, _ ...any) pg.Row {
		if strings.Contains(sql, "UPDATE tenant_mana_pools") {
			return &mpRow{fn: func(dest ...any) error {
				if !updateWins {
					return pg.ErrNoRows
				}
				if len(dest) != 1 {
					return errors.New("UPDATE must RETURN exactly one column")
				}
				*(dest[0].(*int64)) = 4
				return nil
			}}
		}
		return &mpRow{fn: func(dest ...any) error {
			*(dest[0].(*string)) = existingPoolID
			return nil
		}}
	}
}

func TestManaPoolRepository_Save_UpdateCarriesAVersionGuard(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: updateRowFn(mpPoolID, true)}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.Version = 4
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	var updateSQL string
	for _, s := range q.tx.querySQL {
		if strings.Contains(s, "UPDATE tenant_mana_pools") {
			updateSQL = s
		}
	}
	if updateSQL == "" {
		t.Fatal("no UPDATE was emitted")
	}
	// The SQL is column-aligned, so match on collapsed whitespace rather than
	// on an exact single space.
	flat := strings.Join(strings.Fields(updateSQL), " ")
	if !strings.Contains(flat, "AND version < $10") {
		t.Errorf("UPDATE must refuse a row already at or beyond this version; SQL was:\n%s", updateSQL)
	}
	if !strings.Contains(updateSQL, "RETURNING") {
		t.Errorf("UPDATE must RETURN so a zero-row result is detectable; SQL was:\n%s", updateSQL)
	}
	if len(q.tx.execSQL) != 0 {
		t.Errorf("the guarded UPDATE must not go through Exec, which cannot report rows affected: %v", q.tx.execSQL)
	}
}

func TestManaPoolRepository_Save_ReturnsConflictWhenAnotherWriterWon(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: updateRowFn(mpPoolID, false)}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.Version = 4
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if !errors.Is(err, pg.ErrVersionConflict) {
		t.Fatalf("a lost update must surface as ErrVersionConflict so the caller can Nack and be redelivered, got %v", err)
	}
}

// The whole point of the sentinel is that the caller can tell a conflict from
// a broken database, because one is retryable and the other is not.
func TestManaPoolRepository_Save_ConflictIsDistinctFromAScanFailure(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset by peer")
	q := &mpQuerier{tx: &mpTx{rowFn: func(sql string, _ ...any) pg.Row {
		if strings.Contains(sql, "UPDATE tenant_mana_pools") {
			return &mpRow{fn: func(...any) error { return boom }}
		}
		return &mpRow{fn: func(dest ...any) error {
			*(dest[0].(*string)) = mpPoolID
			return nil
		}}
	}}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	p.Version = 4
	err := pg.NewManaPoolRepository(q).Save(context.Background(), p)
	if errors.Is(err, pg.ErrVersionConflict) {
		t.Fatalf("a transport failure must NOT be reported as a version conflict: %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("want the underlying error surfaced, got %v", err)
	}
}

// An INSERT has no prior version to guard against; the UNIQUE constraint on
// tenant_id is what bounds a creation race.
func TestManaPoolRepository_Save_InsertPathIsUnguarded(t *testing.T) {
	t.Parallel()
	q := &mpQuerier{tx: &mpTx{rowFn: func(string, ...any) pg.Row {
		return &mpRow{fn: func(...any) error { return pg.ErrNoRows }}
	}}}
	p := newTestPool(t, poolpkg.ManualPolicy{})
	if err := pg.NewManaPoolRepository(q).Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.tx.execSQL) != 1 || !strings.Contains(q.tx.execSQL[0], "INSERT INTO tenant_mana_pools") {
		t.Fatalf("expected one INSERT through Exec, got %v", q.tx.execSQL)
	}
}
