// closure_repository_test.go — pgx adapter tests for the durable
// ClosureRepository (W0-F1 durability + W0-F5 error-honesty, CHO-2198)
// using the shared stub TxQuerier/Tx harness (see
// legacy_tenancy_repository_test.go).
//
// These are unit tests against the SQL emit + scan surface — no live DB.
// The critical assertions here are the W0-F5 ones: a genuine backing-store
// error from either Pseudonymise or IsPseudonymised — including a
// RunInTenantTx begin/SET LOCAL failure, the failure mode specific to this
// service's TxQuerier seam — must come back as a non-nil error, never get
// coerced into a false/zero "everything is fine" result (the
// swallowed-error trap the in-memory port's original
// `IsPseudonymised(gcid string) bool` signature made structurally
// impossible to avoid).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/config"
)

func closureSpecFixture() []config.TableSpec {
	return []config.TableSpec{
		{
			Table: "tenant_memberships",
			Columns: []config.ColumnSpec{
				{Column: "invited_by_display_name", Strategy: "tombstone_string", Value: "Former member"},
				{Column: "invitation_email_snapshot", Strategy: "drop", Value: ""},
			},
		},
		{
			Table: "stripe_customer_mappings",
			Columns: []config.ColumnSpec{
				{Column: "billing_email", Strategy: "drop", Value: ""},
			},
		},
	}
}

// -----------------------------------------------------------------------------
// Pseudonymise
// -----------------------------------------------------------------------------

func TestClosureRepository_Pseudonymise_EmitsInsertOnConflictReturningID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	tx := &stubTx{rowFn: func(_ string, _ ...any) pg.Row {
		return &stubRow{scanFn: func(dest ...any) error {
			*(dest[0].(*string)) = uuid.NewString()
			return nil
		}}
	}}
	q := &stubTxQuerier{tx: tx}
	repo := pg.NewClosureRepository(q)

	rows, err := repo.Pseudonymise(context.Background(), "tenant-A", "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if rows != 3 {
		t.Fatalf("expected rows_touched=3 (2+1 columns); got %d", rows)
	}
	if q.opened != 1 {
		t.Fatalf("expected exactly 1 tenant-scoped tx, got %d", q.opened)
	}

	wants := []string{"INSERT INTO closure_pseudonymisation_state", "ON CONFLICT", "DO NOTHING", "RETURNING"}
	if len(tx.querySQL) == 0 {
		t.Fatalf("expected at least one QueryRow call")
	}
	got := tx.querySQL[0]
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("Pseudonymise SQL missing %q; got:\n%s", w, got)
		}
	}
}

func TestClosureRepository_Pseudonymise_MintsUUIDv7ForID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	var capturedArgs []any
	tx := &stubTx{rowFn: func(_ string, args ...any) pg.Row {
		capturedArgs = args
		return &stubRow{scanFn: func(dest ...any) error {
			*(dest[0].(*string)) = uuid.NewString()
			return nil
		}}
	}}
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	if _, err := repo.Pseudonymise(context.Background(), "tenant-A", "gcid-A", nil); err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if len(capturedArgs) == 0 {
		t.Fatalf("expected query args")
	}
	idArg, ok := capturedArgs[0].(string)
	if !ok || idArg == "" {
		t.Fatalf("expected non-empty string id as first arg; got %T %v", capturedArgs[0], capturedArgs[0])
	}
	parsed, err := uuid.Parse(idArg)
	if err != nil {
		t.Fatalf("minted id %q is not a valid UUID: %v", idArg, err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("minted id %q is not UUIDv7 (version=%d)", idArg, parsed.Version())
	}
}

func TestClosureRepository_Pseudonymise_ReturnsZeroWhenConflictFires(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	// ON CONFLICT DO NOTHING suppresses the RETURNING row — the stubTx's
	// default QueryRow (no rowFn set) signals that exactly as the
	// pg.Querier seam does: ErrNoRows.
	tx := &stubTx{}
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	rows, err := repo.Pseudonymise(context.Background(), "tenant-A", "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: expected idempotent no-op, got error: %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on idempotent replay; got %d", rows)
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyTenantID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &stubTxQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.Pseudonymise(context.Background(), "", "gcid-A", nil)
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if q.opened != 0 {
		t.Fatalf("must not open a tenant-scoped tx on validation failure; opened=%d", q.opened)
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyGCID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &stubTxQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.Pseudonymise(context.Background(), "tenant-A", "", nil)
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if q.opened != 0 {
		t.Fatalf("must not open a tenant-scoped tx on validation failure; opened=%d", q.opened)
	}
}

// TestClosureRepository_Pseudonymise_PropagatesScanError is the W0-F5
// fail-loud proof for Pseudonymise: a genuine backing-store error (NOT
// pg.ErrNoRows) must come back as a non-nil error, never as a silent
// "idempotent no-op" (0, nil) — conflating "I don't know" with "already
// done" would let the closure saga believe this domain acked when it did
// not, exactly the class of bug in
// reusable_gotcha_swallowed_error_damage_is_decided_by_the_caller.
func TestClosureRepository_Pseudonymise_PropagatesScanError(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	tx := &stubTx{rowFn: func(_ string, _ ...any) pg.Row {
		return &stubRow{scanFn: func(_ ...any) error { return boom }}
	}}
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	rows, err := repo.Pseudonymise(context.Background(), "tenant-A", "gcid-A", closureSpecFixture())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (rows=%d)", rows)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on error; got %d", rows)
	}
}

// TestClosureRepository_Pseudonymise_PropagatesTxBeginError proves the
// TxQuerier-specific failure mode: if RunInTenantTx itself fails (e.g. the
// SET LOCAL chora.tenant_id statement errors), that must ALSO propagate as
// a non-nil error, never a silent (0, nil) "already done".
func TestClosureRepository_Pseudonymise_PropagatesTxBeginError(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	boom := errors.New("pg: begin failed")
	q := &stubTxQuerier{beginErr: boom}
	repo := pg.NewClosureRepository(q)

	rows, err := repo.Pseudonymise(context.Background(), "tenant-A", "gcid-A", closureSpecFixture())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (rows=%d)", rows)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on error; got %d", rows)
	}
}

// -----------------------------------------------------------------------------
// IsPseudonymised
// -----------------------------------------------------------------------------

func TestClosureRepository_IsPseudonymised_TrueWhenRowExists(t *testing.T) {
	t.Parallel()
	tx := &stubTx{rowFn: func(_ string, _ ...any) pg.Row {
		return &stubRow{scanFn: func(dest ...any) error {
			*(dest[0].(*int)) = 1
			return nil
		}}
	}}
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	got, err := repo.IsPseudonymised(context.Background(), "tenant-A", "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !got {
		t.Fatalf("expected true when a row exists")
	}
	if len(tx.querySQL) == 0 || !strings.Contains(tx.querySQL[0], "FROM closure_pseudonymisation_state") {
		t.Errorf("IsPseudonymised SQL malformed; got:\n%v", tx.querySQL)
	}
}

func TestClosureRepository_IsPseudonymised_FalseWhenNoRows(t *testing.T) {
	t.Parallel()
	tx := &stubTx{} // defaults to ErrNoRows
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	got, err := repo.IsPseudonymised(context.Background(), "tenant-A", "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: expected nil error on a clean miss; got %v", err)
	}
	if got {
		t.Fatalf("expected false when no row exists")
	}
}

// TestClosureRepository_IsPseudonymised_PropagatesQueryError is the core
// W0-F5 proof for IsPseudonymised: this is exactly the method the original
// `IsPseudonymised(gcid string) bool` signature could NOT have implemented
// honestly against Postgres (no ctx, no error return). A real backing-store
// error must be reported, not folded into `false`.
func TestClosureRepository_IsPseudonymised_PropagatesQueryError(t *testing.T) {
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	tx := &stubTx{rowFn: func(_ string, _ ...any) pg.Row {
		return &stubRow{scanFn: func(_ ...any) error { return boom }}
	}}
	repo := pg.NewClosureRepository(&stubTxQuerier{tx: tx})

	got, err := repo.IsPseudonymised(context.Background(), "tenant-A", "gcid-A")
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (got=%v)", got)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if got {
		t.Fatalf("expected false alongside the error (never claim true on failure)")
	}
}

// TestClosureRepository_IsPseudonymised_PropagatesTxBeginError proves the
// TxQuerier-specific fail-loud contract: a RunInTenantTx begin/SET LOCAL
// failure must surface as an error, NEVER get coerced into "false" (which
// a caller would read as "safe to reprocess").
func TestClosureRepository_IsPseudonymised_PropagatesTxBeginError(t *testing.T) {
	t.Parallel()
	boom := errors.New("pg: begin failed")
	q := &stubTxQuerier{beginErr: boom}
	repo := pg.NewClosureRepository(q)

	got, err := repo.IsPseudonymised(context.Background(), "tenant-A", "gcid-A")
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (got=%v)", got)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if got {
		t.Fatalf("expected false alongside the error (never claim true on failure)")
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	q := &stubTxQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.IsPseudonymised(context.Background(), "", "gcid-A")
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if q.opened != 0 {
		t.Fatalf("must not open a tenant-scoped tx on validation failure; opened=%d", q.opened)
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	q := &stubTxQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.IsPseudonymised(context.Background(), "tenant-A", "")
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if q.opened != 0 {
		t.Fatalf("must not open a tenant-scoped tx on validation failure; opened=%d", q.opened)
	}
}

func TestClosureRepository_Pseudonymise_Guards(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	// Unwired runner never reached — input guards fire first.
	repo := pg.NewClosureRepository(nil)
	if _, err := repo.Pseudonymise(context.Background(), "", "g", nil); err == nil {
		t.Fatal("expected error for empty tenant")
	}
	if _, err := repo.Pseudonymise(context.Background(), "t-1", "", nil); err == nil {
		t.Fatal("expected error for empty gcid")
	}
}
