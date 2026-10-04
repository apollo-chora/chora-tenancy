// Package pg is the pgx-backed adapter for chora-tenancy domain ports.
//
// Same shape as services/chora-identity/internal/adapter/pg — a small
// Querier seam decouples the SQL emit/scan layer from pgx so unit tests
// stub without a live DB; PgxPoolQuerier wraps *pgxpool.Pool for prod.
//
// Resilience (`feedback_resilience_priority`):
//
//   - Tenant-scoped tables enforce RLS via SET LOCAL chora.tenant_id
//     within transactions; the SQL layer here keeps the SET LOCAL
//     contract via rls.RunInTx (when called from the tenant_id-aware
//     handler stack)
//   - Soft delete: every read filters `deleted_at IS NULL`
//   - Cross-DB queries forbidden — chora-tenancy reads only chora_tenancy
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is the minimal Exec + QueryRow surface this package needs.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) error
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// Row is the minimal Scan surface used by repos.
type Row interface {
	Scan(dest ...any) error
}

// ErrNoRows is the pg-package-local sentinel for a not-found row.
var ErrNoRows = errors.New("pg: no rows in result set")

// PgxPoolQuerier wraps a *pgxpool.Pool with the local Querier shape.
type PgxPoolQuerier struct {
	pool *pgxpool.Pool
}

// NewPgxPoolQuerier wraps a pgxpool.Pool.
func NewPgxPoolQuerier(pool *pgxpool.Pool) *PgxPoolQuerier {
	return &PgxPoolQuerier{pool: pool}
}

// Exec runs a non-returning SQL statement.
func (q *PgxPoolQuerier) Exec(ctx context.Context, sql string, args ...any) error {
	if _, err := q.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg.Exec: %w", err)
	}
	return nil
}

// QueryRow runs a single-row SQL query.
func (q *PgxPoolQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &pgxPoolRow{r: q.pool.QueryRow(ctx, sql, args...)}
}

type pgxPoolRow struct {
	r pgx.Row
}

func (r *pgxPoolRow) Scan(dest ...any) error {
	err := r.r.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}
	return err
}

// Pool returns the underlying *pgxpool.Pool. Used by callers that need
// to begin transactions for SET LOCAL chora.tenant_id wrapping.
func (q *PgxPoolQuerier) Pool() *pgxpool.Pool {
	return q.pool
}

// -----------------------------------------------------------------------------
// Tenant-scoped transaction surface (A7)
//
// RLS-protected tables (members, add_on_subscriptions, invoices,
// payment_methods, addon_usage_events) carry the `tenant_isolation` policy
// `tenant_id = current_setting('chora.tenant_id', true)::uuid`. The
// chora_tenancy_app_rw role is NOBYPASSRLS (migration 0013), so a bare
// SELECT/INSERT against those tables sees ZERO rows / fails the WITH CHECK
// unless `SET LOCAL chora.tenant_id` runs first inside the SAME
// transaction. SET LOCAL (never SET) so PgBouncer transaction-pooling does
// not leak the GUC across requests.
//
// TxQuerier is the seam: RunInTenantTx opens a tx, runs the SET LOCAL,
// invokes fn with a Tx, then commits (or rolls back on fn error). Adapters
// for RLS-protected tables depend on TxQuerier; adapters for the
// non-RLS-protected `tenants` + `add_ons` registries use the plain
// Querier / QueryRunner.
// -----------------------------------------------------------------------------

// Tx is the minimal Exec + Query + QueryRow surface a repository needs
// inside an open, tenant-scoped transaction.
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) error
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// TxQuerier opens a transaction with `SET LOCAL chora.tenant_id` already
// applied and runs fn against it. fn returning an error rolls the tx back;
// nil commits.
type TxQuerier interface {
	RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, Tx) error) error
}

// platformScope is the blessed cross-tenant operator sentinel (mirrors
// libs/chora-go-common/rls.ValidateTenantID + the migration-0024
// platform-sentinel-aware transaction_ledger policy). Only RunInScopeTx
// accepts it — the write path (RunInTenantTx) stays UUID-only.
const platformScope = "platform"

// RunInTenantTx satisfies TxQuerier for the production pgxpool wrapper.
//
// Sequence: pool.Begin → SET LOCAL chora.tenant_id → fn(tx) → Commit
// (or Rollback when fn errors / Commit fails). tenantID is interpolated
// into the SET LOCAL statement after a UUID-shape check — Postgres parses
// SET LOCAL (not the prepared-stmt path), so a defence-in-depth identifier
// guard rejects anything that is not hex + dash before it reaches the wire.
func (q *PgxPoolQuerier) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, Tx) error) error {
	if err := validateTenantID(tenantID); err != nil {
		return err
	}
	return q.runInSessionTx(ctx, tenantID, fn)
}

// RunInScopeTx is RunInTenantTx widened to additionally accept the blessed
// "platform" operator span-all sentinel (ADR-205 / ADR-165). With
// scopeID == "platform" it emits SET LOCAL chora.tenant_id = 'platform',
// which the migration-0024 platform-sentinel-aware transaction_ledger RLS
// policy reads as "all tenants" — the CORRECT operator span-all under a
// NOBYPASSRLS app role + FORCE RLS (avoids the CHO-1931 empty-result class
// of bug). The READ side (the projection read repo) uses this so a single
// code path serves learner/tenant/master-single-franchisee (a tenant UUID)
// and master span-all ("platform"). The write side keeps RunInTenantTx.
//
// The caller is responsible for the IMDA-D1 cross-tenant audit BEFORE any
// span-all read (the gRPC adapter emits it); this runner is plumbing only.
func (q *PgxPoolQuerier) RunInScopeTx(ctx context.Context, scopeID string, fn func(context.Context, Tx) error) error {
	if err := validateScopeID(scopeID); err != nil {
		return err
	}
	return q.runInSessionTx(ctx, scopeID, fn)
}

// runInSessionTx is the shared begin → SET LOCAL chora.tenant_id → fn → commit
// body. sessionTenant is interpolated into the SET LOCAL after the caller's
// validator has already vetted it (UUID, or the "platform" sentinel for the
// scope variant).
func (q *PgxPoolQuerier) runInSessionTx(ctx context.Context, sessionTenant string, fn func(context.Context, Tx) error) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg.runInSessionTx: begin: %w", err)
	}
	// Roll back on any non-committed exit. After a successful Commit the
	// Rollback is a no-op (pgx returns ErrTxClosed, swallowed here).
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", sessionTenant)); err != nil {
		return fmt.Errorf("pg.runInSessionTx: SET LOCAL chora.tenant_id: %w", err)
	}

	if err := fn(ctx, &pgxTx{tx: tx}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg.runInSessionTx: commit: %w", err)
	}
	committed = true
	return nil
}

// validateTenantID rejects values unsafe to interpolate into a SET LOCAL
// statement: anything other than hex digits + dash (UUID shape). Mirrors
// libs/chora-go-common/rls.ValidateTenantID (minus the platform sentinel)
// without pulling that package into the adapter import graph for one helper.
func validateTenantID(id string) error {
	if id == "" {
		return errors.New("pg.RunInTenantTx: empty tenant_id")
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F', r == '-':
			continue
		default:
			return fmt.Errorf("pg.RunInTenantTx: tenant_id %q has forbidden characters", id)
		}
	}
	return nil
}

// validateScopeID accepts a UUID-shaped tenant id OR the "platform" sentinel
// (the only non-UUID value the read side may set as chora.tenant_id).
func validateScopeID(id string) error {
	if id == platformScope {
		return nil
	}
	return validateTenantID(id)
}

// pgxTx adapts pgx.Tx to the local Tx surface.
type pgxTx struct {
	tx pgx.Tx
}

func (t *pgxTx) Exec(ctx context.Context, sql string, args ...any) error {
	if _, err := t.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg.Tx.Exec: %w", err)
	}
	return nil
}

func (t *pgxTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pg.Tx.Query: %w", err)
	}
	return &pgxRowsAdapter{r: rows}, nil
}

func (t *pgxTx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &pgxPoolRow{r: t.tx.QueryRow(ctx, sql, args...)}
}

// Compile-time check: PgxPoolQuerier satisfies TxQuerier.
var _ TxQuerier = (*PgxPoolQuerier)(nil)
