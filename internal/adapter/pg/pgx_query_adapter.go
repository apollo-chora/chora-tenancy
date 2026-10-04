// pgx_query_adapter.go — pgx-side adapter that wraps pgx.Rows in the
// local Rows surface for the QueryRunner port.
//
// Trivial pass-through wrappers; covered by integration tests against a
// real Cloud SQL fixture in M14. NOT exercised in unit tests (no way to
// construct a pgx.Rows without a real pool).
package pg

import (
	"context"
	"fmt"
)

// Query satisfies QueryRunner. Delegates to the underlying *pgxpool.Pool.
func (q *PgxPoolQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	pgxRows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pg.Query: %w", err)
	}
	return &pgxRowsAdapter{r: pgxRows}, nil
}

// pgxRowsAdapter wraps pgx.Rows to satisfy the local Rows surface (cheap
// dependency-light decoupling from pgx in the repository layer).
type pgxRowsAdapter struct {
	r pgxRowsLike
}

// pgxRowsLike is the subset of pgx.Rows we use.
type pgxRowsLike interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

func (a *pgxRowsAdapter) Next() bool             { return a.r.Next() }
func (a *pgxRowsAdapter) Scan(dest ...any) error { return a.r.Scan(dest...) }
func (a *pgxRowsAdapter) Close() error           { a.r.Close(); return nil }
func (a *pgxRowsAdapter) Err() error             { return a.r.Err() }
