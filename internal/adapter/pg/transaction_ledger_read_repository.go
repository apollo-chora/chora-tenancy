// transaction_ledger_read_repository.go — pg adapter for the ADR-205
// transaction_ledger CQRS projection READ port (B3 / CHO-1939). Companion to
// transaction_ledger_write_repository.go (B2).
//
// Every read runs inside RunInScopeTx so the RLS session value (chora.tenant_id)
// is set before the SELECT — chora_tenancy_app_rw is NOBYPASSRLS and the tables
// are FORCE ROW LEVEL SECURITY, so a bare SELECT with no GUC sees ZERO rows.
// The GUCTenantID the gRPC adapter resolves is either:
//   - a tenant UUID → RLS pins that tenant (learner / tenant / master-single-
//     franchisee). learner scope adds a learner_gcid predicate.
//   - "platform"    → the migration-0024 platform-sentinel-aware policy returns
//     ALL tenants (master span-all, ADR-165). The audit MUST already have been
//     emitted by the gRPC layer BEFORE this read (IMDA D1).
//
// Keyset cursor: base64.URLEncoding(JSON). occurred_at sort keys on
// (occurred_at, ledger_id); amount sort keys on
// (COALESCE(amount_minor, ABS(mana_units), 0), ledger_id) — fiat + mana share
// one deterministic magnitude axis (cross-unit value is NOT normalised; ADR-205
// defers cross-currency). The +1 row-fetch detects has_more without a 2nd round
// trip. occurred_at is NOT NULL in the schema, so no NULLS-LAST handling.
package pg

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// ScopeTxQuerier is the read-side transaction runner: RunInScopeTx accepts a
// tenant UUID OR the "platform" span-all sentinel. *PgxPoolQuerier satisfies it.
type ScopeTxQuerier interface {
	RunInScopeTx(ctx context.Context, scopeID string, fn func(context.Context, Tx) error) error
}

// TransactionLedgerReadRepo reads the projection scope-aware under RLS.
type TransactionLedgerReadRepo struct {
	tx ScopeTxQuerier
}

// NewTransactionLedgerReadRepo wires the read repo to a scope-aware runner.
func NewTransactionLedgerReadRepo(tx ScopeTxQuerier) *TransactionLedgerReadRepo {
	return &TransactionLedgerReadRepo{tx: tx}
}

var _ tl.ReadRepository = (*TransactionLedgerReadRepo)(nil)
var _ tl.FranchiseeLister = (*TransactionLedgerReadRepo)(nil)

// ListFranchisees returns one offset page of the managed tenants a MASTER may
// narrow the ledger to — the WHOLE descendant subtree (recursive, ADR-217 D3)
// of the platform-root chora-master tenant, name-searchable. The `tenants`
// registry is RLS-FREE (Hub+ reads children cross-tenant, migration 0001), so
// the 'platform' span-all sentinel here only supplies the multi-row Tx —
// nothing extra is exposed (the WITH RECURSIVE bounds the result to
// chora-master's descendants). Callers fetch limit+1 to detect a next page.
func (r *TransactionLedgerReadRepo) ListFranchisees(ctx context.Context, q string, limit, offset int) ([]tl.Franchisee, error) {
	if r == nil || r.tx == nil {
		return nil, errors.New("pg/transaction_ledger_read: repo not wired")
	}
	if limit <= 0 {
		limit = tl.DefaultFranchiseePageSize
	}
	if offset < 0 {
		offset = 0
	}
	// ADR-217 D3: the operator's managed-tenant scope is the WHOLE descendant
	// subtree of chora-master (recursive) — not just direct children — so a
	// franchisee's own branches (grandchildren+) are in scope. Anchored on the
	// immutable chora-master id (ADR-182 D1); the root itself is excluded (the
	// recursion starts from its children). Depth-bounded (<= 64) as a cycle
	// backstop — the migration-0026 parent-invariant trigger already prevents
	// cycles at write time.
	const sql = `
WITH RECURSIVE descendants AS (
    SELECT t.tenant_id, t.name, 1 AS depth
    FROM tenants t
    WHERE t.parent_tenant_id = '00000000-0000-7000-8000-000000000001'
      AND t.deleted_at IS NULL
    UNION ALL
    SELECT c.tenant_id, c.name, d.depth + 1
    FROM tenants c
    JOIN descendants d ON c.parent_tenant_id = d.tenant_id
    WHERE c.deleted_at IS NULL AND d.depth < 64
)
SELECT tenant_id::text, name
FROM descendants
WHERE ($1 = '' OR name ILIKE '%' || $1 || '%')
ORDER BY name, tenant_id
LIMIT $2 OFFSET $3`
	var out []tl.Franchisee
	err := r.tx.RunInScopeTx(ctx, tl.PlatformScope, func(ctx context.Context, tx Tx) error {
		rs, qerr := tx.Query(ctx, sql, strings.TrimSpace(q), limit, offset)
		if qerr != nil {
			return fmt.Errorf("pg/transaction_ledger_read: list franchisees: %w", qerr)
		}
		defer rs.Close()
		for rs.Next() {
			var f tl.Franchisee
			if scanErr := rs.Scan(&f.TenantID, &f.Name); scanErr != nil {
				return fmt.Errorf("pg/transaction_ledger_read: scan franchisee: %w", scanErr)
			}
			out = append(out, f)
		}
		return rs.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ledgerSelectCols is the shared projection SELECT list (alias l). amount_sort_key
// is computed server-side so the Go cursor value is byte-identical to the SQL
// ORDER BY expression for amount sorts.
const ledgerSelectCols = `
    l.ledger_id::text, l.occurred_at, l.tenant_id::text, l.learner_gcid::text,
    l.kind, l.source_domain, l.source_ref_id, l.label,
    l.currency, l.amount_minor, l.mana_units, l.status,
    (l.kind = 'mana_spend_daily' AND EXISTS (
        SELECT 1 FROM transaction_ledger_detail d WHERE d.ledger_id = l.ledger_id
    )) AS has_detail,
    l.metadata_jsonb::text,
    COALESCE(l.amount_minor, ABS(l.mana_units), 0)::bigint AS amount_sort_key`

const amountSortExpr = `COALESCE(l.amount_minor, ABS(l.mana_units), 0)`

// List returns one keyset page of unified rows.
func (r *TransactionLedgerReadRepo) List(ctx context.Context, q tl.ListQuery) (tl.ListPage, error) {
	if r == nil || r.tx == nil {
		return tl.ListPage{}, errors.New("pg/transaction_ledger_read: repo not wired")
	}
	cur, err := decodeLedgerCursor(q.Cursor, q.Filters.Sort)
	if err != nil {
		return tl.ListPage{}, fmt.Errorf("pg/transaction_ledger_read: invalid cursor: %w", err)
	}

	conds, args := commonFilterConds(q.Filters)
	if cur != nil {
		conds, args = appendCursorCond(conds, args, q.Filters.Sort, cur)
	}

	order := orderByClause(q.Filters.Sort)
	args = append(args, q.Limit+1)
	limitArg := len(args)

	sql := strings.Join([]string{
		"SELECT", ledgerSelectCols,
		"FROM transaction_ledger l",
		whereClause(conds),
		order,
		fmt.Sprintf("LIMIT $%d", limitArg),
	}, " ")

	var rows []tl.LedgerRow
	keys := make([]int64, 0) // amount_sort_key per row (parallel slice, cursor use only)
	err = r.tx.RunInScopeTx(ctx, q.GUCTenantID, func(ctx context.Context, tx Tx) error {
		rs, qerr := tx.Query(ctx, sql, args...)
		if qerr != nil {
			return fmt.Errorf("pg/transaction_ledger_read: list query: %w", qerr)
		}
		defer rs.Close()
		for rs.Next() {
			row, key, scanErr := scanLedgerRow(rs.Scan)
			if scanErr != nil {
				return scanErr
			}
			rows = append(rows, row)
			keys = append(keys, key)
		}
		return rs.Err()
	})
	if err != nil {
		return tl.ListPage{}, err
	}

	page := tl.ListPage{Items: rows}
	if len(rows) > q.Limit {
		page.Items = rows[:q.Limit]
		last := page.Items[q.Limit-1]
		next, encErr := encodeLedgerCursor(q.Filters.Sort, last, keys[q.Limit-1])
		if encErr != nil {
			return tl.ListPage{}, fmt.Errorf("pg/transaction_ledger_read: encode cursor: %w", encErr)
		}
		page.NextPageToken = next
	}
	return page, nil
}

// GetDetail returns the parent row + one keyset page of its per-call details.
func (r *TransactionLedgerReadRepo) GetDetail(ctx context.Context, q tl.DetailQuery) (tl.DetailPage, error) {
	if r == nil || r.tx == nil {
		return tl.DetailPage{}, errors.New("pg/transaction_ledger_read: repo not wired")
	}
	if strings.TrimSpace(q.LedgerID) == "" {
		return tl.DetailPage{}, errors.New("pg/transaction_ledger_read: ledger_id required")
	}
	dcur, err := decodeDetailCursor(q.Cursor)
	if err != nil {
		return tl.DetailPage{}, fmt.Errorf("pg/transaction_ledger_read: invalid detail cursor: %w", err)
	}

	// Parent fetch (RLS-scoped; learner scope constrains to own rows).
	parentConds := []string{"l.ledger_id = $1::uuid"}
	parentArgs := []any{q.LedgerID}
	if q.LearnerGCID != "" {
		parentArgs = append(parentArgs, q.LearnerGCID)
		parentConds = append(parentConds, fmt.Sprintf("l.learner_gcid = $%d::uuid", len(parentArgs)))
	}
	parentSQL := strings.Join([]string{
		"SELECT", ledgerSelectCols,
		"FROM transaction_ledger l",
		whereClause(parentConds),
		"LIMIT 1",
	}, " ")

	// Detail page (keyset over occurred_at desc, detail_id desc).
	detConds := []string{"d.ledger_id = $1::uuid"}
	detArgs := []any{q.LedgerID}
	if dcur != nil {
		detArgs = append(detArgs, dcur.OA, dcur.ID)
		detConds = append(detConds, fmt.Sprintf("(d.occurred_at, d.detail_id) < ($%d, $%d::uuid)", len(detArgs)-1, len(detArgs)))
	}
	detArgs = append(detArgs, q.Limit+1)
	detSQL := strings.Join([]string{
		`SELECT d.detail_id::text, d.ledger_id::text, d.tenant_id::text, d.learner_gcid::text,
                d.occurred_at, d.action_code, d.mana_units, d.model, d.trace_id, d.metadata_jsonb::text
         FROM transaction_ledger_detail d`,
		whereClause(detConds),
		"ORDER BY d.occurred_at DESC, d.detail_id DESC",
		fmt.Sprintf("LIMIT $%d", len(detArgs)),
	}, " ")

	var page tl.DetailPage
	err = r.tx.RunInScopeTx(ctx, q.GUCTenantID, func(ctx context.Context, tx Tx) error {
		// Parent.
		prow, _, scanErr := scanLedgerRow(tx.QueryRow(ctx, parentSQL, parentArgs...).Scan)
		if scanErr != nil {
			if errors.Is(scanErr, ErrNoRows) {
				page.Found = false
				return nil
			}
			return scanErr
		}
		page.Parent = prow
		page.Found = true

		// Details.
		rs, qerr := tx.Query(ctx, detSQL, detArgs...)
		if qerr != nil {
			return fmt.Errorf("pg/transaction_ledger_read: detail query: %w", qerr)
		}
		defer rs.Close()
		for rs.Next() {
			d, scanErr := scanDetailRow(rs.Scan)
			if scanErr != nil {
				return scanErr
			}
			page.Details = append(page.Details, d)
		}
		return rs.Err()
	})
	if err != nil {
		return tl.DetailPage{}, err
	}

	if len(page.Details) > q.Limit {
		page.Details = page.Details[:q.Limit]
		last := page.Details[q.Limit-1]
		next, encErr := encodeDetailCursor(last)
		if encErr != nil {
			return tl.DetailPage{}, fmt.Errorf("pg/transaction_ledger_read: encode detail cursor: %w", encErr)
		}
		page.NextPageToken = next
	}
	return page, nil
}

// GetSummary returns server-computed KPIs over the filtered window.
func (r *TransactionLedgerReadRepo) GetSummary(ctx context.Context, q tl.SummaryQuery) (tl.Summary, error) {
	if r == nil || r.tx == nil {
		return tl.Summary{}, errors.New("pg/transaction_ledger_read: repo not wired")
	}
	conds, baseArgs := commonFilterConds(q.Filters)
	where := whereClause(conds)

	out := tl.Summary{
		AmountMinorByCurrency: map[string]int64{},
		CountByKind:           map[string]int64{},
		CountByStatus:         map[string]int64{},
		WindowFrom:            q.Filters.From,
		WindowTo:              q.Filters.To,
	}

	err := r.tx.RunInScopeTx(ctx, q.GUCTenantID, func(ctx context.Context, tx Tx) error {
		// Totals + mana topped-up / spent (spend stored negative → negate).
		totalsSQL := "SELECT count(*), " +
			"COALESCE(sum(mana_units) FILTER (WHERE kind='mana_topup'),0), " +
			"COALESCE(-sum(mana_units) FILTER (WHERE kind='mana_spend_daily'),0) " +
			"FROM transaction_ledger l " + where
		if err := tx.QueryRow(ctx, totalsSQL, baseArgs...).Scan(
			&out.TotalCount, &out.TotalManaToppedUp, &out.TotalManaSpent); err != nil {
			return fmt.Errorf("pg/transaction_ledger_read: summary totals: %w", err)
		}

		// Per-currency net (captured − refunded), fiat rows only.
		curSQL := "SELECT currency, " +
			"COALESCE(sum(amount_minor) FILTER (WHERE status='captured'),0) " +
			"- COALESCE(sum(amount_minor) FILTER (WHERE status='refunded'),0) AS net " +
			"FROM transaction_ledger l " + where +
			condJoin(where, "currency IS NOT NULL") + " GROUP BY currency"
		if err := scanMapInt64(ctx, tx, curSQL, baseArgs, out.AmountMinorByCurrency); err != nil {
			return fmt.Errorf("pg/transaction_ledger_read: summary by currency: %w", err)
		}

		// Counts by kind.
		kindSQL := "SELECT kind, count(*) FROM transaction_ledger l " + where + " GROUP BY kind"
		if err := scanMapInt64(ctx, tx, kindSQL, baseArgs, out.CountByKind); err != nil {
			return fmt.Errorf("pg/transaction_ledger_read: summary by kind: %w", err)
		}

		// Counts by status.
		statusSQL := "SELECT status, count(*) FROM transaction_ledger l " + where + " GROUP BY status"
		if err := scanMapInt64(ctx, tx, statusSQL, baseArgs, out.CountByStatus); err != nil {
			return fmt.Errorf("pg/transaction_ledger_read: summary by status: %w", err)
		}
		return nil
	})
	if err != nil {
		return tl.Summary{}, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Shared WHERE / ORDER BY building
// -----------------------------------------------------------------------------

// commonFilterConds builds the window + kind + status + learner predicates
// shared by List + Summary. The window (From/To) is always present (the gRPC
// layer resolves the default ~90d window). Tenant isolation is RLS — never a
// SQL predicate here.
func commonFilterConds(f tl.ReadFilters) ([]string, []any) {
	var conds []string
	var args []any

	args = append(args, f.From.UTC())
	conds = append(conds, fmt.Sprintf("l.occurred_at >= $%d", len(args)))
	args = append(args, f.To.UTC())
	conds = append(conds, fmt.Sprintf("l.occurred_at < $%d", len(args)))

	if f.Kind != "" {
		args = append(args, f.Kind)
		conds = append(conds, fmt.Sprintf("l.kind = $%d", len(args)))
	}
	if f.Status != "" {
		args = append(args, f.Status)
		conds = append(conds, fmt.Sprintf("l.status = $%d", len(args)))
	}
	if len(f.LearnerGCIDs) > 0 {
		// OR over a SET of learners. pgx encodes the []string as a text[]; the
		// ::uuid[] cast converts it. A 1-element slice is the single-learner
		// back-compat case (identical result set to the former `= $N::uuid`).
		args = append(args, f.LearnerGCIDs)
		conds = append(conds, fmt.Sprintf("l.learner_gcid = ANY($%d::uuid[])", len(args)))
	}
	if len(f.ManagedTenantIDs) > 0 {
		// MASTER multi-franchisee narrowing. Tenant isolation is otherwise RLS
		// (no predicate); this deliberate span-all narrowing runs under the
		// 'platform' scope + OR over the selected franchisee tenant_ids. Set by
		// the gRPC adapter ONLY for a 2+ select (a single franchisee uses the
		// RLS GUC path instead), so this predicate never fights the GUC.
		args = append(args, f.ManagedTenantIDs)
		conds = append(conds, fmt.Sprintf("l.tenant_id = ANY($%d::uuid[])", len(args)))
	}
	return conds, args
}

// appendCursorCond adds the keyset continuation predicate for the active sort.
func appendCursorCond(conds []string, args []any, sort tl.SortSpec, cur *ledgerCursor) ([]string, []any) {
	cmp := "<"
	if !sort.Desc {
		cmp = ">"
	}
	switch sort.Field {
	case tl.SortByAmount:
		args = append(args, cur.Amt, cur.ID)
		conds = append(conds, fmt.Sprintf("(%s, l.ledger_id) %s ($%d, $%d::uuid)", amountSortExpr, cmp, len(args)-1, len(args)))
	default: // SortByOccurredAt
		args = append(args, cur.OA, cur.ID)
		conds = append(conds, fmt.Sprintf("(l.occurred_at, l.ledger_id) %s ($%d, $%d::uuid)", cmp, len(args)-1, len(args)))
	}
	return conds, args
}

func orderByClause(sort tl.SortSpec) string {
	dir := "DESC"
	if !sort.Desc {
		dir = "ASC"
	}
	if sort.Field == tl.SortByAmount {
		return fmt.Sprintf("ORDER BY %s %s, l.ledger_id %s", amountSortExpr, dir, dir)
	}
	return fmt.Sprintf("ORDER BY l.occurred_at %s, l.ledger_id %s", dir, dir)
}

func whereClause(conds []string) string {
	if len(conds) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(conds, " AND ")
}

// condJoin appends an extra predicate to an existing WHERE clause (or starts
// one). Used by the per-currency summary to add `currency IS NOT NULL`.
func condJoin(existingWhere, extra string) string {
	if existingWhere == "" {
		return " WHERE " + extra
	}
	return " AND " + extra
}

// scanMapInt64 runs a `SELECT key, int64value ... GROUP BY key` query and folds
// the rows into dest. A NULL key is skipped.
func scanMapInt64(ctx context.Context, tx Tx, sql string, args []any, dest map[string]int64) error {
	rs, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		var k *string
		var v int64
		if err := rs.Scan(&k, &v); err != nil {
			return err
		}
		if k != nil {
			dest[*k] = v
		}
	}
	return rs.Err()
}

// -----------------------------------------------------------------------------
// Row scanning
// -----------------------------------------------------------------------------

// scanLedgerRow scans the ledgerSelectCols shape. Returns the row + the
// amount_sort_key (for cursor encoding on amount sorts).
func scanLedgerRow(scan func(...any) error) (tl.LedgerRow, int64, error) {
	var (
		row                                 tl.LedgerRow
		learnerGCID, currency, metadataJSON *string
		amountMinor, manaUnits              *int64
		amountSortKey                       int64
	)
	if err := scan(
		&row.LedgerID, &row.OccurredAt, &row.TenantID, &learnerGCID,
		&row.Kind, &row.SourceDomain, &row.SourceRefID, &row.Label,
		&currency, &amountMinor, &manaUnits, &row.Status,
		&row.HasDetail, &metadataJSON, &amountSortKey,
	); err != nil {
		return tl.LedgerRow{}, 0, mapScanErr(err, "scan ledger row")
	}
	row.OccurredAt = row.OccurredAt.UTC()
	row.LearnerGCID = derefStr(learnerGCID)
	row.Currency = derefStr(currency)
	row.AmountMinor = derefInt64(amountMinor)
	row.ManaUnits = derefInt64(manaUnits)
	row.MetadataJSON = derefStr(metadataJSON)
	return row, amountSortKey, nil
}

func scanDetailRow(scan func(...any) error) (tl.DetailRow, error) {
	var (
		d                                         tl.DetailRow
		learnerGCID, model, traceID, metadataJSON *string
	)
	if err := scan(
		&d.DetailID, &d.LedgerID, &d.TenantID, &learnerGCID,
		&d.OccurredAt, &d.ActionCode, &d.ManaUnits, &model, &traceID, &metadataJSON,
	); err != nil {
		return tl.DetailRow{}, mapScanErr(err, "scan detail row")
	}
	d.OccurredAt = d.OccurredAt.UTC()
	d.LearnerGCID = derefStr(learnerGCID)
	d.Model = derefStr(model)
	d.TraceID = derefStr(traceID)
	d.MetadataJSON = derefStr(metadataJSON)
	return d, nil
}

func mapScanErr(err error, what string) error {
	if errors.Is(err, ErrNoRows) {
		return ErrNoRows
	}
	return fmt.Errorf("pg/transaction_ledger_read: %s: %w", what, err)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// -----------------------------------------------------------------------------
// Cursor encoding (base64 URL-safe JSON)
// -----------------------------------------------------------------------------

// ledgerCursor is the decoded list page token. Sort guards against reusing a
// cursor minted under a different sort (which would key the wrong axis).
type ledgerCursor struct {
	Sort string     `json:"s"`
	OA   *time.Time `json:"oa,omitempty"`
	Amt  *int64     `json:"amt,omitempty"`
	ID   string     `json:"id"`
}

func sortToken(s tl.SortSpec) string {
	field := "oa"
	if s.Field == tl.SortByAmount {
		field = "amt"
	}
	if s.Desc {
		return field + ":d"
	}
	return field + ":a"
}

func encodeLedgerCursor(sort tl.SortSpec, row tl.LedgerRow, amountKey int64) (string, error) {
	c := ledgerCursor{Sort: sortToken(sort), ID: row.LedgerID}
	if sort.Field == tl.SortByAmount {
		amt := amountKey
		c.Amt = &amt
	} else {
		oa := row.OccurredAt.UTC()
		c.OA = &oa
	}
	return encodeJSONCursor(c)
}

func decodeLedgerCursor(s string, sort tl.SortSpec) (*ledgerCursor, error) {
	if s == "" {
		return nil, nil
	}
	var c ledgerCursor
	if err := decodeJSONCursor(s, &c); err != nil {
		return nil, err
	}
	if c.ID == "" {
		return nil, errors.New("cursor missing id")
	}
	if c.Sort != sortToken(sort) {
		return nil, fmt.Errorf("cursor sort %q does not match request sort %q", c.Sort, sortToken(sort))
	}
	if sort.Field == tl.SortByAmount && c.Amt == nil {
		return nil, errors.New("amount cursor missing amt")
	}
	if sort.Field == tl.SortByOccurredAt && c.OA == nil {
		return nil, errors.New("occurred_at cursor missing oa")
	}
	return &c, nil
}

// detailCursor is the decoded detail-drill page token.
type detailCursor struct {
	OA time.Time `json:"oa"`
	ID string    `json:"id"`
}

func encodeDetailCursor(d tl.DetailRow) (string, error) {
	return encodeJSONCursor(detailCursor{OA: d.OccurredAt.UTC(), ID: d.DetailID})
}

func decodeDetailCursor(s string) (*detailCursor, error) {
	if s == "" {
		return nil, nil
	}
	var c detailCursor
	if err := decodeJSONCursor(s, &c); err != nil {
		return nil, err
	}
	if c.ID == "" {
		return nil, errors.New("detail cursor missing id")
	}
	return &c, nil
}

func encodeJSONCursor(v any) (string, error) {
	bz, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bz), nil
}

func decodeJSONCursor(s string, dest any) error {
	bz, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(bz, dest)
}
