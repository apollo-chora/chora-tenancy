// transaction_ledger_write_repository.go — pg adapter for the ADR-205
// transaction_ledger CQRS projection write port (B2 / CHO-1938).
//
// Both methods run inside RunInTenantTx so RLS (`SET LOCAL chora.tenant_id`)
// is set before the write — the chora_tenancy_app_rw role is NOBYPASSRLS, so a
// bare INSERT would fail the WITH CHECK / see zero rows. The natural-key
// ON CONFLICT (from migration 0024) makes the projection idempotent:
//   - UpsertPurchase: redelivered captured = no-op; refunded flips status
//     (refund is sticky — robust to out-of-order delivery).
//   - AccumulateSpend: mana_units accumulate into the (tenant, learner, day)
//     rollup + the per-call detail is appended atomically in the same tx.
package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// nilUUID is the COALESCE sentinel matching uq_transaction_ledger_natural_key.
const nilUUID = "00000000-0000-0000-0000-000000000000"

// TransactionLedgerWriteRepo upserts unified ledger rows under tenant RLS.
type TransactionLedgerWriteRepo struct {
	tx TxQuerier
}

// NewTransactionLedgerWriteRepo wires the projection write repo to a
// tenant-scoped transaction runner.
func NewTransactionLedgerWriteRepo(tx TxQuerier) *TransactionLedgerWriteRepo {
	return &TransactionLedgerWriteRepo{tx: tx}
}

var _ transactionledger.WriteRepository = (*TransactionLedgerWriteRepo)(nil)

// UpsertPurchase inserts or merges a purchase / mana_topup row.
func (r *TransactionLedgerWriteRepo) UpsertPurchase(
	ctx context.Context, e transactionledger.Entry,
) error {
	meta, err := tlMetaJSONB(e.Metadata)
	if err != nil {
		return err
	}
	return r.tx.RunInTenantTx(ctx, e.TenantID, func(ctx context.Context, tx Tx) error {
		return tx.Exec(ctx, `
INSERT INTO transaction_ledger
  (occurred_at, tenant_id, learner_gcid, kind, source_domain, source_ref_id,
   label, currency, amount_minor, mana_units, status, metadata_jsonb)
VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
ON CONFLICT (tenant_id, kind, source_domain, source_ref_id,
             COALESCE(learner_gcid, '`+nilUUID+`'::uuid))
DO UPDATE SET
  status = CASE WHEN transaction_ledger.status = 'refunded'
                THEN 'refunded' ELSE EXCLUDED.status END,
  label = EXCLUDED.label,
  metadata_jsonb = EXCLUDED.metadata_jsonb`,
			e.OccurredAt, e.TenantID, tlNullStr(e.LearnerGCID), e.Kind,
			e.SourceDomain, e.SourceRefID, e.Label, tlNullStr(e.Currency),
			tlNullInt64(e.AmountMinor), tlNullInt64(e.ManaUnits), e.Status, meta)
	})
}

// AccumulateSpend folds a metered spend into the day rollup + appends detail.
func (r *TransactionLedgerWriteRepo) AccumulateSpend(
	ctx context.Context, e transactionledger.Entry, d transactionledger.Detail,
) error {
	metaE, err := tlMetaJSONB(e.Metadata)
	if err != nil {
		return err
	}
	metaD, err := tlMetaJSONB(d.Metadata)
	if err != nil {
		return err
	}
	return r.tx.RunInTenantTx(ctx, e.TenantID, func(ctx context.Context, tx Tx) error {
		var ledgerID string
		row := tx.QueryRow(ctx, `
INSERT INTO transaction_ledger
  (occurred_at, tenant_id, learner_gcid, kind, source_domain, source_ref_id,
   label, mana_units, status, metadata_jsonb)
VALUES ($1, $2::uuid, $3::uuid, 'mana_spend_daily', $4, $5, $6, $7, 'posted', $8::jsonb)
ON CONFLICT (tenant_id, kind, source_domain, source_ref_id,
             COALESCE(learner_gcid, '`+nilUUID+`'::uuid))
DO UPDATE SET
  mana_units  = COALESCE(transaction_ledger.mana_units, 0) + EXCLUDED.mana_units,
  occurred_at = GREATEST(transaction_ledger.occurred_at, EXCLUDED.occurred_at)
RETURNING ledger_id`,
			e.OccurredAt, e.TenantID, tlNullStr(e.LearnerGCID), e.SourceDomain,
			e.SourceRefID, e.Label, e.ManaUnits, metaE)
		if err := row.Scan(&ledgerID); err != nil {
			return fmt.Errorf("pg.AccumulateSpend: upsert day rollup: %w", err)
		}
		if err := tx.Exec(ctx, `
INSERT INTO transaction_ledger_detail
  (ledger_id, tenant_id, learner_gcid, occurred_at, action_code, mana_units,
   model, trace_id, metadata_jsonb)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9::jsonb)`,
			ledgerID, d.TenantID, tlNullStr(d.LearnerGCID), d.OccurredAt,
			d.ActionCode, d.ManaUnits, tlNullStr(d.Model), tlNullStr(d.TraceID),
			metaD); err != nil {
			return fmt.Errorf("pg.AccumulateSpend: append detail: %w", err)
		}
		return nil
	})
}

// tlNullStr → *string (nil when empty) so an empty UUID/text param binds as
// SQL NULL rather than the invalid empty string.
func tlNullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// tlNullInt64 → *int64 (nil when zero) so the unused fiat/mana leg is NULL.
func tlNullInt64(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

// tlMetaJSONB marshals metadata to a JSON string ('{}' when empty) for the
// jsonb column (text→jsonb cast via $N::jsonb).
func tlMetaJSONB(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("pg: marshal ledger metadata: %w", err)
	}
	return string(b), nil
}
