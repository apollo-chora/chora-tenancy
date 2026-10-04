// transaction_export_repository.go — pg adapter for the ADR-205 async-export
// job store (B5 / CHO-1941). Implements transactionledger.ExportJobRepository
// over transaction_export_jobs (migration 0025), under the same RunInScopeTx
// RLS path as the read repo.
//
// guc derivation: a job's RLS session value is its tenant_id, EXCEPT a MASTER
// span-all job (tenant_id = NilTenantUUID) which is created / polled / claimed
// under the 'platform' sentinel (no real tenant's GUC equals the nil uuid).
// ClaimPending always runs under 'platform' — the worker claims across tenants.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// exportClaimReservationSecs hides a just-claimed (or crashed-worker) job from
// peer claims for this window — doubles as the stuck-BUILDING reaper.
const exportClaimReservationSecs = 120

// exportMaxAttempts caps re-claims of a poison job before the worker fails it.
const exportMaxAttempts = 3

// TransactionExportRepo persists export jobs under tenant RLS.
type TransactionExportRepo struct {
	tx ScopeTxQuerier
}

// NewTransactionExportRepo wires the repo to a scope-aware runner.
func NewTransactionExportRepo(tx ScopeTxQuerier) *TransactionExportRepo {
	return &TransactionExportRepo{tx: tx}
}

var _ tl.ExportJobRepository = (*TransactionExportRepo)(nil)

// Create inserts a PENDING job under the job-tenant's RLS context.
func (r *TransactionExportRepo) Create(ctx context.Context, in tl.NewExportJob) (tl.ExportJob, error) {
	if r == nil || r.tx == nil {
		return tl.ExportJob{}, errors.New("pg/transaction_export: repo not wired")
	}
	filtersJSON, err := marshalExportFilters(in.Filters)
	if err != nil {
		return tl.ExportJob{}, err
	}
	var job tl.ExportJob
	err = r.tx.RunInScopeTx(ctx, tl.GUCForTenant(in.TenantID), func(ctx context.Context, tx Tx) error {
		row := tx.QueryRow(ctx, `
INSERT INTO transaction_export_jobs
  (tenant_id, owner_gcid, learner_gcid, scope, franchisee_tenant_id,
   filters_jsonb, format, status)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::uuid, $6::jsonb, $7, 'pending')
RETURNING job_id::text, created_at`,
			in.TenantID, in.OwnerGCID, tlNullStr(in.LearnerGCID), in.Scope,
			tlNullStr(in.ManagedTenantID), filtersJSON, in.Format)
		var jobID string
		var createdAt time.Time
		if err := row.Scan(&jobID, &createdAt); err != nil {
			return fmt.Errorf("pg/transaction_export: insert: %w", err)
		}
		job = tl.ExportJob{
			JobID: jobID, TenantID: in.TenantID, OwnerGCID: in.OwnerGCID,
			LearnerGCID: in.LearnerGCID, Scope: in.Scope, ManagedTenantID: in.ManagedTenantID,
			Filters: in.Filters, Format: in.Format, Status: tl.ExportPending,
			CreatedAt: createdAt.UTC(),
		}
		return nil
	})
	if err != nil {
		return tl.ExportJob{}, err
	}
	return job, nil
}

// Get reads one job under the caller's scope (+ optional learner constraint).
func (r *TransactionExportRepo) Get(ctx context.Context, gucTenantID, jobID, learnerFilterGCID string) (tl.ExportJob, bool, error) {
	if r == nil || r.tx == nil {
		return tl.ExportJob{}, false, errors.New("pg/transaction_export: repo not wired")
	}
	conds := []string{"job_id = $1::uuid"}
	args := []any{jobID}
	if learnerFilterGCID != "" {
		args = append(args, learnerFilterGCID)
		conds = append(conds, fmt.Sprintf("learner_gcid = $%d::uuid", len(args)))
	}
	sql := "SELECT " + exportJobCols + " FROM transaction_export_jobs WHERE " +
		strings.Join(conds, " AND ") + " LIMIT 1"

	var (
		job   tl.ExportJob
		found bool
	)
	err := r.tx.RunInScopeTx(ctx, gucTenantID, func(ctx context.Context, tx Tx) error {
		j, scanErr := scanExportJob(tx.QueryRow(ctx, sql, args...).Scan)
		if scanErr != nil {
			if errors.Is(scanErr, ErrNoRows) {
				return nil
			}
			return scanErr
		}
		job, found = j, true
		return nil
	})
	if err != nil {
		return tl.ExportJob{}, false, err
	}
	return job, found, nil
}

// ClaimPending claims up to limit PENDING (or stale-BUILDING) jobs, flipping
// them to BUILDING under the 'platform' sentinel (worker spans all tenants).
func (r *TransactionExportRepo) ClaimPending(ctx context.Context, limit int) ([]tl.ExportJob, error) {
	if r == nil || r.tx == nil {
		return nil, errors.New("pg/transaction_export: repo not wired")
	}
	var jobs []tl.ExportJob
	err := r.tx.RunInScopeTx(ctx, tl.PlatformScope, func(ctx context.Context, tx Tx) error {
		rs, qerr := tx.Query(ctx, `
UPDATE transaction_export_jobs
   SET status = 'building', last_attempt_at = now(), attempt_count = attempt_count + 1
 WHERE job_id IN (
     SELECT job_id FROM transaction_export_jobs
      WHERE (status = 'pending'
             OR (status = 'building'
                 AND last_attempt_at < now() - make_interval(secs => $2)))
      ORDER BY created_at ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
 )
RETURNING `+exportJobCols, limit, exportClaimReservationSecs)
		if qerr != nil {
			return fmt.Errorf("pg/transaction_export: claim: %w", qerr)
		}
		defer rs.Close()
		for rs.Next() {
			j, scanErr := scanExportJob(rs.Scan)
			if scanErr != nil {
				return scanErr
			}
			jobs = append(jobs, j)
		}
		return rs.Err()
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// MarkReady stamps the signed URL + object + expiry and flips to READY.
func (r *TransactionExportRepo) MarkReady(ctx context.Context, jobID, gcsObject, downloadURL string, expiresAt time.Time) error {
	return r.update(ctx, jobID, func(tx Tx) error {
		return tx.Exec(ctx, `
UPDATE transaction_export_jobs
   SET status = 'ready', gcs_object = $2, download_url = $3, expires_at = $4, error = NULL
 WHERE job_id = $1::uuid`, jobID, gcsObject, downloadURL, expiresAt.UTC())
	})
}

// MarkFailed records a short error + flips to FAILED.
func (r *TransactionExportRepo) MarkFailed(ctx context.Context, jobID, errMsg string) error {
	return r.update(ctx, jobID, func(tx Tx) error {
		return tx.Exec(ctx, `
UPDATE transaction_export_jobs SET status = 'failed', error = $2 WHERE job_id = $1::uuid`,
			jobID, truncateErr(errMsg))
	})
}

// update runs a by-job_id write under 'platform' (worker context).
func (r *TransactionExportRepo) update(ctx context.Context, jobID string, fn func(Tx) error) error {
	if r == nil || r.tx == nil {
		return errors.New("pg/transaction_export: repo not wired")
	}
	return r.tx.RunInScopeTx(ctx, tl.PlatformScope, func(ctx context.Context, tx Tx) error {
		return fn(tx)
	})
}

// exportJobCols is the shared SELECT/RETURNING column list (scan order).
const exportJobCols = `
    job_id::text, tenant_id::text, owner_gcid::text, learner_gcid::text, scope,
    franchisee_tenant_id::text, filters_jsonb::text, format, status,
    gcs_object, download_url, expires_at, error, attempt_count, created_at`

func scanExportJob(scan func(...any) error) (tl.ExportJob, error) {
	var (
		j                                         tl.ExportJob
		learnerGCID, franchisee, gcsObject, dlURL *string
		errMsg                                    *string
		filtersJSON                               string
		expiresAt                                 *time.Time
	)
	if err := scan(
		&j.JobID, &j.TenantID, &j.OwnerGCID, &learnerGCID, &j.Scope,
		&franchisee, &filtersJSON, &j.Format, &j.Status,
		&gcsObject, &dlURL, &expiresAt, &errMsg, &j.AttemptCount, &j.CreatedAt,
	); err != nil {
		return tl.ExportJob{}, mapScanErr(err, "scan export job")
	}
	j.LearnerGCID = derefStr(learnerGCID)
	j.ManagedTenantID = derefStr(franchisee)
	j.GCSObject = derefStr(gcsObject)
	j.DownloadURL = derefStr(dlURL)
	j.Error = derefStr(errMsg)
	j.CreatedAt = j.CreatedAt.UTC()
	if expiresAt != nil {
		j.ExpiresAt = expiresAt.UTC()
	}
	f, err := unmarshalExportFilters(filtersJSON)
	if err != nil {
		return tl.ExportJob{}, err
	}
	j.Filters = f
	return j, nil
}

// exportFiltersJSON is the on-disk shape of the frozen ReadFilters.
// LearnerGCID (singular) is retained read-only for back-compat with jobs
// persisted before the multi-learner (ADR-205 / CHO-1930) change; new jobs
// write LearnerGCIDs.
type exportFiltersJSON struct {
	Kind         string   `json:"kind,omitempty"`
	Status       string   `json:"status,omitempty"`
	From         string   `json:"from,omitempty"` // RFC3339
	To           string   `json:"to,omitempty"`
	SortField    int      `json:"sort_field"`
	SortDesc     bool     `json:"sort_desc"`
	LearnerGCID  string   `json:"learner_gcid,omitempty"`  // legacy singular (read-only)
	LearnerGCIDs []string `json:"learner_gcids,omitempty"` // current set
	// ManagedTenantIDs MUST round-trip: a MASTER multi-franchisee export runs
	// under the nil-uuid span-all TenantID, so this frozen predicate is the ONLY
	// thing scoping the worker's replay to the selected franchisees (anti-leak,
	// ADR-208 / CHO-1930). Drop it and a multi-franchisee export spans ALL
	// tenants. The on-disk JSON key is deliberately kept as the legacy
	// `franchisee_tenant_ids` (like the immutable DB column) so in-flight export
	// jobs persisted before the ADR-217 Phase 3 rename still read back — only the
	// Go identifier moved to ManagedTenantIDs.
	ManagedTenantIDs []string `json:"franchisee_tenant_ids,omitempty"`
}

func marshalExportFilters(f tl.ReadFilters) (string, error) {
	out := exportFiltersJSON{
		Kind: f.Kind, Status: f.Status,
		SortField: int(f.Sort.Field), SortDesc: f.Sort.Desc,
		LearnerGCIDs:        f.LearnerGCIDs,
		ManagedTenantIDs: f.ManagedTenantIDs,
	}
	if !f.From.IsZero() {
		out.From = f.From.UTC().Format(time.RFC3339Nano)
	}
	if !f.To.IsZero() {
		out.To = f.To.UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("pg/transaction_export: marshal filters: %w", err)
	}
	return string(b), nil
}

func unmarshalExportFilters(s string) (tl.ReadFilters, error) {
	if strings.TrimSpace(s) == "" {
		return tl.ReadFilters{}, nil
	}
	var in exportFiltersJSON
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return tl.ReadFilters{}, fmt.Errorf("pg/transaction_export: unmarshal filters: %w", err)
	}
	// Prefer the current set; fall back to the legacy singular for old jobs.
	learners := in.LearnerGCIDs
	if len(learners) == 0 && in.LearnerGCID != "" {
		learners = []string{in.LearnerGCID}
	}
	f := tl.ReadFilters{
		Kind: in.Kind, Status: in.Status, LearnerGCIDs: learners,
		ManagedTenantIDs: in.ManagedTenantIDs,
		Sort:                tl.SortSpec{Field: tl.SortField(in.SortField), Desc: in.SortDesc},
	}
	if in.From != "" {
		if t, err := time.Parse(time.RFC3339Nano, in.From); err == nil {
			f.From = t.UTC()
		}
	}
	if in.To != "" {
		if t, err := time.Parse(time.RFC3339Nano, in.To); err == nil {
			f.To = t.UTC()
		}
	}
	return f, nil
}

func truncateErr(s string) string {
	const max = 1000
	if len(s) <= max {
		return s
	}
	return s[:max]
}
