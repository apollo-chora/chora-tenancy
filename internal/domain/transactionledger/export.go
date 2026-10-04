// export.go — async export of the transaction_ledger projection (B5 / CHO-1941,
// ADR-205 D5.6). A CreateTransactionExport RPC persists a PENDING ExportJob; a
// background worker claims it, reads the ledger under the job's frozen scope +
// filters, serialises CSV/JSON, uploads to GCS, signs a URL, and flips the job
// to READY (or FAILED). This file is proto-free: domain types + ports + the
// serialiser (the testable core); the pg repo, GCS adapter, and worker are
// adapters.
package transactionledger

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// NilTenantUUID is the tenant_id slot for a MASTER span-all export job: no
// concrete tenant, visible only under the 'platform' RLS sentinel (no real
// tenant's GUC equals the nil uuid). Matches migration 0025.
const NilTenantUUID = "00000000-0000-0000-0000-000000000000"

// GUCForTenant maps an export job's tenant_id to its RLS session value: the
// nil-uuid span-all sentinel → PlatformScope ("platform"); any concrete tenant
// → itself. Used by the job repo (create/poll) + the worker (per-job ledger
// read) so a MASTER span-all job reads across tenants while learner/tenant/
// franchisee jobs stay pinned.
func GUCForTenant(tenantID string) string {
	if tenantID == NilTenantUUID {
		return PlatformScope
	}
	return tenantID
}

// Export formats (the OpenAPI `format` values + the migration CHECK).
const (
	FormatCSV  = "csv"
	FormatJSON = "json"
)

// Export job lifecycle (the migration CHECK + the OpenAPI ExportJob.status).
const (
	ExportPending  = "pending"
	ExportBuilding = "building"
	ExportReady    = "ready"
	ExportFailed   = "failed"
)

// ExportJob is the async export handle (read model + lifecycle).
type ExportJob struct {
	JobID              string
	TenantID           string // concrete tenant | NilTenantUUID (span-all)
	OwnerGCID          string
	LearnerGCID        string // learner scope bound learner ("" otherwise)
	Scope              string // "learner" | "tenant" | "master"
	ManagedTenantID string
	Filters            ReadFilters // frozen at create time (window included)
	Format             string
	Status             string
	GCSObject          string
	DownloadURL        string
	ExpiresAt          time.Time
	Error              string
	AttemptCount       int
	CreatedAt          time.Time
}

// AttemptExceeds reports whether the job has been claimed more than max times
// without completing — the worker FAILs such a poison job loud.
func (j ExportJob) AttemptExceeds(max int) bool { return j.AttemptCount > max }

// NewExportJob is the create input the repo persists as a PENDING row.
type NewExportJob struct {
	TenantID           string // concrete tenant | NilTenantUUID
	OwnerGCID          string
	LearnerGCID        string
	Scope              string
	ManagedTenantID string
	Format             string
	Filters            ReadFilters // resolved (window already defaulted) by the gRPC layer
}

// ExportJobRepository is the persistence port for export jobs.
type ExportJobRepository interface {
	// Create inserts a PENDING job and returns it (with JobID + CreatedAt).
	Create(ctx context.Context, in NewExportJob) (ExportJob, error)

	// Get reads one job under the caller's scope. gucTenantID is the RLS
	// session value (tenant uuid | "platform"); learnerFilterGCID, when set,
	// additionally constrains to that learner's own job (learner scope). found
	// is false when no visible job matches.
	Get(ctx context.Context, gucTenantID, jobID, learnerFilterGCID string) (ExportJob, bool, error)

	// ClaimPending atomically claims up to limit PENDING jobs (flips them to
	// BUILDING) across all tenants — the worker runs under the 'platform'
	// sentinel. Mirrors the outbox FetchPending claim (reservation window +
	// crashed-worker reaper).
	ClaimPending(ctx context.Context, limit int) ([]ExportJob, error)

	// MarkReady stamps the signed download URL + expiry + object and flips to
	// READY.
	MarkReady(ctx context.Context, jobID, gcsObject, downloadURL string, expiresAt time.Time) error

	// MarkFailed records a short error + flips to FAILED.
	MarkFailed(ctx context.Context, jobID, errMsg string) error
}

// ExportStorage is the object-store port (GCS in prod, in-mem fake in tests).
type ExportStorage interface {
	// Upload writes data at object with the given content type.
	Upload(ctx context.Context, object string, data []byte, contentType string) error
	// SignedURL returns a time-limited download URL + its expiry.
	SignedURL(ctx context.Context, object string, ttl time.Duration) (url string, expiresAt time.Time, err error)
}

// exportColumns is the canonical column order for both CSV + JSON object keys.
var exportColumns = []string{
	"ledger_id", "occurred_at", "tenant_id", "learner_gcid", "kind",
	"source_domain", "source_ref_id", "label", "currency", "amount_minor",
	"mana_units", "status",
}

// SerializeExport renders ledger rows to the requested format. Returns the
// bytes + the content type. CSV carries a header row; JSON is an array of
// objects keyed by exportColumns.
func SerializeExport(format string, rows []LedgerRow) ([]byte, string, error) {
	switch format {
	case FormatCSV:
		return serializeCSV(rows)
	case FormatJSON:
		return serializeJSON(rows)
	default:
		return nil, "", fmt.Errorf("transactionledger: unknown export format %q", format)
	}
}

func serializeCSV(rows []LedgerRow) ([]byte, string, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(exportColumns); err != nil {
		return nil, "", fmt.Errorf("transactionledger: csv header: %w", err)
	}
	for _, r := range rows {
		if err := w.Write(exportRecord(r)); err != nil {
			return nil, "", fmt.Errorf("transactionledger: csv row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, "", fmt.Errorf("transactionledger: csv flush: %w", err)
	}
	return buf.Bytes(), "text/csv; charset=utf-8", nil
}

func serializeJSON(rows []LedgerRow) ([]byte, string, error) {
	objs := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		objs = append(objs, map[string]any{
			"ledger_id":     r.LedgerID,
			"occurred_at":   r.OccurredAt.UTC().Format(time.RFC3339Nano),
			"tenant_id":     r.TenantID,
			"learner_gcid":  r.LearnerGCID,
			"kind":          r.Kind,
			"source_domain": r.SourceDomain,
			"source_ref_id": r.SourceRefID,
			"label":         r.Label,
			"currency":      r.Currency,
			"amount_minor":  r.AmountMinor,
			"mana_units":    r.ManaUnits,
			"status":        r.Status,
		})
	}
	b, err := json.Marshal(objs)
	if err != nil {
		return nil, "", fmt.Errorf("transactionledger: json marshal: %w", err)
	}
	return b, "application/json; charset=utf-8", nil
}

// exportRecord projects one row to the CSV field order (exportColumns).
func exportRecord(r LedgerRow) []string {
	return []string{
		r.LedgerID,
		r.OccurredAt.UTC().Format(time.RFC3339Nano),
		r.TenantID,
		r.LearnerGCID,
		r.Kind,
		r.SourceDomain,
		r.SourceRefID,
		r.Label,
		r.Currency,
		strconv.FormatInt(r.AmountMinor, 10),
		strconv.FormatInt(r.ManaUnits, 10),
		r.Status,
	}
}
