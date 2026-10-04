// Package exportworker is the background builder for ADR-205 async transaction
// exports (B5 / CHO-1941). It mirrors the outbox dispatcher's poll-claim shape:
// claim PENDING jobs (under the 'platform' sentinel, across tenants), then for
// each — read the ledger under the job's frozen scope + filters, serialise
// CSV/JSON, upload to GCS, sign a download URL, and flip the job to READY (or
// FAILED). Pod-death resilient: an un-finished BUILDING job is re-claimed after
// the reservation window (the repo's ClaimPending reaper) up to a max attempt
// count, after which it FAILs loud.
package exportworker

import (
	"context"
	"fmt"
	"time"

	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

// Logger is the minimal logging surface (satisfied by the service logger).
type Logger interface {
	Printf(format string, args ...any)
}

// Config tunes the worker.
type Config struct {
	// ClaimLimit is the max jobs claimed per tick. Default 4.
	ClaimLimit int
	// PageSize is the ledger read page size while building. Default 100.
	PageSize int
	// MaxRows caps an export; exceeding it FAILS the job loud (no silent
	// truncation) — narrow the filter or use a tighter window. Default 200000.
	MaxRows int
	// SignedURLTTL is the download link lifetime. Default 1h.
	SignedURLTTL time.Duration
	// ObjectPrefix is the GCS key prefix. Default "transaction-exports".
	ObjectPrefix string
	// PollInterval is the RunLoop tick. Default 5s.
	PollInterval time.Duration
	// MaxAttempts FAILS a job that has been claimed this many times without
	// completing (poison-job guard). Default 3.
	MaxAttempts int
}

func (c *Config) withDefaults() {
	if c.ClaimLimit <= 0 {
		c.ClaimLimit = 4
	}
	if c.PageSize <= 0 {
		c.PageSize = 100
	}
	if c.MaxRows <= 0 {
		c.MaxRows = 200000
	}
	if c.SignedURLTTL <= 0 {
		c.SignedURLTTL = time.Hour
	}
	if c.ObjectPrefix == "" {
		c.ObjectPrefix = "transaction-exports"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
}

// Worker builds claimed export jobs.
type Worker struct {
	jobs  tl.ExportJobRepository
	read  tl.ReadRepository
	store tl.ExportStorage
	cfg   Config
	log   Logger
	now   func() time.Time
}

// New constructs a Worker. now defaults to time.Now().UTC when nil.
func New(jobs tl.ExportJobRepository, read tl.ReadRepository, store tl.ExportStorage, cfg Config, log Logger) *Worker {
	cfg.withDefaults()
	return &Worker{
		jobs:  jobs,
		read:  read,
		store: store,
		cfg:   cfg,
		log:   log,
		now:   func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the time source (tests).
func (w *Worker) WithClock(now func() time.Time) *Worker {
	w.now = now
	return w
}

// RunOnce claims + processes one batch. Returns the number of jobs processed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	jobs, err := w.jobs.ClaimPending(ctx, w.cfg.ClaimLimit)
	if err != nil {
		return 0, fmt.Errorf("exportworker: claim: %w", err)
	}
	for i := range jobs {
		w.process(ctx, jobs[i])
	}
	return len(jobs), nil
}

// RunLoop ticks RunOnce until ctx is cancelled. Intended as a background
// goroutine started in main.go.
func (w *Worker) RunLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := w.RunOnce(ctx); err != nil && w.log != nil {
				w.log.Printf("exportworker: run error: %v", err)
			}
		}
	}
}

// process builds one job, marking it READY or FAILED. Failures are recorded on
// the job (never swallowed) so the poll surfaces them.
func (w *Worker) process(ctx context.Context, job tl.ExportJob) {
	if job.AttemptExceeds(w.cfg.MaxAttempts) {
		w.fail(ctx, job, fmt.Sprintf("export abandoned after %d attempts", w.cfg.MaxAttempts))
		return
	}
	rows, err := w.buildRows(ctx, job)
	if err != nil {
		w.fail(ctx, job, err.Error())
		return
	}
	data, contentType, err := tl.SerializeExport(job.Format, rows)
	if err != nil {
		w.fail(ctx, job, err.Error())
		return
	}
	object := w.objectKey(job)
	if err := w.store.Upload(ctx, object, data, contentType); err != nil {
		w.fail(ctx, job, "upload: "+err.Error())
		return
	}
	url, expiresAt, err := w.store.SignedURL(ctx, object, w.cfg.SignedURLTTL)
	if err != nil {
		w.fail(ctx, job, "sign: "+err.Error())
		return
	}
	if err := w.jobs.MarkReady(ctx, job.JobID, object, url, expiresAt); err != nil && w.log != nil {
		w.log.Printf("exportworker: MarkReady(%s) failed: %v", job.JobID, err)
	}
}

func (w *Worker) fail(ctx context.Context, job tl.ExportJob, msg string) {
	if w.log != nil {
		w.log.Printf("exportworker: job %s failed: %s", job.JobID, msg)
	}
	if err := w.jobs.MarkFailed(ctx, job.JobID, msg); err != nil && w.log != nil {
		w.log.Printf("exportworker: MarkFailed(%s) failed: %v", job.JobID, err)
	}
}

// buildRows paginates the ledger under the job's frozen scope + filters,
// bounded by MaxRows (exceeding it is a loud failure, not a silent cap).
func (w *Worker) buildRows(ctx context.Context, job tl.ExportJob) ([]tl.LedgerRow, error) {
	guc := tl.GUCForTenant(job.TenantID)
	var rows []tl.LedgerRow
	cursor := ""
	for {
		page, err := w.read.List(ctx, tl.ListQuery{
			GUCTenantID: guc,
			Filters:     job.Filters,
			Cursor:      cursor,
			Limit:       w.cfg.PageSize,
		})
		if err != nil {
			return nil, fmt.Errorf("read page: %w", err)
		}
		rows = append(rows, page.Items...)
		if len(rows) > w.cfg.MaxRows {
			return nil, fmt.Errorf("export exceeds %d rows — narrow the filter or window", w.cfg.MaxRows)
		}
		if page.NextPageToken == "" {
			return rows, nil
		}
		cursor = page.NextPageToken
	}
}

// objectKey is the GCS object path: {prefix}/{tenant-or-platform}/{job_id}.{ext}.
func (w *Worker) objectKey(job tl.ExportJob) string {
	dir := tl.GUCForTenant(job.TenantID) // concrete tenant uuid | "platform"
	ext := "json"
	if job.Format == tl.FormatCSV {
		ext = "csv"
	}
	return w.cfg.ObjectPrefix + "/" + dir + "/" + job.JobID + "." + ext
}
