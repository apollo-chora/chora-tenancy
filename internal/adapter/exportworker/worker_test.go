package exportworker_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/exportworker"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// ---- fakes ----

type fakeJobRepo struct {
	pending       []tl.ExportJob
	claimed       bool
	readyID       string
	readyURL      string
	readyObj      string
	failedID      string
	failedMsg     string
	claimErr      error
	markReadyErr  error
	markFailedErr error
}

func (f *fakeJobRepo) Create(context.Context, tl.NewExportJob) (tl.ExportJob, error) {
	return tl.ExportJob{}, errors.New("unused")
}
func (f *fakeJobRepo) Get(context.Context, string, string, string) (tl.ExportJob, bool, error) {
	return tl.ExportJob{}, false, errors.New("unused")
}
func (f *fakeJobRepo) ClaimPending(_ context.Context, _ int) ([]tl.ExportJob, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if f.claimed {
		return nil, nil
	}
	f.claimed = true
	return f.pending, nil
}
func (f *fakeJobRepo) MarkReady(_ context.Context, jobID, gcsObject, downloadURL string, _ time.Time) error {
	if f.markReadyErr != nil {
		return f.markReadyErr
	}
	f.readyID, f.readyObj, f.readyURL = jobID, gcsObject, downloadURL
	return nil
}
func (f *fakeJobRepo) MarkFailed(_ context.Context, jobID, errMsg string) error {
	if f.markFailedErr != nil {
		return f.markFailedErr
	}
	f.failedID, f.failedMsg = jobID, errMsg
	return nil
}

type stubRead struct {
	pages []tl.ListPage
	idx   int
	err   error
}

func (s *stubRead) List(_ context.Context, _ tl.ListQuery) (tl.ListPage, error) {
	if s.err != nil {
		return tl.ListPage{}, s.err
	}
	if s.idx >= len(s.pages) {
		return tl.ListPage{}, nil
	}
	p := s.pages[s.idx]
	s.idx++
	return p, nil
}
func (s *stubRead) GetDetail(context.Context, tl.DetailQuery) (tl.DetailPage, error) {
	return tl.DetailPage{}, errors.New("unused")
}
func (s *stubRead) GetSummary(context.Context, tl.SummaryQuery) (tl.Summary, error) {
	return tl.Summary{}, errors.New("unused")
}

type fakeStorage struct {
	uploadedObject string
	uploadedBytes  []byte
	uploadedCT     string
	uploadErr      error
}

func (f *fakeStorage) Upload(_ context.Context, object string, data []byte, contentType string) error {
	if f.uploadErr != nil {
		return f.uploadErr
	}
	f.uploadedObject, f.uploadedBytes, f.uploadedCT = object, data, contentType
	return nil
}
func (f *fakeStorage) SignedURL(_ context.Context, object string, ttl time.Duration) (string, time.Time, error) {
	return "https://signed/" + object, time.Date(2026, 6, 29, 13, 0, 0, 0, time.UTC), nil
}

func tenantJob(id string) tl.ExportJob {
	return tl.ExportJob{
		JobID: id, TenantID: "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b", Scope: "tenant",
		Format: tl.FormatCSV, Status: tl.ExportBuilding, AttemptCount: 1,
		Filters: tl.ReadFilters{From: time.Now().Add(-24 * time.Hour), To: time.Now(), Sort: tl.ParseSort("")},
	}
}

func TestRunOnce_BuildsUploadsSignsAndMarksReady(t *testing.T) {
	jobs := &fakeJobRepo{pending: []tl.ExportJob{tenantJob("J1")}}
	read := &stubRead{pages: []tl.ListPage{
		{Items: []tl.LedgerRow{{LedgerID: "L1", Kind: tl.KindPurchase, Status: tl.StatusCaptured, OccurredAt: time.Now()}}, NextPageToken: "p2"},
		{Items: []tl.LedgerRow{{LedgerID: "L2", Kind: tl.KindManaTopup, Status: tl.StatusCaptured, OccurredAt: time.Now()}}, NextPageToken: ""},
	}}
	store := &fakeStorage{}
	w := exportworker.New(jobs, read, store, exportworker.Config{PageSize: 1}, nil)

	n, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d; want 1", n)
	}
	if jobs.readyID != "J1" {
		t.Fatalf("MarkReady not called for J1 (failed=%q: %q)", jobs.failedID, jobs.failedMsg)
	}
	if !strings.HasSuffix(store.uploadedObject, "J1.csv") {
		t.Errorf("object key = %q; want .../J1.csv", store.uploadedObject)
	}
	if !strings.HasPrefix(store.uploadedCT, "text/csv") {
		t.Errorf("content type = %q", store.uploadedCT)
	}
	// both pages' rows present in the CSV (L1 + L2 + header = 3 lines).
	lines := strings.Count(strings.TrimSpace(string(store.uploadedBytes)), "\n") + 1
	if lines != 3 {
		t.Errorf("csv lines = %d; want 3 (header + 2 rows)", lines)
	}
	if jobs.readyURL != "https://signed/"+store.uploadedObject {
		t.Errorf("ready url = %q", jobs.readyURL)
	}
}

func TestRunOnce_ReadError_MarksFailed(t *testing.T) {
	jobs := &fakeJobRepo{pending: []tl.ExportJob{tenantJob("J2")}}
	w := exportworker.New(jobs, &stubRead{err: errors.New("db down")}, &fakeStorage{}, exportworker.Config{}, nil)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if jobs.failedID != "J2" || !strings.Contains(jobs.failedMsg, "db down") {
		t.Fatalf("expected J2 failed with read error, got id=%q msg=%q", jobs.failedID, jobs.failedMsg)
	}
}

func TestRunOnce_UploadError_MarksFailed(t *testing.T) {
	jobs := &fakeJobRepo{pending: []tl.ExportJob{tenantJob("J3")}}
	read := &stubRead{pages: []tl.ListPage{{Items: []tl.LedgerRow{{LedgerID: "L1", Kind: tl.KindPurchase, Status: tl.StatusCaptured, OccurredAt: time.Now()}}}}}
	w := exportworker.New(jobs, read, &fakeStorage{uploadErr: errors.New("gcs 503")}, exportworker.Config{}, nil)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if jobs.failedID != "J3" || !strings.Contains(jobs.failedMsg, "gcs 503") {
		t.Fatalf("expected J3 failed with upload error, got %q/%q", jobs.failedID, jobs.failedMsg)
	}
}

func TestRunOnce_MaxRowsExceeded_MarksFailed(t *testing.T) {
	jobs := &fakeJobRepo{pending: []tl.ExportJob{tenantJob("J4")}}
	// each page returns 2 rows + a next token forever; MaxRows=3 trips after page 2.
	big := tl.ListPage{Items: []tl.LedgerRow{
		{LedgerID: "a", Kind: tl.KindPurchase, Status: tl.StatusCaptured, OccurredAt: time.Now()},
		{LedgerID: "b", Kind: tl.KindPurchase, Status: tl.StatusCaptured, OccurredAt: time.Now()},
	}, NextPageToken: "more"}
	w := exportworker.New(jobs, &stubRead{pages: []tl.ListPage{big, big, big}}, &fakeStorage{}, exportworker.Config{MaxRows: 3, PageSize: 2}, nil)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if jobs.failedID != "J4" || !strings.Contains(jobs.failedMsg, "exceeds") {
		t.Fatalf("expected J4 failed with max-rows, got %q/%q", jobs.failedID, jobs.failedMsg)
	}
}

func TestRunOnce_PoisonJob_AbandonedAfterMaxAttempts(t *testing.T) {
	j := tenantJob("J5")
	j.AttemptCount = 5 // already past MaxAttempts
	jobs := &fakeJobRepo{pending: []tl.ExportJob{j}}
	read := &stubRead{} // must NOT be read
	w := exportworker.New(jobs, read, &fakeStorage{}, exportworker.Config{MaxAttempts: 3}, nil)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if jobs.failedID != "J5" || !strings.Contains(jobs.failedMsg, "abandoned") {
		t.Fatalf("expected J5 abandoned, got %q/%q", jobs.failedID, jobs.failedMsg)
	}
	if read.idx != 0 {
		t.Error("poison job should not have read the ledger")
	}
}

func TestWorker_WithClock_Chain(t *testing.T) {
	t.Parallel()
	w := exportworker.New(&fakeJobRepo{}, &stubRead{}, &fakeStorage{}, exportworker.Config{}, nil)
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if got := w.WithClock(func() time.Time { return now }); got != w {
		t.Fatalf("WithClock should return the receiver")
	}
}

func TestWorker_RunLoop_ExitsOnCancel(t *testing.T) {
	j := tenantJob("R1")
	j.AttemptCount = 5 // poison → process() marks failed without touching read
	jobs := &fakeJobRepo{pending: []tl.ExportJob{j}}
	read := &stubRead{}
	w := exportworker.New(jobs, read, &fakeStorage{}, exportworker.Config{
		PollInterval: time.Millisecond,
		MaxAttempts:  3,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.RunLoop(ctx)
		close(done)
	}()
	// Let the loop tick at least once, then cancel.
	deadline := time.After(200 * time.Millisecond)
	ticking := false
	for !ticking {
		select {
		case <-deadline:
			t.Fatalf("RunLoop did not tick")
		case <-time.After(5 * time.Millisecond):
			if jobs.failedID != "" {
				ticking = true
			}
		}
	}
	cancel()
	select {
	case <-done:
		// ok — RunLoop returned on ctx cancel
	case <-time.After(2 * time.Second):
		t.Fatalf("RunLoop did not exit after cancel")
	}
	if jobs.failedID != "R1" {
		t.Fatalf("expected R1 processed during loop, got %q", jobs.failedID)
	}
}

// recordLogger captures Printf lines from the worker.
type recordLogger struct{ lines int }

func (l *recordLogger) Printf(format string, args ...any) { l.lines++ }

func TestWorker_RunOnce_ClaimError_Reports(t *testing.T) {
	t.Parallel()
	jobs := &fakeJobRepo{claimErr: errors.New("claim boom")}
	w := exportworker.New(jobs, &stubRead{}, &fakeStorage{}, exportworker.Config{}, nil)
	if _, err := w.RunOnce(context.Background()); err == nil {
		t.Fatal("expected claim error")
	}
}

func TestWorker_Process_LoggerPaths(t *testing.T) {
	t.Parallel()
	// Poison job → fail() with a logger (the log.Printf branch).
	j := tenantJob("L1")
	j.AttemptCount = 5
	jobs := &fakeJobRepo{pending: []tl.ExportJob{j}, markFailedErr: errors.New("mark failed boom")}
	logger := &recordLogger{}
	w := exportworker.New(jobs, &stubRead{}, &fakeStorage{}, exportworker.Config{MaxAttempts: 3}, logger)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if logger.lines == 0 {
		t.Fatal("expected fail() to log")
	}
	// MarkReady failure warns via the logger.
	j2 := tenantJob("L2")
	jobs2 := &fakeJobRepo{pending: []tl.ExportJob{j2}, markReadyErr: errors.New("ready boom")}
	logger2 := &recordLogger{}
	w2 := exportworker.New(jobs2, onePageReader(), &fakeStorage{}, exportworker.Config{PageSize: 5}, logger2)
	if _, err := w2.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if logger2.lines == 0 {
		t.Fatal("expected MarkReady failure to log")
	}
}

// onePageReader returns a stubRead serving a single ledger row page.
func onePageReader() *stubRead {
	row := tl.LedgerRow{
		LedgerID: "x1", OccurredAt: time.Now(), TenantID: "t", Kind: tl.KindPurchase,
		SourceDomain: "payments", SourceRefID: "r", Status: tl.StatusCaptured,
	}
	return &stubRead{pages: []tl.ListPage{{Items: []tl.LedgerRow{row}}}}
}
