// Package gcs adapts the transactionledger.ExportStorage port to Google Cloud
// Storage for the ADR-205 async transaction export (B5 / CHO-1941). It writes
// the built CSV/JSON object and mints a V4 GET signed download URL.
//
// Wiring: cmd/server/main.go constructs NewExportStorage(client, bucket, ...)
// where the bucket comes from env CHORA_TENANCY_EXPORT_BUCKET. Empty env →
// ErrExportStorageNotWired so main.go leaves the worker disabled (export jobs
// stay PENDING; the BFF reports them) per feedback_no_stubs_real_wiring.
//
// Live V4 signing needs ADC credentials capable of iam.serviceAccounts.signBlob.
// On GKE the chora-tenancy KSA federates via Workload Identity to a GSA with
// roles/iam.serviceAccountTokenCreator on itself; pass the explicit
// SignBytesFunc (iamcredentials.SignBlob) + GoogleAccessID, mirroring
// chora-creation's atom-media signer (keyless, no P12). Without an explicit
// signer the library auto-resolves via ADC (the default path).
package gcs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"

	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// ErrExportStorageNotWired is returned when the bucket is empty.
var ErrExportStorageNotWired = errors.New("gcs: transaction export bucket not set")

// defaultSignTimeout caps the SignBlob round-trip (well under upstream budgets).
const defaultSignTimeout = 10 * time.Second

// SignBytesFunc signs a V4 canonical payload (prod: iamcredentials.SignBlob).
type SignBytesFunc func(ctx context.Context, payload []byte) ([]byte, error)

// Option configures the ExportStorage adapter.
type Option func(*ExportStorage)

// WithGoogleAccessID pins the GSA email embedded as GoogleAccessID in the V4
// envelope. REQUIRED when WithSignBytesFunc is set.
func WithGoogleAccessID(id string) Option {
	return func(s *ExportStorage) { s.googleAccessID = strings.TrimSpace(id) }
}

// WithSignBytesFunc wires the explicit keyless V4 signer (prod).
func WithSignBytesFunc(fn SignBytesFunc) Option {
	return func(s *ExportStorage) { s.signBytes = fn }
}

// WithSignTimeout overrides the per-sign deadline.
func WithSignTimeout(d time.Duration) Option {
	return func(s *ExportStorage) {
		if d > 0 {
			s.signTimeout = d
		}
	}
}

// ExportStorage is the GCS-backed transactionledger.ExportStorage.
type ExportStorage struct {
	client         *storage.Client
	bucket         string
	googleAccessID string
	signBytes      SignBytesFunc
	signTimeout    time.Duration
}

// NewExportStorage constructs the adapter. Returns ErrExportStorageNotWired
// when bucket is empty so the caller can leave the worker disabled.
func NewExportStorage(client *storage.Client, bucket string, opts ...Option) (*ExportStorage, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, ErrExportStorageNotWired
	}
	if client == nil {
		return nil, errors.New("gcs: storage client required")
	}
	s := &ExportStorage{client: client, bucket: strings.TrimSpace(bucket), signTimeout: defaultSignTimeout}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

var _ tl.ExportStorage = (*ExportStorage)(nil)

// Upload writes data to object with the given content type.
func (s *ExportStorage) Upload(ctx context.Context, object string, data []byte, contentType string) error {
	w := s.client.Bucket(s.bucket).Object(object).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: write %s: %w", object, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs: close %s: %w", object, err)
	}
	return nil
}

// SignedURL mints a V4 GET signed URL for object, valid for ttl.
func (s *ExportStorage) SignedURL(ctx context.Context, object string, ttl time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().UTC().Add(ttl)
	opts := &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: expiresAt,
		Scheme:  storage.SigningSchemeV4,
	}
	// Explicit keyless path (prod): bypasses the library's ADC auto-resolve.
	if s.signBytes != nil {
		if s.googleAccessID == "" {
			return "", time.Time{}, errors.New("gcs: GoogleAccessID required when SignBytesFunc is set")
		}
		signCtx, cancel := context.WithTimeout(ctx, s.signTimeout)
		defer cancel()
		opts.GoogleAccessID = s.googleAccessID
		opts.SignBytes = func(p []byte) ([]byte, error) { return s.signBytes(signCtx, p) }
		url, err := storage.SignedURL(s.bucket, object, opts)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("gcs: SignedURL: %w", err)
		}
		return url, expiresAt, nil
	}
	// Default ADC path.
	url, err := s.client.Bucket(s.bucket).SignedURL(object, opts)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("gcs: SignedURL: %w", err)
	}
	return url, expiresAt, nil
}
