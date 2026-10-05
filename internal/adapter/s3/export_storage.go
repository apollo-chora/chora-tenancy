// Package s3 adapts the transactionledger.ExportStorage port to S3-compatible
// object storage for the ADR-205 async transaction export (B5 / CHO-1941). It
// writes the built CSV/JSON object and mints a presigned GET download URL.
//
// Wiring: cmd/server/main.go constructs NewExportStorage(bucket) where the
// bucket comes from env CHORA_TENANCY_EXPORT_BUCKET. Empty env →
// ErrExportStorageNotWired so main.go leaves the worker disabled (export jobs
// stay PENDING; the BFF reports them).
//
// Endpoint + credentials come from objectstore.ConfigFromEnv
// (S3_ENDPOINT, S3_ACCESS_KEY_ID/S3_ACCESS_KEY,
// S3_SECRET_ACCESS_KEY/S3_SECRET_KEY, S3_REGION, S3_FORCE_PATH_STYLE), so the
// same adapter targets MinIO locally and any S3-compatible endpoint in
// production. A presigned URL replaces the previous GCS V4 signed URL.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/objectstore"

	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// ErrExportStorageNotWired is returned when the bucket is empty.
var ErrExportStorageNotWired = errors.New("s3: transaction export bucket not set")

// ObjectStore is the subset of *objectstore.Store the adapter depends on.
// Exported so tests (and alternative wiring) can inject an implementation.
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// Option configures the ExportStorage adapter.
type Option func(*ExportStorage)

// WithStore injects a prebuilt object store (tests / custom wiring).
func WithStore(store ObjectStore) Option {
	return func(s *ExportStorage) { s.store = store }
}

// WithNow overrides the clock used to stamp the URL expiry (tests).
func WithNow(now func() time.Time) Option {
	return func(s *ExportStorage) {
		if now != nil {
			s.now = now
		}
	}
}

// ExportStorage is the S3-compatible transactionledger.ExportStorage.
type ExportStorage struct {
	store ObjectStore
	now   func() time.Time
}

// NewExportStorage constructs the adapter. Returns ErrExportStorageNotWired
// when bucket is empty so the caller can leave the worker disabled. The
// object store is built from objectstore.ConfigFromEnv unless WithStore
// supplies one.
func NewExportStorage(bucket string, opts ...Option) (*ExportStorage, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, ErrExportStorageNotWired
	}
	s := &ExportStorage{now: func() time.Time { return time.Now().UTC() }}
	for _, o := range opts {
		o(s)
	}
	if s.store == nil {
		cfg := objectstore.ConfigFromEnv()
		cfg.Bucket = strings.TrimSpace(bucket)
		store, err := objectstore.New(cfg)
		if err != nil {
			return nil, fmt.Errorf("s3: object store: %w", err)
		}
		s.store = store
	}
	return s, nil
}

var _ tl.ExportStorage = (*ExportStorage)(nil)

// Upload writes data to object with the given content type.
func (s *ExportStorage) Upload(ctx context.Context, object string, data []byte, contentType string) error {
	if s == nil || s.store == nil {
		return errors.New("s3: export storage not initialised")
	}
	if err := s.store.Put(ctx, object, bytes.NewReader(data), int64(len(data)), contentType); err != nil {
		return fmt.Errorf("s3: upload %s: %w", object, err)
	}
	return nil
}

// SignedURL mints a presigned GET URL for object, valid for ttl.
func (s *ExportStorage) SignedURL(ctx context.Context, object string, ttl time.Duration) (string, time.Time, error) {
	if s == nil || s.store == nil {
		return "", time.Time{}, errors.New("s3: export storage not initialised")
	}
	if ttl <= 0 {
		return "", time.Time{}, fmt.Errorf("s3: signed URL ttl must be positive, got %s", ttl)
	}
	url, err := s.store.PresignGet(ctx, object, ttl)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("s3: signed URL: %w", err)
	}
	return url, s.now().Add(ttl), nil
}
