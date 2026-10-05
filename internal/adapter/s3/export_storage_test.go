package s3_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	s3adapter "github.com/apollo-chora/chora-tenancy/internal/adapter/s3"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// fakeStore records Put / PresignGet calls and can inject failures.
type fakeStore struct {
	key         string
	data        []byte
	contentType string
	ttl         time.Duration
	putErr      error
	signErr     error
}

func (f *fakeStore) Put(_ context.Context, key string, r io.Reader, size int64, contentType string) error {
	if f.putErr != nil {
		return f.putErr
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.key, f.data, f.contentType = key, b, contentType
	if size != int64(len(b)) {
		return errors.New("size mismatch")
	}
	return nil
}

func (f *fakeStore) PresignGet(_ context.Context, key string, ttl time.Duration) (string, error) {
	if f.signErr != nil {
		return "", f.signErr
	}
	f.key, f.ttl = key, ttl
	return "https://minio.local/bucket/" + key + "?sig=x", nil
}

func TestNewExportStorage_EmptyBucket_NotWired(t *testing.T) {
	if _, err := s3adapter.NewExportStorage("  "); !errors.Is(err, s3adapter.ErrExportStorageNotWired) {
		t.Fatalf("empty bucket err = %v; want ErrExportStorageNotWired", err)
	}
}

func TestNewExportStorage_WithStore_SatisfiesPort(t *testing.T) {
	s, err := s3adapter.NewExportStorage("exports", s3adapter.WithStore(&fakeStore{}))
	if err != nil {
		t.Fatalf("NewExportStorage: %v", err)
	}
	var _ tl.ExportStorage = s
}

func TestUpload_DelegatesToPut(t *testing.T) {
	store := &fakeStore{}
	s, err := s3adapter.NewExportStorage("exports", s3adapter.WithStore(store))
	if err != nil {
		t.Fatalf("NewExportStorage: %v", err)
	}
	if err := s.Upload(context.Background(), "transaction-exports/platform/j1.csv", []byte("a,b\n1,2\n"), "text/csv"); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if store.key != "transaction-exports/platform/j1.csv" {
		t.Errorf("key = %q", store.key)
	}
	if string(store.data) != "a,b\n1,2\n" {
		t.Errorf("data = %q", store.data)
	}
	if store.contentType != "text/csv" {
		t.Errorf("contentType = %q", store.contentType)
	}
}

func TestUpload_PropagatesStoreError(t *testing.T) {
	store := &fakeStore{putErr: errors.New("s3 down")}
	s, _ := s3adapter.NewExportStorage("exports", s3adapter.WithStore(store))
	err := s.Upload(context.Background(), "k", []byte("x"), "text/csv")
	if err == nil || !strings.Contains(err.Error(), "s3 down") {
		t.Fatalf("expected wrapped store error, got %v", err)
	}
}

func TestSignedURL_DelegatesToPresignGet(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{}
	s, _ := s3adapter.NewExportStorage("exports", s3adapter.WithStore(store), s3adapter.WithNow(func() time.Time { return now }))
	url, expiresAt, err := s.SignedURL(context.Background(), "transaction-exports/platform/j1.csv", time.Hour)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	if !strings.Contains(url, "transaction-exports/platform/j1.csv") {
		t.Errorf("url = %q", url)
	}
	if store.ttl != time.Hour {
		t.Errorf("ttl = %s", store.ttl)
	}
	if !expiresAt.Equal(now.Add(time.Hour)) {
		t.Errorf("expiresAt = %s; want %s", expiresAt, now.Add(time.Hour))
	}
}

func TestSignedURL_RejectsNonPositiveTTL(t *testing.T) {
	s, _ := s3adapter.NewExportStorage("exports", s3adapter.WithStore(&fakeStore{}))
	if _, _, err := s.SignedURL(context.Background(), "k", 0); err == nil {
		t.Fatal("ttl=0 should error")
	}
}

func TestSignedURL_PropagatesStoreError(t *testing.T) {
	store := &fakeStore{signErr: errors.New("sign failed")}
	s, _ := s3adapter.NewExportStorage("exports", s3adapter.WithStore(store))
	if _, _, err := s.SignedURL(context.Background(), "k", time.Hour); err == nil || !strings.Contains(err.Error(), "sign failed") {
		t.Fatalf("expected wrapped sign error, got %v", err)
	}
}
