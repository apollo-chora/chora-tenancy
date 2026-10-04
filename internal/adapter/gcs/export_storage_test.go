package gcs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	tenancygcs "github.com/apollo-chora/chora-tenancy/internal/adapter/gcs"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

// Upload + SignedURL hit live GCS + IAM signing and are deploy-verified (mirrors
// chora-creation's atom-media signer). These unit tests cover the constructor +
// option wiring + the port conformance.

func TestNewExportStorage_EmptyBucket_NotWired(t *testing.T) {
	if _, err := tenancygcs.NewExportStorage(&storage.Client{}, "  "); !errors.Is(err, tenancygcs.ErrExportStorageNotWired) {
		t.Fatalf("empty bucket err = %v; want ErrExportStorageNotWired", err)
	}
}

func TestNewExportStorage_NilClient_Errors(t *testing.T) {
	if _, err := tenancygcs.NewExportStorage(nil, "bucket"); err == nil {
		t.Fatal("nil client should error")
	}
}

func TestNewExportStorage_WithOptions(t *testing.T) {
	s, err := tenancygcs.NewExportStorage(
		&storage.Client{}, "exports-bucket",
		tenancygcs.WithGoogleAccessID("gsa@chora-489812.iam.gserviceaccount.com"),
		tenancygcs.WithSignTimeout(5*time.Second),
		tenancygcs.WithSignBytesFunc(func(context.Context, []byte) ([]byte, error) { return []byte("sig"), nil }),
	)
	if err != nil {
		t.Fatalf("NewExportStorage: %v", err)
	}
	// Conformance: the adapter satisfies the domain port.
	var _ tl.ExportStorage = s
}
