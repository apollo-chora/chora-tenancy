// export_storage_branch_test.go — branch coverage for the GCS V4
// signed-URL keyless path (SignBytesFunc + GoogleAccessID) and its
// validation failure modes. The signed-URL computation is pure once the
// signer is provided, so no live GCS is needed.
package gcs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	gcsadapter "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/gcs"
)

func TestSignedURL_MissingGoogleAccessID_Errors(t *testing.T) {
	t.Parallel()
	// signBytes set without GoogleAccessID → loud error before signing.
	s, err := gcsadapter.NewExportStorage(
		&storage.Client{},
		"chora-test-bucket",
		gcsadapter.WithSignBytesFunc(func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("must not be called")
		}),
	)
	if err != nil {
		t.Fatalf("NewExportStorage: %v", err)
	}
	_, _, err = s.SignedURL(context.Background(), "exports/x.csv", time.Hour)
	if err == nil || !strings.Contains(err.Error(), "GoogleAccessID") {
		t.Fatalf("expected GoogleAccessID error, got %v", err)
	}
}

func TestSignedURL_KeylessPath_MintsURL(t *testing.T) {
	t.Parallel()
	s, err := gcsadapter.NewExportStorage(
		&storage.Client{},
		"chora-test-bucket",
		gcsadapter.WithGoogleAccessID("svc@project.iam.gserviceaccount.com"),
		gcsadapter.WithSignBytesFunc(func(_ context.Context, _ []byte) ([]byte, error) {
			return make([]byte, 32), nil // zero signature is a valid base64 input
		}),
		gcsadapter.WithSignTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewExportStorage: %v", err)
	}
	url, expiresAt, err := s.SignedURL(context.Background(), "exports/2026-07.csv", time.Hour)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	if !strings.HasPrefix(url, "https://") || !strings.Contains(url, "chora-test-bucket") {
		t.Fatalf("unexpected signed URL %q", url)
	}
	if expiresAt.Before(time.Now().UTC().Add(59 * time.Minute)) {
		t.Fatalf("expiry looks wrong: %v", expiresAt)
	}
	// Signer failure propagates.
	s2, _ := gcsadapter.NewExportStorage(
		&storage.Client{},
		"chora-test-bucket",
		gcsadapter.WithGoogleAccessID("a@b.iam.gserviceaccount.com"),
		gcsadapter.WithSignBytesFunc(func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("sign blob boom")
		}),
	)
	if _, _, err := s2.SignedURL(context.Background(), "exports/x.csv", time.Hour); err == nil || !strings.Contains(err.Error(), "SignedURL") {
		t.Fatalf("expected signer error propagation, got %v", err)
	}
}
