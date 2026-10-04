// legacy_tenant_save_error_test.go — CHO-2201 RED spec.
//
// The httpapi.TenantStore.Save port was void (no ctx, no error) and the pg
// adapter did `_ = s.q.Exec(context.Background(), …)` — a failed INSERT/UPSERT
// (RLS denial, constraint, connection blip) was discarded and the caller was
// told the write succeeded. This spec pins that Save must RETURN the Exec
// error so the handler can refuse to report a phantom success.
package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	tenancy "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
)

func TestLegacyTenantStore_Save_ReturnsExecError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("SQLSTATE 23505 unique_violation")
	q := &stubQueryRunner{execErr: wantErr}
	store := pg.NewLegacyTenantStore(q)
	tn, err := tenancy.NewTenant("New School", false, "")
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	gotErr := store.Save(context.Background(), tn)
	if gotErr == nil {
		t.Fatalf("Save must RETURN the Exec error, got nil (swallowed — CHO-2201)")
	}
	if !errors.Is(gotErr, wantErr) {
		t.Errorf("Save error = %v, want a wrap of %v", gotErr, wantErr)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected exactly 1 Exec, got %d", len(q.execCalls))
	}
}

func TestLegacyTenantStore_Save_HappyPath_NoError(t *testing.T) {
	t.Parallel()
	q := &stubQueryRunner{} // execErr nil
	store := pg.NewLegacyTenantStore(q)
	tn, err := tenancy.NewTenant("Good School", false, "")
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	if err := store.Save(context.Background(), tn); err != nil {
		t.Fatalf("Save on a healthy querier must return nil, got %v", err)
	}
}
