// tenant_save_error_test.go — CHO-2201 RED spec.
//
// When the TenantStore.Save port fails, the legacy /api/tenants create +
// set-parent handlers MUST surface a 5xx and NOT report a phantom success
// (a 201/200 for a tenant that was never persisted — the H+ admin UI would
// then show a tenant the DB never accepted). Fake store fails every Save.
package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	httpapi "github.com/apollo-chora/chora-tenancy/internal/adapter/http"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/inmem"
	domain "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

// saveErrTenantStore satisfies httpapi.TenantStore but fails every Save.
// Get/List delegate to a real in-memory repo so the set-parent path can
// first read a seeded tenant before the failing Save.
type saveErrTenantStore struct {
	inner *inmem.TenantRepo
	err   error
}

func (s *saveErrTenantStore) Save(ctx context.Context, t *domain.Tenant) error { return s.err }
func (s *saveErrTenantStore) Get(id string) (*domain.Tenant, bool)             { return s.inner.Get(id) }
func (s *saveErrTenantStore) List(offset, limit int) ([]*domain.Tenant, int) {
	return s.inner.List(offset, limit)
}

func newServerWithTenantStore(ts httpapi.TenantStore) http.Handler {
	return httpapi.NewServer(httpapi.Deps{
		Tenants:        ts,
		AddOnCatalog:   domain.NewAddOnCatalog(),
		Entitlements:   domain.NewEntitlementRegistry(),
		Invoices:       inmem.NewInvoiceRepo(),
		PaymentMethods: inmem.NewPaymentMethodRepo(),
	})
}

func TestCreateTenant_PersistFailure_Returns5xx(t *testing.T) {
	t.Parallel()
	store := &saveErrTenantStore{inner: inmem.NewTenantRepo(), err: errors.New("insert failed")}
	srv := newServerWithTenantStore(store)
	rec, body := do(t, srv, "POST", "/api/tenants", map[string]interface{}{
		"display_name": "Acme School",
	}, true)
	if rec.Code < 500 {
		t.Fatalf("expected 5xx when persistence fails, got %d (phantom success — CHO-2201); body=%v", rec.Code, body)
	}
}

func TestSetParent_PersistFailure_Returns5xx(t *testing.T) {
	t.Parallel()
	inner := inmem.NewTenantRepo()
	seed, err := domain.NewTenant("Seed", false, "")
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	if err := inner.Save(context.Background(), seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}
	store := &saveErrTenantStore{inner: inner, err: errors.New("upsert failed")}
	srv := newServerWithTenantStore(store)
	rec, body := do(t, srv, "POST", "/api/tenants/"+seed.ID+"/parent", map[string]interface{}{
		"parent_tenant_id": "01970000-0000-7000-8000-0000000000ff",
	}, true)
	if rec.Code < 500 {
		t.Fatalf("expected 5xx when persistence fails, got %d (phantom success — CHO-2201); body=%v", rec.Code, body)
	}
}
