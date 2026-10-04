// Package pg_test exercises the pgx-backed Tenant repository adapter.
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// stub Querier shared with tenancy adapter unit tests.
type stubExecCall struct {
	sql  string
	args []any
}

type stubRowCall struct {
	sql  string
	args []any
}

type stubQuerier struct {
	execCalls    []stubExecCall
	rowCalls     []stubRowCall
	rowResponses []func(dest ...any) error
}

func (s *stubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execCalls = append(s.execCalls, stubExecCall{sql: sql, args: args})
	return nil
}
func (s *stubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	s.rowCalls = append(s.rowCalls, stubRowCall{sql: sql, args: args})
	if len(s.rowResponses) == 0 {
		return &stubRow{scanFn: func(dest ...any) error { return pg.ErrNoRows }}
	}
	r := s.rowResponses[0]
	s.rowResponses = s.rowResponses[1:]
	return &stubRow{scanFn: r}
}

type stubRow struct {
	scanFn func(dest ...any) error
}

func (r *stubRow) Scan(dest ...any) error { return r.scanFn(dest...) }

// ----------------------------------------------------------------------------

func TestTenantRepository_Save_UpsertsByID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewTenantRepositoryWithQuerier(q)

	tt, err := tenant.NewLightTenant(tenant.LightTenantInput{
		DisplayName: "Phyllis Coaching",
		OwnerGCID:   "01970000-0000-7000-8000-aaaaaaaaaaaa",
		Slug:        "phyllis-coaching",
	})
	if err != nil {
		t.Fatalf("NewLightTenant: %v", err)
	}
	if err := r.Save(context.Background(), tt); err != nil {
		t.Fatalf("save: %v", err)
	}
	if len(q.execCalls) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(q.execCalls))
	}
	if !strings.Contains(q.execCalls[0].sql, "INSERT INTO tenants") {
		t.Errorf("expected INSERT INTO tenants, got %q", q.execCalls[0].sql)
	}
	if !strings.Contains(q.execCalls[0].sql, "ON CONFLICT") {
		t.Errorf("expected UPSERT, got %q", q.execCalls[0].sql)
	}
}

func TestTenantRepository_Get_HappyPath(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQuerier{
		rowResponses: []func(dest ...any) error{
			func(dest ...any) error {
				// Column order (CHO-1682 + CHO-1693): tenant_id, name,
				// parent_tenant_id, branding, self_hosted, status, slug,
				// country, default_currency, stripe_customer_id,
				// activated_at, wizard_completed_at, created_at,
				// updated_at, deleted_at. wizard_completed_at sits
				// between activated_at and created_at and is **time.Time
				// (nullable on a fresh tenant); the stub leaves it nil
				// here to exercise the omit-when-null branch of the
				// production scanner.
				*dest[0].(*string) = "01970000-0000-7000-8000-aaaaaaaaaaaa"
				*dest[1].(*string) = "Phyllis Coaching"
				if v, ok := dest[2].(*string); ok {
					*v = ""
				}
				*dest[3].(*[]byte) = []byte(`{}`)
				*dest[4].(*bool) = false
				*dest[5].(*string) = "active"
				*dest[6].(*string) = "phyllis-coaching"
				*dest[7].(*string) = "SG"
				*dest[8].(*string) = "SGD"
				if v, ok := dest[9].(*string); ok {
					*v = ""
				}
				if v, ok := dest[10].(**time.Time); ok {
					_ = v
				}
				if v, ok := dest[11].(**time.Time); ok {
					_ = v
				}
				*dest[12].(*time.Time) = now
				*dest[13].(*time.Time) = now
				if v, ok := dest[14].(**time.Time); ok {
					_ = v
				}
				return nil
			},
		},
	}
	r := pg.NewTenantRepositoryWithQuerier(q)

	got, ok, err := r.Get(context.Background(), "01970000-0000-7000-8000-aaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if got.DisplayName != "Phyllis Coaching" {
		t.Errorf("DisplayName = %q", got.DisplayName)
	}
	if got.Slug != "phyllis-coaching" {
		t.Errorf("Slug = %q", got.Slug)
	}
	if got.Country != "SG" {
		t.Errorf("Country = %q", got.Country)
	}
}

func TestTenantRepository_Get_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewTenantRepositoryWithQuerier(q)
	_, ok, err := r.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestTenantRepository_FindBySlug_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewTenantRepositoryWithQuerier(q)
	_, ok, err := r.FindBySlug(context.Background(), "missing")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestTenantRepository_FindBySlug_HappyPath(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQuerier{
		rowResponses: []func(dest ...any) error{
			func(dest ...any) error {
				*dest[0].(*string) = "01970000-0000-7000-8000-aaaaaaaaaaaa"
				*dest[1].(*string) = "Phyllis"
				if v, ok := dest[2].(*string); ok {
					*v = ""
				}
				*dest[3].(*[]byte) = []byte(`{"primary_color_hex":"#123456","logo_url":"https://example/logo.png","custom_domain":"academy.example"}`)
				*dest[4].(*bool) = true
				*dest[5].(*string) = "active"
				*dest[6].(*string) = "phyllis"
				*dest[7].(*string) = "SG"
				*dest[8].(*string) = "SGD"
				if v, ok := dest[9].(*string); ok {
					*v = "cus_phyllis"
				}
				if v, ok := dest[10].(**time.Time); ok {
					_ = v
				}
				// CHO-1693 — wizard_completed_at slot. Set non-nil to
				// pin the post-CHO-1682 column position; the test
				// assertion below verifies the round-trip.
				if v, ok := dest[11].(**time.Time); ok {
					stamp := now
					*v = &stamp
				}
				*dest[12].(*time.Time) = now
				*dest[13].(*time.Time) = now
				if v, ok := dest[14].(**time.Time); ok {
					_ = v
				}
				return nil
			},
		},
	}
	r := pg.NewTenantRepositoryWithQuerier(q)
	got, ok, err := r.FindBySlug(context.Background(), "phyllis")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if got.SelfHosted != true {
		t.Errorf("SelfHosted: %v", got.SelfHosted)
	}
	if got.StripeCustomerID != "cus_phyllis" {
		t.Errorf("StripeCustomerID: %q", got.StripeCustomerID)
	}
	if got.Branding.PrimaryColorHex != "#123456" {
		t.Errorf("Branding.PrimaryColorHex: %q", got.Branding.PrimaryColorHex)
	}
	// CHO-1693 — wizard_completed_at is the post-CHO-1682 slot at
	// dest[11]. Verify the round-trip carries a non-nil timestamp from
	// the scanner back onto the aggregate. Pins the stub-vs-production
	// column-order alignment so a future column addition (which would
	// shift `wizard_completed_at` to a different index) fails this
	// assertion instead of silently re-introducing the panic.
	if got.WizardCompletedAt == nil {
		t.Errorf("WizardCompletedAt: got nil; want non-nil now-fixture")
	} else if !got.WizardCompletedAt.Equal(now) {
		t.Errorf("WizardCompletedAt: got %v; want %v", *got.WizardCompletedAt, now)
	}
}
