// tenant_repository.go — pgx-backed implementation of tenant.Repository.
//
// Schema reference: services/chora-tenancy/migrations/0001_initial.sql +
// 0004_phyllis_mvp.sql (slug + country + currency + stripe_customer_id +
// activated_at).
//
// SQL contract:
//
//   - Save: UPSERT on conflict(tenant_id). The slug UNIQUE index lives
//     under a partial WHERE (slug IS NOT NULL AND deleted_at IS NULL),
//     so collisions on slug surface as 23505 unique-violations and are
//     caught + wrapped as tenant.ErrTenantSlugExists.
//   - Get: SELECT WHERE tenant_id=$1 AND deleted_at IS NULL.
//   - FindBySlug: SELECT WHERE slug=$1 AND deleted_at IS NULL.
//
// Note: tenants is NOT tenant-scoped (it owns the tenant_id space) — no
// SET LOCAL chora.tenant_id is required for these queries. RLS-protected
// children (members, add_on_subscriptions, invoices, payment_methods)
// require the SET LOCAL contract; their adapters are added in M14+.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
)

// TenantRepository is the pgx-backed implementation of tenant.Repository.
type TenantRepository struct {
	q Querier
}

// NewTenantRepository constructs the production-wired repository.
func NewTenantRepository(querier *PgxPoolQuerier) *TenantRepository {
	return &TenantRepository{q: querier}
}

// NewTenantRepositoryWithQuerier accepts the lower-level Querier; used
// by unit tests with a stub.
func NewTenantRepositoryWithQuerier(q Querier) *TenantRepository {
	return &TenantRepository{q: q}
}

const tenantColumns = `tenant_id, name, COALESCE(parent_tenant_id::text, ''),
    branding, self_hosted, status::text,
    COALESCE(slug, ''), country, default_currency,
    COALESCE(stripe_customer_id, ''), activated_at,
    wizard_completed_at,
    created_at, updated_at, deleted_at`

// Save upserts the Tenant by ID.
func (r *TenantRepository) Save(ctx context.Context, t *tenant.Tenant) error {
	if t == nil {
		return errors.New("pg.TenantRepository.Save: nil tenant")
	}
	branding, err := json.Marshal(map[string]any{
		"primary_color_hex": t.Branding.PrimaryColorHex,
		"logo_url":          t.Branding.LogoURL,
		"custom_domain":     t.Branding.CustomDomain,
	})
	if err != nil {
		return fmt.Errorf("pg.Save: marshal branding: %w", err)
	}

	q := `
        INSERT INTO tenants (
            tenant_id, name, parent_tenant_id, branding, self_hosted,
            status, slug, country, default_currency, stripe_customer_id,
            activated_at, wizard_completed_at,
            created_at, updated_at, deleted_at
        )
        VALUES ($1, $2, NULLIF($3, '')::uuid, $4::jsonb, $5,
                $6::tenant_status,
                NULLIF($7, ''), $8, $9, NULLIF($10, ''),
                $11, $12,
                $13, $14, $15)
        ON CONFLICT (tenant_id) DO UPDATE SET
            name = EXCLUDED.name,
            parent_tenant_id = EXCLUDED.parent_tenant_id,
            branding = EXCLUDED.branding,
            status = EXCLUDED.status,
            slug = EXCLUDED.slug,
            country = EXCLUDED.country,
            default_currency = EXCLUDED.default_currency,
            stripe_customer_id = EXCLUDED.stripe_customer_id,
            activated_at = EXCLUDED.activated_at,
            wizard_completed_at = EXCLUDED.wizard_completed_at,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at
    `
	err = r.q.Exec(ctx, q,
		t.ID,
		t.DisplayName,
		t.ParentTenantID,
		string(branding),
		t.SelfHosted,
		string(t.Status),
		t.Slug,
		t.Country,
		t.DefaultCurrency,
		t.StripeCustomerID,
		t.ActivatedAt,
		t.WizardCompletedAt,
		t.CreatedAt,
		t.UpdatedAt,
		t.DeletedAt,
	)
	if err != nil {
		// Surface uniqueness violation on slug as a domain-level error.
		if isUniqueViolation(err, "idx_tenants_slug_unique") {
			return fmt.Errorf("%w: %s", tenant.ErrTenantSlugExists, t.Slug)
		}
		return err
	}
	return nil
}

// Get returns a Tenant by ID.
func (r *TenantRepository) Get(ctx context.Context, id string) (*tenant.Tenant, bool, error) {
	q := `
        SELECT ` + tenantColumns + `
        FROM tenants
        WHERE tenant_id = $1 AND deleted_at IS NULL
    `
	row := r.q.QueryRow(ctx, q, id)
	t, err := scanTenant(row)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("pg.Get: %w", err)
	}
	return t, true, nil
}

// FindBySlug returns the (single) non-deleted tenant with the given slug.
func (r *TenantRepository) FindBySlug(ctx context.Context, slug string) (*tenant.Tenant, bool, error) {
	q := `
        SELECT ` + tenantColumns + `
        FROM tenants
        WHERE slug = $1 AND deleted_at IS NULL
        LIMIT 1
    `
	row := r.q.QueryRow(ctx, q, slug)
	t, err := scanTenant(row)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("pg.FindBySlug: %w", err)
	}
	return t, true, nil
}

func scanTenant(row Row) (*tenant.Tenant, error) {
	var (
		t                 tenant.Tenant
		parentID          string
		branding          []byte
		status            string
		slug              string
		country           string
		currency          string
		stripeCustomer    string
		activatedAt       *time.Time
		wizardCompletedAt *time.Time
		deletedAt         *time.Time
	)
	err := row.Scan(
		&t.ID,
		&t.DisplayName,
		&parentID,
		&branding,
		&t.SelfHosted,
		&status,
		&slug,
		&country,
		&currency,
		&stripeCustomer,
		&activatedAt,
		&wizardCompletedAt,
		&t.CreatedAt,
		&t.UpdatedAt,
		&deletedAt,
	)
	if err != nil {
		return nil, err
	}
	if parentID != "" {
		t.ParentTenantID = parentID
	}
	if len(branding) > 0 {
		var b map[string]any
		if jerr := json.Unmarshal(branding, &b); jerr == nil {
			if v, ok := b["primary_color_hex"].(string); ok {
				t.Branding.PrimaryColorHex = v
			}
			if v, ok := b["logo_url"].(string); ok {
				t.Branding.LogoURL = v
			}
			if v, ok := b["custom_domain"].(string); ok {
				t.Branding.CustomDomain = v
			}
		}
	}
	t.Status = tenant.Status(status)
	t.Slug = slug
	t.Country = country
	t.DefaultCurrency = currency
	t.StripeCustomerID = stripeCustomer
	if activatedAt != nil {
		v := activatedAt.UTC()
		t.ActivatedAt = &v
	}
	if wizardCompletedAt != nil {
		v := wizardCompletedAt.UTC()
		t.WizardCompletedAt = &v
	}
	if deletedAt != nil {
		v := deletedAt.UTC()
		t.DeletedAt = &v
	}
	return &t, nil
}

// isUniqueViolation reports whether err is a Postgres 23505
// unique-violation tagged with the supplied constraint name. We match
// substring-of-error rather than the typed pgconn.PgError to keep the
// adapter dependency-light (the test seam doesn't model PgError).
func isUniqueViolation(err error, constraint string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") && strings.Contains(msg, constraint)
}

// Compile-time check.
var _ tenant.Repository = (*TenantRepository)(nil)
