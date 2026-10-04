// legacy_tenancy_repository.go — pgx adapters backing the legacy /api/*
// HTTP layer (A7, docs/m13/handoff-fe-to-be-service-2026-05-14.md §A7).
//
// WHY THIS FILE EXISTS
//
//	The A6-wired gateway routes `GET /api/tenants/{id}` and
//	`GET /api/feature-flags` (→ chora-tenancy `GET /api/tenants/{id}/
//	entitlements`) reached chora-tenancy but returned `404 "tenant not
//	found"` / empty `{"items":[]}`. The cause was NOT missing data — the
//	Phyllis demo tenant rows EXIST in the chora_tenancy `tenants` table
//	(verified live, 2026-05-14). The cause was that `httpapi.Deps` was
//	wired to `inmem.NewTenantRepo()` + `tenancy.NewEntitlementRegistry()`
//	— in-memory stores that start EMPTY on every pod. Per the no-stubs /
//	no-debts directive an in-mem stub on a demo-critical read path is the
//	exact debt forbidden; these adapters wire the legacy handlers to the
//	real Postgres tables instead.
//
// PATTERN — mirrors the D0.4 chora-delivery CataloguePort rewire: a
// hexagonal port (httpapi.TenantStore / AddOnStore / EntitlementStore) +
// a pgx adapter (these Legacy* types); the in-memory adapters stay as the
// dev / unit-test fallback. cmd/server wires whichever the pgx pool
// availability dictates.
//
// SCHEMA — chora_tenancy migrations 0001_initial.sql + 0002_addon_lifecycle.sql:
//   - `tenants`              : NOT RLS-protected (the registry owns the
//     tenant_id space; H+ admin reads cross-tenant). Plain Querier.
//   - `add_ons`              : NOT RLS-protected (flat marketplace catalog).
//     Plain Querier.
//   - `add_on_subscriptions` : RLS-protected (`tenant_isolation` policy).
//     The chora_tenancy_app_rw role is NOBYPASSRLS (migration 0013), so
//     every read/write MUST run inside a `SET LOCAL chora.tenant_id`
//     transaction — the TxQuerier seam (runtime.go).
//
// The adapters traffic in the `tenancy` domain types (tenancy.Tenant,
// tenancy.TenantEntitlement, tenancy.AddOn) so the legacy handler DTOs in
// handlers.go are unchanged. NOTE: the separate v2/v1-light path
// (V2Deps + the `tenant` domain + pg.TenantRepository) is untouched — this
// file is scoped strictly to the legacy `/api/*` family the gateway hits.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	tenancy "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
)

// -----------------------------------------------------------------------------
// LegacyTenantStore — tenants table (NOT RLS-protected)
// -----------------------------------------------------------------------------

// LegacyTenantStore is the pgx adapter satisfying httpapi.TenantStore.
//
// Reads/writes the `tenants` table. `tenants` is NOT RLS-protected (the
// registry owns the tenant_id space), so a plain QueryRunner is enough —
// no SET LOCAL chora.tenant_id contract.
type LegacyTenantStore struct {
	q QueryRunner
}

// NewLegacyTenantStore constructs the production-wired adapter.
func NewLegacyTenantStore(q QueryRunner) *LegacyTenantStore {
	return &LegacyTenantStore{q: q}
}

// legacyTenantColumns is the SELECT projection for the tenancy.Tenant
// aggregate as the legacy handlers shape it. The `tenants` table also
// carries slug/country/default_currency/stripe_customer_id/activated_at
// (migration 0004) but the legacy tenancy.Tenant struct does not model
// them — they belong to the v1-light `tenant` domain. Projecting only the
// legacy columns keeps the scan signature aligned with tenancy.Tenant.
const legacyTenantColumns = `tenant_id, name,
    COALESCE(parent_tenant_id::text, ''),
    branding, self_hosted, status::text,
    created_at, updated_at, deleted_at`

// Save upserts a tenant by ID. The legacy `POST /api/tenants` +
// `POST /api/tenants/{id}/parent` handlers call this; idempotent on
// conflict(tenant_id).
//
// The httpapi.TenantStore port carries the request ctx and RETURNS the
// persistence error (CHO-2201): a failed INSERT/UPSERT (RLS denial,
// constraint violation, connection blip) is surfaced to the handler so it
// can refuse to report a phantom success. The branding marshal error is
// likewise returned rather than silently degraded to `{}`.
func (s *LegacyTenantStore) Save(ctx context.Context, t *tenancy.Tenant) error {
	if t == nil {
		return nil
	}
	branding, err := json.Marshal(t.BrandingConfig)
	if err != nil {
		return fmt.Errorf("pg.LegacyTenantStore.Save(%s): marshal branding: %w", t.ID, err)
	}
	const q = `
        INSERT INTO tenants (
            tenant_id, name, parent_tenant_id, branding, self_hosted,
            status, created_at, updated_at, deleted_at
        )
        VALUES ($1, $2, NULLIF($3, '')::uuid, $4::jsonb, $5,
                $6::tenant_status, $7, $8, $9)
        ON CONFLICT (tenant_id) DO UPDATE SET
            name = EXCLUDED.name,
            parent_tenant_id = EXCLUDED.parent_tenant_id,
            branding = EXCLUDED.branding,
            self_hosted = EXCLUDED.self_hosted,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at
    `
	if err := s.q.Exec(ctx, q,
		t.ID,
		t.DisplayName,
		t.ParentTenantID,
		string(branding),
		t.SelfHosted,
		string(t.Status),
		t.CreatedAt,
		t.UpdatedAt,
		t.DeletedAt,
	); err != nil {
		return fmt.Errorf("pg.LegacyTenantStore.Save(%s): %w", t.ID, err)
	}
	return nil
}

// Get returns the (non-deleted) tenant by ID. The gateway-facing
// `GET /api/tenants/{id}` route depends on this — for the Phyllis demo
// tenants `11111111-…` (mtm-singapore) + `22222222-…` (chen-coaching) it
// now returns ok=true from the real `tenants` table.
func (s *LegacyTenantStore) Get(id string) (*tenancy.Tenant, bool) {
	const q = `
        SELECT ` + legacyTenantColumns + `
        FROM tenants
        WHERE tenant_id = $1 AND deleted_at IS NULL
    `
	row := s.q.QueryRow(context.Background(), q, id)
	t, err := scanLegacyTenant(row)
	if err != nil {
		// ErrNoRows → not found; any other error is treated as not-found
		// too (the port has no error channel) but is distinguishable in
		// logs from the wrapped message.
		if !errors.Is(err, ErrNoRows) {
			fmt.Printf("pg.LegacyTenantStore.Get(%s): %v\n", id, err)
		}
		return nil, false
	}
	return t, true
}

// List returns non-closed tenants for the H+ admin marketplace,
// 0-indexed offset+limit pagination + the total pre-pagination count.
func (s *LegacyTenantStore) List(offset, limit int) ([]*tenancy.Tenant, int) {
	const q = `
        SELECT ` + legacyTenantColumns + `
        FROM tenants
        WHERE deleted_at IS NULL AND status <> 'closed'
        ORDER BY tenant_id ASC
    `
	rows, err := s.q.Query(context.Background(), q)
	if err != nil {
		fmt.Printf("pg.LegacyTenantStore.List: %v\n", err)
		return []*tenancy.Tenant{}, 0
	}
	defer rows.Close()

	all := make([]*tenancy.Tenant, 0, 16)
	for rows.Next() {
		t, scanErr := scanLegacyTenant(rows)
		if scanErr != nil {
			fmt.Printf("pg.LegacyTenantStore.List scan: %v\n", scanErr)
			return []*tenancy.Tenant{}, 0
		}
		all = append(all, t)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("pg.LegacyTenantStore.List rows.Err: %v\n", err)
		return []*tenancy.Tenant{}, 0
	}

	total := len(all)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if limit <= 0 || end > total {
		end = total
	}
	return all[offset:end], total
}

// scanLegacyTenant scans one row of legacyTenantColumns into a
// tenancy.Tenant. scanner is satisfied by both Row and Rows.
func scanLegacyTenant(scanner interface{ Scan(...any) error }) (*tenancy.Tenant, error) {
	var (
		t         tenancy.Tenant
		parentID  string
		branding  []byte
		status    string
		deletedAt *time.Time
	)
	if err := scanner.Scan(
		&t.ID,
		&t.DisplayName,
		&parentID,
		&branding,
		&t.SelfHosted,
		&status,
		&t.CreatedAt,
		&t.UpdatedAt,
		&deletedAt,
	); err != nil {
		return nil, err
	}
	t.ParentTenantID = parentID
	t.Status = tenancy.TenantStatus(status)
	t.BrandingConfig = map[string]interface{}{}
	if len(branding) > 0 {
		var b map[string]interface{}
		if err := json.Unmarshal(branding, &b); err == nil && b != nil {
			t.BrandingConfig = b
		}
	}
	if deletedAt != nil {
		v := deletedAt.UTC()
		t.DeletedAt = &v
	}
	t.CreatedAt = t.CreatedAt.UTC()
	t.UpdatedAt = t.UpdatedAt.UTC()
	return &t, nil
}

// -----------------------------------------------------------------------------
// LegacyAddOnStore — add_ons table (NOT RLS-protected)
// -----------------------------------------------------------------------------

// LegacyAddOnStore is the pgx adapter satisfying httpapi.AddOnStore.
//
// Reads/writes the `add_ons` flat catalog. NOT RLS-protected — plain
// QueryRunner.
type LegacyAddOnStore struct {
	q QueryRunner
}

// NewLegacyAddOnStore constructs the production-wired adapter.
func NewLegacyAddOnStore(q QueryRunner) *LegacyAddOnStore {
	return &LegacyAddOnStore{q: q}
}

// legacyAddOnColumns is the SELECT projection for the tenancy.AddOn
// aggregate. `add_ons` also has currency/capabilities/public/
// self_hosted_only — the legacy tenancy.AddOn struct models
// IncludedCapabilities but the legacy handler DTO does not surface it, so
// the scan keeps to the four fields tenancy.AddOn carries plus created_at.
const legacyAddOnColumns = `add_on_id, name, description, monthly_price_cents, created_at`

// Register inserts an AddOn into the catalog. Idempotent on conflict —
// the `add_ons` table has UNIQUE(name); ON CONFLICT (name) keeps re-seed
// safe.
func (s *LegacyAddOnStore) Register(a *tenancy.AddOn) {
	if a == nil {
		return
	}
	caps := a.IncludedCapabilities
	if caps == nil {
		caps = []string{}
	}
	const q = `
        INSERT INTO add_ons (
            add_on_id, name, description, monthly_price_cents,
            currency, capabilities, created_at
        )
        VALUES ($1, $2, $3, $4, 'SGD', $5, $6)
        ON CONFLICT (name) DO UPDATE SET
            description = EXCLUDED.description,
            monthly_price_cents = EXCLUDED.monthly_price_cents,
            capabilities = EXCLUDED.capabilities
    `
	_ = s.q.Exec(context.Background(), q,
		a.ID,
		a.Name,
		a.Description,
		a.MonthlyPriceCents,
		caps,
		a.CreatedAt,
	)
}

// Get returns an AddOn by ID.
func (s *LegacyAddOnStore) Get(id string) (*tenancy.AddOn, bool) {
	const q = `
        SELECT ` + legacyAddOnColumns + `
        FROM add_ons
        WHERE add_on_id = $1
    `
	row := s.q.QueryRow(context.Background(), q, id)
	a, err := scanLegacyAddOn(row)
	if err != nil {
		if !errors.Is(err, ErrNoRows) {
			fmt.Printf("pg.LegacyAddOnStore.Get(%s): %v\n", id, err)
		}
		return nil, false
	}
	return a, true
}

// List returns all add-ons sorted by ID (UUIDv7 ⇒ creation order).
func (s *LegacyAddOnStore) List() []*tenancy.AddOn {
	const q = `
        SELECT ` + legacyAddOnColumns + `
        FROM add_ons
        ORDER BY add_on_id ASC
    `
	rows, err := s.q.Query(context.Background(), q)
	if err != nil {
		fmt.Printf("pg.LegacyAddOnStore.List: %v\n", err)
		return []*tenancy.AddOn{}
	}
	defer rows.Close()

	out := make([]*tenancy.AddOn, 0, 16)
	for rows.Next() {
		a, scanErr := scanLegacyAddOn(rows)
		if scanErr != nil {
			fmt.Printf("pg.LegacyAddOnStore.List scan: %v\n", scanErr)
			return []*tenancy.AddOn{}
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("pg.LegacyAddOnStore.List rows.Err: %v\n", err)
		return []*tenancy.AddOn{}
	}
	return out
}

func scanLegacyAddOn(scanner interface{ Scan(...any) error }) (*tenancy.AddOn, error) {
	var (
		a         tenancy.AddOn
		createdAt time.Time
	)
	if err := scanner.Scan(
		&a.ID,
		&a.Name,
		&a.Description,
		&a.MonthlyPriceCents,
		&createdAt,
	); err != nil {
		return nil, err
	}
	a.CreatedAt = createdAt.UTC()
	a.IncludedCapabilities = []string{}
	return &a, nil
}

// -----------------------------------------------------------------------------
// LegacyEntitlementStore — add_on_subscriptions table (RLS-protected)
// -----------------------------------------------------------------------------

// LegacyEntitlementStore is the pgx adapter satisfying
// httpapi.EntitlementStore.
//
// `add_on_subscriptions` carries the `tenant_isolation` RLS policy and the
// chora_tenancy_app_rw role is NOBYPASSRLS — so EVERY read/write runs
// inside a `SET LOCAL chora.tenant_id` transaction via the TxQuerier seam.
// This is the exact bug class A7 was filed for: an RLS-protected table
// read without the tenant GUC returns zero rows even when the data exists.
type LegacyEntitlementStore struct {
	q TxQuerier
}

// NewLegacyEntitlementStore constructs the production-wired adapter.
func NewLegacyEntitlementStore(q TxQuerier) *LegacyEntitlementStore {
	return &LegacyEntitlementStore{q: q}
}

// legacyEntitlementColumns is the canonical 9-column entitlement
// projection scanned by scanLegacyEntitlement. The trailing column is the
// add-on's stable machine code (add_ons.code, migration 0021 — CHO-1717
// §4.6 addon_code bridge); the write paths (Subscribe / Cancel) source it
// via a correlated scalar subquery because RETURNING cannot JOIN, while
// the read path (ListActiveByTenant) uses the s./a. qualified JOIN form
// below. Keep both lists + the scan in lockstep.
const legacyEntitlementColumns = `subscription_id, tenant_id, add_on_id,
    monthly_price_cents_snap, status::text, started_at, updated_at,
    cancelled_at,
    (SELECT ao.code FROM add_ons ao
      WHERE ao.add_on_id = add_on_subscriptions.add_on_id)`

// legacyEntitlementJoinColumns is the same 9-column projection for reads
// that FROM add_on_subscriptions s LEFT JOIN add_ons a.
const legacyEntitlementJoinColumns = `s.subscription_id, s.tenant_id, s.add_on_id,
    s.monthly_price_cents_snap, s.status::text, s.started_at, s.updated_at,
    s.cancelled_at, a.code`

// ListActiveByTenant returns the tenant's ACTIVE entitlements. This backs
// the gateway-facing `GET /api/tenants/{id}/entitlements` route — the
// TenantEntitlement list IS the feature-flag set the FE consumes through
// `GET /api/feature-flags`.
//
// Runs inside a `SET LOCAL chora.tenant_id` tx so the RLS policy resolves.
// Returns a non-nil (possibly empty) slice — the handler json-encodes it
// directly into `{"items":[...]}`.
func (s *LegacyEntitlementStore) ListActiveByTenant(tenantID string) []*tenancy.TenantEntitlement {
	out := make([]*tenancy.TenantEntitlement, 0, 8)
	err := s.q.RunInTenantTx(context.Background(), tenantID, func(ctx context.Context, tx Tx) error {
		// LEFT JOIN add_ons for the addon_code bridge (CHO-1717 §4.6):
		// add_on_id is a NOT NULL FK so the join always matches, but
		// a.code itself may be NULL pre-backfill — scan handles it.
		const q = `
            SELECT ` + legacyEntitlementJoinColumns + `
            FROM add_on_subscriptions s
            LEFT JOIN add_ons a ON a.add_on_id = s.add_on_id
            WHERE s.tenant_id = $1 AND s.status = 'active'
            ORDER BY s.subscription_id ASC
        `
		rows, qerr := tx.Query(ctx, q, tenantID)
		if qerr != nil {
			return qerr
		}
		defer rows.Close()
		for rows.Next() {
			e, scanErr := scanLegacyEntitlement(rows)
			if scanErr != nil {
				return scanErr
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		fmt.Printf("pg.LegacyEntitlementStore.ListActiveByTenant(%s): %v\n", tenantID, err)
		// Return whatever was scanned before the error (likely empty) —
		// the port has no error channel; a partial list is never worse
		// than the previous always-empty in-memory stub.
		return out
	}
	// Stable sort defensively — the SQL ORDER BY already sorts, but the
	// in-memory adapter sorted in Go so callers may rely on it.
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].ID, out[j].ID) < 0
	})
	return out
}

// Subscribe creates a new active TenantEntitlement OR reactivates a
// previously-cancelled one for the (tenant_id, add_on_id) pair. Idempotent
// for already-active entries. Runs inside a `SET LOCAL chora.tenant_id` tx
// so the RLS WITH CHECK on the INSERT/UPDATE passes.
//
// The `add_on_subscriptions` table has UNIQUE(tenant_id, add_on_id); the
// INSERT ... ON CONFLICT (tenant_id, add_on_id) absorbs the re-subscribe
// case and flips status back to 'active'.
func (s *LegacyEntitlementStore) Subscribe(tenantID, addOnID string, monthlyPriceCents int64) (*tenancy.TenantEntitlement, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", tenancy.ErrInvalidArgument)
	}
	if strings.TrimSpace(addOnID) == "" {
		return nil, fmt.Errorf("%w: addon_id required", tenancy.ErrInvalidArgument)
	}
	// Construct the domain aggregate first so the new-row path has a
	// UUIDv7 + timestamps; on conflict the DB keeps the existing
	// subscription_id and we re-read it.
	ent, err := tenancy.NewTenantEntitlement(tenantID, addOnID, monthlyPriceCents)
	if err != nil {
		return nil, err
	}
	var result *tenancy.TenantEntitlement
	txErr := s.q.RunInTenantTx(context.Background(), tenantID, func(ctx context.Context, tx Tx) error {
		const q = `
            INSERT INTO add_on_subscriptions (
                subscription_id, tenant_id, add_on_id, status,
                monthly_price_cents_snap, started_at, cancelled_at,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, 'active', $4, $5, NULL, $6, $7)
            ON CONFLICT (tenant_id, add_on_id) DO UPDATE SET
                status = 'active',
                monthly_price_cents_snap = EXCLUDED.monthly_price_cents_snap,
                cancelled_at = NULL,
                ended_at = NULL,
                updated_at = EXCLUDED.updated_at
            RETURNING ` + legacyEntitlementColumns + `
        `
		row := tx.QueryRow(ctx, q,
			ent.ID, tenantID, addOnID, monthlyPriceCents,
			ent.ActivatedAt, ent.UpdatedAt, ent.UpdatedAt,
		)
		e, scanErr := scanLegacyEntitlement(row)
		if scanErr != nil {
			return scanErr
		}
		result = e
		return nil
	})
	if txErr != nil {
		return nil, fmt.Errorf("pg.LegacyEntitlementStore.Subscribe: %w", txErr)
	}
	return result, nil
}

// Cancel marks the (tenant_id, add_on_id) entitlement cancelled. Returns
// tenancy.ErrEntitlementNotFound for an unknown pair (the legacy handler
// maps that to a 404). Idempotent — cancelling an already-cancelled row
// returns the existing record. Runs inside a `SET LOCAL chora.tenant_id`
// tx.
func (s *LegacyEntitlementStore) Cancel(tenantID, addOnID string) (*tenancy.TenantEntitlement, error) {
	var result *tenancy.TenantEntitlement
	notFound := false
	txErr := s.q.RunInTenantTx(context.Background(), tenantID, func(ctx context.Context, tx Tx) error {
		const q = `
            UPDATE add_on_subscriptions SET
                status = 'cancelled',
                cancelled_at = COALESCE(cancelled_at, now()),
                ended_at = COALESCE(ended_at, now()),
                updated_at = now()
            WHERE tenant_id = $1 AND add_on_id = $2
            RETURNING ` + legacyEntitlementColumns + `
        `
		row := tx.QueryRow(ctx, q, tenantID, addOnID)
		e, scanErr := scanLegacyEntitlement(row)
		if scanErr != nil {
			if errors.Is(scanErr, ErrNoRows) {
				notFound = true
				return nil
			}
			return scanErr
		}
		result = e
		return nil
	})
	if txErr != nil {
		return nil, fmt.Errorf("pg.LegacyEntitlementStore.Cancel: %w", txErr)
	}
	if notFound {
		return nil, tenancy.ErrEntitlementNotFound
	}
	return result, nil
}

func scanLegacyEntitlement(scanner interface{ Scan(...any) error }) (*tenancy.TenantEntitlement, error) {
	var (
		e           tenancy.TenantEntitlement
		status      string
		startedAt   time.Time
		updatedAt   time.Time
		cancelledAt *time.Time
		addOnCode   *string // add_ons.code is nullable pre-backfill
	)
	if err := scanner.Scan(
		&e.ID,
		&e.TenantID,
		&e.AddOnID,
		&e.MonthlyPriceCentsSnapshot,
		&status,
		&startedAt,
		&updatedAt,
		&cancelledAt,
		&addOnCode,
	); err != nil {
		return nil, err
	}
	e.Status = tenancy.EntitlementStatus(status)
	e.ActivatedAt = startedAt.UTC()
	e.UpdatedAt = updatedAt.UTC()
	if cancelledAt != nil {
		v := cancelledAt.UTC()
		e.CancelledAt = &v
	}
	if addOnCode != nil {
		e.AddOnCode = *addOnCode
	}
	return &e, nil
}
