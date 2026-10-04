// setup_wizard_addon_repository.go — pgx-backed persistence for the
// Setup Wizard step 2 add-on selections (CHO-1692).
//
// Replaces the in-memory SubscriptionRegistry write path the wizard's
// MeAddOnsHandler used during CHO-1664 — that registry didn't survive
// pod restart and made the wizard's re-entry hydration impossible. The
// admin add-on lifecycle endpoints continue to use the existing
// SubscriptionRegistry + add_on_subscriptions table; the wizard's
// "declared intent" model is intentionally kept in a separate
// setup_wizard_addon_selections table to avoid interference.
//
// SQL contract:
//   - Upsert: INSERT ... ON CONFLICT (tenant_id, add_on_code) DO UPDATE
//     SET status = EXCLUDED.status, updated_at = now(). Idempotent-additive
//     re-clicks of Next in the wizard are no-ops.
//   - ListByTenant: SELECT add_on_code FROM ... WHERE tenant_id = $1.
//     The RLS policy on the table requires SET LOCAL chora.tenant_id
//     to match — wrapping the SELECT in RunInTenantTx satisfies this
//     and matches the prod authorization model.
package pg

import (
	"context"
	"fmt"
)

// SetupWizardAddonRepository is the pgx-backed adapter.
type SetupWizardAddonRepository struct {
	q TxQuerier
}

// NewSetupWizardAddonRepository constructs the repository.
func NewSetupWizardAddonRepository(q TxQuerier) *SetupWizardAddonRepository {
	return &SetupWizardAddonRepository{q: q}
}

// AddonSelection is the wire shape returned by ListByTenant.
type AddonSelection struct {
	AddOnCode string
	Status    string
}

// Upsert idempotently records a (tenant, code) pair in the requested
// status. Wraps the write in RunInTenantTx so the RLS tenant_isolation
// policy is satisfied. A second call with the same code is a no-op
// for created_at + only refreshes updated_at + status.
func (r *SetupWizardAddonRepository) Upsert(ctx context.Context, tenantID, code, status string) error {
	if tenantID == "" || code == "" {
		return fmt.Errorf("setup_wizard_addon: tenant_id + add_on_code required")
	}
	return r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		const sql = `
            INSERT INTO setup_wizard_addon_selections (tenant_id, add_on_code, status, created_at, updated_at)
            VALUES ($1::uuid, $2, $3, now(), now())
            ON CONFLICT (tenant_id, add_on_code) DO UPDATE
                SET status = EXCLUDED.status, updated_at = now()
        `
		return tx.Exec(ctx, sql, tenantID, code, status)
	})
}

// ReplaceSet enforces REPLACE semantics on the wizard's add-on
// selections — the input `codes` is treated as the COMPLETE set the
// user wants persisted for (tenant). Inside a single tx:
//   1. UPSERT each code in `codes` (new rows in `pending_activation`;
//      existing rows refresh updated_at without status change).
//   2. DELETE every row for `tenantID` whose code is NOT in `codes`.
//
// Empty `codes` is a valid input — it means "remove all selections".
// The admin-side add_on_subscriptions table is NOT touched; that lane
// owns its own activation/deactivation lifecycle.
func (r *SetupWizardAddonRepository) ReplaceSet(ctx context.Context, tenantID string, codes []string) error {
	if tenantID == "" {
		return fmt.Errorf("setup_wizard_addon: tenant_id required")
	}
	return r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		// Upsert each requested code.
		for _, code := range codes {
			if code == "" {
				continue
			}
			const upsertSQL = `
                INSERT INTO setup_wizard_addon_selections (tenant_id, add_on_code, status, created_at, updated_at)
                VALUES ($1::uuid, $2, 'pending_activation', now(), now())
                ON CONFLICT (tenant_id, add_on_code) DO UPDATE
                    SET updated_at = now()
            `
			if err := tx.Exec(ctx, upsertSQL, tenantID, code); err != nil {
				return err
			}
		}
		// Delete codes not in the new set. When `codes` is empty the
		// WHERE NOT IN clause needs a sentinel — use a NULL-safe
		// placeholder so the SQL stays well-formed.
		if len(codes) == 0 {
			const deleteAllSQL = `DELETE FROM setup_wizard_addon_selections WHERE tenant_id = $1::uuid`
			return tx.Exec(ctx, deleteAllSQL, tenantID)
		}
		// Build the IN clause with parameterised placeholders to keep
		// the prepared-statement cache effective.
		placeholders := make([]string, 0, len(codes))
		args := make([]any, 0, 1+len(codes))
		args = append(args, tenantID)
		for i, code := range codes {
			placeholders = append(placeholders, fmt.Sprintf("$%d", i+2))
			args = append(args, code)
		}
		deleteSQL := fmt.Sprintf(
			`DELETE FROM setup_wizard_addon_selections WHERE tenant_id = $1::uuid AND add_on_code NOT IN (%s)`,
			joinComma(placeholders),
		)
		return tx.Exec(ctx, deleteSQL, args...)
	})
}

// joinComma joins string slices with ", " — kept local to avoid a
// strings.Join import for this single call site.
func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

// ListByTenant returns all selections for the calling tenant, ordered
// by add_on_code so the FE renders consistently.
func (r *SetupWizardAddonRepository) ListByTenant(ctx context.Context, tenantID string) ([]AddonSelection, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("setup_wizard_addon: tenant_id required")
	}
	out := []AddonSelection{}
	err := r.q.RunInTenantTx(ctx, tenantID, func(ctx context.Context, tx Tx) error {
		const sql = `
            SELECT add_on_code, status
            FROM setup_wizard_addon_selections
            WHERE tenant_id = $1::uuid
            ORDER BY add_on_code ASC
        `
		rows, err := tx.Query(ctx, sql, tenantID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s AddonSelection
			if err := rows.Scan(&s.AddOnCode, &s.Status); err != nil {
				return err
			}
			out = append(out, s)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
