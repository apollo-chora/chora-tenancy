-- =============================================================================
-- chora-tenancy : 0018_seed_core_add_on.up.sql
--
-- Domain        : Tenancy + Billing (7 supporting)
-- Database      : chora_tenancy
-- Author        : CHO-1654 (H+ Setup-Tenant Phase 1 hot-fix)
-- Story         : CHO-1654 — seeds the `core` add_on row that
--                 bootstrap_repository.go looks up at H+ Setup-Tenant time.
-- Sibling stories: CHO-1628 (Phase 1 endpoint), CHO-1632 (Phase 3 BFF route),
--                 CHO-1642 (Phase 4 form), CHO-1648 (Phase 5 circular dep).
--
-- Context
--   services/chora-tenancy/internal/domain/bootstrap/bootstrap.go:56 hardcodes
--   `CoreAddOnCode = "core"`. services/chora-tenancy/internal/adapter/pg/
--   bootstrap_repository.go:139-146 runs
--       SELECT add_on_id FROM add_ons WHERE name = $1
--   inside the bootstrap transaction. Without this seed the lookup returns
--   ErrCoreAddOnNotFound and the handler returns 500 internal_error to the H+
--   FE form. Production was unblocked via direct INSERT through a database
--   port-forward on 2026-06-03; this migration backfills the seed so fresh
--   environments don't repeat the trap.
--
-- Idempotency
--   ON CONFLICT (name) DO NOTHING. add_ons.name is UNIQUE (add_ons_name_key).
--   Re-applying against the already-seeded production state is a no-op; the
--   existing add_on_id is preserved.
--
-- =============================================================================

BEGIN;

INSERT INTO add_ons (
    add_on_id,
    name,
    description,
    monthly_price_cents,
    currency,
    capabilities,
    public,
    self_hosted_only
)
VALUES (
    'a7d00000-0000-7000-8000-000000000000',
    'core',
    'Core tenant entitlement granted on H+ Setup-Tenant bootstrap (CHO-1628 / CHO-1654). Always present, always free.',
    0,
    'SGD',
    '{}',
    true,
    false
)
ON CONFLICT (name) DO NOTHING;

COMMIT;
