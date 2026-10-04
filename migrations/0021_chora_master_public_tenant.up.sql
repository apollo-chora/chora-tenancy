-- =============================================================================
-- chora-tenancy : 0021_chora_master_public_tenant.up.sql
--
-- ADR-182 — Two-tier tenancy: public chora-master tenant + author role +
-- multi-role members + cplus_social add-on + addon code bridge.
--
-- 1. tenant_member_role gains 'author' (tenancy surfaces map already derives
--    author → [aplus]; identity mirrors the value in its own enum migration).
-- 2. members UNIQUE(tenant_id, gcid) → UNIQUE(tenant_id, gcid, role): the
--    list_memberships_by_gcid() function (0011) has ALWAYS aggregated
--    ARRAY_AGG(role) per (gcid, tenant) — this unlocks the multi-role rows
--    it was written for (learner+author in chora-master, ADR-182 D2).
-- 3. chora-master tenant row — fixed UUID so every consumer can reference it
--    via config without a lookup; slug 'chora-master' is the stable handle
--    the UpsertMembership RPC resolves (no-inline-config on the caller side).
-- 4. add_ons.code — stable snake_case code on the add-on catalog. Closes the
--    FE A8 gap ([[a8-addon-code-or-catalog]]): the TenantEntitlement wire DTO
--    can now carry addon_code so FeatureFlagService.isEnabled('<code>')
--    works against live data instead of fail-loud false.
-- 5. cplus_social add-on (price 0) + an active chora-master subscription —
--    C+ serves the public space; private tenants opt in via H+ (ADR-182 D5).
--
-- RLS note: runner connects as the migrate role which OWNS these tables;
-- plain ENABLE (not FORCE) RLS exempts the owner, so the cross-tenant
-- INSERTs below need no SET LOCAL chora.tenant_id (seed 12_tenancy_addons
-- precedent).
-- =============================================================================

-- 1. author role (PG ≥12 allows ADD VALUE in a tx as long as the new value
--    is not used before commit — nothing below uses 'author').
ALTER TYPE tenant_member_role ADD VALUE IF NOT EXISTS 'author';

-- 2. one row per (tenant, gcid, role)
ALTER TABLE members DROP CONSTRAINT IF EXISTS members_tenant_id_gcid_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_members_tenant_gcid_role
    ON members (tenant_id, gcid, role);

-- 3. chora-master public tenant (idempotent; fixed UUID per ADR-182 D1)
INSERT INTO tenants (tenant_id, name, slug, status)
VALUES ('00000000-0000-7000-8000-000000000001', 'Chora', 'chora-master', 'active')
ON CONFLICT (tenant_id) DO NOTHING;

-- 4. add_ons.code — stable machine code for FE feature gating
ALTER TABLE add_ons ADD COLUMN IF NOT EXISTS code VARCHAR(64) NULL;
UPDATE add_ons
SET    code = lower(regexp_replace(name, '[^a-zA-Z0-9]+', '_', 'g'))
WHERE  code IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_add_ons_code ON add_ons (code);

-- 5. cplus_social add-on + chora-master entitlement
INSERT INTO add_ons (add_on_id, name, description, monthly_price_cents,
                     currency, capabilities, public, code)
VALUES (
    'a7d00006-0000-7000-8000-000000000006',
    'ChoraCircle Social',
    'C+ Content Sharing surface: social feed, profiles, leaderboards, duels, wallet, bounties, connections. Entitled to the public chora-master space by default; private tenants opt in via H+ (ADR-182 D5).',
    0,
    'SGD',
    ARRAY['sharing.feed','sharing.profile','sharing.leaderboards','sharing.duels','sharing.wallet','sharing.bounties','sharing.connections'],
    TRUE,
    'cplus_social'
)
ON CONFLICT (add_on_id) DO NOTHING;

INSERT INTO add_on_subscriptions (tenant_id, add_on_id, status, monthly_price_cents_snap)
VALUES ('00000000-0000-7000-8000-000000000001',
        'a7d00006-0000-7000-8000-000000000006',
        'active', 0)
ON CONFLICT (tenant_id, add_on_id) DO NOTHING;
