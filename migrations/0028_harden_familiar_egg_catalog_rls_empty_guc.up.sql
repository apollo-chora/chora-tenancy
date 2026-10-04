-- 0028: harden familiar_egg_catalog RLS against an empty-string chora.tenant_id GUC.
--
-- The platform-global catalog read (GET /api/familiar-eggs/catalog) legitimately
-- runs with NO tenant context; the tenancy app leaves chora.tenant_id = '' on that
-- pooled connection. The prior policies cast current_setting('chora.tenant_id',
-- true)::uuid -- and while missing_ok=true guards an UNSET GUC (-> NULL), it does
-- NOT guard an EMPTY STRING: ''::uuid raises `invalid input syntax for type uuid:
-- ""` (SQLSTATE 22P02). That failed the catalog list every time, surfacing in the
-- browser as a stuck "Loading egg catalog..." (gateway wrapped the tenancy 500 as a
-- 502). NULLIF(current_setting(...), '') maps '' -> NULL so an empty/absent tenant
-- context sees ONLY platform rows (tenant_id IS NULL) and never leaks tenant rows --
-- strictly safer, and correct for a platform-global catalog read.
--
-- Applied live via the migrate role 2026-07-02 (CHO-2005 marketplace unblock); this
-- migration records it so the IaC/next-env stays in sync (idempotent re-apply).

ALTER POLICY tenant_or_platform_visibility ON familiar_egg_catalog
  USING ((tenant_id IS NULL) OR (tenant_id = NULLIF(current_setting('chora.tenant_id'::text, true), '')::uuid));

ALTER POLICY tenant_admin_mutation ON familiar_egg_catalog
  USING (tenant_id = NULLIF(current_setting('chora.tenant_id'::text, true), '')::uuid);
