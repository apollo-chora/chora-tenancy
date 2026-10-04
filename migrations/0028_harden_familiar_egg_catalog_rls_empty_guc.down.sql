-- Revert 0028: restore the original ::uuid cast (fragile on empty-string GUC).
ALTER POLICY tenant_or_platform_visibility ON familiar_egg_catalog
  USING ((tenant_id IS NULL) OR (tenant_id = (current_setting('chora.tenant_id'::text, true))::uuid));

ALTER POLICY tenant_admin_mutation ON familiar_egg_catalog
  USING (tenant_id = (current_setting('chora.tenant_id'::text, true))::uuid);
