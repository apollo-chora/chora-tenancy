// registry_entitlement_fallthrough_test.go: the entitlement READ must not
// serve a boot snapshot as truth (UX Track U, package E1).
//
// CHO-1811 wrapped the PG-backed store so the entitlement endpoint tracks the
// in-memory registry, which is the runtime source of truth for deactivation.
// The boot hydrator seeds that registry once, so a tenant CREATED AFTER BOOT
// holds durable add_on_subscriptions rows that no reader can see until the pod
// restarts. E1 grants add-ons inside the creation transaction, so that gap is
// the difference between "durable" and "durable and readable".
//
// The fix falls through to the durable table when the registry has never heard
// of the tenant. The discriminator matters: the registry keeps a row for every
// tenant it has ever seen in ANY status, because Deactivate and Unsubscribe
// flip Status and never remove from byTenant. So "no rows at all" means never
// seen, while "rows, none active" means seen and deliberately emptied. Falling
// through on the second one would resurrect every runtime deactivation from the
// stale PG rows, which is the exact bug CHO-1811 exists to prevent.
package main

import (
	"testing"

	tenancy "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
)

// TestRegistryEntitlementStore_ListActive_FallsThroughWhenRegistryNeverSawTenant
// is the E1 acceptance: a process that never hydrated must still read the
// durable grant. The registry here is brand new, exactly as it is in a pod that
// booted before the tenant existed.
func TestRegistryEntitlementStore_ListActive_FallsThroughWhenRegistryNeverSawTenant(t *testing.T) {
	delegate := newFakeEntitlementStore()
	delegate.items = []*tenancy.TenantEntitlement{
		{
			ID:        "sub-1",
			TenantID:  "tenant-new",
			AddOnID:   "addon-uuid-1",
			AddOnCode: "tms",
			Status:    tenancy.EntitlementStatusActive,
		},
		{
			ID:        "sub-2",
			TenantID:  "tenant-new",
			AddOnID:   "addon-uuid-2",
			AddOnCode: "cms",
			Status:    tenancy.EntitlementStatusActive,
		},
	}
	reg := newTestRegistry(t)
	s := newRegistryEntitlementStore(reg, delegate)

	ents := s.ListActiveByTenant("tenant-new")

	if delegate.listCalls != 1 {
		t.Fatalf("expected exactly one durable read for a tenant the registry never saw, got %d", delegate.listCalls)
	}
	if len(ents) != 2 {
		t.Fatalf("expected the two durable entitlements, got %d: %+v", len(ents), ents)
	}
	codes := map[string]bool{}
	for _, e := range ents {
		if e.TenantID != "tenant-new" {
			t.Fatalf("durable row lost its tenant: %+v", e)
		}
		if e.Status != tenancy.EntitlementStatusActive {
			t.Fatalf("expected active status, got %s", e.Status)
		}
		codes[e.AddOnCode] = true
	}
	if !codes["tms"] || !codes["cms"] {
		t.Fatalf("expected both durable add-on codes, got %v", codes)
	}
}

// TestRegistryEntitlementStore_ListActive_KnownTenantWithNothingActiveDoesNotFallThrough
// is a regression fence, not a RED: the behaviour it pins already holds on main
// and it exists to refuse the naive fix. A fall-through keyed on "the ACTIVE
// list is empty" passes the acceptance test above and silently undoes every
// runtime deactivation, because the PG row still reads active. Verified against
// that naive implementation before landing: it fails there and passes here.
func TestRegistryEntitlementStore_ListActive_KnownTenantWithNothingActiveDoesNotFallThrough(t *testing.T) {
	delegate := newFakeEntitlementStore()
	// The stale durable row the naive fix would resurrect.
	delegate.items = []*tenancy.TenantEntitlement{{
		ID:        "sub-stale",
		TenantID:  "tenant-known",
		AddOnID:   "addon-uuid-1",
		AddOnCode: "tms",
		Status:    tenancy.EntitlementStatusActive,
	}}
	reg := newTestRegistry(t)
	if _, err := reg.Subscribe("tenant-known", "tms", "gcid-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := newRegistryAddOnActivator(reg).AnchorDeactivation("tenant-known", "tms"); err != nil {
		t.Fatalf("AnchorDeactivation: %v", err)
	}
	s := newRegistryEntitlementStore(reg, delegate)

	ents := s.ListActiveByTenant("tenant-known")

	if len(ents) != 0 {
		t.Fatalf("a runtime deactivation must not be undone by the durable read, got %+v", ents)
	}
	if delegate.listCalls != 0 {
		t.Fatalf("the durable table must not be consulted for a tenant the registry knows, got %d reads", delegate.listCalls)
	}
}

// TestRegistryEntitlementStore_ListActive_FallThroughSpeaksTheRegistryVocabulary
// pins the two arms to ONE add-on vocabulary.
//
// The bootstrap saga still writes the legacy code "core" into
// add_on_subscriptions, and pg.TranslateAddOnCode maps it to the canonical
// "base" the v2 catalogue and the H+ marketplace know. That translation lives
// in the boot hydrator, on the way INTO the registry, so the durable table
// never had it applied. Passing durable rows straight through would make this
// one endpoint answer "base" or "core" depending on whether the serving pod
// happened to have hydrated that tenant, and the FE gate keys on the code.
func TestRegistryEntitlementStore_ListActive_FallThroughSpeaksTheRegistryVocabulary(t *testing.T) {
	delegate := newFakeEntitlementStore()
	delegate.items = []*tenancy.TenantEntitlement{
		{
			ID:        "sub-core",
			TenantID:  "tenant-new",
			AddOnID:   "addon-uuid-core",
			AddOnCode: "core",
			Status:    tenancy.EntitlementStatusActive,
		},
		{
			ID:        "sub-tms",
			TenantID:  "tenant-new",
			AddOnID:   "addon-uuid-1",
			AddOnCode: "tms",
			Status:    tenancy.EntitlementStatusActive,
		},
	}
	reg := newTestRegistry(t)
	s := newRegistryEntitlementStore(reg, delegate)

	ents := s.ListActiveByTenant("tenant-new")

	codes := map[string]bool{}
	for _, e := range ents {
		codes[e.AddOnCode] = true
	}
	if !codes["base"] {
		t.Fatalf("the durable legacy code core must reach the reader as base, got %v", codes)
	}
	if codes["core"] {
		t.Fatalf("core leaked to the reader; the hydrated arm never emits it, got %v", codes)
	}
	if !codes["tms"] {
		t.Fatalf("an untranslated code must pass through unchanged, got %v", codes)
	}
	// The delegate's own rows must not be rewritten under it: the durable
	// store hands out fresh structs, but this wrapper is not entitled to
	// assume that.
	if delegate.items[0].AddOnCode != "core" {
		t.Fatalf("the wrapper mutated the delegate's row in place: %+v", delegate.items[0])
	}
}

// TestRegistryEntitlementStore_ListActive_FallThroughSkipsNilRows pins the
// direction the wrapper fails in. A nil row from the delegate is a malformed
// list rather than a signalled error, and the port has no error channel to
// raise it on; dropping it hands the FE gate one entitlement fewer, which is
// the closed direction. Copying it instead would panic in a read path.
func TestRegistryEntitlementStore_ListActive_FallThroughSkipsNilRows(t *testing.T) {
	delegate := newFakeEntitlementStore()
	delegate.items = []*tenancy.TenantEntitlement{
		nil,
		{
			ID:        "sub-tms",
			TenantID:  "tenant-new",
			AddOnID:   "addon-uuid-1",
			AddOnCode: "tms",
			Status:    tenancy.EntitlementStatusActive,
		},
	}
	reg := newTestRegistry(t)
	s := newRegistryEntitlementStore(reg, delegate)

	ents := s.ListActiveByTenant("tenant-new")

	if len(ents) != 1 || ents[0].AddOnCode != "tms" {
		t.Fatalf("expected the one well-formed row, got %+v", ents)
	}
}

// TestRegistryEntitlementStore_ListActive_UnknownTenantWithNoDurableRows pins
// the slice contract the handler depends on: it json-encodes the result
// straight into {"items":[...]}, so a nil would serialise as null rather than
// an empty array.
func TestRegistryEntitlementStore_ListActive_UnknownTenantWithNoDurableRows(t *testing.T) {
	delegate := newFakeEntitlementStore()
	reg := newTestRegistry(t)
	s := newRegistryEntitlementStore(reg, delegate)

	ents := s.ListActiveByTenant("tenant-absent")

	if delegate.listCalls != 1 {
		t.Fatalf("expected one durable read, got %d", delegate.listCalls)
	}
	if ents == nil {
		t.Fatalf("expected a non-nil empty slice, got nil")
	}
	if len(ents) != 0 {
		t.Fatalf("expected no entitlements, got %+v", ents)
	}
}
