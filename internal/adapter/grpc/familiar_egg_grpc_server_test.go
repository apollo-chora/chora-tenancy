// familiar_egg_grpc_server_test.go — TDD coverage for the gRPC adapter
// wrapping the FamiliarEgg PreviewEggOdds RPC.
//
// Coverage classes (per #12 contract):
//   - Happy path: returns sorted (DESC by probability) BreedOdds + total_weight
//     + distribution_updated_at, mirroring the REST endpoint shape.
//   - 404 NotFound: SKU does not exist.
//   - Tenant-scope rejection: SKU's tenant_id != caller tenant AND != NULL.
//   - Platform-global SKU: tenant_id IS NULL → any caller can read.
//   - Invalid args: empty tenant_id, empty egg_sku.
//   - Repo error surfaces as Internal.
//
// Tests use direct method invocation (no real network) — this matches the
// established chora-guardrail + chora-a2a-gateway gRPC test pattern.
package grpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

// fakeCatalog is a test stub for the CatalogLookup port.
type fakeCatalog struct {
	entry      *familiareag.CatalogEntry
	found      bool
	err        error
	lastSKU    string
	lastTenant string
}

func (f *fakeCatalog) GetBySKU(_ context.Context, tenantID, sku string) (*familiareag.CatalogEntry, bool, error) {
	f.lastSKU = sku
	f.lastTenant = tenantID
	return f.entry, f.found, f.err
}

func newCatalogEntry(tenantID string) *familiareag.CatalogEntry {
	updated := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	return &familiareag.CatalogEntry{
		SKU:         "egg.test.v1",
		TenantID:    tenantID,
		DisplayName: "Test Egg",
		Currency:    "SGD",
		PriceCents:  500,
		BreedDistribution: familiareag.BreedDistribution{
			"owl":     35,
			"fox":     25,
			"cat":     20,
			"wolf":    10,
			"raven":   5,
			"turtle":  3,
			"dragon":  1.5,
			"phoenix": 0.5,
		},
		BreedDistributionUpdated: updated,
		SoftExpiryDays:           30,
		HardExpiryDays:           60,
	}
}

func TestPreviewEggOdds_HappyPath_PlatformSku(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{entry: newCatalogEntry(""), found: true}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	resp, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err != nil {
		t.Fatalf("PreviewEggOdds err: %v", err)
	}
	if resp.GetEggSku() != "egg.test.v1" {
		t.Errorf("egg_sku = %q; want egg.test.v1", resp.GetEggSku())
	}
	if got, want := len(resp.GetOdds()), 8; got != want {
		t.Fatalf("len(odds) = %d; want %d", got, want)
	}
	// Sorted DESC by probability — first row must be owl (35).
	if got := resp.GetOdds()[0].GetSpecies(); got != "owl" {
		t.Errorf("odds[0].species = %q; want owl", got)
	}
	if got := resp.GetOdds()[0].GetProbability(); got != 35 {
		t.Errorf("odds[0].probability = %v; want 35", got)
	}
	// Sum should be 100.0 (within tolerance).
	if got := resp.GetTotalWeight(); got < 99.99 || got > 100.01 {
		t.Errorf("total_weight = %v; want ~100.0", got)
	}
	// distribution_updated_at present.
	if resp.GetDistributionUpdatedAt() == nil {
		t.Error("distribution_updated_at should be set")
	}
	// Rarity classification surfaced.
	if got := resp.GetOdds()[0].GetRarity(); got != "common" {
		t.Errorf("odds[0].rarity = %q; want common", got)
	}
}

func TestPreviewEggOdds_HappyPath_TenantSkuMatchesCaller(t *testing.T) {
	t.Parallel()
	entry := newCatalogEntry("tenant-A")
	cat := &fakeCatalog{entry: entry, found: true}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	resp, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err != nil {
		t.Fatalf("expected success, got err: %v", err)
	}
	if resp == nil {
		t.Fatal("nil resp")
	}
}

// The caller's tenant MUST reach the catalog port. familiar_egg_catalog
// carries FORCE RLS, so a lookup that drops the tenant runs with an empty
// chora.tenant_id GUC and can only ever see platform-global SKUs: every
// tenant-scoped SKU 404s, and the reveal path 500s behind it.
func TestPreviewEggOdds_PassesCallerTenantToCatalog(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{entry: newCatalogEntry("tenant-A"), found: true}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	if _, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	}); err != nil {
		t.Fatalf("PreviewEggOdds err: %v", err)
	}
	if cat.lastTenant != "tenant-A" {
		t.Errorf("catalog lookup tenant = %q; want tenant-A", cat.lastTenant)
	}
	if cat.lastSKU != "egg.test.v1" {
		t.Errorf("catalog lookup sku = %q; want egg.test.v1", cat.lastSKU)
	}
}

func TestPreviewEggOdds_NotFound(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{found: false}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	_, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.missing.v1",
	})
	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("status code = %v; want NotFound", got)
	}
}

func TestPreviewEggOdds_TenantScopeRejection(t *testing.T) {
	t.Parallel()
	// SKU owned by tenant-B; caller is tenant-A → PermissionDenied.
	cat := &fakeCatalog{entry: newCatalogEntry("tenant-B"), found: true}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	_, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Errorf("status code = %v; want PermissionDenied", got)
	}
}

func TestPreviewEggOdds_InvalidArgs_EmptyTenant(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	_, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "",
		EggSku:   "egg.test.v1",
	})
	if err == nil {
		t.Fatal("expected InvalidArgument on empty tenant_id")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("status code = %v; want InvalidArgument", got)
	}
}

func TestPreviewEggOdds_InvalidArgs_EmptySku(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	_, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "",
	})
	if err == nil {
		t.Fatal("expected InvalidArgument on empty egg_sku")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("status code = %v; want InvalidArgument", got)
	}
}

func TestPreviewEggOdds_RepoError(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{err: errors.New("connection refused")}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	_, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err == nil {
		t.Fatal("expected error from repo failure")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("status code = %v; want Internal", got)
	}
}

func TestPreviewEggOdds_OddsSortStable(t *testing.T) {
	t.Parallel()
	// Verify DESC by probability, alphabetical tie-break.
	entry := newCatalogEntry("")
	entry.BreedDistribution = familiareag.BreedDistribution{
		"owl":  25,
		"fox":  25,
		"cat":  50,
	}
	cat := &fakeCatalog{entry: entry, found: true}
	srv := tenancygrpc.NewFamiliarEggServer(cat)

	resp, err := srv.PreviewEggOdds(context.Background(), &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err != nil {
		t.Fatalf("PreviewEggOdds err: %v", err)
	}
	if got := resp.GetOdds()[0].GetSpecies(); got != "cat" {
		t.Errorf("odds[0].species = %q; want cat (50)", got)
	}
	// owl + fox both at 25 — fox alphabetically first.
	if got := resp.GetOdds()[1].GetSpecies(); got != "fox" {
		t.Errorf("odds[1].species = %q; want fox", got)
	}
	if got := resp.GetOdds()[2].GetSpecies(); got != "owl" {
		t.Errorf("odds[2].species = %q; want owl", got)
	}
}

// NewServer compile-check that the constructor returns a value implementing
// tenancyv1.CompanionEggServer.
func TestFamiliarEggServer_ImplementsProtoInterface(t *testing.T) {
	t.Parallel()
	var _ tenancyv1.CompanionEggServer = tenancygrpc.NewFamiliarEggServer(&fakeCatalog{})
}
