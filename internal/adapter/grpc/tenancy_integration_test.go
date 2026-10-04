//go:build integration

// tenancy_integration_test.go — `//go:build integration` tagged smoke for
// chora-tenancy. Runs in Cloud Build stage 3 (integration-test) invoked
// via `go test -race -tags=integration ./...`.
//
// Exercises the FamiliarEgg PreviewEggOdds RPC end-to-end via bufconn plus
// the gRPC Health/Check round-trip — the same wire path internal callers
// take + the Cloud Deploy verify-Job probe contract.
//
// Domain-meaningful: PreviewEggOdds is the lootbox-rolled breed distribution
// query that the H+ Setup-Tenant wizard + per-user mana subscription flow
// surface to learners (Familiar egg purchase preview). Returns the 8-row
// breed distribution sorted DESC. Mirrors familiar_egg_grpc_bufconn_test.go
// pattern but with `//go:build integration` so stage 3 picks it up.
//
// The existing internal/adapter/pg/{integration_test.go, legacy_tenancy_integration_test.go}
// require CHORA_TEST_DSN + Cloud SQL — they auto-skip in CI without the env
// var. This file always runs.
package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

// integFakeCatalog is the integration-test catalog stub. Distinct name from
// the unit-test `fakeCatalog` so they don't clash inside the same package.
type integFakeCatalog struct {
	entry *familiareag.CatalogEntry
	found bool
}

func (f *integFakeCatalog) GetBySKU(_ context.Context, _, _ string) (*familiareag.CatalogEntry, bool, error) {
	return f.entry, f.found, nil
}

func startIntegrationTenancyServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()

	// FamiliarEgg server — exercises a real PreviewEggOdds round-trip.
	updated := time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC)
	catalog := &integFakeCatalog{
		entry: &familiareag.CatalogEntry{
			SKU:         "egg.integration.v1",
			TenantID:    "",
			DisplayName: "Integration Egg",
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
		},
		found: true,
	}
	tenancyv1.RegisterCompanionEggServer(srv, tenancygrpc.NewFamiliarEggServer(catalog))

	// Health — mirrors main.go:489-494 registration.
	healthSrv := healthgrpc.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("chora.services.tenancy.v1.FamiliarEgg", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("chora.services.tenancy.v1.Tenancy", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("integration bufconn server stopped: %v", err)
		}
	}()

	//nolint:staticcheck // bufconn dial requires the legacy DialContext API.
	conn, err := grpc.DialContext(
		context.Background(),
		"bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("integration bufconn dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
	return conn, cleanup
}

// Integration smoke: gRPC Health/Check responds SERVING for the default
// surface AND for the service-scoped registrations. Mirrors the verify-Job
// RPC contract that runs in Cloud Deploy's VERIFY stage post-DEPLOY.
func TestIntegration_Tenancy_HealthCheck_AllSurfacesServing(t *testing.T) {
	t.Parallel()
	conn, cleanup := startIntegrationTenancyServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := healthpb.NewHealthClient(conn)
	cases := []struct {
		name    string
		service string
	}{
		{"default", ""},
		{"FamiliarEgg", "chora.services.tenancy.v1.FamiliarEgg"},
		{"Tenancy", "chora.services.tenancy.v1.Tenancy"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: tc.service})
			if err != nil {
				t.Fatalf("Health/Check(%q): %v", tc.service, err)
			}
			if resp.Status != healthpb.HealthCheckResponse_SERVING {
				t.Errorf("Health/Check(%q) = %v; want SERVING", tc.service, resp.Status)
			}
		})
	}
}

// Integration smoke: PreviewEggOdds RPC end-to-end via bufconn. Returns the
// 8-row breed distribution sorted DESC by probability with total ~100.
// Mirrors the H+ Setup-Tenant wizard / per-user mana-subscription egg
// preview flow.
func TestIntegration_Tenancy_FamiliarEgg_PreviewOdds_RoundTrip(t *testing.T) {
	t.Parallel()
	conn, cleanup := startIntegrationTenancyServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := tenancyv1.NewCompanionEggClient(conn)
	resp, err := client.PreviewEggOdds(ctx, &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-integration",
		EggSku:   "egg.integration.v1",
	})
	if err != nil {
		t.Fatalf("PreviewEggOdds: %v", err)
	}
	if resp.GetEggSku() != "egg.integration.v1" {
		t.Errorf("egg_sku = %q; want egg.integration.v1", resp.GetEggSku())
	}
	if got := len(resp.GetOdds()); got != 8 {
		t.Fatalf("len(odds) = %d; want 8", got)
	}
	// Sorted DESC — first row must be owl (35 highest weight).
	if got := resp.GetOdds()[0].GetSpecies(); got != "owl" {
		t.Errorf("odds[0].species = %q; want owl", got)
	}
	// Total weight sums to ~100 (lootbox normalised distribution).
	if got := resp.GetTotalWeight(); got < 99.99 || got > 100.01 {
		t.Errorf("total_weight = %v; want ~100.0", got)
	}
}
