// familiar_egg_grpc_bufconn_test.go — in-process gRPC integration test for
// the FamiliarEgg server using google.golang.org/grpc/test/bufconn.
//
// Verifies end-to-end serialization + status code propagation: spin up a
// real *grpc.Server on a bufconn.Listener, register the FamiliarEgg server,
// dial it with grpc.NewClient + the bufconn DialContext, and round-trip
// PreviewEggOdds. This complements the direct-method-invocation tests
// (familiar_egg_grpc_server_test.go) by exercising the gRPC marshaling
// + transport layer.
package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
)

// bufconnBufferSize is large enough for the small PreviewEggOdds payload.
const bufconnBufferSize = 1024 * 1024

// startBufconnServer spins up an in-process *grpc.Server backed by the
// FamiliarEgg adapter and the supplied catalog stub. Returns the client
// stub + cleanup. Test failure surfaces as t.Fatalf inside.
func startBufconnServer(t *testing.T, catalog tenancygrpc.CatalogLookup) (tenancyv1.CompanionEggClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufconnBufferSize)
	srv := grpc.NewServer()
	tenancyv1.RegisterCompanionEggServer(srv, tenancygrpc.NewFamiliarEggServer(catalog))

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("bufconn server stopped: %v", err)
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
		t.Fatalf("bufconn dial: %v", err)
	}
	client := tenancyv1.NewCompanionEggClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestBufconn_PreviewEggOdds_HappyPath(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{entry: newCatalogEntry(""), found: true}
	client, cleanup := startBufconnServer(t, cat)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.PreviewEggOdds(ctx, &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err != nil {
		t.Fatalf("PreviewEggOdds: %v", err)
	}
	if resp.GetEggSku() != "egg.test.v1" {
		t.Errorf("egg_sku = %q", resp.GetEggSku())
	}
	if got := len(resp.GetOdds()); got != 8 {
		t.Errorf("len(odds) = %d; want 8", got)
	}
	if resp.GetTotalWeight() < 99.99 || resp.GetTotalWeight() > 100.01 {
		t.Errorf("total_weight = %v; want ~100", resp.GetTotalWeight())
	}
	if cat.lastSKU != "egg.test.v1" {
		t.Errorf("catalog.lastSKU = %q; want egg.test.v1", cat.lastSKU)
	}
}

func TestBufconn_PreviewEggOdds_NotFound(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{found: false}
	client, cleanup := startBufconnServer(t, cat)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.PreviewEggOdds(ctx, &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.missing.v1",
	})
	if err == nil {
		t.Fatal("expected NotFound")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v; want NotFound", status.Code(err))
	}
}

func TestBufconn_PreviewEggOdds_TenantScopeRejection(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalog{entry: newCatalogEntry("tenant-B"), found: true}
	client, cleanup := startBufconnServer(t, cat)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.PreviewEggOdds(ctx, &tenancyv1.PreviewEggOddsRequest{
		TenantId: "tenant-A",
		EggSku:   "egg.test.v1",
	})
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code = %v; want PermissionDenied", status.Code(err))
	}
}
