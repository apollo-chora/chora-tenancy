// Package grpc — gRPC adapters for chora-tenancy.
//
// This file owns the FamiliarEgg gRPC server (Iter G.4 PROD-C #12), which
// is the typed-RPC successor to the GET /api/familiar-eggs/{sku}/odds REST
// endpoint. Per ddd-enforcement HARD RULE: chora-consumption MUST NOT
// query the chora_tenancy DB directly — this server is the sanctioned
// cross-domain read path.
//
// Per hexagonal SKILL: adapter depends on the domain catalog (via the
// CatalogLookup port) and on chora-contracts generated stubs only. Pure
// validation + tenant-scope check + sort. No business logic.
//
// Tenant scoping (per #12 contract):
//   - SKU's tenant_id == "" → platform-global; any caller may read
//   - SKU's tenant_id == caller tenant_id → permitted
//   - Otherwise → PermissionDenied (RLS-equivalent at the API boundary;
//     the chora_tenancy DB also enforces RLS on familiar_egg_catalog so
//     this is defence-in-depth)
//
// mTLS is provided by Cloud Service Mesh at the L4 layer; no special
// config in the server. The gRPC port (:9090) follows the existing
// chora-{a2a,guardrail} convention.
package grpc

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	familiareag "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/familiar_egg"
)

// tracerName for OTLP spans emitted by this server.
const tracerName = "chora-tenancy.familiar_egg"

// CatalogLookup is the minimal port the gRPC server needs to satisfy
// PreviewEggOdds. Implemented in production by pg.CatalogRepository.
//
// tenantID scopes the lookup: the pg implementation runs it under
// `SET LOCAL chora.tenant_id` so the familiar_egg_catalog RLS policy
// resolves the tenant's own SKUs alongside the platform-global ones.
//
// Returning found=false (with err=nil) signals NotFound to the gRPC
// adapter — keeps the gRPC translation layer thin.
type CatalogLookup interface {
	GetBySKU(ctx context.Context, tenantID, sku string) (*familiareag.CatalogEntry, bool, error)
}

// FamiliarEggServer is the gRPC server adapter for the FamiliarEgg service.
type FamiliarEggServer struct {
	tenancyv1.UnimplementedCompanionEggServer
	catalog CatalogLookup
}

// NewFamiliarEggServer constructs the server with the supplied catalog port.
func NewFamiliarEggServer(catalog CatalogLookup) *FamiliarEggServer {
	return &FamiliarEggServer{catalog: catalog}
}

// tracer lazily retrieves the package-level tracer.
func tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(tracerName)
}

// PreviewEggOdds implements tenancyv1.CompanionEggServer.
//
// Returns the IMDA D2 transparency probability table for the requested
// egg SKU. Enforces tenant scoping at the gRPC boundary; the chora_tenancy
// DB layer enforces RLS on the same column (defence-in-depth).
func (s *FamiliarEggServer) PreviewEggOdds(
	ctx context.Context,
	req *tenancyv1.PreviewEggOddsRequest,
) (*tenancyv1.PreviewEggOddsResponse, error) {
	ctx, span := tracer().Start(ctx, "FamiliarEgg.PreviewEggOdds")
	defer span.End()

	tenantID := strings.TrimSpace(req.GetTenantId())
	sku := strings.TrimSpace(req.GetEggSku())

	span.SetAttributes(
		attribute.String("chora.tenant_id", tenantID),
		attribute.String("chora.familiar_egg.sku", sku),
	)

	if tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id required")
	}
	if sku == "" {
		return nil, status.Error(codes.InvalidArgument, "egg_sku required")
	}

	entry, found, err := s.catalog.GetBySKU(ctx, tenantID, sku)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "catalog lookup: %v", err)
	}
	if !found || entry == nil {
		return nil, status.Errorf(codes.NotFound, "familiar_egg SKU %q not found", sku)
	}

	// Tenant scoping: SKU tenant_id must be empty (platform-global) OR
	// match the caller's tenant. Any other combination → PermissionDenied.
	//
	// The pg lookup above is now itself tenant-scoped (RLS), so a
	// cross-tenant SKU already reports not-found and this branch does not
	// fire in production. It stays as the defence-in-depth check the
	// package contract promises: any CatalogLookup that returns a
	// cross-tenant row is refused here rather than served.
	if entry.TenantID != "" && entry.TenantID != tenantID {
		return nil, status.Errorf(codes.PermissionDenied,
			"familiar_egg SKU %q not accessible to tenant %q", sku, tenantID)
	}

	// Marshal the BreedDistribution into the proto response. domain.Odds()
	// sorts DESC by probability + alphabetical tie-break per ADR-149 §Breed
	// lootbox, so we preserve that ordering on the wire.
	odds := entry.BreedDistribution.Odds()
	out := &tenancyv1.PreviewEggOddsResponse{
		EggSku:                entry.SKU,
		TotalWeight:           entry.BreedDistribution.Total(),
		DistributionUpdatedAt: timestamppb.New(entry.BreedDistributionUpdated),
		Odds:                  make([]*tenancyv1.BreedOdds, 0, len(odds)),
	}
	for _, o := range odds {
		out.Odds = append(out.Odds, &tenancyv1.BreedOdds{
			Species:     o.Species,
			Probability: o.Probability,
			Rarity:      o.Rarity,
		})
	}

	span.SetAttributes(
		attribute.Int("chora.familiar_egg.odds_count", len(out.Odds)),
		attribute.Float64("chora.familiar_egg.total_weight", out.TotalWeight),
	)
	return out, nil
}
