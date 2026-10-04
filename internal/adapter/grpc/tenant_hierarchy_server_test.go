// tenant_hierarchy_server_test.go — ADR-217 Phase 2.3b (CHO-2010). Drives the
// GetTenantHierarchy gRPC adapter via a fake HierarchyReader: validation, proto
// mapping, and error-code translation. (stubLister is shared from
// create_subtenant_server_test.go — same grpc_test package.)
package grpc_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"
)

type fakeHierarchyReader struct {
	out        tenancygrpc.TenantHierarchy
	err        error
	calls      int
	lastTenant string
}

func (f *fakeHierarchyReader) GetHierarchy(_ context.Context, tenantID string) (tenancygrpc.TenantHierarchy, error) {
	f.calls++
	f.lastTenant = tenantID
	return f.out, f.err
}

func newHierarchyServer(hr tenancygrpc.HierarchyReader) *tenancygrpc.MembershipServer {
	return tenancygrpc.NewMembershipServer(stubLister{}).WithHierarchyReader(hr)
}

const hTestTenant = "00000000-0000-7000-8000-000000000001" // chora-master

func TestGetTenantHierarchy_happyPath_parentMapsChildren(t *testing.T) {
	fr := &fakeHierarchyReader{out: tenancygrpc.TenantHierarchy{
		IsParent: true,
		Children: []tenancygrpc.ChildTenantSummary{
			{TenantID: "11111111-0000-7000-8000-000000000001", TenantName: "Mighty Mind", UserCount: 7, AtomCount: 0},
			{TenantID: "22222222-0000-7000-8000-000000000002", TenantName: "Chen's Coaching", UserCount: 3, AtomCount: 0},
		},
	}}
	srv := newHierarchyServer(fr)

	resp, err := srv.GetTenantHierarchy(context.Background(), &tenancyv1.GetTenantHierarchyRequest{TenantId: hTestTenant})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if fr.lastTenant != hTestTenant {
		t.Errorf("reader got tenant %q, want %q", fr.lastTenant, hTestTenant)
	}
	if !resp.GetIsParent() {
		t.Error("is_parent = false, want true")
	}
	if len(resp.GetChildren()) != 2 {
		t.Fatalf("children len = %d, want 2", len(resp.GetChildren()))
	}
	c0 := resp.GetChildren()[0]
	if c0.GetTenantId() != "11111111-0000-7000-8000-000000000001" || c0.GetTenantName() != "Mighty Mind" ||
		c0.GetUserCount() != 7 || c0.GetAtomCount() != 0 {
		t.Errorf("child[0] mapped wrong: %+v", c0)
	}
}

func TestGetTenantHierarchy_happyPath_leafEmpty(t *testing.T) {
	fr := &fakeHierarchyReader{out: tenancygrpc.TenantHierarchy{IsParent: false, Children: nil}}
	srv := newHierarchyServer(fr)
	resp, err := srv.GetTenantHierarchy(context.Background(), &tenancyv1.GetTenantHierarchyRequest{TenantId: hTestTenant})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetIsParent() {
		t.Error("is_parent = true, want false")
	}
	if len(resp.GetChildren()) != 0 {
		t.Errorf("children len = %d, want 0", len(resp.GetChildren()))
	}
}

func TestGetTenantHierarchy_validation_invalidArgument(t *testing.T) {
	cases := map[string]string{
		"empty":    "",
		"non-uuid": "not-a-uuid",
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			fr := &fakeHierarchyReader{}
			srv := newHierarchyServer(fr)
			_, err := srv.GetTenantHierarchy(context.Background(), &tenancyv1.GetTenantHierarchyRequest{TenantId: id})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("want InvalidArgument, got %v", err)
			}
			if fr.calls != 0 {
				t.Errorf("reader must not be called on invalid input; got %d", fr.calls)
			}
		})
	}
}

func TestGetTenantHierarchy_readerError_mapsInternal(t *testing.T) {
	fr := &fakeHierarchyReader{err: errors.New("boom")}
	srv := newHierarchyServer(fr)
	_, err := srv.GetTenantHierarchy(context.Background(), &tenancyv1.GetTenantHierarchyRequest{TenantId: hTestTenant})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal, got %v", err)
	}
}

func TestGetTenantHierarchy_nilReader_returnsUnimplemented(t *testing.T) {
	srv := tenancygrpc.NewMembershipServer(stubLister{}) // no WithHierarchyReader
	_, err := srv.GetTenantHierarchy(context.Background(), &tenancyv1.GetTenantHierarchyRequest{TenantId: hTestTenant})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
}
