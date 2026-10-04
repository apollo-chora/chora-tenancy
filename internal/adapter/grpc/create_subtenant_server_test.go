// create_subtenant_server_test.go — ADR-217 Phase 2.1 (CHO-2006). Drives the
// CreateSubTenant gRPC adapter via a fake SubTenantCreator: validation, proto
// mapping, hosting-mode defaulting, and error-code translation.
package grpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"
)

// stubLister satisfies tenancygrpc.MembershipLister so the server can be built;
// CreateSubTenant never calls it.
type stubLister struct{}

func (stubLister) ListByGCID(context.Context, string, bool) ([]tenancygrpc.MembershipRow, error) {
	return nil, nil
}

type fakeSubTenantCreator struct {
	out       *bootstrap.Output
	err       error
	calls     int
	lastInput bootstrap.Input
}

func (f *fakeSubTenantCreator) CreateSubTenant(_ context.Context, in bootstrap.Input) (*bootstrap.Output, error) {
	f.calls++
	f.lastInput = in
	return f.out, f.err
}

func newSubTenantServer(fc tenancygrpc.SubTenantCreator) *tenancygrpc.MembershipServer {
	return tenancygrpc.NewMembershipServer(stubLister{}).WithSubTenantCreator(fc)
}

const (
	testParent = "00000000-0000-7000-8000-000000000001"
	testOwner  = "019eded8-0000-7000-8000-000000000abc"
)

func TestCreateSubTenant_happyPath_mapsRequestAndResponse(t *testing.T) {
	created := time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
	fc := &fakeSubTenantCreator{out: &bootstrap.Output{TenantID: "child-tenant-id", CreatedAt: created}}
	srv := newSubTenantServer(fc)

	resp, err := srv.CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Bishan Branch",
		HostingMode:    tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE,
		OwnerGcid:      testOwner,
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	// Domain received the mapped input.
	if fc.lastInput.ParentTenantID != testParent || fc.lastInput.OwnerGCID != testOwner ||
		fc.lastInput.HostingMode != bootstrap.HostingModeFranchise || fc.lastInput.Name != "Bishan Branch" {
		t.Errorf("mapped input wrong: %+v", fc.lastInput)
	}
	// Response Tenant reflects the persisted child.
	tn := resp.GetTenant()
	if tn.GetTenantId() != "child-tenant-id" {
		t.Errorf("tenant_id = %q", tn.GetTenantId())
	}
	if tn.GetParentTenantId() != testParent {
		t.Errorf("parent_tenant_id = %q", tn.GetParentTenantId())
	}
	if tn.GetHostingMode() != tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE {
		t.Errorf("hosting_mode = %v", tn.GetHostingMode())
	}
	if tn.GetOwnerGcid() != testOwner {
		t.Errorf("owner_gcid = %q", tn.GetOwnerGcid())
	}
	if tn.GetState() != tenancyv1.TenantState_TENANT_STATE_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", tn.GetState())
	}
	if !tn.GetCreatedAt().AsTime().Equal(created) {
		t.Errorf("created_at = %v, want %v", tn.GetCreatedAt().AsTime(), created)
	}
}

func TestCreateSubTenant_unspecifiedHostingMode_defaultsFranchise(t *testing.T) {
	fc := &fakeSubTenantCreator{out: &bootstrap.Output{TenantID: "id", CreatedAt: time.Now().UTC()}}
	srv := newSubTenantServer(fc)
	resp, err := srv.CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent, DisplayName: "X", OwnerGcid: testOwner,
		// HostingMode UNSPECIFIED
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fc.lastInput.HostingMode != bootstrap.HostingModeFranchise {
		t.Errorf("domain hosting_mode = %q, want FRANCHISE", fc.lastInput.HostingMode)
	}
	if resp.GetTenant().GetHostingMode() != tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE {
		t.Errorf("response hosting_mode = %v, want FRANCHISE", resp.GetTenant().GetHostingMode())
	}
}

func TestCreateSubTenant_validation_returnsInvalidArgument(t *testing.T) {
	cases := map[string]*tenancyv1.CreateSubTenantRequest{
		"empty parent": {DisplayName: "X", OwnerGcid: testOwner},
		"empty name":   {ParentTenantId: testParent, OwnerGcid: testOwner},
		"empty owner":  {ParentTenantId: testParent, DisplayName: "X"},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			fc := &fakeSubTenantCreator{out: &bootstrap.Output{}}
			srv := newSubTenantServer(fc)
			_, err := srv.CreateSubTenant(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("want InvalidArgument, got %v", err)
			}
			if fc.calls != 0 {
				t.Errorf("creator must not be called on invalid input; got %d", fc.calls)
			}
		})
	}
}

func TestCreateSubTenant_domainInvalidArgument_mapsInvalidArgument(t *testing.T) {
	fc := &fakeSubTenantCreator{err: bootstrap.ErrInvalidArgument}
	srv := newSubTenantServer(fc)
	_, err := srv.CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent, DisplayName: "X", OwnerGcid: testOwner, HostingMode: tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestCreateSubTenant_persistError_mapsFailedPrecondition(t *testing.T) {
	// e.g. the migration-0026 parent-invariant trigger rejects a foreign-root parent.
	fc := &fakeSubTenantCreator{err: errors.New("ERROR: parent_tenant_id must resolve to chora-master (SQLSTATE 23514)")}
	srv := newSubTenantServer(fc)
	_, err := srv.CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent, DisplayName: "X", OwnerGcid: testOwner, HostingMode: tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

func TestCreateSubTenant_nilCreator_returnsUnimplemented(t *testing.T) {
	srv := tenancygrpc.NewMembershipServer(stubLister{}) // no WithSubTenantCreator
	_, err := srv.CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent, DisplayName: "X", OwnerGcid: testOwner,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
}
