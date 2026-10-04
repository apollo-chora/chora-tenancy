// create_subtenant_addons_test.go: E1 (UX refactor wave W2), the RPC half of
// granting chosen add-ons at creation.
//
// The domain and the repository already write durable add_on_subscriptions
// rows for whatever codes reach bootstrap.Input. This is the seam that decides
// whether any code ever does: a request field that the server drops on the
// floor would leave the whole feature looking implemented and doing nothing,
// with a green test suite underneath it.
//
// An unrecognised code is a caller error, not a state conflict, so it must
// leave as INVALID_ARGUMENT and reach the operator as a 400. The existing
// create path maps every persist failure to FAILED_PRECONDITION and therefore
// 409, which for a mistyped add-on code would tell the operator "this parent
// cannot take a new organisation" about a problem in a different field.
package grpc_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
)

func TestCreateSubTenant_PassesAddOnCodesToTheDomain(t *testing.T) {
	fc := &fakeSubTenantCreator{out: &bootstrap.Output{TenantID: testParent}}
	if _, err := newSubTenantServer(fc).CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Northwind Academy",
		OwnerGcid:      testOwner,
		AddOnCodes:     []string{"tms", "cms"},
	}); err != nil {
		t.Fatalf("CreateSubTenant: %v", err)
	}
	if got, want := fc.lastInput.AddOnCodes, []string{"tms", "cms"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("AddOnCodes reaching the domain = %v, want %v; a dropped field leaves the whole "+
			"feature looking implemented and doing nothing", got, want)
	}
}

func TestCreateSubTenant_NoAddOnCodesIsUnchangedBehaviour(t *testing.T) {
	// The regression fence: every existing caller sends nothing.
	fc := &fakeSubTenantCreator{out: &bootstrap.Output{TenantID: testParent}}
	if _, err := newSubTenantServer(fc).CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Northwind Academy",
		OwnerGcid:      testOwner,
	}); err != nil {
		t.Fatalf("CreateSubTenant: %v", err)
	}
	if len(fc.lastInput.AddOnCodes) != 0 {
		t.Fatalf("AddOnCodes = %v, want none", fc.lastInput.AddOnCodes)
	}
}

func TestCreateSubTenant_BlankAddOnCodeIsInvalidArgument(t *testing.T) {
	// bootstrap.New refuses a blank entry with ErrInvalidArgument, and the
	// existing arm already maps that to INVALID_ARGUMENT. Pinned because the
	// blank arrives from a checkbox list, which is exactly where an empty
	// string sneaks in.
	fc := &fakeSubTenantCreator{err: bootstrap.ErrInvalidArgument}
	_, err := newSubTenantServer(fc).CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Northwind Academy",
		OwnerGcid:      testOwner,
		AddOnCodes:     []string{""},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", got)
	}
}

func TestCreateSubTenant_UnknownAddOnCodeIsInvalidArgumentNotPrecondition(t *testing.T) {
	// The repository refuses an unrecognised code with a named error. Left on
	// the default arm it would surface as FAILED_PRECONDITION and reach the
	// operator as a 409 saying the PARENT cannot take a new organisation,
	// which is a true sentence about the wrong field. It is a caller error in
	// add_on_codes, so it must be 400.
	fc := &fakeSubTenantCreator{
		// Exactly the shape the service produces: the repository wraps the
		// sentinel, the service wraps that with %w, so errors.Is sees through.
		err: fmt.Errorf("bootstrap: persist sub-tenant: %w %q", bootstrap.ErrUnknownAddOnCode, "not-a-real-add-on"),
	}
	_, err := newSubTenantServer(fc).CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Northwind Academy",
		OwnerGcid:      testOwner,
		AddOnCodes:     []string{"not-a-real-add-on"},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument for an unknown add-on code", got)
	}
	if st, _ := status.FromError(err); !containsCode(st.Message(), "not-a-real-add-on") {
		t.Errorf("message %q must name the offending code, or the operator cannot tell which "+
			"checkbox was wrong", st.Message())
	}
}

// A genuine parent-invariant rejection must KEEP its FAILED_PRECONDITION. The
// new arm must not swallow the case it sits next to.
func TestCreateSubTenant_ParentRejectionStaysFailedPrecondition(t *testing.T) {
	fc := &fakeSubTenantCreator{err: errors.New("bootstrap: persist sub-tenant: parent outside the chora-master tree")}
	_, err := newSubTenantServer(fc).CreateSubTenant(context.Background(), &tenancyv1.CreateSubTenantRequest{
		ParentTenantId: testParent,
		DisplayName:    "Northwind Academy",
		OwnerGcid:      testOwner,
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition for a parent rejection", got)
	}
}

func containsCode(msg, code string) bool {
	for i := 0; i+len(code) <= len(msg); i++ {
		if msg[i:i+len(code)] == code {
			return true
		}
	}
	return false
}
