// membership_owner_reason_test.go: S7-B1, the two ownership refusals must
// reach the caller as a NAMED reason, not as prose.
//
// Both refusals share FAILED_PRECONDITION with the pre-existing suspension
// guard, so a status code alone cannot tell them apart, and chora-identity
// has to turn each into a different HTTP 409 with a different message. The
// mechanism is errdetails.ErrorInfo (AIP-193): a machine-readable `reason`
// plus a `domain`, carried in the status details and therefore across the
// wire. Matching on the status MESSAGE would work today and break the first
// time somebody rewords it.
package grpc_test

import (
	"context"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
)

// reasonOf extracts the errdetails.ErrorInfo reason from a gRPC error, or ""
// when the error carries none.
func reasonOf(t *testing.T, err error) (reason, domain string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err = %v, want a gRPC status", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason(), info.GetDomain()
		}
	}
	return "", ""
}

func TestRemoveMembership_LastOwnerCarriesNamedReason(t *testing.T) {
	t.Parallel()
	f := &fakeMembershipUpserter{err: tenancygrpc.ErrLastOwnerProtected}
	_, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: choraMasterID,
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", got)
	}
	reason, domain := reasonOf(t, err)
	if reason != tenancygrpc.ReasonLastOwnerProtected {
		t.Errorf("reason = %q, want %q", reason, tenancygrpc.ReasonLastOwnerProtected)
	}
	if domain != tenancygrpc.ErrorInfoDomain {
		t.Errorf("domain = %q, want %q", domain, tenancygrpc.ErrorInfoDomain)
	}
}

func TestUpsertMembership_OwnerStripCarriesNamedReason(t *testing.T) {
	t.Parallel()
	f := &fakeMembershipUpserter{err: tenancygrpc.ErrOwnerRoleProtected}
	_, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:            upsertGcid,
		TenantId:        choraMasterID,
		Roles:           []string{"instructor"},
		ReplaceExisting: true,
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", got)
	}
	reason, domain := reasonOf(t, err)
	if reason != tenancygrpc.ReasonOwnerRoleProtected {
		t.Errorf("reason = %q, want %q", reason, tenancygrpc.ReasonOwnerRoleProtected)
	}
	if domain != tenancygrpc.ErrorInfoDomain {
		t.Errorf("domain = %q, want %q", domain, tenancygrpc.ErrorInfoDomain)
	}
}

// The suspension refusal predates this work and shares the status code, so it
// must carry its own reason too. Without one the client's mapping would have
// to be "whatever is left over is suspended", which is exactly the shape that
// silently mislabels the next refusal somebody adds.
func TestMembershipRefusals_SuspendedAlsoCarriesAReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		call func(*tenancygrpc.MembershipServer) error
	}{
		{
			name: "RemoveMembership",
			call: func(s *tenancygrpc.MembershipServer) error {
				_, err := s.RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
					Gcid: upsertGcid, TenantId: choraMasterID,
				})
				return err
			},
		},
		{
			name: "UpsertMembership",
			call: func(s *tenancygrpc.MembershipServer) error {
				_, err := s.UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
					Gcid: upsertGcid, TenantId: choraMasterID, Roles: []string{"learner"},
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.call(newUpsertServer(&fakeMembershipUpserter{err: tenancygrpc.ErrMembershipSuspended}))
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("code = %v, want FailedPrecondition", got)
			}
			if reason, _ := reasonOf(t, err); reason != tenancygrpc.ReasonMembershipSuspended {
				t.Errorf("reason = %q, want %q", reason, tenancygrpc.ReasonMembershipSuspended)
			}
		})
	}
}

// The refusals must not leak into the grantable vocabulary: `owner` stays
// non-upsertable, so no caller can route around the guard by simply asking
// for the owner role back.
func TestUpsertMembership_OwnerStillNotGrantable(t *testing.T) {
	t.Parallel()
	_, err := newUpsertServer(&fakeMembershipUpserter{}).UpsertMembership(
		context.Background(), &tenancyv1.UpsertMembershipRequest{
			Gcid: upsertGcid, TenantId: choraMasterID, Roles: []string{"owner"},
		})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument for role %q", got, "owner")
	}
}
