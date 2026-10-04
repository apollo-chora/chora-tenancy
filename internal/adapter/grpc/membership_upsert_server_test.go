// membership_upsert_server_test.go — TDD coverage for the UpsertMembership
// RPC (ADR-182 D2/D3: chora-master auto-enrolment + admin add-member
// authoritative write).
//
// Coverage classes:
//   - Happy path by slug: resolve "chora-master" → tenant id; upsert
//     [learner, author]; response membership carries aggregated roles +
//     derived surfaces + created=true.
//   - Happy path by tenant_id: no slug resolution; created=false when all
//     rows pre-existed (idempotent re-call).
//   - Validation: empty gcid / non-uuid gcid / both tenant_id+slug /
//     neither / empty roles / "owner" / unknown role → InvalidArgument.
//   - Unknown slug → NotFound.
//   - Suspended or soft-deleted membership → FailedPrecondition (no
//     resurrection by upsert).
//   - Repo error → Internal.
//   - Writer not wired (read-only server constructor) → Unimplemented.
//
// Direct method invocation, no network — membership_server_test pattern.
package grpc_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/apollo-chora/chora-tenancy/internal/adapter/grpc"
)

const (
	upsertGcid        = "018f6f3a-0000-7000-8000-000000000abc"
	choraMasterID     = "00000000-0000-7000-8000-000000000001"
	upsertOtherTenant = "22222222-2222-7222-8222-222222222222"
)

// fakeMembershipUpserter is a test stub for the MembershipUpsertRepo port.
type fakeMembershipUpserter struct {
	// slug → tenant id; nil = every slug unknown
	slugs map[string]string
	// roles returned as the post-upsert aggregate
	allRoles []string
	created  bool
	err      error

	lastTenantID string
	lastGcid     string
	lastRoles    []string
	lastReplace  bool

	// RemoveAllRoles config (WS2b).
	removedCount       int
	lastRemoveTenantID string
	lastRemoveGcid     string
}

func (f *fakeMembershipUpserter) ResolveTenantIDBySlug(_ context.Context, slug string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	id, ok := f.slugs[slug]
	if !ok {
		return "", tenancygrpc.ErrTenantNotFound
	}
	return id, nil
}

func (f *fakeMembershipUpserter) UpsertRoles(_ context.Context, tenantID, gcid string, roles []string) ([]string, bool, error) {
	f.lastTenantID = tenantID
	f.lastGcid = gcid
	f.lastRoles = append([]string(nil), roles...)
	if f.err != nil {
		return nil, false, f.err
	}
	return f.allRoles, f.created, nil
}

func (f *fakeMembershipUpserter) ReplaceRoles(_ context.Context, tenantID, gcid string, roles []string) ([]string, bool, error) {
	f.lastTenantID = tenantID
	f.lastGcid = gcid
	f.lastRoles = append([]string(nil), roles...)
	f.lastReplace = true
	if f.err != nil {
		return nil, false, f.err
	}
	return f.allRoles, f.created, nil
}

func (f *fakeMembershipUpserter) RemoveAllRoles(_ context.Context, tenantID, gcid string) (int, error) {
	f.lastRemoveTenantID = tenantID
	f.lastRemoveGcid = gcid
	if f.err != nil {
		return 0, f.err
	}
	return f.removedCount, nil
}

func newUpsertServer(f *fakeMembershipUpserter) *tenancygrpc.MembershipServer {
	return tenancygrpc.NewMembershipServerWithWriter(&fakeMembershipLister{}, f)
}

// -----------------------------------------------------------------------------
// RemoveMembership (WS2b / CHO-1869) — member-centric soft-delete of all roles.
// -----------------------------------------------------------------------------

func TestRemoveMembership_HappyPath_byTenantID(t *testing.T) {
	f := &fakeMembershipUpserter{removedCount: 2}
	resp, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
	})
	if err != nil {
		t.Fatalf("RemoveMembership: %v", err)
	}
	if resp.GetRemovedRoleCount() != 2 {
		t.Errorf("removed_role_count = %d, want 2", resp.GetRemovedRoleCount())
	}
	if f.lastRemoveTenantID != upsertOtherTenant || f.lastRemoveGcid != upsertGcid {
		t.Errorf("repo called with (%q, %q)", f.lastRemoveTenantID, f.lastRemoveGcid)
	}
}

func TestRemoveMembership_bySlug(t *testing.T) {
	f := &fakeMembershipUpserter{slugs: map[string]string{"chora-master": choraMasterID}, removedCount: 1}
	resp, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:       upsertGcid,
		TenantSlug: "chora-master",
	})
	if err != nil {
		t.Fatalf("RemoveMembership: %v", err)
	}
	if resp.GetRemovedRoleCount() != 1 {
		t.Errorf("removed_role_count = %d, want 1", resp.GetRemovedRoleCount())
	}
	if f.lastRemoveTenantID != choraMasterID {
		t.Errorf("slug not resolved: repo tenant = %q want %q", f.lastRemoveTenantID, choraMasterID)
	}
}

func TestRemoveMembership_zeroRemoved_NotFound(t *testing.T) {
	f := &fakeMembershipUpserter{removedCount: 0}
	_, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (no live membership) (err=%v)", status.Code(err), err)
	}
}

func TestRemoveMembership_suspended_FailedPrecondition(t *testing.T) {
	f := &fakeMembershipUpserter{err: tenancygrpc.ErrMembershipSuspended}
	_, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition (suspended) (err=%v)", status.Code(err), err)
	}
}

func TestRemoveMembership_validation(t *testing.T) {
	srv := newUpsertServer(&fakeMembershipUpserter{})
	cases := []struct {
		name string
		req  *tenancyv1.RemoveMembershipRequest
	}{
		{"missing gcid", &tenancyv1.RemoveMembershipRequest{TenantId: upsertOtherTenant}},
		{"neither tenant", &tenancyv1.RemoveMembershipRequest{Gcid: upsertGcid}},
		{"both tenant", &tenancyv1.RemoveMembershipRequest{Gcid: upsertGcid, TenantId: upsertOtherTenant, TenantSlug: "x"}},
		{"bad gcid", &tenancyv1.RemoveMembershipRequest{Gcid: "not-a-uuid", TenantId: upsertOtherTenant}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := srv.RemoveMembership(context.Background(), c.req); status.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestRemoveMembership_unknownSlug_NotFound(t *testing.T) {
	f := &fakeMembershipUpserter{slugs: map[string]string{}} // every slug unknown
	_, err := newUpsertServer(f).RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:       upsertGcid,
		TenantSlug: "ghost",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (unknown slug) (err=%v)", status.Code(err), err)
	}
}

func TestRemoveMembership_unimplementedWithoutWriter(t *testing.T) {
	srv := tenancygrpc.NewMembershipServer(&fakeMembershipLister{}) // read-only ctor
	_, err := srv.RemoveMembership(context.Background(), &tenancyv1.RemoveMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
	}
}

func TestUpsertMembership_HappyPathBySlug(t *testing.T) {
	f := &fakeMembershipUpserter{
		slugs:    map[string]string{"chora-master": choraMasterID},
		allRoles: []string{"learner", "author"},
		created:  true,
	}
	resp, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:       upsertGcid,
		TenantSlug: "chora-master",
		Roles:      []string{"learner", "author"},
	})
	if err != nil {
		t.Fatalf("UpsertMembership: %v", err)
	}
	if !resp.GetCreated() {
		t.Errorf("created = false, want true")
	}
	m := resp.GetMembership()
	if m.GetTenantId() != choraMasterID {
		t.Errorf("tenant_id = %q, want %q", m.GetTenantId(), choraMasterID)
	}
	if m.GetTenantSlug() != "chora-master" {
		t.Errorf("tenant_slug = %q, want chora-master", m.GetTenantSlug())
	}
	gotRoles := append([]string(nil), m.GetRoles()...)
	sort.Strings(gotRoles)
	if len(gotRoles) != 2 || gotRoles[0] != "author" || gotRoles[1] != "learner" {
		t.Errorf("roles = %v, want [author learner]", gotRoles)
	}
	// learner→[aplus,cplus] + author→[aplus] = [aplus,cplus]
	gotSurfaces := append([]string(nil), m.GetSurfaces()...)
	sort.Strings(gotSurfaces)
	if len(gotSurfaces) != 2 || gotSurfaces[0] != "aplus" || gotSurfaces[1] != "cplus" {
		t.Errorf("surfaces = %v, want [aplus cplus]", gotSurfaces)
	}
	if f.lastTenantID != choraMasterID || f.lastGcid != upsertGcid {
		t.Errorf("repo called with (%q, %q)", f.lastTenantID, f.lastGcid)
	}
}

func TestUpsertMembership_HappyPathByTenantIDIdempotent(t *testing.T) {
	f := &fakeMembershipUpserter{
		allRoles: []string{"learner"},
		created:  false,
	}
	resp, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
		Roles:    []string{"learner"},
	})
	if err != nil {
		t.Fatalf("UpsertMembership: %v", err)
	}
	if resp.GetCreated() {
		t.Errorf("created = true, want false on idempotent re-call")
	}
	if f.lastTenantID != upsertOtherTenant {
		t.Errorf("repo tenant = %q, want %q", f.lastTenantID, upsertOtherTenant)
	}
}

func TestUpsertMembership_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *tenancyv1.UpsertMembershipRequest
	}{
		{"empty gcid", &tenancyv1.UpsertMembershipRequest{TenantId: upsertOtherTenant, Roles: []string{"learner"}}},
		{"non-uuid gcid", &tenancyv1.UpsertMembershipRequest{Gcid: "not-a-uuid", TenantId: upsertOtherTenant, Roles: []string{"learner"}}},
		{"both tenant refs", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, TenantId: upsertOtherTenant, TenantSlug: "chora-master", Roles: []string{"learner"}}},
		{"neither tenant ref", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, Roles: []string{"learner"}}},
		{"empty roles", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, TenantId: upsertOtherTenant}},
		{"owner rejected", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, TenantId: upsertOtherTenant, Roles: []string{"owner"}}},
		{"unknown role", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, TenantId: upsertOtherTenant, Roles: []string{"wizard"}}},
		{"non-uuid tenant_id", &tenancyv1.UpsertMembershipRequest{Gcid: upsertGcid, TenantId: "nope", Roles: []string{"learner"}}},
	}
	f := &fakeMembershipUpserter{slugs: map[string]string{"chora-master": choraMasterID}}
	srv := newUpsertServer(f)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := srv.UpsertMembership(context.Background(), tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestUpsertMembership_UnknownSlugNotFound(t *testing.T) {
	f := &fakeMembershipUpserter{slugs: map[string]string{}}
	_, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:       upsertGcid,
		TenantSlug: "ghost-tenant",
		Roles:      []string{"learner"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestUpsertMembership_SuspendedFailedPrecondition(t *testing.T) {
	f := &fakeMembershipUpserter{err: tenancygrpc.ErrMembershipSuspended}
	_, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
		Roles:    []string{"learner"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestUpsertMembership_RepoErrorInternal(t *testing.T) {
	f := &fakeMembershipUpserter{err: errors.New("pg down")}
	_, err := newUpsertServer(f).UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
		Roles:    []string{"learner"},
	})
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal (err=%v)", status.Code(err), err)
	}
}

func TestUpsertMembership_WriterNotWiredUnimplemented(t *testing.T) {
	srv := tenancygrpc.NewMembershipServer(&fakeMembershipLister{})
	_, err := srv.UpsertMembership(context.Background(), &tenancyv1.UpsertMembershipRequest{
		Gcid:     upsertGcid,
		TenantId: upsertOtherTenant,
		Roles:    []string{"learner"},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
	}
}
