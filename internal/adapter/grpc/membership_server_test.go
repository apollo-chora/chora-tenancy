// membership_server_test.go — TDD coverage for the gRPC adapter wrapping
// the ListMembershipsByGCID RPC (Bucket 1, 2026-05-14 multi-tenant identity).
//
// Coverage classes (per the user-stated arch invariant: one GCID, many
// tenants, many roles per tenant):
//
//   - Happy path single-tenant: gcid with one membership returns one row +
//     default_tenant_id = that tenant.
//   - Multi-tenant: gcid with N memberships returns N rows; default tenant
//     is the most-recently-active one (max joined_at, falling back to
//     invited_at).
//   - Multi-role: a single membership with [learner, author] surfaces both
//     roles in the response + dedupes surfaces[] (learner→[aplus, cplus] +
//     author→[aplus] = [aplus, cplus]).
//   - Empty memberships: gcid with NO memberships returns empty list (NOT
//     an error per ddd-enforcement — caller surfaces as HTTP 403).
//   - Invalid argument: empty gcid → codes.InvalidArgument.
//   - Repo error surfaces as codes.Internal.
//   - Role → surfaces map: each canonical role yields the documented
//     surface set (asserted via a table-driven test on the exported helper).
//
// Tests use direct method invocation (no real network) — matches the
// established familiar_egg_grpc_server_test pattern.
package grpc_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/tenancy/v1"

	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
)

// fakeMembershipLister is a test stub for the MembershipLister port.
type fakeMembershipLister struct {
	rows     []tenancygrpc.MembershipRow
	err      error
	lastGCID string
	// lastIncludeSuspended records what the server passed down, so a test can
	// prove the request field is PLUMBED rather than defaulted. Without this
	// the server could ignore the field entirely and every test would agree.
	lastIncludeSuspended bool
}

func (f *fakeMembershipLister) ListByGCID(_ context.Context, gcid string, includeSuspended bool) ([]tenancygrpc.MembershipRow, error) {
	f.lastGCID = gcid
	f.lastIncludeSuspended = includeSuspended
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

// -----------------------------------------------------------------------------
// ListMembershipsByGCID
// -----------------------------------------------------------------------------

func TestListMembershipsByGCID_HappyPath_SingleTenant(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := &fakeMembershipLister{
		rows: []tenancygrpc.MembershipRow{{
			TenantID:   "01970000-0000-7000-8000-000000000001",
			TenantSlug: "mightymind-academy",
			Roles:      []string{"learner"},
			JoinedAt:   now,
		}},
	}
	srv := tenancygrpc.NewMembershipServer(repo)

	resp, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "01970000-0000-7000-8000-0000000000aa",
	})
	if err != nil {
		t.Fatalf("ListMembershipsByGCID: %v", err)
	}
	if len(resp.GetMemberships()) != 1 {
		t.Fatalf("memberships len = %d, want 1", len(resp.GetMemberships()))
	}
	m := resp.GetMemberships()[0]
	if m.GetTenantId() != "01970000-0000-7000-8000-000000000001" {
		t.Errorf("tenant_id = %q", m.GetTenantId())
	}
	if m.GetTenantSlug() != "mightymind-academy" {
		t.Errorf("tenant_slug = %q", m.GetTenantSlug())
	}
	if got, want := m.GetRoles(), []string{"learner"}; !equalStrings(got, want) {
		t.Errorf("roles = %v want %v", got, want)
	}
	if got, want := sortedStrings(m.GetSurfaces()), []string{"aplus", "cplus"}; !equalStrings(got, want) {
		t.Errorf("surfaces = %v want %v", got, want)
	}
	if !m.GetIsDefault() {
		t.Errorf("single membership must be flagged is_default=true")
	}
	if resp.GetDefaultTenantId() != m.GetTenantId() {
		t.Errorf("default_tenant_id = %q, want %q", resp.GetDefaultTenantId(), m.GetTenantId())
	}
}

func TestListMembershipsByGCID_MultiTenant_DefaultIsMostRecentlyActive(t *testing.T) {
	t.Parallel()
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	repo := &fakeMembershipLister{
		rows: []tenancygrpc.MembershipRow{
			{
				TenantID:   "01970000-0000-7000-8000-000000000001",
				TenantSlug: "academy-A",
				Roles:      []string{"learner", "author"},
				JoinedAt:   older,
			},
			{
				TenantID:   "01970000-0000-7000-8000-000000000002",
				TenantSlug: "academy-B",
				Roles:      []string{"instructor", "tenant_admin"},
				JoinedAt:   newer,
			},
		},
	}
	srv := tenancygrpc.NewMembershipServer(repo)

	resp, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "01970000-0000-7000-8000-0000000000aa",
	})
	if err != nil {
		t.Fatalf("ListMembershipsByGCID: %v", err)
	}
	if len(resp.GetMemberships()) != 2 {
		t.Fatalf("memberships len = %d, want 2", len(resp.GetMemberships()))
	}
	// Default tenant is academy-B (newer joined_at).
	if resp.GetDefaultTenantId() != "01970000-0000-7000-8000-000000000002" {
		t.Errorf("default_tenant_id = %q, want academy-B", resp.GetDefaultTenantId())
	}
	// Exactly one membership has is_default=true.
	var defaults int
	for _, m := range resp.GetMemberships() {
		if m.GetIsDefault() {
			defaults++
			if m.GetTenantId() != "01970000-0000-7000-8000-000000000002" {
				t.Errorf("is_default flagged on wrong tenant: %q", m.GetTenantId())
			}
		}
	}
	if defaults != 1 {
		t.Errorf("exactly one membership must have is_default=true, got %d", defaults)
	}
}

func TestListMembershipsByGCID_MultiRole_SurfaceUnionDeduped(t *testing.T) {
	t.Parallel()
	repo := &fakeMembershipLister{
		rows: []tenancygrpc.MembershipRow{{
			TenantID:   "01970000-0000-7000-8000-000000000001",
			TenantSlug: "academy-A",
			// learner→[aplus,cplus], author→[aplus] — union dedup → [aplus,cplus]
			Roles:    []string{"learner", "author"},
			JoinedAt: time.Now().UTC(),
		}},
	}
	srv := tenancygrpc.NewMembershipServer(repo)

	resp, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "01970000-0000-7000-8000-0000000000aa",
	})
	if err != nil {
		t.Fatalf("ListMembershipsByGCID: %v", err)
	}
	if len(resp.GetMemberships()) != 1 {
		t.Fatalf("memberships len = %d, want 1", len(resp.GetMemberships()))
	}
	got := sortedStrings(resp.GetMemberships()[0].GetSurfaces())
	want := []string{"aplus", "cplus"}
	if !equalStrings(got, want) {
		t.Errorf("surfaces = %v want %v (learner+author dedup)", got, want)
	}
}

func TestListMembershipsByGCID_EmptyMemberships_NotAnError(t *testing.T) {
	t.Parallel()
	repo := &fakeMembershipLister{rows: nil}
	srv := tenancygrpc.NewMembershipServer(repo)

	resp, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "01970000-0000-7000-8000-0000000000aa",
	})
	if err != nil {
		t.Fatalf("empty memberships must NOT error: %v", err)
	}
	if len(resp.GetMemberships()) != 0 {
		t.Errorf("memberships should be empty, got %d", len(resp.GetMemberships()))
	}
	if resp.GetDefaultTenantId() != "" {
		t.Errorf("default_tenant_id must be empty when no memberships, got %q", resp.GetDefaultTenantId())
	}
}

func TestListMembershipsByGCID_InvalidArgument_EmptyGCID(t *testing.T) {
	t.Parallel()
	repo := &fakeMembershipLister{}
	srv := tenancygrpc.NewMembershipServer(repo)

	_, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "  ",
	})
	if err == nil {
		t.Fatalf("expected InvalidArgument for empty gcid")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v want InvalidArgument", status.Code(err))
	}
}

func TestListMembershipsByGCID_RepoError_SurfacesAsInternal(t *testing.T) {
	t.Parallel()
	repo := &fakeMembershipLister{err: errors.New("pg: connection refused")}
	srv := tenancygrpc.NewMembershipServer(repo)

	_, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
		Gcid: "01970000-0000-7000-8000-0000000000aa",
	})
	if err == nil {
		t.Fatalf("expected Internal err")
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("got code %v want Internal", status.Code(err))
	}
}

// -----------------------------------------------------------------------------
// RolesToSurfaces — table-driven test on the exported helper.
// -----------------------------------------------------------------------------

func TestRolesToSurfaces(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		roles []string
		want  []string
	}{
		{"learner only", []string{"learner"}, []string{"aplus", "cplus"}},
		{"author only", []string{"author"}, []string{"aplus"}},
		{"instructor only", []string{"instructor"}, []string{"rplus"}},
		{"tenant_admin only", []string{"tenant_admin"}, []string{"hplus"}},
		{"auditor only", []string{"auditor"}, []string{"oplus"}},
		{"learner+author dedup", []string{"learner", "author"}, []string{"aplus", "cplus"}},
		{"super-multi", []string{"learner", "author", "instructor", "tenant_admin", "auditor"},
			[]string{"aplus", "cplus", "hplus", "oplus", "rplus"}},
		{"unknown role is ignored", []string{"learner", "ufo_pilot"}, []string{"aplus", "cplus"}},
		{"empty roles", []string{}, []string{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := sortedStrings(tenancygrpc.RolesToSurfaces(c.roles))
			want := sortedStrings(c.want)
			if !equalStrings(got, want) {
				t.Errorf("RolesToSurfaces(%v) = %v want %v", c.roles, got, want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// RolesToSurfaces totality over the canonical role contract (ADR-239 D5).
// -----------------------------------------------------------------------------

// TestRolesToSurfaces_TotalOverCanonicalRoleContract pins the role-to-surface
// derivation TOTAL over the platform's canonical role vocabulary (the 11
// chora_identity role_catalog tokens, migs 0014 + 0020 + 0035; mirrored by
// chora-identity domain/identity/membership.go CanonicalRoles) plus the map's
// legacy alias tokens. Every token gets an EXPLICIT routing decision here:
// either its surfaces, or a documented reason it contributes none from THIS
// membership-keyed map. House rule: enum maps must be total over the
// contract; a new canonical role must fail this test until a routing
// decision is recorded (the gap ADR-239 exists to close for PROCTOR).
//
// The "none from this map" rows are DECISIONS, not omissions (ADR-239):
//   - support_agent: no surface decision exists yet (ADR-239 O3).
//   - training_admin: a derived session label over an instructor membership
//     (ADR-239 D1; identity grant lane substitutes INSTRUCTOR, gateway mint
//     stamps the label). It never stands alone in a members row, so it
//     routes via its paired instructor entry.
//   - proctor: a JWT-extension claim, never a members row (ADR-191 D1 /
//     identity mig 0035). Surface routing happens at the session layer
//     (ADR-239 D2), mirroring platform_operator.
//   - platform_operator: JWT-only (ADR-165, no tenant_memberships row);
//     surfaces come from the gateway mint full-rail widening plus the FE
//     surfaceGuard god-mode check, never from this map.
func TestRolesToSurfaces_TotalOverCanonicalRoleContract(t *testing.T) {
	t.Parallel()

	// The 11 canonical role_catalog tokens (lowercase form; the helper
	// lowercases input so the canonical UPPERCASE JWT form is asserted in a
	// second pass below).
	contract := map[string][]string{
		"learner":           {"aplus", "cplus"},
		"author":            {"aplus"},
		"instructor":        {"rplus"},
		"admin":             {"hplus"},
		"auditor":           {"oplus"},
		"owner":             {"hplus"},
		"support_agent":     {}, // no surface decision yet (ADR-239 O3)
		"training_admin":    {}, // derived label; routes via paired instructor row (ADR-239 D1)
		"tenant_admin":      {"hplus"},
		"proctor":           {}, // JWT-extension; session-layer routing (ADR-239 D2, mig 0035)
		"platform_operator": {}, // JWT-only; mint widening + FE god-mode (ADR-165)
	}

	// Legacy alias tokens carried by the map for forward-compat vocabulary.
	aliases := map[string][]string{
		"content_manager": {"aplus"},
		"trainer":         {"rplus"},
		"supervisor":      {"rplus"},
		"super_admin":     {"hplus"},
		"platform_ops":    {"oplus"},
	}

	for token, want := range contract {
		token, want := token, want
		t.Run("canonical/"+token, func(t *testing.T) {
			t.Parallel()
			got := sortedStrings(tenancygrpc.RolesToSurfaces([]string{token}))
			if !equalStrings(got, sortedStrings(want)) {
				t.Errorf("RolesToSurfaces(%q) = %v want %v", token, got, want)
			}
			// The canonical UPPERCASE JWT form must resolve identically
			// (the helper trims + lowercases before lookup).
			upper := sortedStrings(tenancygrpc.RolesToSurfaces([]string{strings.ToUpper(token)}))
			if !equalStrings(upper, sortedStrings(want)) {
				t.Errorf("RolesToSurfaces(%q) = %v want %v (case-insensitive contract)", strings.ToUpper(token), upper, want)
			}
		})
	}

	for token, want := range aliases {
		token, want := token, want
		t.Run("alias/"+token, func(t *testing.T) {
			t.Parallel()
			got := sortedStrings(tenancygrpc.RolesToSurfaces([]string{token}))
			if !equalStrings(got, sortedStrings(want)) {
				t.Errorf("RolesToSurfaces(%q) = %v want %v", token, got, want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// _ keeps strings import referenced if helper code paths get pruned.
var _ = strings.TrimSpace

// -----------------------------------------------------------------------------
// include_suspended (E3 slice 8)
// -----------------------------------------------------------------------------

// TestListMembershipsByGCID_IncludeSuspended_IsPlumbedNotDefaulted proves the
// request field reaches the repository. A server that ignored the field would
// pass every other test in this file, because they all leave it unset and the
// proto3 zero value is the historical behaviour.
func TestListMembershipsByGCID_IncludeSuspended_IsPlumbedNotDefaulted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		send bool
	}{
		{"unset stays false, so mint keeps excluding suspended members", false},
		{"true reaches the repo, so the closure pre-flight sees a suspended owner", true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeMembershipLister{}
			srv := tenancygrpc.NewMembershipServer(repo)

			if _, err := srv.ListMembershipsByGCID(context.Background(), &tenancyv1.ListMembershipsByGCIDRequest{
				Gcid:             "01970000-0000-7000-8000-0000000000aa",
				IncludeSuspended: tc.send,
			}); err != nil {
				t.Fatalf("ListMembershipsByGCID: %v", err)
			}
			if repo.lastIncludeSuspended != tc.send {
				t.Fatalf("repo received includeSuspended=%v, want %v; the request field is not plumbed through the server",
					repo.lastIncludeSuspended, tc.send)
			}
		})
	}
}
