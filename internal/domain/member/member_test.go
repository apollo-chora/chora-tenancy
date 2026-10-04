// Package member_test holds the RED-phase TDD specs for the Member
// management sub-domain inside chora-tenancy.
//
// Aligned with:
//   - Comic Ch4 P9 P1 — "12 students onboarded ... All from one screen"
//   - CLAUDE.md §1 — UUIDv7, soft delete + pseudonymise
//   - docs/design/ux_tenant_admin.md — 11-role taxonomy
//
// Bulk invite contract:
//   - Dedupe by email (case-insensitive, trimmed)
//   - Each email must have at least one role
//   - Max 1000 invites per batch (over-limit batches rejected)
//   - Returns `Invited` member records (status = "invited")
package member_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/member"
)

const (
	tenantA = "01970000-0000-7000-8000-0000000000aa"
)

// -----------------------------------------------------------------------------
// Member constructor
// -----------------------------------------------------------------------------

func TestNewMember_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()
	m, err := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	if err != nil {
		t.Fatalf("NewMember: unexpected error: %v", err)
	}
	if m.ID == "" {
		t.Fatalf("expected non-empty member ID")
	}
	if m.Email != "alice@acme.com" {
		t.Fatalf("email mismatch: got %q", m.Email)
	}
	if m.Status != member.StatusInvited {
		t.Fatalf("expected invited status, got %q", m.Status)
	}
	if len(m.Roles) != 1 || m.Roles[0] != "learner" {
		t.Fatalf("expected [learner], got %v", m.Roles)
	}
}

func TestNewMember_NormalisesEmailLowerAndTrims(t *testing.T) {
	t.Parallel()
	m, err := member.NewMember(tenantA, "  Alice@ACME.com  ", []string{"learner"})
	if err != nil {
		t.Fatalf("NewMember: unexpected error: %v", err)
	}
	if m.Email != "alice@acme.com" {
		t.Fatalf("expected normalised email, got %q", m.Email)
	}
}

func TestNewMember_RejectsEmptyEmail(t *testing.T) {
	t.Parallel()
	if _, err := member.NewMember(tenantA, "", []string{"learner"}); err == nil {
		t.Fatalf("expected error for empty email")
	}
	if _, err := member.NewMember(tenantA, "  ", []string{"learner"}); err == nil {
		t.Fatalf("expected error for whitespace email")
	}
}

func TestNewMember_RejectsInvalidEmail(t *testing.T) {
	t.Parallel()
	if _, err := member.NewMember(tenantA, "notanemail", []string{"learner"}); err == nil {
		t.Fatalf("expected error for non-email")
	}
	if _, err := member.NewMember(tenantA, "alice@", []string{"learner"}); err == nil {
		t.Fatalf("expected error for incomplete email")
	}
}

func TestNewMember_RejectsEmptyRoles(t *testing.T) {
	t.Parallel()
	if _, err := member.NewMember(tenantA, "alice@acme.com", nil); err == nil {
		t.Fatalf("expected error for nil roles")
	}
	if _, err := member.NewMember(tenantA, "alice@acme.com", []string{}); err == nil {
		t.Fatalf("expected error for empty roles")
	}
}

func TestNewMember_RejectsUnknownRole(t *testing.T) {
	t.Parallel()
	if _, err := member.NewMember(tenantA, "alice@acme.com", []string{"emperor"}); err == nil {
		t.Fatalf("expected error for unknown role")
	}
}

func TestNewMember_AcceptsAllValidRoles(t *testing.T) {
	t.Parallel()
	for _, r := range []string{
		"super_admin", "platform_ops", "tenant_admin", "instructor", "trainer",
		"supervisor", "content_manager", "support_agent", "learner", "guardian",
	} {
		if _, err := member.NewMember(tenantA, "x@acme.com", []string{r}); err != nil {
			t.Fatalf("expected role %q valid: %v", r, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Status transitions: invited → active → suspended
// -----------------------------------------------------------------------------

func TestMember_Activate_TransitionsFromInvited(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m.Activate("gcid-1")
	if m.Status != member.StatusActive {
		t.Fatalf("expected active, got %q", m.Status)
	}
	if m.GCID != "gcid-1" {
		t.Fatalf("expected gcid attached on activation")
	}
}

func TestMember_Suspend_TransitionsFromActive(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m.Activate("gcid-1")
	m.Suspend("policy violation")
	if m.Status != member.StatusSuspended {
		t.Fatalf("expected suspended, got %q", m.Status)
	}
	if m.SuspensionReason != "policy violation" {
		t.Fatalf("expected suspension reason saved")
	}
}

func TestMember_Suspend_RejectsEmptyReason(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m.Activate("gcid-1")
	if err := m.SuspendWithError("   "); err == nil {
		t.Fatalf("expected error for empty reason")
	}
}

func TestMember_UpdateRoles_ReplacesRoleList(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	if err := m.UpdateRoles([]string{"learner", "instructor"}); err != nil {
		t.Fatalf("UpdateRoles: unexpected error: %v", err)
	}
	if len(m.Roles) != 2 {
		t.Fatalf("expected 2 roles after update, got %d", len(m.Roles))
	}
}

func TestMember_UpdateRoles_RejectsEmpty(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	if err := m.UpdateRoles(nil); err == nil {
		t.Fatalf("expected error for nil roles")
	}
}

func TestMember_UpdateRoles_RejectsUnknown(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	if err := m.UpdateRoles([]string{"emperor"}); err == nil {
		t.Fatalf("expected error for unknown role")
	}
}

// -----------------------------------------------------------------------------
// Bulk invite (Comic Ch4 P9 P1 "12 students onboarded ... All from one screen")
// -----------------------------------------------------------------------------

func TestBulkInvite_DedupesEmailCaseInsensitive(t *testing.T) {
	t.Parallel()
	reqs := []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
		{Email: "ALICE@ACME.COM", Roles: []string{"learner"}},
		{Email: "bob@acme.com", Roles: []string{"learner"}},
	}
	result, err := member.BulkInvite(tenantA, reqs)
	if err != nil {
		t.Fatalf("BulkInvite: unexpected error: %v", err)
	}
	if len(result.Invited) != 2 {
		t.Fatalf("expected 2 unique invites, got %d", len(result.Invited))
	}
	if result.DuplicateCount != 1 {
		t.Fatalf("expected 1 duplicate, got %d", result.DuplicateCount)
	}
}

func TestBulkInvite_RejectsEmptyEmailRows(t *testing.T) {
	t.Parallel()
	reqs := []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
		{Email: "  ", Roles: []string{"learner"}},
	}
	result, err := member.BulkInvite(tenantA, reqs)
	if err != nil {
		t.Fatalf("BulkInvite: unexpected error: %v", err)
	}
	if len(result.Invited) != 1 {
		t.Fatalf("expected 1 invite (empty row skipped), got %d", len(result.Invited))
	}
	if result.RejectedCount != 1 {
		t.Fatalf("expected 1 rejection")
	}
}

func TestBulkInvite_RejectsRowsWithoutRoles(t *testing.T) {
	t.Parallel()
	reqs := []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
		{Email: "bob@acme.com", Roles: nil},
	}
	result, _ := member.BulkInvite(tenantA, reqs)
	if len(result.Invited) != 1 {
		t.Fatalf("expected 1 invite (bob rejected for missing roles), got %d", len(result.Invited))
	}
	if result.RejectedCount != 1 {
		t.Fatalf("expected 1 rejection for missing roles")
	}
}

// Max 1000/batch — over-limit batches rejected wholesale.
func TestBulkInvite_RejectsBatchLargerThan1000(t *testing.T) {
	t.Parallel()
	reqs := make([]member.InviteRequest, 1001)
	for i := range reqs {
		reqs[i] = member.InviteRequest{
			Email: emailFor(i),
			Roles: []string{"learner"},
		}
	}
	if _, err := member.BulkInvite(tenantA, reqs); err == nil {
		t.Fatalf("expected error for 1001-row batch")
	}
	if _, err := member.BulkInvite(tenantA, reqs); err != member.ErrBatchTooLarge {
		t.Fatalf("expected ErrBatchTooLarge")
	}
}

// 1000 exactly should be allowed.
func TestBulkInvite_AllowsExactlyMaxBatch(t *testing.T) {
	t.Parallel()
	reqs := make([]member.InviteRequest, 1000)
	for i := range reqs {
		reqs[i] = member.InviteRequest{
			Email: emailFor(i),
			Roles: []string{"learner"},
		}
	}
	result, err := member.BulkInvite(tenantA, reqs)
	if err != nil {
		t.Fatalf("BulkInvite (1000 rows): unexpected error: %v", err)
	}
	if len(result.Invited) != 1000 {
		t.Fatalf("expected 1000 invites, got %d", len(result.Invited))
	}
}

func TestBulkInvite_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	reqs := []member.InviteRequest{{Email: "alice@acme.com", Roles: []string{"learner"}}}
	if _, err := member.BulkInvite("", reqs); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

// -----------------------------------------------------------------------------
// MemberRegistry — list / filter / update
// -----------------------------------------------------------------------------

func TestMemberRegistry_AddAndList(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	m1, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m2, _ := member.NewMember(tenantA, "bob@acme.com", []string{"instructor"})
	reg.Save(m1)
	reg.Save(m2)
	all := reg.ListByTenant(tenantA, member.ListFilter{})
	if len(all) != 2 {
		t.Fatalf("expected 2 members, got %d", len(all))
	}
}

func TestMemberRegistry_ListByRole(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	m1, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m2, _ := member.NewMember(tenantA, "bob@acme.com", []string{"instructor"})
	reg.Save(m1)
	reg.Save(m2)
	out := reg.ListByTenant(tenantA, member.ListFilter{Role: "instructor"})
	if len(out) != 1 {
		t.Fatalf("expected 1 instructor, got %d", len(out))
	}
	if out[0].Email != "bob@acme.com" {
		t.Fatalf("expected bob, got %q", out[0].Email)
	}
}

func TestMemberRegistry_ListByStatus(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	m1, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m2, _ := member.NewMember(tenantA, "bob@acme.com", []string{"learner"})
	m1.Activate("gcid-1")
	reg.Save(m1)
	reg.Save(m2)
	active := reg.ListByTenant(tenantA, member.ListFilter{Status: "active"})
	if len(active) != 1 {
		t.Fatalf("expected 1 active member, got %d", len(active))
	}
	invited := reg.ListByTenant(tenantA, member.ListFilter{Status: "invited"})
	if len(invited) != 1 {
		t.Fatalf("expected 1 invited member, got %d", len(invited))
	}
}

func TestMemberRegistry_FindByEmail(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	reg.Save(m)
	got, ok := reg.FindByEmail(tenantA, "alice@acme.com")
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != m.ID {
		t.Fatalf("ID mismatch")
	}
	if _, ok := reg.FindByEmail(tenantA, "no-such@acme.com"); ok {
		t.Fatalf("expected miss")
	}
}

func TestMemberRegistry_Get(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	reg.Save(m)
	got, ok := reg.Get(m.ID)
	if !ok {
		t.Fatalf("expected hit")
	}
	if got.ID != m.ID {
		t.Fatalf("ID mismatch")
	}
}

// Bulk invite respects existing members in the registry — duplicates against
// already-saved members are skipped.
func TestMemberRegistry_BulkInviteSkipsExisting(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	existing, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	reg.Save(existing)
	reqs := []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
		{Email: "bob@acme.com", Roles: []string{"learner"}},
	}
	result, err := reg.BulkInvite(tenantA, reqs)
	if err != nil {
		t.Fatalf("BulkInvite: unexpected error: %v", err)
	}
	if len(result.Invited) != 1 {
		t.Fatalf("expected 1 NEW invite (bob), got %d", len(result.Invited))
	}
	if result.AlreadyExists != 1 {
		t.Fatalf("expected 1 already-exists count, got %d", result.AlreadyExists)
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func emailFor(i int) string {
	// Use a fast lookup-friendly format for high-volume tests.
	var sb strings.Builder
	sb.WriteString("user")
	itoa(&sb, i)
	sb.WriteString("@acme.com")
	return sb.String()
}

func itoa(sb *strings.Builder, n int) {
	if n == 0 {
		sb.WriteByte('0')
		return
	}
	if n < 0 {
		sb.WriteByte('-')
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	sb.Write(buf[pos:])
}
