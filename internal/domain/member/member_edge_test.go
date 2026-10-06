// Package member_test — A-Tenant-Lifecycle (S6.2) coverage gap fills for the
// member domain. These tests target BulkInvite + Registry.BulkInvite branches
// that the S2.2 base suite did not exercise.
package member_test

import (
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/member"
)

func TestRegistry_BulkInvite_DedupesAcrossBatches(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	first := []member.InviteRequest{{Email: "alice@acme.com", Roles: []string{"learner"}}}
	res, err := reg.BulkInvite(tenantA, first)
	if err != nil {
		t.Fatalf("first BulkInvite: %v", err)
	}
	if len(res.Invited) != 1 {
		t.Fatalf("expected 1 invited; got %d", len(res.Invited))
	}
	// Second batch with the same email — should bump AlreadyExists, not Invited.
	res, err = reg.BulkInvite(tenantA, first)
	if err != nil {
		t.Fatalf("second BulkInvite: %v", err)
	}
	if len(res.Invited) != 0 {
		t.Fatalf("expected 0 invited on dup batch; got %d", len(res.Invited))
	}
	if res.AlreadyExists != 1 {
		t.Fatalf("expected AlreadyExists=1; got %d", res.AlreadyExists)
	}
}

func TestRegistry_BulkInvite_RejectsBatchTooLarge(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	reqs := make([]member.InviteRequest, member.MaxBatchSize+1)
	for i := range reqs {
		reqs[i] = member.InviteRequest{Email: "x", Roles: []string{"learner"}}
	}
	if _, err := reg.BulkInvite(tenantA, reqs); err == nil {
		t.Fatalf("expected ErrBatchTooLarge")
	}
}

func TestRegistry_BulkInvite_RequiresTenantID(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	if _, err := reg.BulkInvite(" ", []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
	}); err == nil {
		t.Fatalf("expected ErrInvalidArgument for empty tenant_id")
	}
}

func TestRegistry_BulkInvite_RejectsInvalidEmailAndEmptyRoles(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	res, err := reg.BulkInvite(tenantA, []member.InviteRequest{
		{Email: "  ", Roles: []string{"learner"}},
		{Email: "not-an-email", Roles: []string{"learner"}},
		{Email: "alice@acme.com", Roles: []string{}},
		{Email: "bob@acme.com", Roles: []string{"made-up-role"}},
	})
	if err != nil {
		t.Fatalf("BulkInvite: %v", err)
	}
	if res.RejectedCount != 4 {
		t.Fatalf("expected 4 rejected; got %d", res.RejectedCount)
	}
	if len(res.Invited) != 0 {
		t.Fatalf("expected 0 invited; got %d", len(res.Invited))
	}
}

func TestRegistry_BulkInvite_DedupesWithinSameBatch(t *testing.T) {
	t.Parallel()
	reg := member.NewRegistry()
	res, err := reg.BulkInvite(tenantA, []member.InviteRequest{
		{Email: "alice@acme.com", Roles: []string{"learner"}},
		{Email: "ALICE@acme.com", Roles: []string{"learner"}},   // case-insensitive dup
		{Email: " alice@acme.com ", Roles: []string{"learner"}}, // whitespace dup
		{Email: "bob@acme.com", Roles: []string{"learner"}},
	})
	if err != nil {
		t.Fatalf("BulkInvite: %v", err)
	}
	if len(res.Invited) != 2 {
		t.Fatalf("expected 2 unique invites; got %d", len(res.Invited))
	}
	if res.DuplicateCount != 2 {
		t.Fatalf("expected DuplicateCount=2; got %d", res.DuplicateCount)
	}
}

func TestSuspendWithError_HappyPath(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	if err := m.SuspendWithError("policy violation"); err != nil {
		t.Fatalf("SuspendWithError: %v", err)
	}
	if m.Status != member.StatusSuspended {
		t.Fatalf("expected suspended status, got %q", m.Status)
	}
}

func TestActivate_AlreadyActiveIsNoop(t *testing.T) {
	t.Parallel()
	m, _ := member.NewMember(tenantA, "alice@acme.com", []string{"learner"})
	m.Activate("gcid-admin")
	if m.Status != member.StatusActive {
		t.Fatalf("expected active, got %q", m.Status)
	}
	// Calling Activate again is safe — must not change ActivatedAt
	originalActivatedAt := m.ActivatedAt
	m.Activate("gcid-admin")
	if m.ActivatedAt == nil || originalActivatedAt == nil ||
		!m.ActivatedAt.Equal(*originalActivatedAt) {
		t.Fatalf("Activate replay must not bump ActivatedAt")
	}
}

func TestNewMember_ValidationBranches(t *testing.T) {
	t.Parallel()
	if _, err := member.NewMember("", "a@b.com", []string{"learner"}); err == nil {
		t.Fatal("expected error for empty tenant")
	}
	if _, err := member.NewMember("t", "  ", []string{"learner"}); err == nil {
		t.Fatal("expected error for blank email")
	}
	if _, err := member.NewMember("t", "not-an-email", []string{"learner"}); err == nil {
		t.Fatal("expected error for invalid email")
	}
	if _, err := member.NewMember("t", "a@b.com", []string{"bogus-role"}); err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestUpdateRoles_InvalidRoleRejected(t *testing.T) {
	t.Parallel()
	m, err := member.NewMember("t-1", "a@b.com", []string{"learner"})
	if err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	if err := m.UpdateRoles([]string{"nope"}); err == nil {
		t.Fatal("expected error for invalid role set")
	}
}
