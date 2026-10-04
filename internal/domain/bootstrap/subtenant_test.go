// subtenant_test.go — ADR-217 Phase 2.1 (CHO-2006). RED-phase drive for the
// operator sub-tenant create-path: Service.CreateSubTenant reuses the atomic
// bootstrap Bundle (tenant + owner-member + core entitlement + outbox event)
// but writes parent_tenant_id + hosting_mode, uses the REQUEST's owner_gcid,
// and SKIPS the one-tenant-per-GCID self-onboard idempotency (that rule is for
// user self-onboard, not operator-provisioned managed tenants).
package bootstrap_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

const sampleParentID = "00000000-0000-7000-8000-000000000001" // chora-master

func TestService_CreateSubTenant_happyPath_persistsWithParentAndHostingMode(t *testing.T) {
	svc, repo, sub := newSvcWithFakes()

	out, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:           "Mighty Mind — Bishan Branch",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		HostingMode:    "FRANCHISE",
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if out == nil || out.TenantID == "" || out.CreatedAt.IsZero() {
		t.Fatalf("expected populated Output, got %+v", out)
	}
	// Idempotency check is SKIPPED for operator-provisioned sub-tenants.
	if repo.hasAnyCalls != 0 {
		t.Errorf("HasAnyMembership must NOT be called for sub-tenant create; got %d calls", repo.hasAnyCalls)
	}
	if repo.persistCalls != 1 {
		t.Fatalf("Persist: want 1 call, got %d", repo.persistCalls)
	}
	tn := repo.lastBundle.Tenant
	if tn.ParentTenantID != sampleParentID {
		t.Errorf("persisted parent_tenant_id = %q, want %q", tn.ParentTenantID, sampleParentID)
	}
	if tn.HostingMode != "FRANCHISE" {
		t.Errorf("persisted hosting_mode = %q, want FRANCHISE", tn.HostingMode)
	}
	if tn.OwnerGCID != sampleGCID {
		t.Errorf("persisted owner_gcid = %q, want request owner %q", tn.OwnerGCID, sampleGCID)
	}
	// Owner membership + base add-on subscription still happen (non-half-measure).
	if repo.lastBundle.OwnerMember.GCID != sampleGCID || repo.lastBundle.OwnerMember.Role != bootstrap.MemberRoleOwner {
		t.Errorf("owner member not assembled: %+v", repo.lastBundle.OwnerMember)
	}
	if sub.subscribeCalls != 1 || sub.lastTenantID != out.TenantID {
		t.Errorf("base add-on subscribe: calls=%d tenant=%q", sub.subscribeCalls, sub.lastTenantID)
	}
}

func TestService_CreateSubTenant_defaultsHostingModeToFranchise(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()
	if _, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:           "Unspecified-mode branch",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		// HostingMode omitted → sub-tenant default.
	}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if got := repo.lastBundle.Tenant.HostingMode; got != "FRANCHISE" {
		t.Errorf("default sub-tenant hosting_mode = %q, want FRANCHISE", got)
	}
}

func TestService_CreateSubTenant_emptyParent_returnsInvalidArgument(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()
	_, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:      "No parent",
		OwnerGCID: sampleGCID,
		// ParentTenantID omitted.
	})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	if repo.persistCalls != 0 {
		t.Errorf("must not persist on invalid input; got %d", repo.persistCalls)
	}
}

func TestService_CreateSubTenant_invalidHostingMode_returnsInvalidArgument(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()
	_, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:           "Bad mode",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		HostingMode:    "NOT_A_MODE",
	})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	if repo.persistCalls != 0 {
		t.Errorf("must not persist on invalid hosting_mode; got %d", repo.persistCalls)
	}
}

func TestService_CreateSubTenant_ownerAlreadyMember_stillSucceeds(t *testing.T) {
	// Even if the owner already belongs to a tenant, an operator may provision
	// another (sub-)tenant for them — the self-onboard block does not apply.
	repo := &fakeRepo{hasAnyResult: true}
	sub := &fakeSubscriber{}
	svc := bootstrap.NewService(repo, sub)
	if _, err := svc.CreateSubTenant(context.Background(), bootstrap.Input{
		Name:           "Second tenant for owner",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		HostingMode:    "WHITE_LABEL",
	}); err != nil {
		t.Fatalf("expected success despite existing membership, got %v", err)
	}
	if repo.hasAnyCalls != 0 {
		t.Errorf("HasAnyMembership must not gate operator sub-tenant create; got %d", repo.hasAnyCalls)
	}
	if repo.persistCalls != 1 {
		t.Errorf("want persist once, got %d", repo.persistCalls)
	}
}

func TestNew_setsParentAndHostingMode(t *testing.T) {
	b, err := bootstrap.New(bootstrap.Input{
		Name:           "With parent",
		OwnerGCID:      sampleGCID,
		ParentTenantID: sampleParentID,
		HostingMode:    "WHITE_LABEL",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Tenant.ParentTenantID != sampleParentID {
		t.Errorf("ParentTenantID = %q", b.Tenant.ParentTenantID)
	}
	if b.Tenant.HostingMode != "WHITE_LABEL" {
		t.Errorf("HostingMode = %q", b.Tenant.HostingMode)
	}
}

func TestNew_rootDefaults_parentEmptyHostingPlatform(t *testing.T) {
	// Backward-compat: the existing root bootstrap path passes no parent /
	// hosting_mode and must keep producing a root, PLATFORM_HOSTED tenant.
	b, err := bootstrap.New(bootstrap.Input{Name: "Root tenant", OwnerGCID: sampleGCID})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Tenant.ParentTenantID != "" {
		t.Errorf("root ParentTenantID = %q, want empty", b.Tenant.ParentTenantID)
	}
	if b.Tenant.HostingMode != "PLATFORM_HOSTED" {
		t.Errorf("root HostingMode = %q, want PLATFORM_HOSTED", b.Tenant.HostingMode)
	}
}
