// service_test.go — RED phase for the bootstrap orchestrator
// (CHO-1628 Phase 1).
//
// Drives the Service.Bootstrap method via a fake Repository so the
// orchestration logic is exercised without touching Postgres. The
// real Postgres adapter has its own integration test under
// internal/adapter/pg/bootstrap_repository_test.go.
package bootstrap_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
)

// fakeRepo records the calls Service.Bootstrap makes against it and
// returns canned responses. Used by every service test below.
type fakeRepo struct {
	hasAnyResult bool
	hasAnyErr    error
	persistErr   error
	hasAnyCalls  int
	persistCalls int
	lastGCID     string
	lastBundle   *bootstrap.Bundle
}

func (r *fakeRepo) HasAnyMembership(_ context.Context, gcid string) (bool, error) {
	r.hasAnyCalls++
	r.lastGCID = gcid
	return r.hasAnyResult, r.hasAnyErr
}

func (r *fakeRepo) Persist(_ context.Context, b *bootstrap.Bundle) error {
	r.persistCalls++
	r.lastBundle = b
	return r.persistErr
}

// fakeSubscriber satisfies bootstrap.AddOnSubscriber (CHO-1751 Phase 1).
// Records the args of every Subscribe call so tests can assert the
// orchestrator invokes it once with the bootstrapped tenantID + "base"
// + the caller's GCID, exactly after the pg Persist succeeds.
type fakeSubscriber struct {
	subscribeErr   error
	subscribeCalls int
	lastTenantID   string
	lastAddOnCode  string
	lastOwnerGCID  string
}

func (s *fakeSubscriber) Subscribe(tenantID, addOnCode, ownerGCID string) error {
	s.subscribeCalls++
	s.lastTenantID = tenantID
	s.lastAddOnCode = addOnCode
	s.lastOwnerGCID = ownerGCID
	return s.subscribeErr
}

// newSvcWithFakes is a convenience constructor for the post-CHO-1751 Service
// shape (Repository + AddOnSubscriber). Returns the fakes alongside so the
// caller can assert against their recorded state.
func newSvcWithFakes() (*bootstrap.Service, *fakeRepo, *fakeSubscriber) {
	repo := &fakeRepo{}
	sub := &fakeSubscriber{}
	return bootstrap.NewService(repo, sub), repo, sub
}

func TestService_Bootstrap_happyPath_returnsOutputAndPersistsOnce(t *testing.T) {
	svc, repo, sub := newSvcWithFakes()

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Happy Path Tenant",
		OwnerGCID: sampleGCID,
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil Output")
	}
	if out.TenantID == "" {
		t.Errorf("Output.TenantID should be set")
	}
	if out.OwnerMemberID == "" {
		t.Errorf("Output.OwnerMemberID should be set")
	}
	if out.EntitlementID == "" {
		t.Errorf("Output.EntitlementID should be set")
	}
	if out.CreatedAt.IsZero() {
		t.Errorf("Output.CreatedAt should be set")
	}

	if repo.hasAnyCalls != 1 {
		t.Errorf("HasAnyMembership: want 1 call, got %d", repo.hasAnyCalls)
	}
	if repo.lastGCID != sampleGCID {
		t.Errorf("HasAnyMembership called with wrong GCID: %q", repo.lastGCID)
	}
	if repo.persistCalls != 1 {
		t.Errorf("Persist: want 1 call, got %d", repo.persistCalls)
	}
	if repo.lastBundle == nil {
		t.Fatal("Persist received nil bundle")
	}
	if repo.lastBundle.Tenant.OwnerGCID != sampleGCID {
		t.Errorf("persisted bundle GCID mismatch")
	}
	// CHO-1751 Phase 1 — Subscribe is called exactly once after Persist,
	// with the bootstrapped tenant_id + the canonical base code + the
	// caller's GCID.
	if sub.subscribeCalls != 1 {
		t.Errorf("Subscribe: want 1 call, got %d", sub.subscribeCalls)
	}
	if sub.lastTenantID != out.TenantID {
		t.Errorf("Subscribe tenantID: want %q, got %q", out.TenantID, sub.lastTenantID)
	}
	if sub.lastAddOnCode != bootstrap.BaseAddOnCode {
		t.Errorf("Subscribe addonCode: want %q, got %q", bootstrap.BaseAddOnCode, sub.lastAddOnCode)
	}
	if sub.lastOwnerGCID != sampleGCID {
		t.Errorf("Subscribe ownerGCID: want %q, got %q", sampleGCID, sub.lastOwnerGCID)
	}
}

// TestService_Bootstrap_parentsSelfOnboardToChoraMaster — ADR-217 Phase 2.4
// (CHO-2002). A self-onboarded tenant MUST be created as a direct child of the
// franchisor ROOT (chora-master) so it joins the managed-tenant hierarchy from
// creation, instead of orphaning as its own root and dropping out of the
// operator "managed tenants" subtree filter. All other self-onboard semantics
// are preserved: the one-tenant-per-GCID HasAnyMembership guard still runs, the
// owner is the caller's GCID, and hosting_mode stays PLATFORM_HOSTED (only the
// operator CreateSubTenant path classifies FRANCHISE).
func TestService_Bootstrap_parentsSelfOnboardToChoraMaster(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()

	if _, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Self-Onboarded Tenant",
		OwnerGCID: sampleGCID,
	}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if repo.lastBundle == nil {
		t.Fatal("Persist received nil bundle")
	}
	tn := repo.lastBundle.Tenant

	// NEW behavior: parent = chora-master (franchisor ROOT).
	if tn.ParentTenantID != tenant.ChoraMasterTenantID {
		t.Errorf("self-onboard parent_tenant_id = %q, want chora-master %q",
			tn.ParentTenantID, tenant.ChoraMasterTenantID)
	}
	// Preserved: hosting_mode is NOT franchise — self-onboard stays PLATFORM_HOSTED.
	if tn.HostingMode != bootstrap.HostingModePlatformHosted {
		t.Errorf("self-onboard hosting_mode = %q, want %q (unchanged)",
			tn.HostingMode, bootstrap.HostingModePlatformHosted)
	}
	// Preserved: the one-tenant-per-GCID self-onboard guard still runs.
	if repo.hasAnyCalls != 1 {
		t.Errorf("HasAnyMembership guard must still run once; got %d", repo.hasAnyCalls)
	}
	// Preserved: owner is the caller's GCID.
	if tn.OwnerGCID != sampleGCID {
		t.Errorf("owner_gcid = %q, want caller %q", tn.OwnerGCID, sampleGCID)
	}
}

func TestService_Bootstrap_existingMember_returnsErrAlreadyMemberAndSkipsPersist(t *testing.T) {
	repo := &fakeRepo{hasAnyResult: true}
	sub := &fakeSubscriber{}
	svc := bootstrap.NewService(repo, sub)

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Should Not Persist",
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, bootstrap.ErrAlreadyMember) {
		t.Errorf("expected ErrAlreadyMember, got %v", err)
	}
	if out != nil {
		t.Errorf("expected nil Output on conflict, got %+v", out)
	}
	if repo.persistCalls != 0 {
		t.Errorf("Persist must NOT be called when caller is already a member; got %d", repo.persistCalls)
	}
	if sub.subscribeCalls != 0 {
		t.Errorf("Subscribe must NOT be called when caller is already a member; got %d", sub.subscribeCalls)
	}
}

func TestService_Bootstrap_invalidInput_returnsErrInvalidArgumentAndSkipsRepo(t *testing.T) {
	svc, repo, sub := newSvcWithFakes()

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "", // invalid
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
	if out != nil {
		t.Errorf("expected nil Output on invalid input, got %+v", out)
	}
	// Domain validation must run BEFORE membership lookup — cheap
	// fast-fail and avoids hitting the DB on garbage input.
	if repo.hasAnyCalls != 0 {
		t.Errorf("HasAnyMembership must NOT be called on invalid input; got %d", repo.hasAnyCalls)
	}
	if repo.persistCalls != 0 {
		t.Errorf("Persist must NOT be called on invalid input; got %d", repo.persistCalls)
	}
	if sub.subscribeCalls != 0 {
		t.Errorf("Subscribe must NOT be called on invalid input; got %d", sub.subscribeCalls)
	}
}

func TestService_Bootstrap_emptyGCID_returnsErrInvalidArgumentAndSkipsRepo(t *testing.T) {
	svc, repo, _ := newSvcWithFakes()

	_, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "No GCID",
		OwnerGCID: "",
	})
	if !errors.Is(err, bootstrap.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for empty GCID, got %v", err)
	}
	if repo.hasAnyCalls != 0 {
		t.Errorf("HasAnyMembership must NOT be called when GCID is empty; got %d", repo.hasAnyCalls)
	}
}

func TestService_Bootstrap_repoErrorOnHasAny_propagates(t *testing.T) {
	boom := errors.New("db boom on read")
	repo := &fakeRepo{hasAnyErr: boom}
	sub := &fakeSubscriber{}
	svc := bootstrap.NewService(repo, sub)

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Read Will Fail",
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, boom) {
		t.Errorf("expected wrapped %v, got %v", boom, err)
	}
	if out != nil {
		t.Errorf("expected nil Output on repo read failure")
	}
	if repo.persistCalls != 0 {
		t.Errorf("Persist must NOT be called when HasAnyMembership errored")
	}
	if sub.subscribeCalls != 0 {
		t.Errorf("Subscribe must NOT be called when HasAnyMembership errored")
	}
}

func TestService_Bootstrap_repoErrorOnPersist_propagates(t *testing.T) {
	boom := errors.New("db boom on write")
	repo := &fakeRepo{persistErr: boom}
	sub := &fakeSubscriber{}
	svc := bootstrap.NewService(repo, sub)

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Write Will Fail",
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, boom) {
		t.Errorf("expected wrapped %v, got %v", boom, err)
	}
	if out != nil {
		t.Errorf("expected nil Output on repo write failure")
	}
	// CHO-1751 Phase 1 — Subscribe must NOT fire if Persist failed, so
	// a transient pg error doesn't leave a phantom registry entry that
	// outlives the rolled-back tenant.
	if sub.subscribeCalls != 0 {
		t.Errorf("Subscribe must NOT be called when Persist errored; got %d", sub.subscribeCalls)
	}
}

// TestService_Bootstrap_subscribeError_propagates — CHO-1751 Phase 1.
// If the registry-side Subscribe fails (e.g. catalogue lookup of "base"
// returns ErrAddOnNotFound), the orchestrator surfaces the error so the
// caller can react. The tenant + member + entitlement rows STAY (the pg
// transaction is already committed); cleanup is a retry concern.
func TestService_Bootstrap_subscribeError_propagates(t *testing.T) {
	boom := errors.New("registry subscribe boom")
	repo := &fakeRepo{}
	sub := &fakeSubscriber{subscribeErr: boom}
	svc := bootstrap.NewService(repo, sub)

	out, err := svc.Bootstrap(context.Background(), bootstrap.Input{
		Name:      "Subscribe Will Fail",
		OwnerGCID: sampleGCID,
	})
	if !errors.Is(err, boom) {
		t.Errorf("expected wrapped %v, got %v", boom, err)
	}
	if out != nil {
		t.Errorf("expected nil Output when Subscribe errored, got %+v", out)
	}
	if repo.persistCalls != 1 {
		t.Errorf("Persist should still have been called; got %d", repo.persistCalls)
	}
	if sub.subscribeCalls != 1 {
		t.Errorf("Subscribe should have been attempted; got %d", sub.subscribeCalls)
	}
}

func TestNewService_nilRepo_panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewService(nil, sub) should panic — repo is mandatory")
		}
	}()
	bootstrap.NewService(nil, &fakeSubscriber{})
}

// TestNewService_nilSubscriber_panics — CHO-1751 Phase 1. Match the
// existing nil-repo panic so a wiring bug in cmd/server fails loud at
// boot, not silently on the first POST /api/v1/tenants/bootstrap.
func TestNewService_nilSubscriber_panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewService(repo, nil) should panic — subscriber is mandatory")
		}
	}()
	bootstrap.NewService(&fakeRepo{}, nil)
}
