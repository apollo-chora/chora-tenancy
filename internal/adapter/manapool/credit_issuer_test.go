package manapool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
)

// ---------- DeriveEggRefundIdempotencyKey -----------------------------------

func TestDeriveEggRefundIdempotencyKey_DeterministicWithPrefix(t *testing.T) {
	id := DeriveEggRefundIdempotencyKey("01957c8c-0000-7000-purchase-abc")
	want := "expiry-01957c8c-0000-7000-purchase-abc"
	if id != want {
		t.Errorf("got %q want %q", id, want)
	}
}

func TestDeriveEggRefundIdempotencyKey_TrimsWhitespace(t *testing.T) {
	id := DeriveEggRefundIdempotencyKey("  purchase-xyz\n")
	want := "expiry-purchase-xyz"
	if id != want {
		t.Errorf("got %q want %q", id, want)
	}
}

func TestDeriveEggRefundIdempotencyKey_HasExpectedPrefix(t *testing.T) {
	id := DeriveEggRefundIdempotencyKey("01957c8c-0000-7000-aaaa-aaaaaaaaaaaa")
	if !strings.HasPrefix(id, "expiry-") {
		t.Errorf("missing expiry- prefix; got %q", id)
	}
}

func TestDeriveEggRefundAllocationID_DelegatesToIdempotencyKey(t *testing.T) {
	// Back-compat: the legacy helper still returns the same string so
	// any caller that depended on it pre-0009 keeps working.
	if DeriveEggRefundAllocationID("p-x") != DeriveEggRefundIdempotencyKey("p-x") {
		t.Errorf("back-compat helper diverged from canonical")
	}
}

// ---------- NewCreditIssuer validation --------------------------------------

func TestNewCreditIssuer_RequiresRepo(t *testing.T) {
	_, err := NewCreditIssuer(CreditIssuerConfig{
		Publisher: NewRecorderPublisher(),
	})
	if err == nil {
		t.Fatal("want error when Repo nil")
	}
}

func TestNewCreditIssuer_RequiresPublisher(t *testing.T) {
	_, err := NewCreditIssuer(CreditIssuerConfig{
		Repo: NewInmemAllocationRepo(),
	})
	if err == nil {
		t.Fatal("want error when Publisher nil")
	}
}

func TestNewCreditIssuer_AppliesDefaults(t *testing.T) {
	ci, err := NewCreditIssuer(CreditIssuerConfig{
		Repo:      NewInmemAllocationRepo(),
		Publisher: NewRecorderPublisher(),
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if ci.platformGCID != "platform" {
		t.Errorf("platformGCID default: got %q", ci.platformGCID)
	}
	if ci.now == nil {
		t.Error("now should default to time.Now")
	}
}

func TestNewCreditIssuer_HonoursExplicitOverrides(t *testing.T) {
	clock := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	ci, err := NewCreditIssuer(CreditIssuerConfig{
		Repo:         NewInmemAllocationRepo(),
		Publisher:    NewRecorderPublisher(),
		Now:          func() time.Time { return clock },
		PlatformGCID: "platform-actor-42",
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if ci.platformGCID != "platform-actor-42" {
		t.Errorf("platformGCID: got %q", ci.platformGCID)
	}
}

// ---------- IssueRefundCredit input validation ------------------------------

func TestIssueRefundCredit_RejectsMissingTenantID(t *testing.T) {
	ci := newTestIssuer(t)
	err := ci.IssueRefundCredit(context.Background(), "", "gcid-x", 500, "purchase-1")
	if err == nil || !strings.Contains(err.Error(), "tenantID") {
		t.Errorf("want tenantID error; got %v", err)
	}
}

func TestIssueRefundCredit_RejectsMissingGCID(t *testing.T) {
	ci := newTestIssuer(t)
	err := ci.IssueRefundCredit(context.Background(), "tenant-x", "", 500, "purchase-1")
	if err == nil || !strings.Contains(err.Error(), "gcid") {
		t.Errorf("want gcid error; got %v", err)
	}
}

func TestIssueRefundCredit_RejectsMissingPurchaseID(t *testing.T) {
	ci := newTestIssuer(t)
	err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-x", 500, "")
	if err == nil || !strings.Contains(err.Error(), "purchaseID") {
		t.Errorf("want purchaseID error; got %v", err)
	}
}

func TestIssueRefundCredit_RejectsZeroAmount(t *testing.T) {
	ci := newTestIssuer(t)
	for _, amt := range []int64{0, -1, -999} {
		err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-x", amt, "purchase-1")
		if err == nil || !strings.Contains(err.Error(), "amountCents") {
			t.Errorf("amount=%d: want amountCents error; got %v", amt, err)
		}
	}
}

func TestIssueRefundCredit_TrimsWhitespaceInInputs(t *testing.T) {
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	clock := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub, Now: func() time.Time { return clock }})

	err := ci.IssueRefundCredit(context.Background(), "  tenant-x  ", "  gcid-y  ", 999, "  purchase-z\n")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	got, err := repo.GetByIdempotencyKey(context.Background(), "expiry-purchase-z")
	if err != nil {
		t.Fatalf("expected row at idem=expiry-purchase-z; got err %v", err)
	}
	if got.TenantID != "tenant-x" {
		t.Errorf("TenantID not trimmed: got %q", got.TenantID)
	}
	if got.GCID != "gcid-y" {
		t.Errorf("GCID not trimmed: got %q", got.GCID)
	}
}

// ---------- IssueRefundCredit happy path ------------------------------------

func TestIssueRefundCredit_PersistsAllocationRow(t *testing.T) {
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	clock := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub, Now: func() time.Time { return clock }})

	err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-z")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	row, err := repo.GetByIdempotencyKey(context.Background(), "expiry-purchase-z")
	if err != nil {
		t.Fatalf("row not persisted by idem key: %v", err)
	}
	if row.TenantID != "tenant-x" {
		t.Errorf("TenantID: got %q", row.TenantID)
	}
	if row.GCID != "gcid-y" {
		t.Errorf("GCID: got %q", row.GCID)
	}
	if row.Units != 999 {
		t.Errorf("Units: got %d want 999 (1:1 placeholder per Iter G.8 TODO)", row.Units)
	}
	if row.Reason != allocation.ReasonEggHardExpiryRefund {
		t.Errorf("Reason: got %q want %q", row.Reason, allocation.ReasonEggHardExpiryRefund)
	}
	if row.SourcePoolID != "" {
		t.Errorf("SourcePoolID: should be empty for egg-refund (NULL on pg via 0009); got %q", row.SourcePoolID)
	}
	if row.IdempotencyKey != "expiry-purchase-z" {
		t.Errorf("IdempotencyKey: got %q want expiry-purchase-z", row.IdempotencyKey)
	}
	if row.AllocatedByGCID != "platform" {
		t.Errorf("AllocatedByGCID: got %q", row.AllocatedByGCID)
	}
	if !row.AllocatedAt.Equal(clock) {
		t.Errorf("AllocatedAt: got %v want %v", row.AllocatedAt, clock)
	}
	// AllocationID is now a UUIDv7 (gen'd via allocation.NewUUIDv7()),
	// not a deterministic string. Just sanity-check the shape.
	if !strings.Contains(row.AllocationID, "-") || len(row.AllocationID) < 32 {
		t.Errorf("AllocationID should be UUIDv7-shaped; got %q", row.AllocationID)
	}
}

func TestIssueRefundCredit_PublishesGrantedEvent(t *testing.T) {
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub})

	err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-z")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	events := pub.Recorded()
	if len(events) != 1 {
		t.Fatalf("expected 1 event; got %d", len(events))
	}
	if events[0].Topic != "chora.tenancy.tenant_mana_allocation.granted.v1" {
		t.Errorf("topic: got %q", events[0].Topic)
	}
	if events[0].Envelope.TenantID != "tenant-x" || events[0].Envelope.GCID != "gcid-y" {
		t.Errorf("envelope routing: got %+v", events[0].Envelope)
	}
	payload, ok := events[0].Payload.(allocation.AllocationGranted)
	if !ok {
		t.Fatalf("payload type: got %T", events[0].Payload)
	}
	if !strings.Contains(payload.AllocationID, "-") || len(payload.AllocationID) < 32 {
		t.Errorf("AllocationID should be UUIDv7-shaped; got %q", payload.AllocationID)
	}
	if payload.Units != 999 {
		t.Errorf("Units: got %d", payload.Units)
	}
	if payload.Reason != allocation.ReasonEggHardExpiryRefund {
		t.Errorf("Reason: got %q", payload.Reason)
	}
}

// ---------- Idempotency ------------------------------------------------------

func TestIssueRefundCredit_IdempotentOnRerun(t *testing.T) {
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub})

	// First call writes + publishes.
	if err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-z"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := len(pub.Recorded()); got != 1 {
		t.Fatalf("after first call: expected 1 event; got %d", got)
	}

	// Second call with same purchaseID must be a no-op.
	if err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-z"); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := len(pub.Recorded()); got != 1 {
		t.Errorf("after second call: expected still 1 event; got %d (double-publish)", got)
	}
}

func TestIssueRefundCredit_IdempotentEvenWithDifferentAmountIgnored(t *testing.T) {
	// Pragmatic: if the sweeper accidentally re-runs with a different
	// amount for the same purchase, the issuer treats it as a no-op
	// (the original amount stays). This avoids over-crediting.
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub})

	_ = ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-z")
	_ = ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 5000, "purchase-z")

	row, err := repo.GetByIdempotencyKey(context.Background(), "expiry-purchase-z")
	if err != nil {
		t.Fatalf("row not persisted by idem key: %v", err)
	}
	if row.Units != 999 {
		t.Errorf("Units: idempotency must preserve original 999; got %d", row.Units)
	}
}

func TestIssueRefundCredit_DistinctPurchasesGetDistinctAllocations(t *testing.T) {
	repo := NewInmemAllocationRepo()
	pub := NewRecorderPublisher()
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub})

	_ = ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-A")
	_ = ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-B")

	if _, err := repo.GetByIdempotencyKey(context.Background(), "expiry-purchase-A"); err != nil {
		t.Errorf("purchase-A row missing: %v", err)
	}
	if _, err := repo.GetByIdempotencyKey(context.Background(), "expiry-purchase-B"); err != nil {
		t.Errorf("purchase-B row missing: %v", err)
	}
	if got := len(pub.Recorded()); got != 2 {
		t.Errorf("distinct purchases should yield 2 events; got %d", got)
	}
}

// ---------- Publisher failure ------------------------------------------------

type errSaver struct{ called int }

func (e *errSaver) Get(ctx context.Context, id string) (*allocation.TenantManaAllocation, error) {
	return nil, ErrAllocationNotFound
}
func (e *errSaver) GetByIdempotencyKey(ctx context.Context, key string) (*allocation.TenantManaAllocation, error) {
	return nil, ErrAllocationNotFound
}
func (e *errSaver) Save(ctx context.Context, a *allocation.TenantManaAllocation) error {
	e.called++
	return errors.New("transient save failure")
}

func TestIssueRefundCredit_PropagatesSaveErrorAndSkipsPublish(t *testing.T) {
	repo := &errSaver{}
	pub := NewRecorderPublisher()
	ci, _ := NewCreditIssuer(CreditIssuerConfig{Repo: repo, Publisher: pub})

	err := ci.IssueRefundCredit(context.Background(), "tenant-x", "gcid-y", 999, "purchase-fail")
	if err == nil {
		t.Fatal("expected error from save failure")
	}
	if !strings.Contains(err.Error(), "save allocation") {
		t.Errorf("error should mention save failure; got %v", err)
	}
	if got := len(pub.Recorded()); got != 0 {
		t.Errorf("publisher should NOT fire on save failure; got %d events", got)
	}
}

// ---------- helpers ----------------------------------------------------------

func newTestIssuer(t *testing.T) *CreditIssuer {
	t.Helper()
	ci, err := NewCreditIssuer(CreditIssuerConfig{
		Repo:      NewInmemAllocationRepo(),
		Publisher: NewRecorderPublisher(),
	})
	if err != nil {
		t.Fatalf("newTestIssuer: %v", err)
	}
	return ci
}
