// atom_count_subscriber_test.go — ADR-217 Debt 3 (CHO-2011). Unit-tests the
// atom-count projection subscriber's fold logic + envelope guards + idempotency
// against a fake WriteRepository and the real in-memory inbox.
package events

import (
	"context"
	"errors"
	"testing"

	cgcenv "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
)

type fakeAtomCountRepo struct {
	deltas map[string]int64
	calls  int
	err    error
}

func newFakeAtomCountRepo() *fakeAtomCountRepo {
	return &fakeAtomCountRepo{deltas: map[string]int64{}}
}

func (f *fakeAtomCountRepo) ApplyDelta(_ context.Context, tenantID string, delta int64) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.deltas[tenantID] += delta
	return nil
}

func newAtomCountSub(repo *fakeAtomCountRepo) *AtomCountSubscriber {
	return NewAtomCountSubscriber(repo, idempotent.NewMemoryStore())
}

func atomMsg(topic, eventID, tenant string) eventbus.Message {
	return eventbus.Message{
		Subject:  topic,
		Envelope: cgcenv.Envelope{EventID: eventID, TenantID: tenant},
	}
}

func TestAtomCountSubscriber_CreatedIncrements(t *testing.T) {
	repo := newFakeAtomCountRepo()
	s := newAtomCountSub(repo)
	if err := s.Handle(context.Background(), topicAtomCreated, atomMsg(topicAtomCreated, "e1", "ten-1")); err != nil {
		t.Fatalf("created handle: %v", err)
	}
	if repo.deltas["ten-1"] != 1 {
		t.Fatalf("atom.created must +1, got %d", repo.deltas["ten-1"])
	}
}

func TestAtomCountSubscriber_ArchivedDecrements(t *testing.T) {
	repo := newFakeAtomCountRepo()
	s := newAtomCountSub(repo)
	if err := s.Handle(context.Background(), topicAtomArchived, atomMsg(topicAtomArchived, "e2", "ten-1")); err != nil {
		t.Fatalf("archived handle: %v", err)
	}
	if repo.deltas["ten-1"] != -1 {
		t.Fatalf("atom.archived must -1, got %d", repo.deltas["ten-1"])
	}
}

func TestAtomCountSubscriber_IdempotentOnRedelivery(t *testing.T) {
	repo := newFakeAtomCountRepo()
	s := newAtomCountSub(repo)
	msg := atomMsg(topicAtomCreated, "dup", "ten-1")
	if err := s.Handle(context.Background(), topicAtomCreated, msg); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := s.Handle(context.Background(), topicAtomCreated, msg); err != nil { // same event_id
		t.Fatalf("redelivery handle: %v", err)
	}
	if repo.deltas["ten-1"] != 1 {
		t.Fatalf("redelivery must not double-count; want +1, got %d", repo.deltas["ten-1"])
	}
	if repo.calls != 1 {
		t.Fatalf("inbox must gate the repo to a single apply, got %d calls", repo.calls)
	}
}

func TestAtomCountSubscriber_EmptyTenantNacks(t *testing.T) {
	repo := newFakeAtomCountRepo()
	s := newAtomCountSub(repo)
	if err := s.Handle(context.Background(), topicAtomCreated, atomMsg(topicAtomCreated, "e", "")); err == nil {
		t.Fatal("empty tenant_id must NACK (fail loud), got nil")
	}
	if repo.calls != 0 {
		t.Fatalf("must not touch the repo on a bad envelope, got %d calls", repo.calls)
	}
}

func TestAtomCountSubscriber_EmptyEventIDNacks(t *testing.T) {
	s := newAtomCountSub(newFakeAtomCountRepo())
	if err := s.Handle(context.Background(), topicAtomCreated, atomMsg(topicAtomCreated, "", "ten-1")); err == nil {
		t.Fatal("empty event_id must NACK, got nil")
	}
}

func TestAtomCountSubscriber_UnhandledTopicErrors(t *testing.T) {
	s := newAtomCountSub(newFakeAtomCountRepo())
	const other = "chora.creation.atom.updated.v1"
	if err := s.Handle(context.Background(), other, atomMsg(other, "e", "ten-1")); err == nil {
		t.Fatal("unhandled topic must error, got nil")
	}
}

func TestAtomCountSubscriber_RepoErrorPropagates(t *testing.T) {
	repo := newFakeAtomCountRepo()
	repo.err = errors.New("boom")
	s := newAtomCountSub(repo)
	if err := s.Handle(context.Background(), topicAtomCreated, atomMsg(topicAtomCreated, "e", "ten-1")); err == nil {
		t.Fatal("repo error must propagate (NACK→retry→DLQ), got nil")
	}
}

func TestAtomCountSubscriber_TopicsAndSubscriptionNames(t *testing.T) {
	s := newAtomCountSub(newFakeAtomCountRepo())
	if got := s.Topics(); len(got) != 2 {
		t.Fatalf("want 2 topics, got %d (%v)", len(got), got)
	}
	if got := s.SubscriptionNameForTopic(topicAtomCreated); got != "chora-tenancy-atomcount-atom-created" {
		t.Fatalf("created subscription name = %q", got)
	}
	if got := s.SubscriptionNameForTopic(topicAtomArchived); got != "chora-tenancy-atomcount-atom-archived" {
		t.Fatalf("archived subscription name = %q", got)
	}
	if got := s.SubscriptionNameForTopic("too.short"); got != "" {
		t.Fatalf("malformed topic must map to empty subscription, got %q", got)
	}
}
