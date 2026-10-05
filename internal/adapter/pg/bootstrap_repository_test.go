//go:build integration

// Integration tests for the BootstrapRepository (CHO-1628 Phase 1).
//
// Run with the same DSN env-var harness as the other live-DB integration
// tests in this package:
//
//	export CHORA_TEST_DSN=postgres://chora_tenancy_migrate:chora@localhost:5432/chora_tenancy?sslmode=disable
//	go test -tags integration ./internal/adapter/pg/...
//
// Uses the `migrate` role DSN (not `app_rw`) because Persist needs to
// INSERT into the `tenants` registry (no RLS) AND into RLS-protected
// `members` + `add_on_subscriptions` rows for a tenant that did not
// exist before the transaction opened. app_rw could do this too once
// `SET LOCAL chora.tenant_id` runs, but `migrate` keeps the test cycle
// closer to the bootstrap saga's own transaction shape.
//
// The integration test SKIPs (not FAILs) when the env vars are not set,
// matching the integration_test.go convention.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
)

func TestIntegration_BootstrapRepository_Persist_writesAllThreeRows(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	in := bootstrap.Input{
		Name:      "Integration Bootstrap " + uuid.NewString()[:8],
		OwnerGCID: uuid.NewString(),
	}
	b, err := bootstrap.New(in)
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}

	ctx := context.Background()
	if err := repo.Persist(ctx, b); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Clean-up: soft-delete the tenant and hard-delete the children to
	// keep the dev DB tidy. Sequence reverses the insert order; RLS on
	// the children is bypassed by the migrate role's table-owner status.
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM members WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, b.Tenant.ID)
	})

	// 1. Tenant row landed.
	var name, status string
	if err := pool.QueryRow(ctx,
		`SELECT name, status::text FROM tenants WHERE tenant_id = $1 AND deleted_at IS NULL`,
		b.Tenant.ID,
	).Scan(&name, &status); err != nil {
		t.Fatalf("verify tenants row: %v", err)
	}
	if name != in.Name {
		t.Errorf("tenant.name: got %q want %q", name, in.Name)
	}
	if status != "active" {
		t.Errorf("tenant.status: got %q want %q", status, "active")
	}

	// 2. Members row landed with role=owner. Cross-tenant directory
	// query via the SECURITY DEFINER fn so we don't have to SET LOCAL.
	var memberCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM list_memberships_by_gcid($1::uuid) WHERE tenant_id = $2`,
		in.OwnerGCID, b.Tenant.ID,
	).Scan(&memberCount); err != nil {
		t.Fatalf("verify members row via security-definer fn: %v", err)
	}
	if memberCount != 1 {
		t.Errorf("expected 1 membership row, got %d", memberCount)
	}

	// 3. add_on_subscriptions row landed for the `core` add-on. RLS-
	// protected, so we wrap the SELECT in a SET LOCAL transaction.
	var coreSubCount int
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin verify-tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = '"+b.Tenant.ID+"'"); err != nil {
		t.Fatalf("set local: %v", err)
	}
	if err := tx.QueryRow(ctx, `
        SELECT COUNT(*)::int
        FROM add_on_subscriptions s
        JOIN add_ons a ON a.add_on_id = s.add_on_id
        WHERE s.tenant_id = $1 AND s.status = 'active' AND a.name = 'core'
    `, b.Tenant.ID).Scan(&coreSubCount); err != nil {
		t.Fatalf("verify add_on_subscriptions: %v", err)
	}
	if coreSubCount != 1 {
		t.Errorf("expected 1 active `core` subscription, got %d", coreSubCount)
	}

	// 4. CHO-1630 Phase 2 — outbox_events row landed atomically with
	// the three bootstrap rows. Topic, payload non-empty, status
	// pending (dispatcher will flip to published after the broker
	// ack). outbox_events is not tenant-scoped; bypass SET LOCAL.
	var outboxTopic, outboxStatus, outboxIdemKey string
	var outboxPayloadLen int
	if err := pool.QueryRow(context.Background(), `
        SELECT topic, status, idempotency_key, octet_length(payload)
        FROM outbox_events
        WHERE tenant_id = $1
          AND topic = 'chora.tenancy.tenant.bootstrapped.v1'
    `, b.Tenant.ID).Scan(&outboxTopic, &outboxStatus, &outboxIdemKey, &outboxPayloadLen); err != nil {
		t.Fatalf("verify outbox_events: %v", err)
	}
	if outboxTopic != "chora.tenancy.tenant.bootstrapped.v1" {
		t.Errorf("outbox topic = %q", outboxTopic)
	}
	if outboxStatus != "pending" {
		t.Errorf("outbox status = %q, want pending", outboxStatus)
	}
	if outboxIdemKey != "tenant-bootstrapped:"+b.Tenant.ID {
		t.Errorf("outbox idempotency_key = %q", outboxIdemKey)
	}
	if outboxPayloadLen == 0 {
		t.Errorf("outbox payload empty")
	}

	// Append outbox cleanup to the existing t.Cleanup block. The
	// earlier t.Cleanup deletes children + soft-deletes the tenant
	// but does not touch outbox_events.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM outbox_events WHERE tenant_id = $1`, b.Tenant.ID)
	})
}

func TestIntegration_BootstrapRepository_HasAnyMembership_falseForFreshGCID(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	freshGCID := uuid.NewString()
	got, err := repo.HasAnyMembership(context.Background(), freshGCID)
	if err != nil {
		t.Fatalf("HasAnyMembership: %v", err)
	}
	if got {
		t.Errorf("HasAnyMembership(fresh UUID) should be false")
	}
}

func TestIntegration_BootstrapRepository_HasAnyMembership_trueAfterPersist(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))

	in := bootstrap.Input{
		Name:      "HasAny Probe " + uuid.NewString()[:8],
		OwnerGCID: uuid.NewString(),
	}
	b, err := bootstrap.New(in)
	if err != nil {
		t.Fatalf("bootstrap.New: %v", err)
	}

	ctx := context.Background()
	if err := repo.Persist(ctx, b); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM members WHERE tenant_id = $1`, b.Tenant.ID)
		_, _ = pool.Exec(cleanupCtx,
			`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, b.Tenant.ID)
	})

	got, err := repo.HasAnyMembership(ctx, in.OwnerGCID)
	if err != nil {
		t.Fatalf("HasAnyMembership: %v", err)
	}
	if !got {
		t.Errorf("HasAnyMembership(seeded GCID) should be true")
	}
}

func TestIntegration_BootstrapRepository_Persist_idempotencyViaService(t *testing.T) {
	pool := liveDB(t)
	repo := pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
	// CHO-1751 Phase 1 — the orchestrator now requires an AddOnSubscriber
	// dep. This integration test focuses on the pg side; a no-op subscriber
	// keeps the registry side out of scope.
	svc := bootstrap.NewService(repo, noopBootstrapSubscriber{})

	ctx := context.Background()
	in := bootstrap.Input{
		Name:      "Idem " + uuid.NewString()[:8],
		OwnerGCID: uuid.NewString(),
	}

	out1, err := svc.Bootstrap(ctx, in)
	if err != nil {
		t.Fatalf("first Bootstrap: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM add_on_subscriptions WHERE tenant_id = $1`, out1.TenantID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM members WHERE tenant_id = $1`, out1.TenantID)
		_, _ = pool.Exec(cleanupCtx,
			`UPDATE tenants SET deleted_at = now() WHERE tenant_id = $1`, out1.TenantID)
	})

	// Second attempt for the same GCID must be rejected by the service.
	out2, err := svc.Bootstrap(ctx, in)
	if err == nil {
		t.Fatalf("second Bootstrap should fail; got success out2=%+v", out2)
	}
	if !strings.Contains(err.Error(), "already") {
		t.Errorf("expected ErrAlreadyMember wrap, got %v", err)
	}
}

// noopBootstrapSubscriber satisfies bootstrap.AddOnSubscriber without
// touching the registry — keeps these pg integration tests scoped to the
// repository side. CHO-1751 Phase 1.
type noopBootstrapSubscriber struct{}

func (noopBootstrapSubscriber) Subscribe(_, _, _ string) error { return nil }
