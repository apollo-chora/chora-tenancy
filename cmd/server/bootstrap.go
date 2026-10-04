// bootstrap.go — production wiring helpers for chora-tenancy server.
//
// Same pattern as services/chora-identity/cmd/server/bootstrap.go.
//
// Environment contract:
//
//	CHORA_DB_DSN_SECRET_ID  — Secret Manager secret name resolving to
//	                          a chora_tenancy DSN (app_rw role).
//	CHORA_DB_DSN            — direct DSN (dev override).
//	CHORA_DB_PROJECT        — GCP project for Secret Manager (default
//	                          chora-489812).
//	CHORA_DB_REWRITE_FROM_PORT / TO_PORT — DSN port rewrite (e.g. 6432
//	                                       → 5432 to bypass PgBouncer).
//	CHORA_PUBSUB_PROJECT    — GCP project hosting Pub/Sub topics.
//	CHORA_OUTBOX_DSN        — direct DSN to chora_tenancy for the
//	                          D6.2 outbox PostgresStore (M12.3 W2b).
//	                          Empty = in-memory outbox store (dev).
//	CHORA_OUTBOX_WORKER_ID  — dispatcher worker_id stamped onto
//	                          deadletter rows; defaults to HOSTNAME.
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/apollo-chora/chora-common/db"
	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
)

func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	if dsn == "" {
		log.Printf("tenancy: CHORA_DB_DSN unset — using in-memory repositories")
		return nil, nil
	}

	bootstrapCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := cgcdb.Bootstrap(bootstrapCtx, cgcdb.BootstrapOptions{
		DSN:           dsn,
		AppName:       serviceName + "@" + serviceVersion,
		RuntimeParams: tenancyDBRuntimeParams(),
	})
	if err != nil {
		log.Fatalf("tenancy: pgx pool bootstrap failed: %v", err)
	}
	return pool, pool.Close
}

func bootstrapPubSubClient(ctx context.Context) (cgcpubsub.CloudPubSubClient, func()) {
	project := os.Getenv("PUBSUB_PROJECT_ID")
	if project == "" {
		return nil, nil
	}
	cli, err := cgcpubsub.NewGCPClient(ctx, project)
	if err != nil {
		log.Printf("tenancy: Pub/Sub client init failed: %v — falling back to in-memory recorder", err)
		return nil, nil
	}
	return cli, func() { _ = cli.Close() }
}

func bootstrapOutboxDB(ctx context.Context) (*sql.DB, func()) {
	dsn := os.Getenv("CHORA_OUTBOX_DSN")
	if dsn == "" {
		dsn = os.Getenv("CHORA_DB_DSN")
	}
	if dsn == "" {
		return nil, nil
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		log.Fatalf("tenancy: outbox database ping failed: %v", err)
	}
	return db, func() { _ = db.Close() }
}

// workerID derives the dispatcher worker_id from CHORA_OUTBOX_WORKER_ID
// or HOSTNAME. Returns the fallback chora-tenancy-local when neither is
// set (the dispatcher requires a non-empty worker_id at construction).
func workerID() string {
	if v := os.Getenv("CHORA_OUTBOX_WORKER_ID"); v != "" {
		return v
	}
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	return "chora-tenancy-local"
}

// sqlDBAdapter bridges *sql.DB (driver-typed *sql.Rows) to the outbox's
// SQLDB interface (which uses outbox.SQLRows so tests can stub). Since
// *sql.Rows already satisfies tenancyoutbox.SQLRows's method set
// (Next + Scan + Close + Err), the adapter is a thin wrapper.
type sqlDBAdapter struct {
	db *sql.DB
}

func (a sqlDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

func (a sqlDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (tenancyoutbox.SQLRows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

// Compile-time check: sqlDBAdapter satisfies tenancyoutbox.SQLDB.
var _ tenancyoutbox.SQLDB = sqlDBAdapter{}

// wireV2DepsEvents binds the V2Deps.Events port to the canonical
// OutboxPublisher so every HTTP-handler emission (tenant.created /
// addon.activated / addon.deactivated / addon.upgraded /
// addon.downgraded / addon.usage_recorded / member.invited /
// member.suspended / mana_pool.adjusted / ...) writes a durable
// outbox_events row the Dispatcher drains to Cloud Pub/Sub.
//
// This is the load-bearing composition-root call that closes debt #50
// — pre-fix the v2 handlers defaulted to the in-process
// events.Recorder, which is a ring buffer that never reaches Pub/Sub.
//
// A nil pub argument is treated as a dev-mode safety net: the default
// in-memory recorder set by httpapi.NewDefaultV2Deps stays in place so
// the binary remains runnable even when the outbox publisher could not
// be wired (e.g. no pgx pool, no DSN — environment-driven). Production
// callers MUST pass a real *tenancyoutbox.Publisher; the env gate
// upstream is responsible for failing-loud when prod-mode wiring is
// incomplete.
//
// Per the hexagonal `hexagonal` skill: this helper lives in the
// composition root (cmd/server), the http adapter stays decoupled from
// the outbox adapter, and the domain layer never sees either.
func wireV2DepsEvents(deps *httpapi.V2Deps, pub *tenancyoutbox.Publisher) {
	if deps == nil || pub == nil {
		return
	}
	deps.Events = pub
}

// tenancyDBRuntimeParams returns the per-connection Postgres GUCs that keep a
// DB write from hanging forever (fail-loud). Platform-wide rollout of the
// CHO-2005 fix (2026-07-04); mirrors chora-consumption's
// consumptionDBRuntimeParams. Set on every pooled connection via
// BootstrapOptions.RuntimeParams.
//
//   - lock_timeout=3s: a statement blocked on a row lock ERRORs ("canceling
//     statement due to lock timeout") instead of waiting indefinitely and
//     leaking the request goroutine — under the gateway's 6s per-call
//     timeout, so this service fails loud (500) before the gateway 504s.
//   - idle_in_transaction_session_timeout=60s: reaps a leaked open
//     transaction so its row locks release.
//
// No statement_timeout (owner steer): long read paths must not be capped.
func tenancyDBRuntimeParams() map[string]string {
	return map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	}
}
