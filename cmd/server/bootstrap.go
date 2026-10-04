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
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/5007-Capstone/chora/libs/chora-go-common/db"
	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	cgcsecrets "github.com/5007-Capstone/chora/libs/chora-go-common/secrets"

	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
)

func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	secretID := os.Getenv("CHORA_DB_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		log.Printf("tenancy: CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID unset — using in-memory repositories")
		return nil, nil
	}
	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-489812"
	}
	var fetcher cgcdb.SecretFetcher
	var sclient *cgcsecrets.Client
	if secretID != "" && dsn == "" {
		c, err := cgcsecrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("tenancy: secret manager init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		fetcher = c
	}
	rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT"))
	rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT"))

	// Gate-7 fix: env-driven bootstrap context (default 30s). Under
	// concurrent 11-pod cold-start on GKE, Workload Identity → metadata-
	// server → sqladmin → cloudsql-proxy → Cloud SQL listener saturates
	// the metadata-server and Secret Manager fetch alone can exceed 30s.
	// Set CHORA_BOOTSTRAP_TIMEOUT_SECONDS=90 in the deployment env.
	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	bootstrapCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()
	pool, err := cgcdb.Bootstrap(bootstrapCtx, cgcdb.BootstrapOptions{
		DSN:             dsn,
		SecretID:        secretID,
		SecretFetcher:   fetcher,
		RewriteFromPort: rewriteFrom,
		RewriteToPort:   rewriteTo,
		AppName:         serviceName + "@" + serviceVersion,
		RuntimeParams:   tenancyDBRuntimeParams(),
	})
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("tenancy: pgx pool bootstrap failed (env set, fail-loud — kubelet will CrashLoopBackOff): %v", err)
	}
	shutdown := func() {
		pool.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
	return pool, shutdown
}

func bootstrapPubSubClient(ctx context.Context) (cgcpubsub.CloudPubSubClient, func()) {
	project := os.Getenv("CHORA_PUBSUB_PROJECT")
	if project == "" {
		return nil, nil
	}
	cli, err := cgcpubsub.NewGCPClient(ctx, project)
	if err != nil {
		log.Printf("tenancy: pubsub client init failed: %v — falling back to in-memory recorder", err)
		return nil, nil
	}
	return cli, func() {
		_ = cli.Close()
	}
}

// bootstrapOutboxDB opens a *sql.DB connection to chora_tenancy backing
// the producer-side outbox. Returns (nil, nil) when CHORA_OUTBOX_DSN is
// unset — main() then falls back to the in-memory store.
//
// Per `feedback_no_inline_config` the DSN itself is sourced from
// Workload Identity Federation + Secret Manager. The two-step accept
// (try pgx driver name, then postgres driver name) keeps the binary
// driver-agnostic — whichever driver is registered at compile time
// wins.
func bootstrapOutboxDB(ctx context.Context) (*sql.DB, func()) {
	dsn := os.Getenv("CHORA_OUTBOX_DSN")
	secretID := os.Getenv("CHORA_OUTBOX_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		return nil, nil
	}

	var sclient *cgcsecrets.Client
	if dsn == "" {
		project := os.Getenv("CHORA_DB_PROJECT")
		if project == "" {
			project = "chora-489812"
		}
		c, err := cgcsecrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("tenancy: outbox secret manager init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
		if bootstrapSecs <= 0 {
			bootstrapSecs = 30
		}
		resolveCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
		defer cancel()
		resolved, err := c.GetSecret(resolveCtx, secretID)
		if err != nil {
			_ = c.Close()
			log.Fatalf("tenancy: outbox secret fetch %q failed (env set, fail-loud): %v", secretID, err)
		}
		dsn = resolved
	}

	if rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT")); rewriteFrom != 0 {
		if rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT")); rewriteTo != 0 {
			rewritten, err := cgcdb.RewriteDSNPort(dsn, rewriteFrom, rewriteTo)
			if err != nil {
				if sclient != nil {
					_ = sclient.Close()
				}
				log.Fatalf("tenancy: outbox DSN port rewrite failed (env set, fail-loud): %v", err)
			}
			dsn = rewritten
		}
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		db, err = sql.Open("postgres", dsn)
	}
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("tenancy: outbox sql.Open failed (env set, fail-loud): %v", err)
	}
	if pingErr := db.PingContext(ctx); pingErr != nil {
		_ = db.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("tenancy: outbox db.Ping failed (env set, fail-loud): %v", pingErr)
	}
	return db, func() {
		_ = db.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
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
