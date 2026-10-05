// Package db is the local, dependency-light pgxpool bootstrap for
// chora-tenancy.
//
// It replaces chora-common/db, whose package-level import of
// chora-common/secrets (a managed secret store client) pulled a cloud SDK into
// the module graph even for services that only ever pass a direct DSN.
//
// Configuration is DSN-only: the local stack hands the connection string in
// through the environment (CHORA_DB_DSN / CHORA_OUTBOX_DSN). The pool tuning
// mirrors the shared helper so behaviour is unchanged.
package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultMaxConns          = 20
	defaultMinConns          = 5
	defaultMaxConnIdleTime   = 5 * time.Minute
	defaultMaxConnLifetime   = time.Hour
	defaultHealthCheckPeriod = 30 * time.Second
	// bootPingAttempts / bootPingBase bound the boot-time ping retry so a
	// slow-to-start database does not fail the pod immediately.
	bootPingAttempts = 6
	bootPingBase     = 500 * time.Millisecond
)

// ErrEmptyDSN is returned by Bootstrap when no DSN is supplied.
var ErrEmptyDSN = errors.New("db: DSN is required")

// Options controls Bootstrap.
type Options struct {
	// DSN is the direct connection string. Required.
	DSN string
	// RewriteFromPort, RewriteToPort — when non-zero, rewrite the DSN host
	// port from the source to the target (e.g. 6432 → 5432). Kept for
	// compatibility with DSNs authored for a pooled endpoint.
	RewriteFromPort int
	RewriteToPort   int
	// AppName tags the connection via application_name.
	AppName string
	// RuntimeParams sets per-connection Postgres GUCs (add-only).
	RuntimeParams map[string]string
	// MaxConns / MinConns override the pool ceiling / pre-warm floor.
	MaxConns int32
	MinConns int32
}

// Bootstrap returns a fully-warmed *pgxpool.Pool. The caller owns Close().
func Bootstrap(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	dsn := opts.DSN
	if dsn == "" {
		return nil, ErrEmptyDSN
	}
	if opts.RewriteFromPort != 0 && opts.RewriteToPort != 0 {
		rewritten, err := RewriteDSNPort(dsn, opts.RewriteFromPort, opts.RewriteToPort)
		if err != nil {
			return nil, fmt.Errorf("db.Bootstrap: rewrite port: %w", err)
		}
		dsn = rewritten
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("db.Bootstrap: parse: %w", err)
	}

	cfg.MaxConns = defaultMaxConns
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	cfg.MinConns = defaultMinConns
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	}
	cfg.MaxConnIdleTime = defaultMaxConnIdleTime
	cfg.MaxConnLifetime = defaultMaxConnLifetime
	cfg.HealthCheckPeriod = defaultHealthCheckPeriod

	if opts.AppName != "" {
		ensureRuntimeParams(cfg)
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.AppName
	}
	ensureRuntimeParams(cfg)
	for k, v := range opts.RuntimeParams {
		if _, exists := cfg.ConnConfig.RuntimeParams[k]; !exists {
			cfg.ConnConfig.RuntimeParams[k] = v
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db.Bootstrap: new pool: %w", err)
	}
	if err := pingWithBackoff(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db.Bootstrap: ping after retry: %w", err)
	}
	return pool, nil
}

func ensureRuntimeParams(cfg *pgxpool.Config) {
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
}

func pingWithBackoff(ctx context.Context, pool *pgxpool.Pool) error {
	var err error
	for attempt := 0; attempt < bootPingAttempts; attempt++ {
		if err = pool.Ping(ctx); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bootPingBase * time.Duration(attempt+1)):
		}
	}
	return err
}

// RewriteDSNPort returns a copy of dsn with the host port replaced from
// `from` to `to`. A DSN whose host has no port, or a different port, is
// returned unchanged. Empty input is rejected.
func RewriteDSNPort(dsn string, from, to int) (string, error) {
	if dsn == "" {
		return "", errors.New("db: RewriteDSNPort: empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("db: RewriteDSNPort: parse: %w", err)
	}
	port := u.Port()
	if port == "" {
		return dsn, nil
	}
	got, err := parsePort(port)
	if err != nil || got != from {
		return dsn, nil
	}
	host := u.Hostname()
	if to == 0 {
		u.Host = host
	} else {
		u.Host = fmt.Sprintf("%s:%d", host, to)
	}
	return u.String(), nil
}

func parsePort(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
