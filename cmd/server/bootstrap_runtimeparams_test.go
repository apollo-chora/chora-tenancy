// bootstrap_runtimeparams_test.go — the chora-tenancy pool's fail-loud GUCs.
//
// Platform-wide rollout of the CHO-2005 fix (2026-07-04): with no
// lock_timeout anywhere, any write blocked on a row lock hangs forever and
// leaks the request goroutine (gateway 504 facade). These per-connection
// GUCs make such a wait fail loud and reap leaked idle-in-transaction
// holders. Mirrors chora-consumption's bootstrap_runtimeparams_test.go.
package main

import "testing"

func TestTenancyDBRuntimeParams_BoundLockAndIdleTx(t *testing.T) {
	t.Parallel()
	p := tenancyDBRuntimeParams()

	// lock_timeout must be set and sit UNDER the gateway's 6s per-call
	// timeout so the service errors out (500) before the gateway 504s.
	if got := p["lock_timeout"]; got != "3s" {
		t.Errorf("lock_timeout = %q, want 3s", got)
	}
	// idle_in_transaction_session_timeout reaps a leaked open transaction
	// so its row locks release.
	if got := p["idle_in_transaction_session_timeout"]; got != "60s" {
		t.Errorf("idle_in_transaction_session_timeout = %q, want 60s", got)
	}
	// No statement_timeout: long read paths must not be capped (owner steer).
	if _, set := p["statement_timeout"]; set {
		t.Errorf("statement_timeout must not be set, got %q", p["statement_timeout"])
	}
}
