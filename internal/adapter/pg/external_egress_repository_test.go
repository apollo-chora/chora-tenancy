// Unit specs for the pgx-backed external-egress policy adapter (CHO-2148).
//
// The LOAD-BEARING property under test is atomicity: the policy row and the
// `chora.tenancy.external_egress_policy.updated.v1` outbox row must be written
// inside ONE transaction. If they can drift apart, a crash between them leaves
// chora_tenancy saying "egress ON" while the chora-observability projection the
// model-gateway actually reads still says OFF (or, far worse, the reverse).
//
// These are unit tests against a stub tx-runner — no live database. The live
// SQL is separately covered by the prepare-smoke integration test.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

const (
	egressTenantID = "11111111-1111-7111-8111-111111111111"
	egressActor    = "00000000-0000-7000-8000-000000001999"
)

// ---------------------------------------------------------------------------
// stub tx-runner — records every statement executed inside the single tx
// ---------------------------------------------------------------------------

type egressStubTx struct {
	execs        []stubExecCall
	rows         []stubRowCall
	rowResponses []func(dest ...any) error
}

func (t *egressStubTx) Exec(_ context.Context, sql string, args ...any) error {
	t.execs = append(t.execs, stubExecCall{sql: sql, args: args})
	return nil
}

func (t *egressStubTx) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, errors.New("egressStubTx: Query not used by this adapter")
}

func (t *egressStubTx) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	t.rows = append(t.rows, stubRowCall{sql: sql, args: args})
	if len(t.rowResponses) == 0 {
		return &stubRow{scanFn: func(_ ...any) error { return pg.ErrNoRows }}
	}
	r := t.rowResponses[0]
	t.rowResponses = t.rowResponses[1:]
	return &stubRow{scanFn: r}
}

type egressStubRunner struct {
	tx           *egressStubTx
	gotTenantID  string
	txInvocations int
}

func (r *egressStubRunner) RunInTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.Tx) error) error {
	r.txInvocations++
	r.gotTenantID = tenantID
	return fn(ctx, r.tx)
}

func newEgressRunner(responses ...func(dest ...any) error) *egressStubRunner {
	return &egressStubRunner{tx: &egressStubTx{rowResponses: responses}}
}

func okVersionScan(v int64) func(dest ...any) error {
	return func(dest ...any) error {
		if len(dest) > 0 {
			if p, ok := dest[0].(*int64); ok {
				*p = v
			}
		}
		return nil
	}
}

// ---------------------------------------------------------------------------
// Save — atomicity (the load-bearing property)
// ---------------------------------------------------------------------------

func TestExternalEgressRepository_Save_WritesPolicyAndOutboxInOneTx(t *testing.T) {
	t.Parallel()

	runner := newEgressRunner(okVersionScan(1))
	repo := pg.NewExternalEgressRepository(runner)

	prev := external_egress.DefaultPolicy(egressTenantID)
	next := prev
	if _, err := next.Update(true, 50, egressActor, time.Now().UTC()); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := repo.Save(context.Background(), next, prev); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if runner.txInvocations != 1 {
		t.Fatalf("RunInTenantTx invoked %d times, want exactly 1 — "+
			"the policy row and the outbox row MUST share one transaction",
			runner.txInvocations)
	}
	if runner.gotTenantID != egressTenantID {
		t.Errorf("tx tenant = %q, want %q (SET LOCAL chora.tenant_id must scope the RLS write)",
			runner.gotTenantID, egressTenantID)
	}

	// The upsert is a QueryRow (it RETURNs the version for the OCC check);
	// the outbox insert is an Exec. Both must be on the SAME tx object.
	if len(runner.tx.rows) != 1 {
		t.Fatalf("expected 1 QueryRow (the policy upsert), got %d", len(runner.tx.rows))
	}
	if !strings.Contains(runner.tx.rows[0].sql, "tenant_external_egress_policies") {
		t.Errorf("first statement must upsert the policy table, got: %s", runner.tx.rows[0].sql)
	}

	if len(runner.tx.execs) != 1 {
		t.Fatalf("expected exactly 1 Exec (the outbox insert) inside the tx, got %d", len(runner.tx.execs))
	}
	if !strings.Contains(runner.tx.execs[0].sql, "outbox_events") {
		t.Errorf("the in-tx Exec must be the outbox insert, got: %s", runner.tx.execs[0].sql)
	}
}

func TestExternalEgressRepository_Save_OutboxCarriesTopicVersionAndActor(t *testing.T) {
	t.Parallel()

	runner := newEgressRunner(okVersionScan(2))
	repo := pg.NewExternalEgressRepository(runner)

	prev := external_egress.DefaultPolicy(egressTenantID)
	if _, err := prev.Update(true, 50, egressActor, time.Now().UTC()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	next := prev
	if _, err := next.Update(false, 50, egressActor, time.Now().UTC()); err != nil {
		t.Fatalf("opt-out: %v", err)
	}

	if err := repo.Save(context.Background(), next, prev); err != nil {
		t.Fatalf("Save: %v", err)
	}

	outbox := runner.tx.execs[0]
	joined := argsToString(outbox.args)

	if !strings.Contains(joined, pg.TopicExternalEgressPolicyUpdated) {
		t.Errorf("outbox row must carry topic %q; args = %s",
			pg.TopicExternalEgressPolicyUpdated, joined)
	}
	if !strings.Contains(joined, egressTenantID) {
		t.Errorf("outbox row must carry the tenant_id; args = %s", joined)
	}
	if !strings.Contains(joined, egressActor) {
		t.Errorf("outbox row must carry the actor GCID — an egress change names its actor (IMDA D1); args = %s", joined)
	}
}

// A concurrent writer already advanced the version: the upsert's OCC guard
// matches no row, so the adapter must FAIL LOUD, not silently drop the write.
func TestExternalEgressRepository_Save_ConcurrentUpdate_FailsLoud(t *testing.T) {
	t.Parallel()

	runner := newEgressRunner(func(_ ...any) error { return pg.ErrNoRows })
	repo := pg.NewExternalEgressRepository(runner)

	prev := external_egress.DefaultPolicy(egressTenantID)
	next := prev
	if _, err := next.Update(true, 50, egressActor, time.Now().UTC()); err != nil {
		t.Fatalf("Update: %v", err)
	}

	err := repo.Save(context.Background(), next, prev)
	if !errors.Is(err, external_egress.ErrConcurrentUpdate) {
		t.Fatalf("Save with a stale version: err = %v, want ErrConcurrentUpdate — "+
			"a lost update could silently re-enable egress a tenant just turned OFF", err)
	}
}

// ---------------------------------------------------------------------------
// Get — default-deny on absence
// ---------------------------------------------------------------------------

func TestExternalEgressRepository_Get_NoRow_ReturnsFailClosedDefault(t *testing.T) {
	t.Parallel()

	runner := newEgressRunner(func(_ ...any) error { return pg.ErrNoRows })
	repo := pg.NewExternalEgressRepository(runner)

	p, err := repo.Get(context.Background(), egressTenantID)
	if err != nil {
		t.Fatalf("Get on a never-opted-in tenant must NOT error — absence is the denial, not a fault: %v", err)
	}
	if p.EgressEnabled {
		t.Error("a tenant with no row must read as egress DISABLED (ADR-220 D4 default-deny)")
	}
	if p.Persisted() {
		t.Error("a tenant with no row must report Persisted() == false")
	}
	if p.TenantID != egressTenantID {
		t.Errorf("TenantID = %q, want %q", p.TenantID, egressTenantID)
	}
	// The read must not have written anything.
	if len(runner.tx.execs) != 0 {
		t.Errorf("Get must not write — a read must never fabricate the row whose "+
			"absence keeps a franchise tenant safe; got %d Exec calls", len(runner.tx.execs))
	}
}

func TestExternalEgressRepository_Get_Row_ReturnsPersistedPolicy(t *testing.T) {
	t.Parallel()

	runner := newEgressRunner(func(dest ...any) error {
		// tenant_id, egress_enabled, daily_call_ceiling, version, updated_by_gcid, created_at, updated_at
		if len(dest) < 7 {
			t.Fatalf("Get scans %d columns, expected 7", len(dest))
		}
		*(dest[0].(*string)) = egressTenantID
		*(dest[1].(*bool)) = true
		*(dest[2].(*int32)) = 25
		*(dest[3].(*int64)) = 7
		*(dest[4].(*string)) = egressActor
		*(dest[5].(*time.Time)) = time.Now().UTC()
		*(dest[6].(*time.Time)) = time.Now().UTC()
		return nil
	})
	repo := pg.NewExternalEgressRepository(runner)

	p, err := repo.Get(context.Background(), egressTenantID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !p.EgressEnabled {
		t.Error("EgressEnabled = false, want true")
	}
	if p.DailyCallCeiling != 25 {
		t.Errorf("DailyCallCeiling = %d, want 25", p.DailyCallCeiling)
	}
	if p.Version != 7 {
		t.Errorf("Version = %d, want 7", p.Version)
	}
	if !p.Persisted() {
		t.Error("a tenant WITH a row must report Persisted() == true")
	}
}

func argsToString(args []any) string {
	var sb strings.Builder
	for _, a := range args {
		switch v := a.(type) {
		case string:
			sb.WriteString(v)
		case []byte:
			sb.Write(v)
		}
		sb.WriteString("|")
	}
	return sb.String()
}
