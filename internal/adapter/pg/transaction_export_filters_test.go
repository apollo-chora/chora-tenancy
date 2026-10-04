// transaction_export_filters_test.go — white-box (package pg) unit coverage
// for the frozen-filter JSON round-trip used by the async export worker.
//
// ANTI-LEAK INVARIANT (ADR-208 / CHO-1930): the export worker replays a job
// via GUCForTenant(job.TenantID) + job.Filters. For a MASTER multi-franchisee
// export the job.TenantID is the nil-uuid span-all sentinel, so the ONLY thing
// that keeps the export scoped to the selected franchisees is the
// ManagedTenantIDs predicate riding in the frozen Filters. If it does not
// round-trip through marshal → unmarshal, a multi-franchisee export silently
// spans EVERY tenant. This test pins that round-trip (franchisee + learner).
package pg

import (
	"testing"
	"time"

	tl "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

func TestExportFilters_RoundTrip_CarriesFranchiseeAndLearnerSets(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC)
	in := tl.ReadFilters{
		Kind:                tl.KindPurchase,
		Status:              tl.StatusCaptured,
		From:                from,
		To:                  to,
		Sort:                tl.SortSpec{Field: tl.SortByAmount, Desc: true},
		LearnerGCIDs:        []string{"11111111-1111-7111-8111-111111111111"},
		ManagedTenantIDs: []string{"0197aaaa-bbbb-7ccc-8ddd-eeeeffff0000", "0197bbbb-cccc-7ddd-8eee-ffff11112222"},
	}

	blob, err := marshalExportFilters(in)
	if err != nil {
		t.Fatalf("marshalExportFilters: %v", err)
	}
	out, err := unmarshalExportFilters(blob)
	if err != nil {
		t.Fatalf("unmarshalExportFilters: %v", err)
	}

	if len(out.ManagedTenantIDs) != 2 ||
		out.ManagedTenantIDs[0] != in.ManagedTenantIDs[0] ||
		out.ManagedTenantIDs[1] != in.ManagedTenantIDs[1] {
		t.Fatalf("franchisee set did NOT round-trip (anti-leak): got %v want %v",
			out.ManagedTenantIDs, in.ManagedTenantIDs)
	}
	if len(out.LearnerGCIDs) != 1 || out.LearnerGCIDs[0] != in.LearnerGCIDs[0] {
		t.Errorf("learner set did NOT round-trip: got %v want %v", out.LearnerGCIDs, in.LearnerGCIDs)
	}
	if out.Kind != in.Kind || out.Status != in.Status {
		t.Errorf("kind/status lost: got (%q,%q)", out.Kind, out.Status)
	}
	if !out.From.Equal(in.From) || !out.To.Equal(in.To) {
		t.Errorf("window lost: got [%v,%v]", out.From, out.To)
	}
	if out.Sort.Field != in.Sort.Field || out.Sort.Desc != in.Sort.Desc {
		t.Errorf("sort lost: got %+v", out.Sort)
	}
}
