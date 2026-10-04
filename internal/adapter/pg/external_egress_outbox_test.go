package pg

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/external_egress"
)

func TestBuildExternalEgressOutboxRow_Guards(t *testing.T) {
	now := time.Now().UTC()
	if _, err := buildExternalEgressOutboxRow(external_egress.Policy{}, external_egress.Policy{}, now, ""); err == nil {
		t.Fatal("expected error for empty tenant_id")
	}
	if _, err := buildExternalEgressOutboxRow(
		external_egress.Policy{TenantID: "t-1"}, external_egress.Policy{}, now, ""); err == nil {
		t.Fatal("expected error for empty updated_by_gcid")
	}
}
