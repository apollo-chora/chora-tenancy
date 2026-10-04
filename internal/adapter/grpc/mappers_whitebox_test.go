// mappers_whitebox_test.go — white-box (package grpc) coverage for the
// pure domain ⇄ proto mapper helpers in the transaction-history +
// create-subtenant servers: enum tables, cursor parsing, UUID shape guard
// and DTO projections. The RPC-level behaviour is covered by the external
// bufconn tests; these pin the mappers directly.
package grpc

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"

	"github.com/apollo-chora/chora-tenancy/internal/domain/bootstrap"
	tl "github.com/apollo-chora/chora-tenancy/internal/domain/transactionledger"
)

func TestKindMappers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		proto tenancyv1.TransactionKind
		dom   string
	}{
		{tenancyv1.TransactionKind_TRANSACTION_KIND_PURCHASE, tl.KindPurchase},
		{tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_TOPUP, tl.KindManaTopup},
		{tenancyv1.TransactionKind_TRANSACTION_KIND_MANA_SPEND_DAILY, tl.KindManaSpendDaily},
		{tenancyv1.TransactionKind_TRANSACTION_KIND_UNSPECIFIED, ""},
	}
	for _, tc := range cases {
		if got := protoKindToDomain(tc.proto); got != tc.dom {
			t.Fatalf("protoKindToDomain(%v) = %q", tc.proto, got)
		}
		if got := domainKindToProto(tc.dom); got != tc.proto {
			t.Fatalf("domainKindToProto(%q) = %v", tc.dom, got)
		}
	}
	if got := domainKindToProto("bogus"); got != tenancyv1.TransactionKind_TRANSACTION_KIND_UNSPECIFIED {
		t.Fatalf("unknown kind should be unspecified, got %v", got)
	}
}

func TestStatusMappers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		proto tenancyv1.TransactionStatus
		dom   string
	}{
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_CAPTURED, tl.StatusCaptured},
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_REFUNDED, tl.StatusRefunded},
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_FAILED, tl.StatusFailed},
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_EXPIRED, tl.StatusExpired},
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_POSTED, tl.StatusPosted},
		{tenancyv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED, ""},
	}
	for _, tc := range cases {
		if got := protoStatusToDomain(tc.proto); got != tc.dom {
			t.Fatalf("protoStatusToDomain(%v) = %q", tc.proto, got)
		}
		if got := domainStatusToProto(tc.dom); got != tc.proto {
			t.Fatalf("domainStatusToProto(%q) = %v", tc.dom, got)
		}
	}
	if got := domainStatusToProto("nope"); got != tenancyv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED {
		t.Fatalf("unknown status should be unspecified, got %v", got)
	}
}

func TestScopeAndFormatMappers(t *testing.T) {
	t.Parallel()
	for proto, want := range map[tenancyv1.TransactionScope]string{
		tenancyv1.TransactionScope_TRANSACTION_SCOPE_LEARNER:     "learner",
		tenancyv1.TransactionScope_TRANSACTION_SCOPE_TENANT:      "tenant",
		tenancyv1.TransactionScope_TRANSACTION_SCOPE_MASTER:      "master",
		tenancyv1.TransactionScope_TRANSACTION_SCOPE_UNSPECIFIED: "",
	} {
		if got := scopeName(proto); got != want {
			t.Fatalf("scopeName(%v) = %q want %q", proto, got, want)
		}
	}
	if got := domainFormatToProto(tl.FormatCSV); got != tenancyv1.ExportFormat_EXPORT_FORMAT_CSV {
		t.Fatalf("csv format mismatch")
	}
	if got := domainFormatToProto(tl.FormatJSON); got != tenancyv1.ExportFormat_EXPORT_FORMAT_JSON {
		t.Fatalf("json format mismatch")
	}
	if got := domainFormatToProto("xml"); got != tenancyv1.ExportFormat_EXPORT_FORMAT_UNSPECIFIED {
		t.Fatalf("unknown format should be unspecified")
	}
	if _, err := protoFormatToDomain(tenancyv1.ExportFormat_EXPORT_FORMAT_CSV); err != nil {
		t.Fatalf("csv: %v", err)
	}
	if _, err := protoFormatToDomain(tenancyv1.ExportFormat_EXPORT_FORMAT_JSON); err != nil {
		t.Fatalf("json: %v", err)
	}
	if _, err := protoFormatToDomain(tenancyv1.ExportFormat_EXPORT_FORMAT_UNSPECIFIED); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	for st, want := range map[string]tenancyv1.ExportStatus{
		tl.ExportPending:  tenancyv1.ExportStatus_EXPORT_STATUS_PENDING,
		tl.ExportBuilding: tenancyv1.ExportStatus_EXPORT_STATUS_BUILDING,
		tl.ExportReady:    tenancyv1.ExportStatus_EXPORT_STATUS_READY,
		tl.ExportFailed:   tenancyv1.ExportStatus_EXPORT_STATUS_FAILED,
		"":                tenancyv1.ExportStatus_EXPORT_STATUS_UNSPECIFIED,
	} {
		if got := domainExportStatusToProto(st); got != want {
			t.Fatalf("domainExportStatusToProto(%q) = %v", st, got)
		}
	}
}

func TestHostingModeMappers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		proto tenancyv1.HostingMode
		dom   string
	}{
		{tenancyv1.HostingMode_HOSTING_MODE_PLATFORM_HOSTED, bootstrap.HostingModePlatformHosted},
		{tenancyv1.HostingMode_HOSTING_MODE_WHITE_LABEL, bootstrap.HostingModeWhiteLabel},
		{tenancyv1.HostingMode_HOSTING_MODE_FRANCHISE, bootstrap.HostingModeFranchise},
		{tenancyv1.HostingMode_HOSTING_MODE_SELF_HOST, bootstrap.HostingModeSelfHost},
		{tenancyv1.HostingMode_HOSTING_MODE_UNSPECIFIED, ""},
	}
	for _, tc := range cases {
		if got := hostingModeToString(tc.proto); got != tc.dom {
			t.Fatalf("hostingModeToString(%v) = %q", tc.proto, got)
		}
		if got := stringToHostingMode(tc.dom); got != tc.proto {
			t.Fatalf("stringToHostingMode(%q) = %v", tc.dom, got)
		}
	}
}

func TestParsePageOffset(t *testing.T) {
	t.Parallel()
	if got := parsePageOffset(""); got != 0 {
		t.Fatalf("blank → 0, got %d", got)
	}
	if got := parsePageOffset(" 25 "); got != 25 {
		t.Fatalf("25 → 25, got %d", got)
	}
	if got := parsePageOffset("abc"); got != 0 {
		t.Fatalf("invalid → 0, got %d", got)
	}
	if got := parsePageOffset("-1"); got != 0 {
		t.Fatalf("negative → 0, got %d", got)
	}
}

func TestLooksLikeUUID(t *testing.T) {
	t.Parallel()
	good := "0197a0b1-2c3d-7e4f-8a9b-0c1d2e3f4a5b"
	if !looksLikeUUID(good) {
		t.Fatalf("expected valid UUID")
	}
	if looksLikeUUID("platform") {
		t.Fatalf("platform sentinel must be rejected")
	}
	if looksLikeUUID("short") {
		t.Fatalf("short string must be rejected")
	}
	if looksLikeUUID(stringsReplace(good, 14, "z")) {
		t.Fatalf("non-hex char must be rejected")
	}
	if looksLikeUUID(stringsReplace(good, 8, "x")) {
		t.Fatalf("wrong dash position must be rejected")
	}
	if looksLikeUUID("") {
		t.Fatalf("empty must be rejected")
	}
}

func TestSingleFranchisee(t *testing.T) {
	t.Parallel()
	if got := singleFranchisee(resolvedScope{tenantIDsInView: []string{"f-1"}}); got != "f-1" {
		t.Fatalf("expected f-1, got %q", got)
	}
	if got := singleFranchisee(resolvedScope{}); got != "" {
		t.Fatalf("span-all → empty, got %q", got)
	}
	if got := singleFranchisee(resolvedScope{tenantIDsInView: []string{"f-1", "f-2"}}); got != "" {
		t.Fatalf("multi → empty, got %q", got)
	}
}

func TestSanitizeIDs(t *testing.T) {
	t.Parallel()
	if got := sanitizeIDs([]string{" a ", "b", "  "}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected sanitize result %v", got)
	}
	if got := sanitizeIDs([]string{"", "  "}); got != nil {
		t.Fatalf("all-empty → nil, got %v", got)
	}
	if got := sanitizeIDs(nil); got != nil {
		t.Fatalf("nil → nil, got %v", got)
	}
}

func TestLedgerAndDetailProtoProjections(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	row := tl.LedgerRow{
		LedgerID: "L1", OccurredAt: at, TenantID: "t1", LearnerGCID: "g1",
		Kind: tl.KindPurchase, SourceDomain: "payments", SourceRefID: "pur1",
		Label: "TMS", Currency: "SGD", AmountMinor: 4900, ManaUnits: 7,
		Status: tl.StatusCaptured, HasDetail: true, MetadataJSON: "{}",
	}
	item := toLedgerItemProto(row)
	if item.LedgerId != "L1" || item.TenantId != "t1" || item.Kind != tenancyv1.TransactionKind_TRANSACTION_KIND_PURCHASE {
		t.Fatalf("unexpected item %+v", item)
	}
	if item.Status != tenancyv1.TransactionStatus_TRANSACTION_STATUS_CAPTURED || !item.HasDetail {
		t.Fatalf("status/has_detail wrong: %+v", item)
	}
	if item.Amount == nil || item.Amount.Currency != "SGD" || item.Amount.AmountMinor != 4900 || item.Amount.ManaUnits != 7 {
		t.Fatalf("amount projection wrong: %+v", item.Amount)
	}
	d := tl.DetailRow{
		DetailID: "D1", LedgerID: "L1", TenantID: "t1", LearnerGCID: "g1",
		OccurredAt: at, ActionCode: "mint", ManaUnits: -3, Model: "gpt-x",
		TraceID: "tr-1", MetadataJSON: "{}",
	}
	dp := toDetailItemProto(d)
	if dp.DetailId != "D1" || dp.ActionCode != "mint" || dp.ManaUnits != -3 || dp.Model != "gpt-x" {
		t.Fatalf("unexpected detail proto %+v", dp)
	}
}

// stringsReplace swaps the char at byte i (test-only helper).
func stringsReplace(s string, i int, c string) string {
	if i < 0 || i >= len(s) {
		return s
	}
	return s[:i] + c + s[i+1:]
}
