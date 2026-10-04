// read.go — the scope-aware READ side of the transaction_ledger CQRS
// projection (B3 / CHO-1939, ADR-205). Companion to ledger.go (the write
// side). This file is proto-free: it defines the domain ReadRepository port +
// the proto-independent query/result shapes the gRPC adapter maps onto the
// chora.services.tenancy.v1.TransactionHistoryService contract.
//
// Scope → isolation (ADR-205 D1) is resolved by the gRPC adapter into a
// GUCTenantID the repo sets as the RLS session value:
//
//   - learner / tenant / master-single-franchisee → a tenant UUID (RLS pins
//     that tenant; learner adds a learner_gcid predicate).
//   - master span-all                              → the "platform" sentinel
//     (the migration-0024 platform-sentinel-aware policy returns ALL tenants).
//
// The repo NEVER decides authorization; it executes an already-authorized,
// already-audited query. The gRPC adapter owns the mesh-claim → scope mapping
// and the mandatory cross-tenant audit BEFORE any span-all read (IMDA D1).
package transactionledger

import (
	"context"
	"strings"
	"time"
)

// PlatformScope is the blessed RLS session sentinel for operator span-all
// (matches libs/chora-go-common/rls.ValidateTenantID + the migration-0024
// platform-sentinel-aware policy). When ListQuery.GUCTenantID == PlatformScope
// the read spans every tenant.
const PlatformScope = "platform"

// DefaultWindowDays is the server-bounded first-paint window (ADR-205 D5): a
// List/Summary with no explicit lower bound defaults to the most recent
// ~90 days rather than an unbounded scan.
const DefaultWindowDays = 90

// Page-size policy (ADR-205 / transaction-history proto): the allowed set is
// {10, 20, 50, 100}; list pages default to 20, drill-down detail pages to 50.
const (
	DefaultListPageSize   = 20
	DefaultDetailPageSize = 50
)

var allowedPageSizes = map[int]bool{10: true, 20: true, 50: true, 100: true}

// NormalizePageSize clamps a requested page size to the allowed set, falling
// back to def for 0 (unset) or any out-of-set value. Keeps the keyset page
// bounded regardless of client input.
func NormalizePageSize(requested, def int) int {
	if requested == 0 || !allowedPageSizes[requested] {
		return def
	}
	return requested
}

// DefaultFranchiseePageSize bounds one page of the master's franchisee picker.
const DefaultFranchiseePageSize = 50

// Franchisee is one tenant a MASTER may narrow the ledger to — a child of the
// platform-root chora-master tenant, surfaced for the O+/H+ franchisee filter.
type Franchisee struct {
	TenantID string
	Name     string
}

// FranchiseeLister lists the franchisee tenants a MASTER may filter by, name-
// searchable + offset-paginated (fetch limit+1 to detect a next page). Backed
// by the RLS-free tenants registry (children of chora-master); NO cross-DB. An
// optional server dependency — unset deployments answer the RPC with
// Unavailable rather than a fabricated empty list.
type FranchiseeLister interface {
	ListFranchisees(ctx context.Context, q string, limit, offset int) ([]Franchisee, error)
}

// SortField is the keyset-compatible column a List page orders by.
type SortField int

const (
	// SortByOccurredAt orders by occurred_at (the default timeline order).
	SortByOccurredAt SortField = iota
	// SortByAmount orders by a unified amount magnitude:
	// COALESCE(amount_minor, ABS(mana_units), 0). Fiat minor units and mana
	// units share one numeric axis (cross-unit value is NOT normalised — ADR-205
	// defers cross-currency summing); the order is deterministic + keyset-stable.
	SortByAmount
)

// SortSpec is a parsed sort directive.
type SortSpec struct {
	Field SortField
	Desc  bool
}

// ParseSort maps the contract sort strings ("occurred_at:desc" |
// "occurred_at:asc" | "amount:desc" | "amount:asc") to a SortSpec. Unknown /
// empty input falls back to the default occurred_at:desc.
func ParseSort(s string) SortSpec {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "occurred_at:asc":
		return SortSpec{Field: SortByOccurredAt, Desc: false}
	case "amount:desc":
		return SortSpec{Field: SortByAmount, Desc: true}
	case "amount:asc":
		return SortSpec{Field: SortByAmount, Desc: false}
	default: // "" | "occurred_at:desc" | anything unrecognised
		return SortSpec{Field: SortByOccurredAt, Desc: true}
	}
}

// ReadFilters is the resolved, proto-free filter set shared by List + Summary.
// From/To are ALWAYS set by the gRPC adapter (after applying the default
// window) so the repo never runs an unbounded scan. Kind/Status == "" mean "no
// filter on this axis"; LearnerGCID == "" means "no learner predicate".
type ReadFilters struct {
	Kind   string // "" = all; else KindPurchase | KindManaTopup | KindManaSpendDaily
	Status string // "" = all; else one of the Status* consts
	From   time.Time
	To     time.Time
	Sort   SortSpec
	// LearnerGCIDs restricts to a SET of learners (OR; SQL learner_gcid = ANY).
	// Empty = no learner predicate (tenant/master full view). Learner scope
	// forces this to the caller's own single-element set. A single drilled
	// learner is a 1-element set (back-compat with the former singular field).
	LearnerGCIDs []string
	// ManagedTenantIDs restricts to a SET of franchisee tenants via the SQL
	// predicate `tenant_id = ANY`. Set ONLY for a MASTER multi-franchisee
	// select (2+) that runs span-all ('platform') and narrows in SQL — a single
	// franchisee uses the RLS GUC, not a predicate; empty = span all tenants.
	ManagedTenantIDs []string
}

// LedgerRow is one unified timeline row read back from the projection (the
// read counterpart to write-side Entry — adds LedgerID + HasDetail and carries
// metadata as the raw JSON string the gRPC layer passes straight through).
type LedgerRow struct {
	LedgerID     string
	OccurredAt   time.Time
	TenantID     string
	LearnerGCID  string
	Kind         string
	SourceDomain string
	SourceRefID  string
	Label        string
	Currency     string
	AmountMinor  int64
	ManaUnits    int64
	Status       string
	HasDetail    bool
	MetadataJSON string
}

// DetailRow is one per-call mana-spend row under a mana_spend_daily parent.
type DetailRow struct {
	DetailID     string
	LedgerID     string
	TenantID     string
	LearnerGCID  string
	OccurredAt   time.Time
	ActionCode   string
	ManaUnits    int64
	Model        string
	TraceID      string
	MetadataJSON string
}

// Summary is the server-computed KPI panel over a filtered window (ADR-205 D5).
type Summary struct {
	TotalCount            int64
	AmountMinorByCurrency map[string]int64 // currency → net captured (captured − refunded)
	TotalManaToppedUp     int64
	TotalManaSpent        int64            // positive magnitude
	CountByKind           map[string]int64 // domain Kind* const → count
	CountByStatus         map[string]int64 // domain Status* const → count
	WindowFrom            time.Time
	WindowTo              time.Time
}

// ListQuery is a fully-resolved, already-authorized list request.
type ListQuery struct {
	GUCTenantID string // tenant UUID | PlatformScope
	Filters     ReadFilters
	Cursor      string // opaque page token (empty on first page)
	Limit       int    // normalized page size (the +1 fetch is the repo's concern)
}

// ListPage is one page of unified rows + the cursor for the next page.
type ListPage struct {
	Items         []LedgerRow
	NextPageToken string // "" on the final page
}

// DetailQuery drills a single parent row open into its per-call detail rows.
type DetailQuery struct {
	GUCTenantID string // tenant UUID | PlatformScope
	LedgerID    string
	LearnerGCID string // "" = no extra constraint; set for learner scope (own rows only)
	Cursor      string
	Limit       int
}

// DetailPage echoes the parent row + one page of its detail rows.
type DetailPage struct {
	Parent        LedgerRow
	Found         bool
	Details       []DetailRow
	NextPageToken string
}

// SummaryQuery is a resolved KPI request over the same scope + filtered window.
type SummaryQuery struct {
	GUCTenantID string // tenant UUID | PlatformScope
	Filters     ReadFilters
}

// AuditFilter is the proto-free echo of the filter a cross-tenant (master)
// read was performed under — the IMDA-D1 accountability evidence stamped onto
// the cross_tenant_payments_viewed audit event BEFORE the span-all read
// (ADR-205 D1 / ADR-165). Kind / Status are the domain Kind*/Status* consts
// (or ""); From / To are the resolved window (nil when unbounded on that axis).
type AuditFilter struct {
	Kind   string
	Status string
	From   *time.Time
	To     *time.Time
}

// ReadRepository is the scope-aware read port for the projection. All methods
// run under the GUCTenantID RLS session value; the implementation NEVER joins
// other domain databases (cross-DB forbidden) — it reads the single
// chora_tenancy projection the event subscribers populate.
type ReadRepository interface {
	// List returns one keyset page of unified rows honouring filters + sort +
	// cursor, newest-first by default (ADR-205 D5 bounded window).
	List(ctx context.Context, q ListQuery) (ListPage, error)

	// GetDetail returns the parent row (Found=false when not visible under the
	// scope) + one keyset page of its per-call detail rows.
	GetDetail(ctx context.Context, q DetailQuery) (DetailPage, error)

	// GetSummary returns server-computed KPIs over the filtered window.
	GetSummary(ctx context.Context, q SummaryQuery) (Summary, error)
}
