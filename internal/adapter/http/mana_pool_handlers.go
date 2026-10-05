// Package httpapi — admin mana-pool surface (L1 Tenant lane, CHO-1705 WP-4).
//
// Routes (tenancy-admin.yaml is the contract):
//
//	GET  /api/v1/admin/tenants/{id}/mana-pool             — pool detail | 404
//	POST /api/v1/admin/tenants/{id}/mana-pool             — create | 409 existing
//	POST /api/v1/admin/tenants/{id}/mana-pool:auto-renew  — set monthly quota
//	GET  /api/v1/admin/tenants/{id}/mana-pool/summary     — dashboard cards
//	POST /api/v1/admin/tenants/{id}/mana-pool/allocations — per-user grant
//
// All reads/writes go through V2Deps.PoolRepo — the REAL TenantManaPool
// aggregate store. main.go binds it to the SAME InmemPoolRepo instance the
// ADR-164 payments subscriber credits (PaymentsPoolApplier), so checkout-
// paid top-ups are visible here. Durability is a named deferral: the store
// is in-memory until the pgx port lands (main.go "pgx port follow-on").
//
// Event note: no chora.tenancy.tenant_mana_pool.* topics exist in the
// event topology yet — create/auto-renew intentionally emit nothing
// (publishing to a nonexistent topic dead-letters the outbox dispatcher).
// The allocations grant keeps its pre-existing mana_pool.adjusted.v1
// emission. Recorded as a deferral on CHO-1705 close.
//
// The full BE-USR-3 endpoint suite (Stripe direct :topup, policy PATCH)
// lives unmounted in internal/adapter/manapool/ — the H+ top-up CTA drives
// the canonical /api/v1/checkout/mana-topup payments path instead.
package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// manaAllocationDashboard is a READ CACHE for the dashboard summary only.
//
// ⚠ It was the de-facto system of record for grants until CHO-2414's
// follow-up, and that is exactly how 400 units left a pool with nothing
// naming their recipient: the debit was durable, this map was not, and a
// restart would have erased the entries. The durable record is now the
// tenant_mana_allocations row written by pg.ManaGrantRepository.SaveGrant,
// and the idempotency check that matters lives inside that transaction.
// Never treat this map as the record again. The POOL balance itself is real (see
// handleManaAllocationsPost debiting via PoolRepo).
type manaAllocationDashboard struct {
	mu       sync.Mutex
	byTenant map[string]map[string]manaAllocationEntry // tenantID → allocID → entry
}

type manaAllocationEntry struct {
	AllocationID    string
	TenantID        string
	TargetGCID      string
	Units           int64
	MonthlyCapUnits int64
	Reason          string
	AllocatedAt     time.Time
	AllocatedByGCID string
}

func newManaAllocationDashboard() *manaAllocationDashboard {
	return &manaAllocationDashboard{
		byTenant: make(map[string]map[string]manaAllocationEntry),
	}
}

// get returns the entry when the allocation id is already registered for
// the tenant (idempotent-retry detection BEFORE any pool debit).
func (d *manaAllocationDashboard) get(tenantID, allocationID string) (manaAllocationEntry, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.byTenant[tenantID]; ok {
		if e, ok := t[allocationID]; ok {
			return e, true
		}
	}
	return manaAllocationEntry{}, false
}

func (d *manaAllocationDashboard) put(entry manaAllocationEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.byTenant[entry.TenantID]; !ok {
		d.byTenant[entry.TenantID] = make(map[string]manaAllocationEntry)
	}
	d.byTenant[entry.TenantID][entry.AllocationID] = entry
}

func (d *manaAllocationDashboard) byTenantSummary(tenantID string) (granted, claimed int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.byTenant[tenantID]; ok {
		// Claimed mirrors the consumption-side claim wire (not built);
		// grant-side count only.
		return len(t), 0
	}
	return 0, 0
}

// callerGCID resolves the caller identity: X-GCID (in-cluster/tests) with
// fallback to the LOWERCASE `gcid` header the phyllis gateway call()
// stamps (it does NOT send X-GCID).
func callerGCID(r *http.Request) string {
	if g := strings.TrimSpace(r.Header.Get("X-GCID")); g != "" {
		return g
	}
	return strings.TrimSpace(r.Header.Get("gcid"))
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

func handleManaPoolGet(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	p, err := deps.PoolRepo.GetByTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, pool.ErrPoolNotFound) {
			v2WriteError(w, http.StatusNotFound, "pool_not_found", "no mana pool for tenant")
			return
		}
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusOK, manaPoolDTO(p))
}

// handleManaPoolCreate provisions the pool. Idempotent on tenant: a second
// call returns 409 + the EXISTING pool body (per tenancy-admin.yaml).
// CreateTenantManaPoolRequest.initial_topup_units is intentionally ignored
// (pool.NewPool starts at balance 0; funding rides :topup / checkout).
func handleManaPoolCreate(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	gcid := callerGCID(r)
	if gcid == "" {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed",
			"caller gcid required (X-GCID or gcid header)")
		return
	}
	if existing, err := deps.PoolRepo.GetByTenant(r.Context(), tenantID); err == nil {
		v2WriteJSON(w, http.StatusConflict, manaPoolDTO(existing))
		return
	} else if !errors.Is(err, pool.ErrPoolNotFound) {
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	p, err := pool.NewPool(tenantID, gcid, pool.ManualPolicy{})
	if err != nil {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	if err := deps.PoolRepo.Save(r.Context(), p); err != nil {
		if errors.Is(err, pool.ErrPoolAlreadyExists) {
			// Concurrent create raced us — surface the winner.
			if existing, gerr := deps.PoolRepo.GetByTenant(r.Context(), tenantID); gerr == nil {
				v2WriteJSON(w, http.StatusConflict, manaPoolDTO(existing))
				return
			}
		}
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusCreated, manaPoolDTO(p))
}

type setAutoRenewReq struct {
	// Pointer distinguishes absent (422) from explicit 0 (disable).
	MonthlyTopupUnits *int64 `json:"monthly_topup_units"`
}

// handleManaPoolAutoRenew sets the monthly auto top-up quota — the clean
// "set quota" op (NO Stripe charge here; SetManaPoolAutoRenewRequest).
func handleManaPoolAutoRenew(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	var req setAutoRenewReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if req.MonthlyTopupUnits == nil {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed",
			"monthly_topup_units required (0 disables auto-renew)")
		return
	}
	p, err := deps.PoolRepo.GetByTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, pool.ErrPoolNotFound) {
			v2WriteError(w, http.StatusNotFound, "pool_not_found", "no mana pool for tenant")
			return
		}
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if err := p.SetMonthlyTopupUnits(*req.MonthlyTopupUnits); err != nil {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	if err := deps.PoolRepo.Save(r.Context(), p); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusOK, manaPoolDTO(p))
}

func handleManaPoolSummary(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	granted, claimed := deps.ManaAllocations.byTenantSummary(tenantID)
	out := map[string]interface{}{
		"tenant_id":                 tenantID,
		"pool_exists":               false,
		"balance_units":             int64(0),
		"monthly_topup_units":       int64(0),
		"allocations_granted_count": granted,
		"allocations_claimed_count": claimed,
		"low_balance_threshold":     int64(0),
		"is_low_balance":            false,
		"chora_imda_dimension":      imdaDimensionAccountability,
	}
	p, err := deps.PoolRepo.GetByTenant(r.Context(), tenantID)
	if err != nil {
		if !errors.Is(err, pool.ErrPoolNotFound) {
			v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		// No pool yet → honest zeros (never a fabricated balance).
		v2WriteJSON(w, http.StatusOK, out)
		return
	}
	out["pool_exists"] = true
	out["balance_units"] = p.BalanceUnits
	out["monthly_topup_units"] = p.MonthlyTopupUnits
	out["low_balance_threshold"] = p.LowBalanceThreshold()
	out["is_low_balance"] = p.IsLowBalance()
	v2WriteJSON(w, http.StatusOK, out)
}

type manaAllocationReq struct {
	AllocationID    string `json:"allocation_id"`
	TargetGCID      string `json:"target_gcid"`
	Units           int64  `json:"units"`
	MonthlyCapUnits int64  `json:"monthly_cap_units"`
	Reason          string `json:"reason"`
}

func handleManaAllocationsPost(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	var req manaAllocationReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if strings.TrimSpace(req.AllocationID) == "" {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "allocation_id required")
		return
	}
	if strings.TrimSpace(req.TargetGCID) == "" {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "target_gcid required")
		return
	}
	if req.Units <= 0 {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "units must be > 0")
		return
	}
	if req.MonthlyCapUnits < req.Units {
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed",
			"monthly_cap_units must be >= units")
		return
	}
	// Idempotent retry: registered id → echo without re-debiting.
	if existing, ok := deps.ManaAllocations.get(tenantID, req.AllocationID); ok {
		writeAllocationAccepted(w, existing, true)
		return
	}
	// Grants debit the REAL pool — no pool / overdraw fail loudly.
	p, err := deps.PoolRepo.GetByTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, pool.ErrPoolNotFound) {
			v2WriteError(w, http.StatusNotFound, "pool_not_found", "no mana pool for tenant")
			return
		}
		v2WriteError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if err := p.Debit(req.Units); err != nil {
		if errors.Is(err, pool.ErrInsufficientBalance) {
			v2WriteError(w, http.StatusUnprocessableEntity, "insufficient_balance",
				"pool balance below requested grant")
			return
		}
		v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	entry := manaAllocationEntry{
		AllocationID:    req.AllocationID,
		TenantID:        tenantID,
		TargetGCID:      req.TargetGCID,
		Units:           req.Units,
		MonthlyCapUnits: req.MonthlyCapUnits,
		Reason:          req.Reason,
		AllocatedAt:     time.Now().UTC(),
		AllocatedByGCID: callerGCID(r),
	}

	// CHO-2414 follow-up. The debit, the allocation record and the grant
	// event now land in ONE transaction via GrantStore. Previously the debit
	// committed on its own, the record went into an in-process map, and the
	// publish error was discarded, so two live grants moved 400 units out of
	// a pool and left nothing naming their recipient.
	//
	// No pgx pool means no durable record is possible, so the endpoint
	// refuses rather than debiting into a map. Same fail-loud shape as the
	// external-egress policy handler.
	if deps.GrantStore == nil {
		v2WriteError(w, http.StatusServiceUnavailable, "grant_store_unavailable",
			"mana grants require a durable store; refusing to debit without one")
		return
	}

	alloc := &allocation.TenantManaAllocation{
		AllocationID:    entry.AllocationID,
		TenantID:        tenantID,
		SourcePoolID:    p.PoolID,
		GCID:            entry.TargetGCID,
		Units:           entry.Units,
		Status:          allocation.StatusPending,
		Reason:          allocation.ReasonManual,
		AllocatedByGCID: entry.AllocatedByGCID,
		AllocatedAt:     entry.AllocatedAt,
		UpdatedAt:       entry.AllocatedAt,
	}
	hdr := v2Header(r)
	grantErr := deps.GrantStore.SaveGrant(r.Context(), p, alloc, pg.GrantEvent{
		Topic:       "chora.tenancy.mana_pool.adjusted.v1",
		GCID:        entry.AllocatedByGCID,
		Traceparent: hdr.Traceparent,
		Payload: map[string]any{
			"tenant_id":            tenantID,
			"allocation_id":        entry.AllocationID,
			"target_gcid":          entry.TargetGCID,
			"units":                entry.Units,
			"monthly_cap_units":    entry.MonthlyCapUnits,
			"reason":               entry.Reason,
			"adjusted_by_gcid":     entry.AllocatedByGCID,
			"adjusted_at":          entry.AllocatedAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		},
	})
	switch {
	case errors.Is(grantErr, pg.ErrGrantAlreadyRecorded):
		// Durable idempotency: this allocation_id is already on record and
		// the pool was NOT debited again.
		writeAllocationAccepted(w, entry, true)
		return
	case errors.Is(grantErr, pg.ErrVersionConflict):
		v2WriteError(w, http.StatusConflict, "concurrent_update",
			"the pool changed while this grant was being applied; retry")
		return
	case grantErr != nil:
		v2WriteError(w, http.StatusInternalServerError, "internal", grantErr.Error())
		return
	}

	// Read cache for the dashboard summary only. The durable record is the
	// tenant_mana_allocations row written above; this map is no longer the
	// system of record and must never be treated as one again.
	deps.ManaAllocations.put(entry)
	writeAllocationAccepted(w, entry, false)
}

func writeAllocationAccepted(w http.ResponseWriter, e manaAllocationEntry, retry bool) {
	v2WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"tenant_id":         e.TenantID,
		"allocation_id":     e.AllocationID,
		"target_gcid":       e.TargetGCID,
		"units":             e.Units,
		"monthly_cap_units": e.MonthlyCapUnits,
		"allocated_at":      e.AllocatedAt.Format(time.RFC3339Nano),
		"idempotent_retry":  retry,
	})
}

// manaPoolDTO projects the aggregate per tenancy-admin.yaml TenantManaPool
// (+ the dashboard's low-balance extras).
func manaPoolDTO(p *pool.TenantManaPool) map[string]interface{} {
	var lastToppedUp interface{}
	if p.LastToppedUpAt != nil {
		lastToppedUp = p.LastToppedUpAt.Format(time.RFC3339Nano)
	}
	return map[string]interface{}{
		"pool_id":                  p.PoolID,
		"tenant_id":                p.TenantID,
		"balance_units":            p.BalanceUnits,
		"monthly_topup_units":      p.MonthlyTopupUnits,
		"lifetime_topped_up_units": p.LifetimeToppedUpUnits,
		"lifetime_allocated_units": p.LifetimeAllocatedUnits,
		"version":                  p.Version,
		"created_at":               p.CreatedAt.Format(time.RFC3339Nano),
		"last_topped_up_at":        lastToppedUp,
		"low_balance_threshold":    p.LowBalanceThreshold(),
		"is_low_balance":           p.IsLowBalance(),
	}
}
