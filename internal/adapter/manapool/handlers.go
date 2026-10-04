// Package manapool — HTTP handlers for the 8 BE-USR-3 endpoints
// (matches chora-contracts/openapi/tenancy-admin.yaml v1.2):
//
//	POST   /api/v1/admin/tenants/{tenantId}/mana-pool                       — create
//	GET    /api/v1/admin/tenants/{tenantId}/mana-pool                       — detail
//	POST   /api/v1/admin/tenants/{tenantId}/mana-pool:topup                 — top-up
//	POST   /api/v1/admin/tenants/{tenantId}/mana-pool:auto-renew            — auto-renew
//	PATCH  /api/v1/admin/tenants/{tenantId}/mana-pool/policy                — set policy
//	POST   /api/v1/admin/tenants/{tenantId}/mana-allocations                — manual allocation
//	GET    /api/v1/admin/tenants/{tenantId}/mana-allocations                — list
//	DELETE /api/v1/admin/tenants/{tenantId}/mana-allocations/{allocationId} — revoke
//
// Endpoints require X-Tenant-Id; the X-GCID header carries the admin actor.
package manapool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// HandlerDeps wires the dependencies for NewServer.
type HandlerDeps struct {
	// Pools is the port, not the in-memory adapter: production binds the
	// pgx-backed pg.ManaPoolRepository here (CHO-2414).
	Pools        pool.PoolStore
	Allocations  *InmemAllocationRepo
	Stripe       StripeClient
	Publisher    Publisher
	LearnerCount *InmemLearnerCounter
}

// NewServer returns the routed http.Handler for the BE-USR-3 endpoints.
func NewServer(deps HandlerDeps) http.Handler {
	mux := http.NewServeMux()
	// Pool routes — collapsed under one prefix; sub-handler dispatches.
	mux.HandleFunc("/api/v1/admin/tenants/", tenantRequired(routerHandler(deps)))
	return mux
}

// routerHandler dispatches the 8 endpoints based on URL shape.
func routerHandler(deps HandlerDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Strip prefix.
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) < 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		tenantID := parts[0]

		// URL forms (after split):
		//   parts[0]=tenantId, parts[1]= "mana-pool" | "mana-pool:topup" |
		//                                 "mana-pool:auto-renew" |
		//                                 "mana-pool" then parts[2]="policy"
		//   parts[0]=tenantId, parts[1]= "mana-allocations" [, parts[2]=allocId]
		switch {
		case parts[1] == "mana-pool":
			if len(parts) == 2 {
				poolDetailHandler(deps, tenantID, w, r)
				return
			}
			if len(parts) == 3 && parts[2] == "policy" {
				policyHandler(deps, tenantID, w, r)
				return
			}
			writeError(w, http.StatusNotFound, "not found")

		case parts[1] == "mana-pool:topup":
			topupHandler(deps, tenantID, w, r)

		case parts[1] == "mana-pool:auto-renew":
			autoRenewHandler(deps, tenantID, w, r)

		case parts[1] == "mana-allocations":
			if len(parts) == 2 {
				allocationsHandler(deps, tenantID, w, r)
				return
			}
			if len(parts) == 3 {
				revokeAllocationHandler(deps, tenantID, parts[2], w, r)
				return
			}
			writeError(w, http.StatusNotFound, "not found")

		default:
			writeError(w, http.StatusNotFound, "not found")
		}
	}
}

// -----------------------------------------------------------------------------
// Middleware
// -----------------------------------------------------------------------------

func tenantRequired(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(r.Header.Get("X-Tenant-Id")) == "" {
			writeError(w, http.StatusBadRequest, "X-Tenant-Id header required")
			return
		}
		next(w, r)
	}
}

// -----------------------------------------------------------------------------
// JSON helpers
// -----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]interface{}{
		"error":   http.StatusText(status),
		"message": msg,
	})
}

func decodeBody(r *http.Request, v interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func headerOf(r *http.Request) EnvelopeHeader {
	return EnvelopeHeader{
		TenantID:    r.Header.Get("X-Tenant-Id"),
		GCID:        r.Header.Get("X-GCID"),
		Traceparent: r.Header.Get("traceparent"),
		Tracestate:  r.Header.Get("tracestate"),
	}
}

// -----------------------------------------------------------------------------
// Pool: create + detail
// -----------------------------------------------------------------------------

type allocPolicyConfigDTO struct {
	Policy            string           `json:"policy"`
	UnitsOnEnrollment int64            `json:"units_on_enrollment,omitempty"`
	TierAmounts       map[string]int64 `json:"tier_amounts,omitempty"`
	AllocationTTLDays int              `json:"allocation_ttl_days,omitempty"`
}

type createPoolReq struct {
	InitialTopupUnits    int64                 `json:"initial_topup_units,omitempty"`
	InitialTopupAmtCents int64                 `json:"initial_topup_amount_cents,omitempty"`
	InitialTopupCurrency string                `json:"initial_topup_currency,omitempty"`
	AutoAllocationPolicy *allocPolicyConfigDTO `json:"auto_allocation_policy,omitempty"`
}

func poolDetailHandler(deps HandlerDeps, tenantID string, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req createPoolReq
		if err := decodeBody(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var policy pool.AllocationPolicy = pool.ManualPolicy{}
		if req.AutoAllocationPolicy != nil {
			cfg, err := dtoToPolicyConfig(req.AutoAllocationPolicy)
			if err != nil {
				writeError(w, http.StatusUnprocessableEntity, err.Error())
				return
			}
			pol, err := pool.PolicyFromConfig(cfg)
			if err != nil {
				writeError(w, http.StatusUnprocessableEntity, err.Error())
				return
			}
			policy = pol
		}
		gcid := r.Header.Get("X-GCID")
		if strings.TrimSpace(gcid) == "" {
			writeError(w, http.StatusBadRequest, "X-GCID header required")
			return
		}
		p, err := pool.NewPool(tenantID, gcid, policy)
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		if err := deps.Pools.Save(r.Context(), p); err != nil {
			if errors.Is(err, pool.ErrPoolAlreadyExists) {
				existing, _ := deps.Pools.GetByTenant(r.Context(), tenantID)
				writeJSON(w, http.StatusConflict, poolDTO(existing))
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		deps.Publisher.PublishPoolCreated(headerOf(r), pool.PoolCreated{
			PoolID: p.PoolID, TenantID: p.TenantID,
			InitialBalanceUnits: p.BalanceUnits, MonthlyTopupUnits: p.MonthlyTopupUnits,
			AutoAllocationPolicy: p.AutoAllocationPolicy.Kind(),
			CreatedByGCID:        p.CreatedByGCID, CreatedAt: p.CreatedAt,
		})
		writeJSON(w, http.StatusCreated, poolDTO(p))

	case http.MethodGet:
		p, err := deps.Pools.GetByTenant(r.Context(), tenantID)
		if err != nil {
			writeError(w, http.StatusNotFound, "pool not found")
			return
		}
		writeJSON(w, http.StatusOK, poolDTO(p))

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// -----------------------------------------------------------------------------
// Pool: top-up
// -----------------------------------------------------------------------------

type topupReq struct {
	AmountCents     int64  `json:"amount_cents"`
	Currency        string `json:"currency"`
	Units           int64  `json:"units"`
	PaymentMethodID string `json:"payment_method_id,omitempty"`
}

func topupHandler(deps HandlerDeps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req topupReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.AmountCents < 100 {
		writeError(w, http.StatusUnprocessableEntity, "amount_cents must be >= 100")
		return
	}
	if req.Units < 1 {
		writeError(w, http.StatusUnprocessableEntity, "units must be >= 1")
		return
	}
	if strings.TrimSpace(req.Currency) == "" {
		writeError(w, http.StatusUnprocessableEntity, "currency required")
		return
	}
	p, err := deps.Pools.GetByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "pool not found")
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	resp, err := deps.Stripe.Charge(r.Context(), ChargeRequest{
		TenantID: tenantID, AmountCents: req.AmountCents, Currency: req.Currency,
		PaymentMethodID: req.PaymentMethodID, IdempotencyKey: idemKey,
	})
	if err != nil {
		writeError(w, http.StatusPaymentRequired, "stripe charge failed: "+err.Error())
		return
	}
	gcid := r.Header.Get("X-GCID")
	if err := p.TopUp(req.Units, resp.AmountCents, resp.Currency, resp.PaymentIntentID, false, gcid); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := deps.Pools.Save(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	deps.Publisher.PublishPoolToppedUp(headerOf(r), pool.PoolToppedUp{
		PoolID: p.PoolID, TenantID: p.TenantID, UnitsCredited: req.Units,
		ChargedCents: resp.AmountCents, Currency: resp.Currency,
		StripePaymentIntentID: resp.PaymentIntentID, AutoRenew: false,
		ToppedUpByGCID: gcid, BalanceAfterUnits: p.BalanceUnits,
		RecordedAt: time.Now().UTC(),
	})
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pool_id":                  p.PoolID,
		"units_credited":           req.Units,
		"charged_cents":            resp.AmountCents,
		"currency":                 resp.Currency,
		"stripe_payment_intent_id": resp.PaymentIntentID,
		"balance_after_units":      p.BalanceUnits,
		"recorded_at":              time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// -----------------------------------------------------------------------------
// Pool: auto-renew
// -----------------------------------------------------------------------------

type autoRenewReq struct {
	MonthlyTopupUnits int64  `json:"monthly_topup_units"`
	Currency          string `json:"currency,omitempty"`
}

func autoRenewHandler(deps HandlerDeps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req autoRenewReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	p, err := deps.Pools.GetByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "pool not found")
		return
	}
	if err := p.SetMonthlyTopupUnits(req.MonthlyTopupUnits); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	_ = deps.Pools.Save(r.Context(), p)
	writeJSON(w, http.StatusOK, poolDTO(p))
}

// -----------------------------------------------------------------------------
// Pool: policy
// -----------------------------------------------------------------------------

type updatePolicyReq struct {
	AutoAllocationPolicy allocPolicyConfigDTO `json:"auto_allocation_policy"`
}

func policyHandler(deps HandlerDeps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req updatePolicyReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg, err := dtoToPolicyConfig(&req.AutoAllocationPolicy)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	policy, err := pool.PolicyFromConfig(cfg)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	p, err := deps.Pools.GetByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "pool not found")
		return
	}
	if err := p.SetPolicy(policy); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	_ = deps.Pools.Save(r.Context(), p)
	writeJSON(w, http.StatusOK, poolDTO(p))
}

// -----------------------------------------------------------------------------
// Allocations
// -----------------------------------------------------------------------------

type createAllocationReq struct {
	GCID      string `json:"gcid"`
	Units     int64  `json:"units"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Note      string `json:"note,omitempty"`
}

type revokeAllocationReq struct {
	Reason string `json:"reason"`
}

func allocationsHandler(deps HandlerDeps, tenantID string, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req createAllocationReq
		if err := decodeBody(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.Units <= 0 {
			writeError(w, http.StatusUnprocessableEntity, "units must be > 0")
			return
		}
		if strings.TrimSpace(req.GCID) == "" {
			writeError(w, http.StatusUnprocessableEntity, "gcid required")
			return
		}
		p, err := deps.Pools.GetByTenant(r.Context(), tenantID)
		if err != nil {
			writeError(w, http.StatusNotFound, "pool not found")
			return
		}
		// Debit pool first; if insufficient → 402.
		if err := p.Debit(req.Units); err != nil {
			if errors.Is(err, pool.ErrInsufficientBalance) {
				writeError(w, http.StatusPaymentRequired, "pool balance insufficient")
				return
			}
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		var expiresAt *time.Time
		if req.ExpiresAt != "" {
			t, err := time.Parse(time.RFC3339, req.ExpiresAt)
			if err != nil {
				writeError(w, http.StatusUnprocessableEntity, "invalid expires_at: "+err.Error())
				return
			}
			expiresAt = &t
		}
		gcidAdmin := r.Header.Get("X-GCID")
		a, err := allocation.New(allocation.NewParams{
			TenantID: tenantID, GCID: req.GCID, SourcePoolID: p.PoolID,
			Units: req.Units, Reason: allocation.ReasonManual,
			AllocatedByGCID: gcidAdmin, ExpiresAt: expiresAt,
		})
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		_ = deps.Pools.Save(r.Context(), p)
		_ = deps.Allocations.Save(r.Context(), a)
		deps.Publisher.PublishAllocationGranted(headerOf(r), allocation.AllocationGranted{
			AllocationID: a.AllocationID, TenantID: tenantID, GCID: a.GCID,
			SourcePoolID: a.SourcePoolID, Units: a.Units, Reason: a.Reason,
			AllocatedByGCID: a.AllocatedByGCID, ExpiresAt: a.ExpiresAt,
			AllocatedAt: a.AllocatedAt,
		})
		// If the debit hit the depletion threshold, emit depleted event too.
		if p.IsFullyDepleted() || p.IsLowBalance() {
			deps.Publisher.PublishPoolDepleted(headerOf(r), pool.PoolDepleted{
				PoolID: p.PoolID, TenantID: tenantID, BalanceUnits: p.BalanceUnits,
				ThresholdUnits: p.LowBalanceThreshold(),
				FullyDepleted:  p.IsFullyDepleted(),
				DetectedAt:     time.Now().UTC(),
			})
		}
		writeJSON(w, http.StatusCreated, allocationDTO(a))

	case http.MethodGet:
		var status allocation.Status
		if s := r.URL.Query().Get("status"); s != "" {
			status = allocation.Status(s)
		}
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 200 {
				limit = n
			}
		}
		items, _, err := deps.Allocations.ListByTenant(r.Context(), tenantID,
			ListAllocationsFilter{
				GCID:   r.URL.Query().Get("gcid"),
				Status: status,
				Limit:  limit,
			})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out := make([]map[string]interface{}, 0, len(items))
		for _, a := range items {
			out = append(out, allocationDTO(a))
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"items": out})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func revokeAllocationHandler(deps HandlerDeps, tenantID, allocationID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req revokeAllocationReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	a, err := deps.Allocations.Get(r.Context(), allocationID)
	if err != nil || a.TenantID != tenantID {
		writeError(w, http.StatusNotFound, "allocation not found")
		return
	}
	res, err := a.Revoke(req.Reason, r.Header.Get("X-GCID"))
	if err != nil {
		if errors.Is(err, allocation.ErrInvalidTransition) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if res.UnitsReturnedToPool {
		p, _ := deps.Pools.GetByTenant(r.Context(), tenantID)
		if p != nil {
			_ = p.Credit(res.ReturnedUnits)
			_ = deps.Pools.Save(r.Context(), p)
		}
	}
	_ = deps.Allocations.Save(r.Context(), a)
	deps.Publisher.PublishAllocationRevoked(headerOf(r), allocation.AllocationRevoked{
		AllocationID: a.AllocationID, TenantID: tenantID, GCID: a.GCID,
		UnitsReturnedToPool: res.UnitsReturnedToPool,
		ReturnedUnits:       res.ReturnedUnits,
		Reason:              a.RevokedReason, RevokedByGCID: a.RevokedByGCID,
		RevokedAt: *a.RevokedAt,
	})
	writeJSON(w, http.StatusOK, allocationDTO(a))
}

// -----------------------------------------------------------------------------
// DTO helpers
// -----------------------------------------------------------------------------

func poolDTO(p *pool.TenantManaPool) map[string]interface{} {
	out := map[string]interface{}{
		"pool_id":                  p.PoolID,
		"tenant_id":                p.TenantID,
		"balance_units":            p.BalanceUnits,
		"monthly_topup_units":      p.MonthlyTopupUnits,
		"auto_allocation_policy":   policyDTO(p.AutoAllocationPolicy),
		"lifetime_topped_up_units": p.LifetimeToppedUpUnits,
		"lifetime_allocated_units": p.LifetimeAllocatedUnits,
		"version":                  p.Version,
		"created_at":               p.CreatedAt.Format(time.RFC3339Nano),
	}
	if p.LastToppedUpAt != nil {
		out["last_topped_up_at"] = p.LastToppedUpAt.Format(time.RFC3339Nano)
	}
	return out
}

func policyDTO(p pool.AllocationPolicy) map[string]interface{} {
	out := map[string]interface{}{"policy": string(p.Kind())}
	switch v := p.(type) {
	case pool.OnEnrollmentPolicy:
		out["units_on_enrollment"] = v.DefaultUnits
	case pool.TierBasedPolicy:
		out["tier_amounts"] = v.TierToUnits
	}
	return out
}

func allocationDTO(a *allocation.TenantManaAllocation) map[string]interface{} {
	out := map[string]interface{}{
		"allocation_id":     a.AllocationID,
		"tenant_id":         a.TenantID,
		"gcid":              a.GCID,
		"units":             a.Units,
		"source_pool_id":    a.SourcePoolID,
		"status":            string(a.Status),
		"allocation_reason": string(a.Reason),
		"allocated_by_gcid": a.AllocatedByGCID,
		"allocated_at":      a.AllocatedAt.Format(time.RFC3339Nano),
	}
	if a.ExpiresAt != nil {
		out["expires_at"] = a.ExpiresAt.Format(time.RFC3339Nano)
	}
	if a.ClaimedAt != nil {
		out["claimed_at"] = a.ClaimedAt.Format(time.RFC3339Nano)
	}
	if a.RevokedAt != nil {
		out["revoked_at"] = a.RevokedAt.Format(time.RFC3339Nano)
		out["revoked_reason"] = a.RevokedReason
	}
	return out
}

// dtoToPolicyConfig converts the wire DTO to the domain config struct.
func dtoToPolicyConfig(dto *allocPolicyConfigDTO) (pool.PolicyConfig, error) {
	if dto == nil {
		return pool.PolicyConfig{}, fmt.Errorf("auto_allocation_policy required")
	}
	return pool.PolicyConfig{
		Policy:            dto.Policy,
		UnitsOnEnrollment: dto.UnitsOnEnrollment,
		TierAmounts:       dto.TierAmounts,
		AllocationTTLDays: dto.AllocationTTLDays,
	}, nil
}

// -----------------------------------------------------------------------------
// Subscriber + Monthly ticker
// -----------------------------------------------------------------------------

// SubscriberDeps wires the subscriber + ticker.
type SubscriberDeps struct {
	// Pools is the port, not the in-memory adapter: production binds the
	// pgx-backed pg.ManaPoolRepository here (CHO-2414).
	Pools        pool.PoolStore
	Allocations  *InmemAllocationRepo
	Publisher    Publisher
	LearnerCount *InmemLearnerCounter
}

// EnrollmentEvent is the input shape for the subscriber. Mirrors the
// payload of chora.identity.user_membership.created.v1 (just the fields
// chora-tenancy cares about).
type EnrollmentEvent struct {
	TenantID   string
	GCID       string
	Role       string
	OccurredAt time.Time
}

// EnrollmentSubscriber consumes chora.identity.user_membership.created.v1
// (or wherever learner enrollment is published) and drives the policy
// engine's auto-allocation path.
type EnrollmentSubscriber struct {
	deps   SubscriberDeps
	engine *pool.PolicyEngine
}

// NewEnrollmentSubscriber wires the subscriber + the policy engine.
func NewEnrollmentSubscriber(deps SubscriberDeps) *EnrollmentSubscriber {
	return &EnrollmentSubscriber{
		deps:   deps,
		engine: pool.NewPolicyEngine(deps.Pools, deps.LearnerCount),
	}
}

// HandleEnrollment is invoked once per inbound enrollment event. The
// learner is registered with the LearnerCounter (driving future EqualSplit
// monthly ticks), then the policy engine produces 0 or 1 allocation
// intent. Each intent is materialised into an aggregate row + pool debit
// + Pub/Sub event atomically.
func (s *EnrollmentSubscriber) HandleEnrollment(ctx context.Context, ev EnrollmentEvent) error {
	if strings.TrimSpace(ev.TenantID) == "" || strings.TrimSpace(ev.GCID) == "" {
		return nil
	}
	s.deps.LearnerCount.Register(ev.TenantID, pool.LearnerContext{GCID: ev.GCID, Role: ev.Role})
	intents, err := s.engine.OnLearnerEnrolled(ctx, ev.TenantID, ev.GCID, pool.LearnerContext{GCID: ev.GCID, Role: ev.Role})
	if err != nil {
		return err
	}
	return materialiseIntents(ctx, s.deps, intents, allocation.Reason("on_enrollment"))
}

// MonthlyTicker fires the EqualSplit distribution per tenant.
type MonthlyTicker struct {
	deps   SubscriberDeps
	engine *pool.PolicyEngine
}

// NewMonthlyTicker constructs a ticker.
func NewMonthlyTicker(deps SubscriberDeps) *MonthlyTicker {
	return &MonthlyTicker{
		deps:   deps,
		engine: pool.NewPolicyEngine(deps.Pools, deps.LearnerCount),
	}
}

// RunMonthlyTick fires for one tenant.
func (t *MonthlyTicker) RunMonthlyTick(ctx context.Context, tenantID string) error {
	intents, err := t.engine.OnMonthlyTick(ctx, tenantID)
	if err != nil {
		return err
	}
	return materialiseIntents(ctx, t.deps, intents, allocation.Reason("equal_split"))
}

// materialiseIntents converts engine intents into aggregate rows + pool
// debits + Pub/Sub events.
func materialiseIntents(ctx context.Context, deps SubscriberDeps, intents []pool.AllocationIntent, fallbackReason allocation.Reason) error {
	for _, in := range intents {
		p, err := deps.Pools.GetByTenant(ctx, in.TenantID)
		if err != nil {
			continue
		}
		if err := p.Debit(in.Units); err != nil {
			continue // skip silently on insufficient
		}
		reason := allocation.Reason(in.Reason)
		if reason == "" {
			reason = fallbackReason
		}
		a, err := allocation.New(allocation.NewParams{
			TenantID: in.TenantID, GCID: in.GCID, SourcePoolID: in.SourcePoolID,
			Units: in.Units, Reason: reason,
		})
		if err != nil {
			continue
		}
		_ = deps.Pools.Save(ctx, p)
		_ = deps.Allocations.Save(ctx, a)
		deps.Publisher.PublishAllocationGranted(EnvelopeHeader{TenantID: in.TenantID, GCID: in.GCID}, allocation.AllocationGranted{
			AllocationID: a.AllocationID, TenantID: in.TenantID, GCID: a.GCID,
			SourcePoolID: a.SourcePoolID, Units: a.Units, Reason: a.Reason,
			AllocatedAt: a.AllocatedAt,
		})
	}
	return nil
}
