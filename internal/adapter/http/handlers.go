// Package httpapi is the HTTP/JSON adapter for chora-tenancy.
//
// Hexagonal layout: this layer depends on `internal/domain/tenancy` (the
// pure domain) and on `internal/adapter/inmem` (the persistence adapter).
// The domain layer NEVER imports anything from this package.
//
// Endpoints (per the M11+ Phase D brief):
//
//	GET    /healthz | /healthz/ | /health
//	GET    /readyz  | /readyz/
//	POST   /api/tenants
//	GET    /api/tenants
//	GET    /api/tenants/{id}
//	POST   /api/tenants/{id}/parent           (set parent_tenant_id; rejects self-cycle)
//	POST   /api/addons
//	GET    /api/addons
//	POST   /api/tenants/{id}/entitlements
//	DELETE /api/tenants/{id}/entitlements/{addon_id}
//	GET    /api/tenants/{id}/entitlements
//	POST   /api/tenants/{id}/invoices
//	GET    /api/tenants/{id}/invoices
//
// Middleware extracts X-Tenant-Id from request headers and rejects requests
// missing X-Tenant-Id on /api/* paths.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	domain "github.com/apollo-chora/chora-tenancy/internal/domain/tenancy"
	"github.com/apollo-chora/chora-tenancy/internal/observability"
)

// Deps wires the persistence ports into the legacy /api/* HTTP layer.
//
// A7 (docs/m13/handoff-fe-to-be-service-2026-05-14.md §A7): the fields are
// hexagonal PORT interfaces (ports.go), NOT concrete in-memory stores.
// cmd/server wires the pgx adapters (internal/adapter/pg, the Legacy*
// types — backed by chora_tenancy) when CHORA_DB_DSN(_SECRET_ID) is set,
// and falls back to the in-memory adapters only in local dev / unit tests.
// The handler signatures are unchanged.
type Deps struct {
	Tenants        TenantStore
	AddOnCatalog   AddOnStore
	Entitlements   EntitlementStore
	Invoices       InvoiceStore
	PaymentMethods PaymentMethodStore
}

// NewServer builds the routed http.Handler.
func NewServer(deps Deps) http.Handler {
	mux := http.NewServeMux()

	// Health/readiness — no tenant required.
	//
	// Some managed front-ends intercept the bare /healthz path before it
	// reaches the container and return their own 404. /healthz/ (trailing
	// slash) and /health both reach the container, so we register all three
	// aliases.
	mux.HandleFunc("/healthz", logging(healthHandler))
	mux.HandleFunc("/healthz/", logging(healthHandler))
	mux.HandleFunc("/health", logging(healthHandler))
	mux.HandleFunc("/readyz", logging(readyHandler))
	mux.HandleFunc("/readyz/", logging(readyHandler))

	// /api/tenants — list + create
	mux.HandleFunc("/api/tenants", logging(tenantRequired(tenantsHandler(deps))))
	// /api/tenants/{id}, /api/tenants/{id}/parent, /api/tenants/{id}/entitlements...,
	// /api/tenants/{id}/invoices
	mux.HandleFunc("/api/tenants/", logging(tenantRequired(tenantSubHandler(deps))))

	// /api/addons — list + create (admin-only catalog)
	mux.HandleFunc("/api/addons", logging(tenantRequired(addOnsHandler(deps))))

	return mux
}

// -----------------------------------------------------------------------------
// Middleware
// -----------------------------------------------------------------------------

// logging wraps a handler with structured-log emission per request and
// echoes traceparent so callers can correlate.
func logging(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tc := observability.FromRequest(r)
		w.Header().Set("traceparent", tc.TraceparentHeader())
		sw := &statusWriter{ResponseWriter: w}
		next(sw, r)
		observability.LogRequest(
			tc,
			r.Header.Get("X-Tenant-Id"),
			r.Header.Get("gcid"),
			r.Method,
			r.URL.Path,
			sw.status,
			time.Since(start),
		)
	}
}

// tenantRequired enforces presence of X-Tenant-Id.
func tenantRequired(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(r.Header.Get("X-Tenant-Id")) == "" {
			writeError(w, http.StatusBadRequest, "X-Tenant-Id header required")
			return
		}
		next(w, r)
	}
}

// statusWriter captures the response status for the access log.
type statusWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader stamps the status code and forwards.
func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

// Write defaults status to 200 if not yet set.
func (sw *statusWriter) Write(p []byte) (int, error) {
	if sw.status == 0 {
		sw.status = http.StatusOK
	}
	return sw.ResponseWriter.Write(p)
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

// -----------------------------------------------------------------------------
// Health
// -----------------------------------------------------------------------------

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": observability.ServiceName,
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ready",
		"service": observability.ServiceName,
	})
}

// -----------------------------------------------------------------------------
// Tenants
// -----------------------------------------------------------------------------

type createTenantReq struct {
	DisplayName    string `json:"display_name"`
	SelfHosted     bool   `json:"self_hosted"`
	ParentTenantID string `json:"parent_tenant_id"`
}

type setParentReq struct {
	ParentTenantID string `json:"parent_tenant_id"`
}

func tenantsHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CHO-2077: root-tenant create (POST) + list-ALL (GET) are
		// platform_operator-only (mirrors bootstrap_handler.callerIsPlatformOperator).
		// Defense-in-depth — see handleV1Tenants. writeError shape kept for the
		// legacy /api response contract.
		if (r.Method == http.MethodPost || r.Method == http.MethodGet) &&
			!callerIsPlatformOperator(r.Header) {
			writeError(w, http.StatusForbidden,
				"root tenant management requires the platform_operator role")
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req createTenantReq
			if err := decodeBody(r, &req); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			t, err := domain.NewTenant(req.DisplayName, req.SelfHosted, req.ParentTenantID)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := deps.Tenants.Save(r.Context(), t); err != nil {
				fmt.Printf("httpapi: POST /api/tenants persist failed: %v\n", err)
				writeError(w, http.StatusInternalServerError, "failed to persist tenant")
				return
			}
			writeJSON(w, http.StatusCreated, tenantDTO(t))

		case http.MethodGet:
			offset, limit := paging(r)
			items, total := deps.Tenants.List(offset, limit)
			out := make([]map[string]interface{}, 0, len(items))
			for _, t := range items {
				out = append(out, tenantDTO(t))
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"items": out,
				"total": total,
			})

		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// tenantSubHandler dispatches /api/tenants/{id}, /api/tenants/{id}/parent,
// /api/tenants/{id}/entitlements[...], /api/tenants/{id}/invoices.
func tenantSubHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		tenantID := parts[0]

		// /api/tenants/{id}
		if len(parts) == 1 {
			handleTenantByID(deps, tenantID, w, r)
			return
		}

		switch parts[1] {
		case "parent":
			if len(parts) != 2 {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			handleSetParent(deps, tenantID, w, r)
		case "entitlements":
			handleEntitlementSub(deps, tenantID, parts[2:], w, r)
		case "invoices":
			if len(parts) != 2 {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			handleInvoiceSub(deps, tenantID, w, r)
		default:
			writeError(w, http.StatusNotFound, "not found")
		}
	}
}

func handleTenantByID(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t, ok := deps.Tenants.Get(tenantID)
		if !ok {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		}
		writeJSON(w, http.StatusOK, tenantDTO(t))
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func handleSetParent(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	t, ok := deps.Tenants.Get(tenantID)
	if !ok {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	var req setParentReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := t.SetParent(req.ParentTenantID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := deps.Tenants.Save(r.Context(), t); err != nil {
		fmt.Printf("httpapi: POST /api/tenants/%s/parent persist failed: %v\n", tenantID, err)
		writeError(w, http.StatusInternalServerError, "failed to persist tenant")
		return
	}
	writeJSON(w, http.StatusOK, tenantDTO(t))
}

// -----------------------------------------------------------------------------
// AddOns (catalog)
// -----------------------------------------------------------------------------

type registerAddOnReq struct {
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	MonthlyPriceCents    int64    `json:"monthly_price_cents"`
	IncludedCapabilities []string `json:"included_capabilities"`
}

func addOnsHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req registerAddOnReq
			if err := decodeBody(r, &req); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			a, err := domain.NewAddOn(req.Name, req.Description, req.MonthlyPriceCents, req.IncludedCapabilities)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			deps.AddOnCatalog.Register(a)
			writeJSON(w, http.StatusCreated, addOnDTO(a))

		case http.MethodGet:
			items := deps.AddOnCatalog.List()
			out := make([]map[string]interface{}, 0, len(items))
			for _, a := range items {
				out = append(out, addOnDTO(a))
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"items": out,
				"total": len(items),
			})

		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// -----------------------------------------------------------------------------
// Entitlements
// -----------------------------------------------------------------------------

type subscribeReq struct {
	AddOnID string `json:"addon_id"`
}

func handleEntitlementSub(deps Deps, tenantID string, rest []string, w http.ResponseWriter, r *http.Request) {
	// /api/tenants/{id}/entitlements
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodPost:
			handleSubscribe(deps, tenantID, w, r)
		case http.MethodGet:
			handleListEntitlements(deps, tenantID, w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	// /api/tenants/{id}/entitlements/{addon_id}
	if len(rest) == 1 {
		addOnID := rest[0]
		switch r.Method {
		case http.MethodDelete:
			handleCancelEntitlement(deps, tenantID, addOnID, w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

func handleSubscribe(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if _, ok := deps.Tenants.Get(tenantID); !ok {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	var req subscribeReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	addOn, ok := deps.AddOnCatalog.Get(req.AddOnID)
	if !ok {
		writeError(w, http.StatusNotFound, "addon not found")
		return
	}
	e, err := deps.Entitlements.Subscribe(tenantID, addOn.ID, addOn.MonthlyPriceCents)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Real impl: emit Stripe subscription event here. Placeholder only.
	writeJSON(w, http.StatusCreated, entitlementDTO(e))
}

func handleCancelEntitlement(deps Deps, tenantID, addOnID string, w http.ResponseWriter, r *http.Request) {
	e, err := deps.Entitlements.Cancel(tenantID, addOnID)
	if err != nil {
		if errors.Is(err, domain.ErrEntitlementNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Real impl: emit Stripe cancel-subscription event here. Placeholder only.
	writeJSON(w, http.StatusOK, entitlementDTO(e))
}

func handleListEntitlements(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	items := deps.Entitlements.ListActiveByTenant(tenantID)
	out := make([]map[string]interface{}, 0, len(items))
	for _, e := range items {
		out = append(out, entitlementDTO(e))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": out,
		"total": len(items),
	})
}

// -----------------------------------------------------------------------------
// Invoices
// -----------------------------------------------------------------------------

type generateInvoiceReq struct {
	PeriodStart string `json:"period_start"` // RFC3339
	PeriodEnd   string `json:"period_end"`
}

func handleInvoiceSub(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handleGenerateInvoice(deps, tenantID, w, r)
	case http.MethodGet:
		handleListInvoices(deps, tenantID, w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func handleGenerateInvoice(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if _, ok := deps.Tenants.Get(tenantID); !ok {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	var req generateInvoiceReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	start, err := time.Parse(time.RFC3339, req.PeriodStart)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid period_start: "+err.Error())
		return
	}
	end, err := time.Parse(time.RFC3339, req.PeriodEnd)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid period_end: "+err.Error())
		return
	}
	active := deps.Entitlements.ListActiveByTenant(tenantID)
	inv, err := domain.GenerateInvoice(tenantID, start, end, active)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	deps.Invoices.Save(inv)
	writeJSON(w, http.StatusCreated, invoiceDTO(inv))
}

func handleListInvoices(deps Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	items := deps.Invoices.ListByTenant(tenantID)
	out := make([]map[string]interface{}, 0, len(items))
	for _, inv := range items {
		out = append(out, invoiceDTO(inv))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": out,
		"total": len(items),
	})
}

// -----------------------------------------------------------------------------
// DTOs (JSON shape)
// -----------------------------------------------------------------------------

func tenantDTO(t *domain.Tenant) map[string]interface{} {
	out := map[string]interface{}{
		"id":               t.ID,
		"display_name":     t.DisplayName,
		"branding_config":  t.BrandingConfig,
		"parent_tenant_id": t.ParentTenantID,
		"self_hosted":      t.SelfHosted,
		"status":           string(t.Status),
		"created_at":       t.CreatedAt.Format(time.RFC3339Nano),
		"updated_at":       t.UpdatedAt.Format(time.RFC3339Nano),
	}
	if t.DeletedAt != nil {
		out["deleted_at"] = t.DeletedAt.Format(time.RFC3339Nano)
	}
	return out
}

func addOnDTO(a *domain.AddOn) map[string]interface{} {
	caps := a.IncludedCapabilities
	if caps == nil {
		caps = []string{}
	}
	return map[string]interface{}{
		"id":                    a.ID,
		"name":                  a.Name,
		"description":           a.Description,
		"monthly_price_cents":   a.MonthlyPriceCents,
		"included_capabilities": caps,
		"created_at":            a.CreatedAt.Format(time.RFC3339Nano),
	}
}

func entitlementDTO(e *domain.TenantEntitlement) map[string]interface{} {
	out := map[string]interface{}{
		"id":        e.ID,
		"tenant_id": e.TenantID,
		"addon_id":  e.AddOnID,
		// CHO-1717 §4.6 addon_code bridge — CONTRACT PIN for the FE
		// FeatureFlagService.isEnabled('<code>') lane: the JSON field
		// name is exactly `addon_code` (string; may be empty for
		// legacy rows whose add_ons.code predates migration 0021).
		"addon_code":                   e.AddOnCode,
		"monthly_price_cents_snapshot": e.MonthlyPriceCentsSnapshot,
		"status":                       string(e.Status),
		"activated_at":                 e.ActivatedAt.Format(time.RFC3339Nano),
		"updated_at":                   e.UpdatedAt.Format(time.RFC3339Nano),
	}
	if e.CancelledAt != nil {
		out["cancelled_at"] = e.CancelledAt.Format(time.RFC3339Nano)
	}
	return out
}

func invoiceDTO(inv *domain.Invoice) map[string]interface{} {
	lines := make([]map[string]interface{}, 0, len(inv.LineItems))
	for _, li := range inv.LineItems {
		lines = append(lines, map[string]interface{}{
			"addon_id":         li.AddOnID,
			"entitlement_id":   li.EntitlementID,
			"unit_price_cents": li.UnitPriceCents,
			"quantity":         li.Quantity,
			"line_total_cents": li.LineTotalCents,
		})
	}
	return map[string]interface{}{
		"id":                inv.ID,
		"tenant_id":         inv.TenantID,
		"period_start":      inv.PeriodStart.Format(time.RFC3339),
		"period_end":        inv.PeriodEnd.Format(time.RFC3339),
		"line_items":        lines,
		"total_cents":       inv.TotalCents,
		"stripe_invoice_id": inv.StripeInvoiceID,
		"created_at":        inv.CreatedAt.Format(time.RFC3339Nano),
	}
}

// -----------------------------------------------------------------------------
// Misc
// -----------------------------------------------------------------------------

func paging(r *http.Request) (offset, limit int) {
	limit = 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 200 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return offset, limit
}
