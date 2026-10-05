// Package httpapi (v2 + BE-T-ADD-1) — HTTP/JSON adapter for the new
// chora-tenancy sub-domains (tenant lifecycle + members + add-on
// lifecycle + billing) wired against the v2 test contract AND the
// OpenAPI 1.1.0 admin endpoints.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/manapool"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/adapter/repo/inmem"
	stripestub "github.com/apollo-chora/chora-tenancy/internal/adapter/stripe"
	addon "github.com/apollo-chora/chora-tenancy/internal/domain/add_on"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing"
	"github.com/apollo-chora/chora-tenancy/internal/domain/external_egress"
	"github.com/apollo-chora/chora-tenancy/internal/domain/member"
	"github.com/apollo-chora/chora-tenancy/internal/domain/tenant"
	allocation "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_allocation"
	pool "github.com/apollo-chora/chora-tenancy/internal/domain/tenant_mana_pool"
)

// PublishedEvent is a re-export of events.PublishedEvent for spy interfaces.
type PublishedEvent = events.PublishedEvent

// Publisher is the narrow port the v1 + v2 HTTP handlers use to emit
// chora.tenancy.* events. Production wires the OutboxPublisher
// (durable: writes outbox_events rows that the Dispatcher drains to
// the event bus). Tests wire *events.Recorder (in-process ring buffer
// with spy methods Recorded() / RecordedByTopic()).
//
// Per `feedback_d6_resilience_first_class` Pillar 2 + the hexagonal
// `hexagonal` skill: the port stays NARROW — spy methods belong on the
// concrete test recorder, not the production contract.
type Publisher interface {
	Publish(topic string, h events.Header, payload map[string]interface{}) PublishedEvent
	PublishWithError(topic string, h events.Header, payload map[string]interface{}) (PublishedEvent, error)
}

// ManaGrantStore is the port the grant handler depends on.
// *pg.ManaGrantRepository satisfies it in production.
type ManaGrantStore interface {
	SaveGrant(ctx context.Context, p *pool.TenantManaPool, a *allocation.TenantManaAllocation, ev pg.GrantEvent) error
}

// V2Deps holds the v2 server dependencies.
type V2Deps struct {
	Tenants          *tenant.Registry
	Members          *member.Registry
	Subscriptions    *inmem.SubscriptionStore
	Catalogue        *addon.Catalogue
	Invoices         *billing.InvoiceRepo
	PaymentMethods   *billing.PaymentMethodRepo
	Events           Publisher
	Stripe           *stripestub.Client
	A2APartnerActive func(tenantID, addOnCode string) bool
	// Mana pool surface (L1 CHO-1705 WP-4) — the REAL TenantManaPool store.
	// main.go binds this to the SAME instance the ADR-164 payments
	// subscriber credits, so checkout-paid top-ups show in the H+ read.
	PoolRepo pool.PoolStore
	// GrantStore persists a tenant-to-member grant atomically (debit,
	// allocation row and outbox event in one transaction). Nil in dev with no
	// pgx pool, in which case the grant endpoint refuses with 503 rather than
	// debiting a pool it cannot record against.
	//
	// A port, not the concrete repository: the handler must be testable
	// without a database, and depending on the adapter type directly made it
	// impossible to assert the 503 and conflict paths.
	GrantStore      ManaGrantStore
	ManaAllocations *manaAllocationDashboard
	// PaymentsHTTP (CHO-1767) dispatches change-tier + preview-tier-change
	// to chora-payments' HTTP endpoints (CHO-1764 + CHO-1765). When nil,
	// handlers fall back to the legacy direct-registry / stripestub path —
	// existing tests + standalone smoke without CHORA_PAYMENTS_HTTP_ADDR
	// keep working.
	PaymentsHTTP PaymentsHTTPClient
	// EgressPolicies (CHO-2148) is the source of truth for the per-tenant
	// external web-egress entitlement (Far Sight). A write here publishes
	// chora.tenancy.external_egress_policy.updated.v1 in the SAME transaction;
	// chora-observability projects it into the read-copy the model-gateway
	// consults fail-closed on every grounded call.
	EgressPolicies external_egress.Repository
}

// PaymentsHTTPClient is the minimal surface the v2 handlers need from
// the chora-tenancy payments adapter. Defined here (unexported) so this
// package doesn't import the payments adapter directly — main.go wires
// the concrete impl.
type PaymentsHTTPClient interface {
	ChangeTenantAddonTier(ctx context.Context, in TenancyPaymentsChangeTierIn) (TenancyPaymentsChangeTierOut, error)
	PreviewTenantAddonTier(ctx context.Context, in TenancyPaymentsPreviewIn) (TenancyPaymentsPreviewOut, error)
}

// TenancyPaymentsChangeTierIn / Out mirror the payments adapter shape
// without importing it. cmd/server provides a thin adapter that
// translates between the two struct sets.
type TenancyPaymentsChangeTierIn struct {
	TenantID       string
	AdminGCID      string
	AddonCode      string
	TargetTierCode string
	Currency       string
	ProrationMode  string
	// EffectiveAt — "" / "immediate" / "end_of_cycle" (CHO-1772).
	EffectiveAt string
}

type TenancyPaymentsChangeTierOut struct {
	PurchaseID           string
	StripeSubscriptionID string
	FromTier             string
	ToTier               string
	BillingDeltaCents    int64
	Currency             string
	EffectiveAt          string
	SubscriptionStatus   string
	// CHO-1772 — schedule fields populated on the end_of_cycle path.
	DeferredToCycleEnd   bool
	ScheduleID           string
	ScheduledTierCode    string
	ScheduledEffectiveAt string
}

type TenancyPaymentsPreviewIn struct {
	TenantID       string
	AddonCode      string
	TargetTierCode string
	Currency       string
	ProrationMode  string
	// EffectiveAt — mirrors TenancyPaymentsChangeTierIn.EffectiveAt.
	EffectiveAt string
}

type TenancyPaymentsPreviewOut struct {
	PurchaseID            string
	FromTier              string
	ToTier                string
	BillingDeltaCents     int64
	NextInvoiceTotalCents int64
	Currency              string
	ProrationMode         string
	// CHO-1772 — schedule fields populated on the end_of_cycle path.
	DeferredToCycleEnd bool
	EffectiveAt        string
}

// NewDefaultV2Deps wires a fresh V2Deps with the seed catalogue.
func NewDefaultV2Deps() V2Deps {
	cat := addon.NewSeedCatalogue()
	return V2Deps{
		Tenants:          tenant.NewRegistry(),
		Members:          member.NewRegistry(),
		Subscriptions:    inmem.NewSubscriptionStore(cat),
		Catalogue:        cat,
		Invoices:         billing.NewInvoiceRepo(),
		PaymentMethods:   billing.NewPaymentMethodRepo(),
		Events:           events.NewRecorder(),
		Stripe:           stripestub.New(stripestub.Config{APIURL: "https://stripe.invalid/v1"}),
		A2APartnerActive: func(string, string) bool { return false },
		PoolRepo:         manapool.NewInmemPoolRepo(),
		ManaAllocations:  newManaAllocationDashboard(),
	}
}

// NewV2Server returns the v2 http.Handler with all v2 + BE-T-ADD-1 routes.
//
// Mounts (in order):
//
//	/v1/tenants                  Phyllis MVP §5.2 light-wizard create + GET (v1_handlers.go)
//	/v1/admin/tenants/{id}/...   Phyllis MVP §5.2 go-live + addon lifecycle (v1_handlers.go)
//	/v2/healthz, /v2/tenants     Existing v2 (v2_handlers.go)
//	/api/v1/admin/tenants/...    BE-T-ADD-1 admin endpoints (v2_handlers.go)
//	/api/v1/admin/marketplace... BE-T-ADD-1 marketplace browse (v2_handlers.go)
func NewV2Server(deps V2Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/healthz", v2Logging(v2Health))
	mux.HandleFunc("/v2/healthz/", v2Logging(v2Health))
	mux.HandleFunc("/v2/tenants", v2Logging(v2RequireTenant(handleV2Tenants(deps))))
	mux.HandleFunc("/v2/tenants/", v2Logging(v2RequireTenant(handleV2TenantSub(deps))))
	mux.HandleFunc("/api/v1/admin/tenants/", v2Logging(v2RequireTenant(handleAdminTenantSub(deps))))
	mux.HandleFunc("/api/v1/admin/marketplace/addons", v2Logging(v2RequireTenant(handleMarketplaceAddons(deps))))
	mux.HandleFunc("/api/v1/admin/marketplace/addons/", v2Logging(v2RequireTenant(handleMarketplaceAddonByID(deps))))
	// Phyllis MVP §5.2 + S2.2 fill-gaps routes.
	v1MountInto(mux, deps)
	return mux
}

func v2Logging(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if tp := r.Header.Get("traceparent"); tp != "" {
			w.Header().Set("traceparent", tp)
		}
		next(w, r)
	}
}

func v2RequireTenant(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(r.Header.Get("X-Tenant-Id")) == "" {
			v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
			return
		}
		next(w, r)
	}
}

func v2Health(w http.ResponseWriter, _ *http.Request) {
	v2WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func v2WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func v2WriteError(w http.ResponseWriter, status int, code, msg string) {
	v2WriteJSON(w, status, map[string]interface{}{
		"error": map[string]interface{}{"code": code, "message": msg},
	})
}

func v2Decode(r *http.Request, v interface{}) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func v2Header(r *http.Request) events.Header {
	return events.Header{
		TenantID:    r.Header.Get("X-Tenant-Id"),
		GCID:        firstNonEmpty(r.Header.Get("X-GCID"), r.Header.Get("gcid")),
		Traceparent: r.Header.Get("traceparent"),
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// rolesAllowed (fail-open X-Role RBAC) was DELETED in CHO-2072 — the gateway
// never stamped X-Role and the helper returned true on an empty header, so every
// gate built on it silently no-op'd in production. All admin gates now use the
// fail-closed mesh-header helpers in admin_authz.go (callerHoldsHPlusAdminRole /
// callerHoldsSuperAdminRole).

type v2CreateTenantReq struct {
	DisplayName string `json:"display_name"`
	OwnerGCID   string `json:"owner_gcid"`
	SelfHosted  bool   `json:"self_hosted"`
}

func handleV2Tenants(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CHO-2077: root-tenant create (POST) + list-ALL (GET) are
		// platform_operator-only (mirrors bootstrap_handler.callerIsPlatformOperator).
		// Defense-in-depth — see handleV1Tenants.
		if (r.Method == http.MethodPost || r.Method == http.MethodGet) &&
			!callerIsPlatformOperator(r.Header) {
			v2WriteError(w, http.StatusForbidden, errCodePlatformOperatorRequired,
				"root tenant management requires the platform_operator role")
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req v2CreateTenantReq
			if err := v2Decode(r, &req); err != nil {
				v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
				return
			}
			t, err := deps.Tenants.CreateRoot(req.DisplayName, req.OwnerGCID, req.SelfHosted)
			if err != nil {
				v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
			deps.Events.Publish("chora.tenancy.tenant.created.v1", v2Header(r), map[string]interface{}{
				"tenant_id":    t.ID,
				"display_name": t.DisplayName,
				"owner_gcid":   t.OwnerGCID,
			})
			v2WriteJSON(w, http.StatusCreated, v2TenantDTO(t))
		case http.MethodGet:
			items := deps.Tenants.List()
			out := make([]map[string]interface{}, 0, len(items))
			for _, t := range items {
				out = append(out, v2TenantDTO(t))
			}
			v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": out, "total": len(items)})
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
	}
}

func handleV2TenantSub(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v2/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) == 0 || parts[0] == "" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		tenantID := parts[0]
		if len(parts) == 1 {
			handleV2TenantByID(deps, tenantID, w, r)
			return
		}
		switch parts[1] {
		case "sub-tenants":
			handleV2SubTenants(deps, tenantID, w, r)
		case "members:bulk-invite":
			handleV2BulkInvite(deps, tenantID, w, r)
		case "members":
			handleV2Members(deps, tenantID, parts[2:], w, r)
		case "add-ons":
			handleV2AddOns(deps, tenantID, parts[2:], w, r)
		case "billing":
			if len(parts) < 3 {
				v2WriteError(w, http.StatusNotFound, "not_found", "not found")
				return
			}
			handleV2Billing(deps, tenantID, parts[2], w, r)
		default:
			if strings.HasPrefix(parts[1], "members") {
				handleV2Members(deps, tenantID, parts[1:], w, r)
				return
			}
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
		}
	}
}

func handleV2TenantByID(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t, ok := deps.Tenants.Get(tenantID)
		if !ok {
			v2WriteError(w, http.StatusNotFound, "tenant_not_found", "tenant not found")
			return
		}
		v2WriteJSON(w, http.StatusOK, v2TenantDTO(t))
	case http.MethodPatch:
		t, ok := deps.Tenants.Get(tenantID)
		if !ok {
			v2WriteError(w, http.StatusNotFound, "tenant_not_found", "tenant not found")
			return
		}
		raw := map[string]interface{}{}
		if err := v2Decode(r, &raw); err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		if dn, ok := raw["display_name"].(string); ok && dn != "" {
			if err := t.UpdateDisplayName(dn); err != nil {
				v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
		}
		if bMap, ok := raw["branding"].(map[string]interface{}); ok {
			cfg := tenant.BrandingConfig{}
			if v, ok := bMap["primary_color_hex"].(string); ok {
				cfg.PrimaryColorHex = v
			}
			if v, ok := bMap["logo_url"].(string); ok {
				cfg.LogoURL = v
			}
			if v, ok := bMap["custom_domain"].(string); ok {
				cfg.CustomDomain = v
			}
			if err := t.UpdateBranding(cfg); err != nil {
				v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
		}
		deps.Tenants.Save(t)
		v2WriteJSON(w, http.StatusOK, v2TenantDTO(t))
	default:
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func handleV2SubTenants(deps V2Deps, parentID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	var req v2CreateTenantReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	t, err := deps.Tenants.CreateSubTenant(parentID, req.DisplayName, req.OwnerGCID, req.SelfHosted)
	if err != nil {
		if errors.Is(err, tenant.ErrParentNotFound) {
			v2WriteError(w, http.StatusNotFound, "parent_not_found", err.Error())
			return
		}
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	deps.Events.Publish("chora.tenancy.tenant.created.v1", v2Header(r), map[string]interface{}{
		"tenant_id":     t.ID,
		"sub_tenant_of": parentID,
	})
	v2WriteJSON(w, http.StatusCreated, v2TenantDTO(t))
}

type v2InviteRow struct {
	Email string   `json:"email"`
	Roles []string `json:"roles"`
}

type v2BulkInviteReq struct {
	Invites []v2InviteRow `json:"invites"`
}

func handleV2BulkInvite(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	var req v2BulkInviteReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	rows := make([]member.InviteRequest, 0, len(req.Invites))
	for _, row := range req.Invites {
		rows = append(rows, member.InviteRequest{Email: row.Email, Roles: row.Roles})
	}
	result, err := deps.Members.BulkInvite(tenantID, rows)
	if err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	for _, m := range result.Invited {
		deps.Events.Publish("chora.tenancy.member.invited.v1", v2Header(r), map[string]interface{}{
			"tenant_id": m.TenantID,
			"member_id": m.ID,
			"email":     m.Email,
			"roles":     m.Roles,
		})
	}
	v2WriteJSON(w, http.StatusCreated, map[string]interface{}{
		"invited_count":   len(result.Invited),
		"duplicate_count": result.DuplicateCount,
		"rejected_count":  result.RejectedCount,
		"already_exists":  result.AlreadyExists,
	})
}

func handleV2Members(deps V2Deps, tenantID string, rest []string, w http.ResponseWriter, r *http.Request) {
	if len(rest) == 0 {
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		f := member.ListFilter{
			Role:   r.URL.Query().Get("role"),
			Status: r.URL.Query().Get("status"),
		}
		items := deps.Members.ListByTenant(tenantID, f)
		out := make([]map[string]interface{}, 0, len(items))
		for _, m := range items {
			out = append(out, v2MemberDTO(m))
		}
		v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": out, "total": len(items)})
		return
	}
	memberSeg := rest[0]
	if strings.Contains(memberSeg, ":") {
		bits := strings.SplitN(memberSeg, ":", 2)
		memberID := bits[0]
		switch bits[1] {
		case "suspend":
			handleV2SuspendMember(deps, tenantID, memberID, w, r)
		default:
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
		}
		return
	}
	if len(rest) == 1 {
		switch r.Method {
		case http.MethodPatch:
			handleV2PatchMember(deps, memberSeg, w, r)
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
		return
	}
	v2WriteError(w, http.StatusNotFound, "not_found", "not found")
}

func handleV2PatchMember(deps V2Deps, memberID string, w http.ResponseWriter, r *http.Request) {
	m, ok := deps.Members.Get(memberID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "member_not_found", "member not found")
		return
	}
	var req struct {
		Roles []string `json:"roles"`
	}
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if len(req.Roles) > 0 {
		if err := m.UpdateRoles(req.Roles); err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
	}
	deps.Members.Save(m)
	v2WriteJSON(w, http.StatusOK, v2MemberDTO(m))
}

func handleV2SuspendMember(deps V2Deps, tenantID, memberID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	m, ok := deps.Members.Get(memberID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "member_not_found", "member not found")
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if err := m.SuspendWithError(req.Reason); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	deps.Members.Save(m)
	deps.Events.Publish("chora.tenancy.member.suspended.v1", v2Header(r), map[string]interface{}{
		"tenant_id": tenantID,
		"member_id": memberID,
		"reason":    req.Reason,
	})
	v2WriteJSON(w, http.StatusOK, v2MemberDTO(m))
}

type v2SubscribeReq struct {
	AddOnID string `json:"add_on_id"`
}

func handleV2AddOns(deps V2Deps, tenantID string, rest []string, w http.ResponseWriter, r *http.Request) {
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodGet:
			items := deps.Subscriptions.ListActiveByTenant(tenantID)
			out := make([]map[string]interface{}, 0, len(items))
			for _, s := range items {
				out = append(out, v2SubDTO(deps.Catalogue, s))
			}
			v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": out, "total": len(items)})
		case http.MethodPost:
			var req v2SubscribeReq
			if err := v2Decode(r, &req); err != nil {
				v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
				return
			}
			gcid := r.Header.Get("X-GCID")
			s, err := deps.Subscriptions.Subscribe(tenantID, req.AddOnID, gcid)
			if err != nil {
				if errors.Is(err, addon.ErrAddOnNotFound) {
					v2WriteError(w, http.StatusNotFound, "addon_not_found", err.Error())
					return
				}
				v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
			deps.Events.Publish("chora.tenancy.add_on.subscribed.v1", v2Header(r), map[string]interface{}{
				"tenant_id":  tenantID,
				"addon_code": req.AddOnID,
				"sub_id":     s.ID,
			})
			deps.Events.Publish("chora.tenancy.addon.activated.v1", v2Header(r), map[string]interface{}{
				"tenant_id":     tenantID,
				"addon_plan_id": s.AddOnID,
				"activated_at":  s.ActivatedAt.Format(time.RFC3339Nano),
			})
			v2WriteJSON(w, http.StatusCreated, v2SubDTO(deps.Catalogue, s))
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
		return
	}
	if len(rest) == 1 {
		code := rest[0]
		switch r.Method {
		case http.MethodDelete:
			gcid := r.Header.Get("X-GCID")
			if err := deps.Subscriptions.Unsubscribe(tenantID, code, gcid); err != nil {
				switch {
				case errors.Is(err, addon.ErrAddOnNotFound), errors.Is(err, addon.ErrSubscriptionNotFound):
					v2WriteError(w, http.StatusNotFound, "not_found", err.Error())
				case errors.Is(err, addon.ErrCannotUnsubscribeBase):
					v2WriteError(w, http.StatusBadRequest, "cannot_unsubscribe_base", err.Error())
				default:
					v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				}
				return
			}
			deps.Events.Publish("chora.tenancy.add_on.unsubscribed.v1", v2Header(r), map[string]interface{}{
				"tenant_id":  tenantID,
				"addon_code": code,
			})
			v2WriteJSON(w, http.StatusOK, map[string]interface{}{"status": "grace_period"})
			return
		default:
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
	}
	v2WriteError(w, http.StatusNotFound, "not_found", "not found")
}

func handleV2Billing(deps V2Deps, tenantID, leaf string, w http.ResponseWriter, r *http.Request) {
	switch leaf {
	case "usage":
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		active := deps.Subscriptions.ListActiveByTenant(tenantID)
		subs := make([]billing.SubscriptionUsage, 0, len(active))
		for _, s := range active {
			subs = append(subs, billing.SubscriptionUsage{
				AddOnID:           s.AddOnCode,
				MonthlyPriceCents: s.MonthlyPriceCentsSnapshot,
			})
		}
		now := time.Now().UTC()
		summary, err := billing.AggregateUsage(tenantID, now.Add(-30*24*time.Hour), now, subs)
		if err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
		lines := make([]map[string]interface{}, 0, len(summary.Lines))
		for _, l := range summary.Lines {
			lines = append(lines, map[string]interface{}{
				"addon_id":         l.AddOnID,
				"unit_price_cents": l.UnitPriceCents,
				"quantity":         l.Quantity,
				"line_total_cents": l.LineTotalCents,
			})
		}
		v2WriteJSON(w, http.StatusOK, map[string]interface{}{
			"tenant_id":    tenantID,
			"period_start": summary.PeriodStart.Format(time.RFC3339Nano),
			"period_end":   summary.PeriodEnd.Format(time.RFC3339Nano),
			"lines":        lines,
			"total_cents":  summary.TotalCents,
		})
	case "invoices":
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		items := deps.Invoices.ListByTenant(tenantID)
		out := make([]map[string]interface{}, 0, len(items))
		for _, inv := range items {
			out = append(out, v2InvoiceDTO(inv))
		}
		v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": out, "total": len(items)})
	case "payment-method":
		if r.Method != http.MethodPost {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		var req struct {
			StripePaymentMethodID string `json:"stripe_payment_method_id"`
		}
		if err := v2Decode(r, &req); err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		token, err := deps.Stripe.AttachPaymentMethod(r.Context(), tenantID, req.StripePaymentMethodID)
		if err != nil {
			v2WriteError(w, http.StatusBadGateway, "stripe_error", err.Error())
			return
		}
		pm, err := billing.NewPaymentMethod(tenantID, req.StripePaymentMethodID)
		if err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
		deps.PaymentMethods.Save(pm)
		v2WriteJSON(w, http.StatusCreated, map[string]interface{}{
			"payment_method_id": pm.ID,
			"mock_token":        token,
		})
	default:
		v2WriteError(w, http.StatusNotFound, "not_found", "not found")
	}
}

// =============================================================================
// BE-T-ADD-1 admin endpoints — OpenAPI tenancy-admin v1.1.0
// =============================================================================

func addonByPlanID(cat *addon.Catalogue, idOrCode string) (*addon.AddOn, bool) {
	if a, ok := cat.Get(idOrCode); ok {
		return a, true
	}
	for _, a := range cat.List() {
		if a.ID == idOrCode {
			return a, true
		}
	}
	return nil, false
}

func handleAdminTenantSub(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/tenants/")
		parts := strings.Split(rest, "/")
		if len(parts) < 2 || parts[0] == "" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		tenantID := parts[0]
		// External web-egress entitlement (CHO-2148) — the Far Sight tenant
		// opt-in. GET reads (default-deny when no row); PATCH writes the policy
		// + the policy-changed event in one transaction.
		if parts[1] == "external-egress" && len(parts) == 2 {
			handleExternalEgress(deps, tenantID, w, r)
			return
		}
		// Mana pool routes (S6.2 dashboard + L1 CHO-1705 WP-4 lifecycle).
		// Colon-suffix ops split as parts[1]="mana-pool:auto-renew" — they
		// never enter the plain "mana-pool" branch below.
		if parts[1] == "mana-pool:auto-renew" && len(parts) == 2 {
			if r.Method != http.MethodPost {
				v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
				return
			}
			handleManaPoolAutoRenew(deps, tenantID, w, r)
			return
		}
		if parts[1] == "mana-pool" {
			if len(parts) == 2 {
				switch r.Method {
				case http.MethodGet:
					handleManaPoolGet(deps, tenantID, w, r)
				case http.MethodPost:
					handleManaPoolCreate(deps, tenantID, w, r)
				default:
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
				}
				return
			}
			if len(parts) == 3 && parts[2] == "summary" {
				if r.Method != http.MethodGet {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleManaPoolSummary(deps, tenantID, w, r)
				return
			}
			if len(parts) == 3 && parts[2] == "allocations" {
				if r.Method != http.MethodPost {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleManaAllocationsPost(deps, tenantID, w, r)
				return
			}
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		if parts[1] != "addons" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		if len(parts) == 2 {
			if r.Method != http.MethodGet {
				v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
				return
			}
			handleAdminListAddons(deps, tenantID, w, r)
			return
		}
		seg := parts[2]
		if strings.Contains(seg, ":") {
			bits := strings.SplitN(seg, ":", 2)
			planID := bits[0]
			switch bits[1] {
			case "deactivate":
				handleAdminDeactivateAddon(deps, tenantID, planID, w, r)
			case "activate":
				// CHO-1742 — free ($0) addon Activate-Free path.
				handleAdminActivateAddon(deps, tenantID, planID, w, r)
			case "cancel-deactivation":
				// CHO-1785 — undo the FE end-of-cycle deactivation
				// (CHO-1782) before the Stripe anchor fires. Registry-
				// only flip today: chora-tenancy's deactivate path
				// doesn't currently touch Stripe, so there's no
				// schedule to release. A follow-up will wire the Stripe
				// rollback once the deactivate side calls
				// chora-payments.
				handleAdminCancelDeactivation(deps, tenantID, planID, w, r)
			default:
				v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			}
			return
		}
		if len(parts) == 3 {
			switch r.Method {
			case http.MethodGet:
				handleAdminAddonDetail(deps, tenantID, seg, w, r)
			case http.MethodPatch:
				handleAdminChangeAddonTier(deps, tenantID, seg, w, r)
			default:
				v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			}
			return
		}
		if len(parts) == 4 {
			switch parts[3] {
			case "usage":
				if r.Method != http.MethodGet {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleAdminAddonUsage(deps, tenantID, seg, w, r)
				return
			case "audit":
				if r.Method != http.MethodGet {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleAdminAddonAudit(deps, tenantID, seg, w, r)
				return
			case "preview-tier-change":
				if r.Method != http.MethodPost {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleAdminPreviewTierChange(deps, tenantID, seg, w, r)
				return
			case "request-deactivation":
				if r.Method != http.MethodPost {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleAdminRequestDeactivation(deps, tenantID, seg, w, r)
				return
			case "change-tier":
				if r.Method != http.MethodPost {
					v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
					return
				}
				handleAdminChangeAddonTier(deps, tenantID, seg, w, r)
				return
			}
		}
		v2WriteError(w, http.StatusNotFound, "not_found", "not found")
	}
}

func handleAdminListAddons(deps V2Deps, tenantID string, w http.ResponseWriter, r *http.Request) {
	// CHO-1801 §4 — admin-gate the H+ add-on entitlements list. Reads the
	// gateway-propagated x-mesh-user-roles header fail-CLOSED (see
	// admin_addons_authz.go), mirroring the sibling /admin/tenant-members
	// (identity) + /admin/tenants/me/invoices gates. Without this any tenant
	// member could read the tenant's paid entitlements via this admin route.
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden",
			"caller must hold a tenant-admin role to view add-on entitlements")
		return
	}
	subs := deps.Subscriptions.ListActiveByTenant(tenantID)
	items := make([]map[string]interface{}, 0, len(subs))
	for _, s := range subs {
		a, _ := deps.Catalogue.Get(s.AddOnCode)
		var seatsUsed int64
		for _, snap := range deps.Subscriptions.ListUsage(s.TenantID, s.AddOnID,
			time.Now().UTC().Add(-30*24*time.Hour), time.Now().UTC().Add(24*time.Hour)) {
			if snap.SeatsUsed > seatsUsed {
				seatsUsed = snap.SeatsUsed
			}
		}
		row := map[string]interface{}{
			"tenant_id":      tenantID,
			"addon_plan_id":  s.AddOnID,
			"addon_code":     s.AddOnCode,
			"display_name":   addonDisplayName(a),
			"status":         strings.ToUpper(string(s.Status)),
			"current_tier":   s.CurrentTier,
			"monthly_cost":   s.MonthlyPriceCentsSnapshot,
			"seats_used":     seatsUsed,
			"activated_at":   s.ActivatedAt.Format(time.RFC3339Nano),
			"deactivated_at": v2NullableTime(s.DeactivatedAt),
			// CHO-1776 — dunning state for the FE banner. Defaults false
			// on legacy rows that haven't been touched by the Phase 2a
			// payments subscriber.
			"past_due": s.PastDue,
		}
		// CHO-1772 — surface the pending end-of-cycle tier change so the
		// FE renders "<ScheduledTier> starting <date>" on the tile.
		if s.ScheduledTier != "" && s.ScheduledEffectiveAt != nil {
			row["scheduled_tier_code"] = s.ScheduledTier
			row["scheduled_effective_at"] = s.ScheduledEffectiveAt.Format(time.RFC3339Nano)
		}
		// CHO-1782 — surface the pending end-of-cycle deactivation so the
		// FE tile renders the destructive "Deactivating <date>" pill.
		if s.DeactivationEffectiveAt != nil && s.Status == addon.StatusPendingDeactivation {
			row["deactivation_effective_at"] = s.DeactivationEffectiveAt.Format(time.RFC3339Nano)
			if s.DeactivationReason != "" {
				row["deactivation_reason"] = string(s.DeactivationReason)
			}
		}
		// CHO-1784 — surface the next billing cycle anchor so the FE
		// deactivate modal can use it as effective_at on the end_of_cycle
		// path instead of computing a client-side +30d approximation.
		if s.NextRenewalAt != nil {
			row["next_renewal_at"] = s.NextRenewalAt.Format(time.RFC3339Nano)
		}
		items = append(items, row)
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

func handleAdminAddonDetail(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	// CHO-2073 — admin-gate the add-on detail read (was ungated, leaking a
	// tenant's entitlement/subscription state to any member). Same fail-closed
	// mesh gate as the list (CHO-1801).
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	resp := addonDetailDTO(a)
	if sub, ok := deps.Subscriptions.GetSubscription(tenantID, a.Code); ok {
		resp["current_subscription"] = subscriptionSnapshotDTO(sub)
	}
	v2WriteJSON(w, http.StatusOK, resp)
}

type changeTierReq struct {
	TargetPlanID    string  `json:"target_plan_id"`
	TargetTierCode  string  `json:"target_tier_code"`
	ProrationMode   string  `json:"proration_mode"`
	EffectiveAt     *string `json:"effective_at"`
	ExpectedVersion int64   `json:"expected_version"`
}

func handleAdminChangeAddonTier(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req changeTierReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	target := req.TargetTierCode
	if target == "" {
		target = req.TargetPlanID
	}
	if target == "" {
		v2WriteError(w, http.StatusBadRequest, "invalid_argument", "target_plan_id or target_tier_code required")
		return
	}
	var effAt *time.Time
	if req.EffectiveAt != nil && *req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, *req.EffectiveAt)
		if err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_effective_at", err.Error())
			return
		}
		effAt = &t
	}
	gcid := r.Header.Get("X-GCID")

	// CHO-1767 — when CHORA_PAYMENTS_HTTP_ADDR is wired, dispatch the
	// tier change through chora-payments so Stripe Subscription.update
	// runs. Falls back to legacy direct-registry path when the client
	// isn't wired (unit tests + standalone smoke).
	if deps.PaymentsHTTP != nil {
		handleAdminChangeAddonTierViaPayments(deps, tenantID, a, target, gcid, req.ProrationMode, effAt, w, r)
		return
	}

	sub, delta, kind, err := deps.Subscriptions.ChangeTier(tenantID, a.Code, target, req.ProrationMode, effAt, gcid, req.ExpectedVersion)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrSubscriptionNotFound):
			v2WriteError(w, http.StatusNotFound, "subscription_not_found", err.Error())
		case errors.Is(err, addon.ErrIneligibleForChange):
			v2WriteError(w, http.StatusConflict, "tier_change_ineligible", err.Error())
		case errors.Is(err, addon.ErrConcurrencyConflict):
			v2WriteError(w, http.StatusConflict, "concurrency_conflict", err.Error())
		case errors.Is(err, addon.ErrTierFamilyMismatch),
			errors.Is(err, addon.ErrInvalidProration),
			errors.Is(err, addon.ErrInvalidTier):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		case errors.Is(err, addon.ErrAddOnNotFound):
			v2WriteError(w, http.StatusNotFound, "addon_not_found", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	evs := deps.Subscriptions.ListEvents(tenantID, a.Code)
	var fromTier string
	for _, ev := range evs {
		if ev.Kind == kind {
			fromTier = ev.FromTier
			break
		}
	}
	topic := "chora.tenancy.addon.upgraded.v1"
	if kind == addon.EventDowngraded {
		topic = "chora.tenancy.addon.downgraded.v1"
	}
	effectiveAt := time.Now().UTC()
	if effAt != nil {
		effectiveAt = *effAt
	}
	deps.Events.Publish(topic, v2Header(r), map[string]interface{}{
		"tenant_id":            tenantID,
		"addon_plan_id":        a.ID,
		"from_tier":            fromTier,
		"to_tier":              sub.CurrentTier,
		"effective_at":         effectiveAt.Format(time.RFC3339Nano),
		"billing_delta_cents":  delta,
		"requested_by_gcid":    gcid,
		"chora_imda_dimension": imdaDimensionAccountability,
	})
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"subscription_id":     sub.ID,
		"billing_delta_cents": delta,
		"effective_at":        effectiveAt.Format(time.RFC3339Nano),
		"from_tier":           fromTier,
		"to_tier":             sub.CurrentTier,
	})
}

// handleAdminChangeAddonTierViaPayments is the CHO-1767 dispatch path.
// Calls chora-payments' HTTP change-tier endpoint (CHO-1764), then
// best-effort syncs the local registry + publishes the upgrade/downgrade
// event. Errors from payments map to the FE-recognised codes.
func handleAdminChangeAddonTierViaPayments(deps V2Deps, tenantID string, a *addon.AddOn, targetTier, gcid, prorationMode string, effAt *time.Time, w http.ResponseWriter, r *http.Request) {
	// Default currency from the tier (the catalogue carries SGD per CHO-1787;
	// the catalogue → wire shape is uppercase, payments normalises lowercase).
	currency := "SGD"
	if t, ok := a.FindTier(targetTier); ok && t.Currency != "" {
		currency = t.Currency
	}
	// CHO-1772 — translate the FE's nullable ISO `effective_at` into the
	// 3-value string chora-payments expects. Any non-nil future-dated
	// effective_at maps to end_of_cycle; nil → immediate (existing
	// CHO-1764 behaviour).
	effectiveAtTag := ""
	if effAt != nil {
		effectiveAtTag = "end_of_cycle"
	}
	in := TenancyPaymentsChangeTierIn{
		TenantID:       tenantID,
		AdminGCID:      gcid,
		AddonCode:      a.Code,
		TargetTierCode: targetTier,
		Currency:       currency,
		ProrationMode:  prorationMode,
		EffectiveAt:    effectiveAtTag,
	}
	out, err := deps.PaymentsHTTP.ChangeTenantAddonTier(r.Context(), in)
	if err != nil {
		writePaymentsHTTPError(w, err, "chora-payments change-tier dispatch failed: "+err.Error())
		return
	}

	// Registry sync — CHO-1772 makes this CONDITIONAL on the immediate
	// path. When end_of_cycle drove the response, the user's currently-
	// paid-for tier is still active until Stripe fires
	// subscription_schedule.released; mutating CurrentTier now would
	// lie to /h/addons. The schedule fields are persisted below via
	// SetScheduledTier so the FE can render "<scheduled> starting <date>".
	var (
		regSub         *addon.Subscription
		kind           addon.LifecycleEventKind
		regErr         error
		currentTier    = out.ToTier
		subscriptionID = out.PurchaseID
	)
	if !out.DeferredToCycleEnd {
		regSub, _, kind, regErr = deps.Subscriptions.ChangeTier(tenantID, a.Code, targetTier, prorationMode, effAt, gcid, 0)
		if regErr == nil && regSub != nil {
			currentTier = regSub.CurrentTier
			subscriptionID = regSub.ID
		}
	} else if existing, ok := deps.Subscriptions.GetSubscription(tenantID, a.Code); ok {
		// CHO-1780 follow-up — on the deferred path, leave `currentTier`
		// at `out.ToTier` (the user-picked target) so the result card
		// shows "Starter → Pro" with the schedule context underneath.
		// The previous PR #91 line `currentTier = existing.CurrentTier`
		// flipped the response into a misleading "Starter → Starter"
		// no-op shape. Only the subscription_id needs reading from the
		// existing registry row.
		subscriptionID = existing.ID
	}
	topic := "chora.tenancy.addon.upgraded.v1"
	if kind == addon.EventDowngraded {
		topic = "chora.tenancy.addon.downgraded.v1"
	}
	effectiveAt := time.Now().UTC()
	if effAt != nil {
		effectiveAt = *effAt
	}
	deps.Events.Publish(topic, v2Header(r), map[string]interface{}{
		"tenant_id":            tenantID,
		"addon_plan_id":        a.ID,
		"from_tier":            out.FromTier,
		"to_tier":              out.ToTier,
		"effective_at":         effectiveAt.Format(time.RFC3339Nano),
		"billing_delta_cents":  out.BillingDeltaCents,
		"requested_by_gcid":    gcid,
		"chora_imda_dimension": imdaDimensionAccountability,
	})
	// CHO-1772 — propagate the schedule fields when the end_of_cycle
	// path drove the response so the FE can render "Pro starting
	// YYYY-MM-DD" copy + the tile badge.
	resp := map[string]interface{}{
		"subscription_id":     subscriptionID,
		"billing_delta_cents": out.BillingDeltaCents,
		"effective_at":        effectiveAt.Format(time.RFC3339Nano),
		"from_tier":           out.FromTier,
		"to_tier":             currentTier,
	}
	if out.DeferredToCycleEnd {
		resp["deferred_to_cycle_end"] = true
		if out.ScheduleID != "" {
			resp["schedule_id"] = out.ScheduleID
		}
		if out.ScheduledTierCode != "" {
			resp["scheduled_tier_code"] = out.ScheduledTierCode
		}
		if out.ScheduledEffectiveAt != "" {
			resp["scheduled_effective_at"] = out.ScheduledEffectiveAt
		}
		// CHO-1772 — persist the pending schedule on the local registry
		// row so subsequent /h/addons GETs surface the "Pro starting
		// YYYY-MM-DD" badge on the tile. The schedule_released
		// webhook clears these fields + promotes CurrentTier.
		if out.ScheduledTierCode != "" && out.ScheduledEffectiveAt != "" {
			if effT, perr := time.Parse(time.RFC3339Nano, out.ScheduledEffectiveAt); perr == nil {
				if _, sErr := deps.Subscriptions.SetScheduledTier(tenantID, a.Code, out.ScheduledTierCode, effT); sErr != nil {
					// Non-fatal: the Stripe schedule is already
					// created; surface the response and let the next
					// sync reconcile. Log via the event publish above.
					_ = sErr
				}
			}
		}
	}
	v2WriteJSON(w, http.StatusOK, resp)
}

// PaymentsHTTPError is the structured error chora-tenancy maps from
// chora-payments' 4xx/5xx responses. The Code field drives the FE
// banner via the handler's errors.As mapping. Adapter and tests both
// return this type so the same switch statement covers both paths.
type PaymentsHTTPError struct {
	StatusCode int
	Code       string
	Message    string
}

// Error implements the error interface.
func (e *PaymentsHTTPError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// writePaymentsHTTPError centralises the 4xx code mapping from
// chora-payments responses to the FE-recognised codes. Falls back to
// 502 payments_unavailable for everything else (network errors, 5xx).
func writePaymentsHTTPError(w http.ResponseWriter, err error, fallbackMessage string) {
	var httpErr *PaymentsHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Code {
		case "no_stripe_subscription":
			v2WriteError(w, http.StatusUnprocessableEntity, "no_stripe_subscription",
				"this add-on was activated before subscription billing rolled out; please deactivate and re-subscribe to change tier")
			return
		case "stripe_price_missing":
			v2WriteError(w, http.StatusUnprocessableEntity, "stripe_price_missing", httpErr.Message)
			return
		case "tier_unchanged":
			v2WriteError(w, http.StatusConflict, "tier_unchanged", httpErr.Message)
			return
		case "addon_not_found":
			v2WriteError(w, http.StatusNotFound, "subscription_not_found", httpErr.Message)
			return
		}
	}
	v2WriteError(w, http.StatusBadGateway, "payments_unavailable", fallbackMessage)
}

// SetPaymentsErrSentinels is preserved as a no-op for backwards
// compatibility with the earlier sentinel-Fn design. The current
// errors.As(*PaymentsHTTPError) approach doesn't need it.
func SetPaymentsErrSentinels(_, _, _, _ error) {
	// no-op
	_ = errors.New
	_ = struct{}{}
}

type deactivateReq struct {
	Reason      string  `json:"reason"`
	ReasonText  string  `json:"reason_text"`
	EffectiveAt *string `json:"effective_at"`
}

func handleAdminDeactivateAddon(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	var req deactivateReq
	if err := v2Decode(r, &req); err != nil {
		v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	var effAt *time.Time
	if req.EffectiveAt != nil && *req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, *req.EffectiveAt)
		if err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "invalid_effective_at", err.Error())
			return
		}
		effAt = &t
	}
	gcid := r.Header.Get("X-GCID")
	dreq := addon.DeactivationRequest{
		Reason:          addon.DeactivationReason(req.Reason),
		ReasonText:      req.ReasonText,
		EffectiveAt:     effAt,
		RequestedByGCID: gcid,
	}
	sub, status, ev, err := deps.Subscriptions.Deactivate(tenantID, a.Code, dreq)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrAddOnNotFound), errors.Is(err, addon.ErrSubscriptionNotFound):
			v2WriteError(w, http.StatusNotFound, "not_found", err.Error())
		case errors.Is(err, addon.ErrAlreadyDeactivated):
			v2WriteError(w, http.StatusConflict, "already_deactivated", err.Error())
		case errors.Is(err, addon.ErrComplianceLocked):
			v2WriteError(w, http.StatusLocked, "compliance_locked", err.Error())
		case errors.Is(err, addon.ErrCannotUnsubscribeBase):
			v2WriteError(w, http.StatusBadRequest, "cannot_unsubscribe_base", err.Error())
		case errors.Is(err, addon.ErrReasonTextRequired), errors.Is(err, addon.ErrInvalidArgument):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	requestedAt := time.Now().UTC()
	if ev != nil && !ev.OccurredAt.IsZero() {
		requestedAt = ev.OccurredAt
	}
	partialRevoke := false
	if a.Code == "mcp_gateway" && deps.A2APartnerActive != nil && deps.A2APartnerActive(tenantID, a.Code) {
		partialRevoke = true
		deps.Events.Publish("chora.tenancy.addon.partial_revoke_started.v1", v2Header(r), map[string]interface{}{
			"tenant_id":         tenantID,
			"addon_plan_id":     a.ID,
			"reason":            req.Reason,
			"reason_text":       req.ReasonText,
			"requested_by_gcid": gcid,
		})
	}
	if status == "DEACTIVATION_SCHEDULED" {
		deps.Events.Publish("chora.tenancy.addon.deactivation_requested.v1", v2Header(r), map[string]interface{}{
			"tenant_id":            tenantID,
			"addon_plan_id":        a.ID,
			"reason":               req.Reason,
			"reason_text":          req.ReasonText,
			"requested_by_gcid":    gcid,
			"effective_at":         v2NullableTime(effAt),
			"requested_at":         requestedAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		})
	} else {
		deps.Events.Publish("chora.tenancy.addon.deactivated.v1", v2Header(r), map[string]interface{}{
			"tenant_id":            tenantID,
			"addon_plan_id":        a.ID,
			"reason":               req.Reason,
			"reason_text":          req.ReasonText,
			"deactivated_by_gcid":  gcid,
			"deactivated_at":       requestedAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		})
	}
	resp := map[string]interface{}{
		"tenant_id":      tenantID,
		"addon_plan_id":  a.ID,
		"status":         status,
		"requested_at":   requestedAt.Format(time.RFC3339Nano),
		"effective_at":   v2NullableTime(effAt),
		"reason":         req.Reason,
		"partial_revoke": partialRevoke,
	}
	_ = sub
	v2WriteJSON(w, http.StatusAccepted, resp)
}

// handleAdminCancelDeactivation — CHO-1785. Undoes a pending end-of-cycle
// deactivation (PENDING_DEACTIVATION → ACTIVE). Registry-only flip today:
// chora-tenancy's deactivate path doesn't currently push to chora-payments,
// so there's no Stripe schedule to release. A follow-up will wire the
// Stripe rollback (subscriptions.update(cancel_at_period_end=false))
// once the deactivate side calls chora-payments.
//
// Status mapping:
//
//	200 OK     — deactivation cancelled (or idempotent on ACTIVE).
//	404        — subscription not found.
//	409        — already DEACTIVATED (closed row; admin must re-subscribe).
func handleAdminCancelDeactivation(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	gcid := r.Header.Get("X-GCID")
	sub, ev, err := deps.Subscriptions.CancelScheduledDeactivation(tenantID, a.Code, gcid)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrSubscriptionNotFound):
			v2WriteError(w, http.StatusNotFound, "not_found", err.Error())
		case errors.Is(err, addon.ErrAlreadyDeactivated):
			v2WriteError(w, http.StatusConflict, "already_deactivated", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	// Only emit on the transition (skip when caller hit the already-
	// ACTIVE idempotent branch — ev is nil).
	if ev != nil {
		deps.Events.Publish("chora.tenancy.addon.deactivation_cancelled.v1", v2Header(r), map[string]interface{}{
			"tenant_id":            tenantID,
			"addon_plan_id":        a.ID,
			"addon_code":           a.Code,
			"requested_by_gcid":    gcid,
			"cancelled_at":         ev.OccurredAt.Format(time.RFC3339Nano),
			"chora_imda_dimension": imdaDimensionAccountability,
		})
	}
	v2WriteJSON(w, http.StatusOK, subscriptionSnapshotDTO(sub))
}

// handleAdminActivateAddon — POST /api/v1/admin/tenants/{tenantId}/addons/{addonPlanId}:activate
// (CHO-1742). Free ($0) add-on Activate-Free path. Calls
// SubscriptionRegistry.Subscribe (ACTIVE state immediately, no payment
// step) — the sibling deactivate flow's :deactivate counterpart.
// Idempotent: re-activating an already-ACTIVE row returns the existing
// subscription unchanged. Emits
// `chora.tenancy.tenant_addon.activated.v1` per the post-CHO-1740
// canonical topic name.
func handleAdminActivateAddon(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	gcid := r.Header.Get("X-GCID")
	sub, err := deps.Subscriptions.Subscribe(tenantID, a.Code, gcid)
	if err != nil {
		switch {
		case errors.Is(err, addon.ErrAddOnNotFound):
			v2WriteError(w, http.StatusNotFound, "addon_not_found", err.Error())
		case errors.Is(err, addon.ErrInvalidArgument):
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		default:
			v2WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		}
		return
	}
	activatedAt := time.Now().UTC()
	if sub != nil && !sub.ActivatedAt.IsZero() {
		activatedAt = sub.ActivatedAt.UTC()
	}
	deps.Events.Publish("chora.tenancy.tenant_addon.activated.v1", v2Header(r), map[string]interface{}{
		"tenant_id":     tenantID,
		"addon_plan_id": a.ID,
		"addon_code":    a.Code,
		"sub_id":        sub.ID,
		"current_tier":  sub.CurrentTier,
		"activated_at":  activatedAt.Format(time.RFC3339Nano),
	})
	v2WriteJSON(w, http.StatusOK, v2SubDTO(deps.Catalogue, sub))
}

func handleAdminAddonUsage(deps V2Deps, tenantID, planID string, w http.ResponseWriter, r *http.Request) {
	// CHO-2073 — admin-gate the add-on usage read (was ungated, leaking usage
	// metrics + pseudonymous top-consumer IDs to any member). Same fail-closed
	// mesh gate as the list (CHO-1801).
	if !callerHoldsHPlusAdminRole(r.Header) {
		v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
		return
	}
	a, ok := addonByPlanID(deps.Catalogue, planID)
	if !ok {
		v2WriteError(w, http.StatusNotFound, "addon_not_found", "add-on not found")
		return
	}
	q := r.URL.Query()
	from := time.Now().UTC().Add(-30 * 24 * time.Hour)
	to := time.Now().UTC()
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}
	gran := q.Get("granularity")
	if gran == "" {
		gran = "day"
	}
	snaps := deps.Subscriptions.ListUsage(tenantID, a.ID, from, to)
	buckets := make([]map[string]interface{}, 0, len(snaps))
	var totalSeats, peakSeats, quota int64
	for _, snap := range snaps {
		buckets = append(buckets, map[string]interface{}{
			"bucket_start":   snap.BucketDate.Format(time.RFC3339),
			"seats_used":     snap.SeatsUsed,
			"quota_consumed": snap.QuotaConsumed,
			"api_calls":      snap.APICalls,
			"error_count":    snap.ErrorCount,
		})
		totalSeats += snap.SeatsUsed
		if snap.SeatsUsedPeak > peakSeats {
			peakSeats = snap.SeatsUsedPeak
		}
		quota += snap.QuotaConsumed
	}
	windowKey := from.Format("2006-01-02") + "_" + to.Format("2006-01-02")
	topConsumers := []map[string]interface{}{
		{"user_id_pseudonymous": addon.PseudonymiseGCID(tenantID, windowKey, "gcid-top-1"), "share_pct": 35.0},
		{"user_id_pseudonymous": addon.PseudonymiseGCID(tenantID, windowKey, "gcid-top-2"), "share_pct": 22.5},
		{"user_id_pseudonymous": addon.PseudonymiseGCID(tenantID, windowKey, "gcid-top-3"), "share_pct": 18.0},
	}
	v2WriteJSON(w, http.StatusOK, map[string]interface{}{
		"addon_plan_id":   a.ID,
		"tenant_id":       tenantID,
		"period_from":     from.Format(time.RFC3339),
		"period_to":       to.Format(time.RFC3339),
		"granularity":     gran,
		"seats_used":      totalSeats,
		"seats_used_peak": peakSeats,
		"quota_consumed":  quota,
		"time_series":     buckets,
		"top_consumers":   topConsumers,
	})
}

func handleMarketplaceAddons(deps V2Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		now := time.Now().UTC()
		all := deps.Catalogue.List()
		items := make([]map[string]interface{}, 0, len(all))
		for _, a := range all {
			d := addonDetailDTO(a)
			delete(d, "current_subscription")
			d["installed"] = isAddonInstalled(deps, tenantID, a.Code, now)
			items = append(items, d)
		}
		v2WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
	}
}

// isAddonInstalled is the canonical predicate for the marketplace `installed`
// flag (CHO-1747 / CHO-1745). True iff the calling tenant currently holds a
// non-terminal subscription to the addon code. The predicate is wider than
// `Subscription.IsActive`: it ALSO includes `StatusPendingActivation` so the
// Subscribe-via-Stripe-Checkout race window (post-checkout-start,
// pre-webhook-payment) surfaces an Installed badge instead of re-offering
// Subscribe. Suspended + Deactivated subscriptions return false — the FE
// should re-offer Subscribe in those states.
func isAddonInstalled(deps V2Deps, tenantID, addonCode string, at time.Time) bool {
	if tenantID == "" {
		return false
	}
	sub, ok := deps.Subscriptions.GetSubscription(tenantID, addonCode)
	if !ok {
		return false
	}
	switch sub.Status {
	case addon.StatusActive,
		addon.StatusPendingActivation,
		addon.StatusPendingDeactivation:
		return true
	case addon.StatusGracePeriod:
		if sub.GraceEndsAt == nil {
			return true
		}
		return at.Before(*sub.GraceEndsAt)
	default:
		return false
	}
}

// DTOs ------------------------------------------------------------------------

func addonDisplayName(a *addon.AddOn) string {
	if a == nil {
		return ""
	}
	return a.DisplayName
}

func v2TenantDTO(t *tenant.Tenant) map[string]interface{} {
	out := map[string]interface{}{
		"id":               t.ID,
		"display_name":     t.DisplayName,
		"owner_gcid":       t.OwnerGCID,
		"parent_tenant_id": t.ParentTenantID,
		"self_hosted":      t.SelfHosted,
		"status":           string(t.Status),
		"branding": map[string]interface{}{
			"primary_color_hex": t.Branding.PrimaryColorHex,
			"logo_url":          t.Branding.LogoURL,
			"custom_domain":     t.Branding.CustomDomain,
		},
		"created_at": t.CreatedAt.Format(time.RFC3339Nano),
		"updated_at": t.UpdatedAt.Format(time.RFC3339Nano),
	}
	if t.DeletedAt != nil {
		out["deleted_at"] = t.DeletedAt.Format(time.RFC3339Nano)
	}
	return out
}

func v2MemberDTO(m *member.Member) map[string]interface{} {
	out := map[string]interface{}{
		"id":         m.ID,
		"tenant_id":  m.TenantID,
		"email":      m.Email,
		"roles":      m.Roles,
		"status":     string(m.Status),
		"gcid":       m.GCID,
		"invited_at": m.InvitedAt.Format(time.RFC3339Nano),
		"updated_at": m.UpdatedAt.Format(time.RFC3339Nano),
	}
	if m.ActivatedAt != nil {
		out["activated_at"] = m.ActivatedAt.Format(time.RFC3339Nano)
	}
	return out
}

func v2SubDTO(cat *addon.Catalogue, s *addon.Subscription) map[string]interface{} {
	out := map[string]interface{}{
		"id":            s.ID,
		"tenant_id":     s.TenantID,
		"add_on_id":     s.AddOnCode,
		"addon_plan_id": s.AddOnID,
		"status":        string(s.Status),
		"current_tier":  s.CurrentTier,
		"monthly_cost":  s.MonthlyPriceCentsSnapshot,
		"version":       s.Version,
		"activated_at":  s.ActivatedAt.Format(time.RFC3339Nano),
	}
	if s.GraceEndsAt != nil {
		out["grace_ends_at"] = s.GraceEndsAt.Format(time.RFC3339Nano)
	}
	if s.CancelledAt != nil {
		out["cancelled_at"] = s.CancelledAt.Format(time.RFC3339Nano)
	}
	if cat != nil {
		if a, ok := cat.Get(s.AddOnCode); ok {
			out["display_name"] = a.DisplayName
		}
	}
	return out
}

func v2InvoiceDTO(inv *billing.Invoice) map[string]interface{} {
	return map[string]interface{}{
		"id":           inv.ID,
		"tenant_id":    inv.TenantID,
		"period_start": inv.PeriodStart.Format(time.RFC3339Nano),
		"period_end":   inv.PeriodEnd.Format(time.RFC3339Nano),
		"total_cents":  inv.TotalCents,
		"status":       string(inv.Status),
	}
}

func addonDetailDTO(a *addon.AddOn) map[string]interface{} {
	tiers := make([]map[string]interface{}, 0, len(a.Tiers))
	for _, t := range a.Tiers {
		tier := map[string]interface{}{
			"tier_code":           t.Code,
			"label":               t.DisplayName,
			"monthly_price_cents": t.MonthlyPriceCents,
			"currency":            t.Currency,
		}
		if t.IncludedUnits != nil {
			tier["included_units"] = *t.IncludedUnits
		}
		if t.OverageUnitCents != nil {
			tier["overage_unit_cents"] = *t.OverageUnitCents
		}
		tiers = append(tiers, tier)
	}
	ents := make([]map[string]interface{}, 0, len(a.Entitlements))
	for _, e := range a.Entitlements {
		ent := map[string]interface{}{
			"feature_code": e.FeatureCode,
			"label":        e.Label,
		}
		if e.Limit != nil {
			ent["limit"] = *e.Limit
		}
		if e.LimitUnit != "" {
			ent["limit_unit"] = e.LimitUnit
		}
		ents = append(ents, ent)
	}
	ints := make([]map[string]interface{}, 0, len(a.Integrations))
	for _, in := range a.Integrations {
		ints = append(ints, map[string]interface{}{
			"type":   in.Type,
			"label":  in.Label,
			"status": in.Status,
		})
	}
	residency := append([]string(nil), a.Compliance.DataResidencyRegions...)
	return map[string]interface{}{
		"addon_plan_id": a.ID,
		"code":          a.Code,
		"display_name":  a.DisplayName,
		"category":      a.Category,
		"description":   a.Description,
		"plan_family":   a.PlanFamily,
		// `system` is the marketplace affordance gate (CHO-1745). True for
		// always-on baseline plans (`base` today). The FE uses this to
		// suppress Subscribe / Activate Free / Deactivate buttons. BE 400s
		// (`cannot_unsubscribe_base`, `amount_cents must be > 0`) remain
		// the authorisation boundary; the flag is a hint, not a gate.
		"system": a.AlwaysOn,
		// `installed` is default-false here; both marketplace handlers
		// (list + detail) overlay the per-tenant value at response time.
		"installed":    false,
		"entitlements": ents,
		"integrations": ints,
		"compliance": map[string]interface{}{
			"gdpr":                   a.Compliance.GDPR,
			"pdpa":                   a.Compliance.PDPA,
			"ccpa":                   a.Compliance.CCPA,
			"imda":                   a.Compliance.IMDA,
			"data_residency_regions": residency,
		},
		"pricing_tiers": tiers,
	}
}

func subscriptionSnapshotDTO(s *addon.Subscription) map[string]interface{} {
	snap := map[string]interface{}{
		"activated_at":  s.ActivatedAt.Format(time.RFC3339Nano),
		"current_tier":  s.CurrentTier,
		"status":        strings.ToUpper(string(s.Status)),
		"billing_cycle": "MONTHLY",
		"version":       s.Version,
	}
	// CHO-1780 — surface the pending end-of-cycle tier change so the
	// marketplace detail page can render the same
	// "<scheduled_tier_code> starting <date>" badge as /h/addons. Keys
	// are OMITTED when no schedule is pending so the FE can use a truthy
	// `@if` without false positives.
	if s.ScheduledTier != "" && s.ScheduledEffectiveAt != nil {
		snap["scheduled_tier_code"] = s.ScheduledTier
		snap["scheduled_effective_at"] = s.ScheduledEffectiveAt.Format(time.RFC3339Nano)
	}
	// CHO-1784 — let the FE deactivate modal use the real Stripe cycle
	// anchor as effective_at on the end_of_cycle path instead of its
	// previous now+30d client-side approximation (PR #96). Subscribe
	// populates NextRenewalAt with the first-cycle estimate; nil here
	// means a legacy pre-CHO-1784 row that survived a hot reload —
	// degrades to the client-side fallback gracefully.
	if s.NextRenewalAt != nil {
		snap["next_renewal_at"] = s.NextRenewalAt.Format(time.RFC3339Nano)
	}
	// CHO-1782 — destructive parallel to the scheduled-tier badge: when the
	// admin picks "End of billing cycle" on the deactivate modal the
	// registry flips Status→PENDING_DEACTIVATION + populates the effective-
	// at. Surface both so the three FE panels can render the amber
	// "Deactivating <date>" pill. Keys omitted on ACTIVE so the FE @if
	// stays truthy-only.
	if s.DeactivationEffectiveAt != nil && s.Status == addon.StatusPendingDeactivation {
		snap["deactivation_effective_at"] = s.DeactivationEffectiveAt.Format(time.RFC3339Nano)
		if s.DeactivationReason != "" {
			snap["deactivation_reason"] = string(s.DeactivationReason)
		}
	}
	return snap
}

func v2NullableTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339Nano)
}
