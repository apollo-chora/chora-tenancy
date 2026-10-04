// Package httpapi — Iter G.3 Familiar Egg HTTP surface (ADR-149).
//
// Routes (mounted under root via NewFamiliarEggMux):
//
//	POST /api/familiar-eggs/checkout            — Stripe Checkout session create
//	POST /webhooks/stripe                       — Stripe webhook ingress (sig-verified)
//	GET  /api/familiar-eggs/catalog             — list eggs visible to caller's tenant
//	GET  /api/familiar-eggs/{sku}/odds          — IMDA D2 transparency endpoint
//	POST /api/admin/familiar-eggs/catalog       — H+ admin upsert egg SKU (tenant_admin)
//	DELETE /api/admin/familiar-eggs/catalog/{sku} — H+ admin soft-delete egg SKU
//
// All handlers extract X-Tenant-Id (mandatory) + X-GCID (purchase paths).
// Admin paths authorize on the gateway-stamped x-mesh-user-roles mesh header
// via callerHoldsHPlusAdminRole (CHO-2075 / CHO-2072 fail-closed primitive) —
// NOT a client-suppliable role header. tenant_id is propagated to RLS via
// SET LOCAL chora.tenant_id (responsibility of the pgx layer).
//
// Idempotency on Stripe webhooks anchors on the Stripe event.ID — handled
// via the IdempotencyStore port so a redelivery skips re-publish.
//
// OTLP: every handler is wrapped by the v2Logging middleware which
// surfaces traceparent. Per-route span attributes are emitted on the
// active span (chora.tenant_id, chora.egg.sku, chora.egg.purchase_id,
// chora.stripe.session_id, chora.stripe.event_id).
package httpapi

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events"
	familiareggstripe "github.com/apollo-chora/chora-tenancy/internal/adapter/familiareggstripe"
	familiareggpub "github.com/apollo-chora/chora-tenancy/internal/adapter/pubsub"
	"github.com/apollo-chora/chora-tenancy/internal/domain/billing/webhook"
	familiareag "github.com/apollo-chora/chora-tenancy/internal/domain/familiar_egg"
)

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

// PurchaseStore is the persistence port for FamiliarEgg purchases.
type PurchaseStore interface {
	Insert(ctx context.Context, p *familiareag.Purchase) error
	UpdateState(ctx context.Context, p *familiareag.Purchase) error
	GetByStripeSessionID(ctx context.Context, sessionID string) (*familiareag.Purchase, bool, error)
}

// CatalogStore is the persistence port for FamiliarEgg catalog SKUs.
//
// Every method is tenant-scoped. familiar_egg_catalog carries FORCE RLS and
// the app role is NOBYPASSRLS, so the pg implementation runs each statement
// inside a `SET LOCAL chora.tenant_id` transaction; a lookup without a
// tenant sees platform-global SKUs only and every tenant-scoped SKU
// disappears. Upsert takes its tenant from the entry itself.
type CatalogStore interface {
	GetBySKU(ctx context.Context, tenantID, sku string) (*familiareag.CatalogEntry, bool, error)
	ListForTenant(ctx context.Context, tenantID string, at time.Time) ([]*familiareag.CatalogEntry, error)
	Upsert(ctx context.Context, e *familiareag.CatalogEntry) error
	SoftDelete(ctx context.Context, tenantID, sku string) error
}

// WebhookIdemStore is the port for Stripe webhook event-ID idempotency.
// Process MUST persist seen event IDs and skip duplicates. A new
// stripe_webhook_events table is a fine production wiring; in tests an
// in-memory map suffices.
type WebhookIdemStore interface {
	// MarkSeen records the event ID. Returns (alreadySeen, error).
	MarkSeen(ctx context.Context, eventID string, seenAt time.Time) (bool, error)
}

// FamiliarEggCheckoutClient delegates Stripe Checkout session creation +
// FamiliarEgg purchase persistence to chora-payments (ADR-164). chora-tenancy
// is the FamiliarEgg *originating* service (it owns the aggregate + provisions
// the egg on the chora.payments.familiar_egg_purchase.payment_captured.v1
// event) — but the Stripe SDK + the familiar_egg_purchases rows live in
// chora-payments. The tenancy table was dropped by migration 0017; the checkout
// handler NEVER persists the purchase locally.
type FamiliarEggCheckoutClient interface {
	CreateFamiliarEggCheckoutSession(ctx context.Context, in EggCheckoutInput) (EggCheckoutOutput, error)
}

// EggCheckoutInput mirrors chora-payments' CreateFamiliarEggCheckoutSession
// request. A cmd/server adapter maps it onto the payments gRPC client so this
// package stays decoupled from the payments adapter (per the paymentsHTTPAdapter
// pattern).
type EggCheckoutInput struct {
	IdempotencyKey       string
	TenantID             string
	LearnerGCID          string
	EggSKU               string
	SuggestedFocalAtomID string
	AmountCents          int64
	Currency             string
	SoftExpiryAt         time.Time
	HardExpiryAt         time.Time
	SuccessURL           string
	CancelURL            string
}

// EggCheckoutOutput is the parsed chora-payments response.
type EggCheckoutOutput struct {
	PurchaseID        string
	StripeSessionID   string
	StripeCheckoutURL string
	State             string
}

// ---------------------------------------------------------------------------
// Deps
// ---------------------------------------------------------------------------

// FamiliarEggDeps wires the Iter G.3 handler set.
type FamiliarEggDeps struct {
	Purchases   PurchaseStore
	Catalog     CatalogStore
	Stripe      familiareggstripe.Client
	Publisher   familiareggpub.EggPublisher
	WebhookIdem WebhookIdemStore

	// Payments delegates the Stripe Checkout session + FamiliarEgg purchase
	// persistence to chora-payments (ADR-164). Required for /checkout; the
	// handler fails loud (500) if nil. Stripe/Purchases above remain wired for
	// the (soon-to-be-decommissioned) local webhook handler only.
	Payments FamiliarEggCheckoutClient

	// WebhookSecret is the Stripe webhook signing secret (env
	// STRIPE_WEBHOOK_SECRET). Required for /webhooks/stripe.
	WebhookSecret string

	// SuccessURL / CancelURL are the Stripe success/cancel redirects
	// (env STRIPE_SUCCESS_URL / STRIPE_CANCEL_URL). The /checkout body
	// MAY override SuccessURL via {return_url}; CancelURL always
	// defaults from env.
	SuccessURL string
	CancelURL  string

	// Now is injectable for tests.
	Now func() time.Time

	// NewPurchaseID is injectable so tests can pin deterministic IDs.
	NewPurchaseID func() string

	// SignatureSkew caps clock-skew on Stripe-Signature verification.
	// Defaults to 5*time.Minute.
	SignatureSkew time.Duration
}

// NewFamiliarEggMux returns an http.Handler with all Iter G.3 routes mounted.
func NewFamiliarEggMux(deps FamiliarEggDeps) http.Handler {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	if deps.NewPurchaseID == nil {
		deps.NewPurchaseID = newUUIDv7
	}
	if deps.SignatureSkew == 0 {
		deps.SignatureSkew = 5 * time.Minute
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/familiar-eggs/checkout", v2Logging(handleEggCheckout(deps)))
	mux.HandleFunc("/api/familiar-eggs/catalog", v2Logging(handleEggCatalogList(deps)))
	mux.HandleFunc("/api/familiar-eggs/", v2Logging(handleEggBySKU(deps)))
	mux.HandleFunc("/api/admin/familiar-eggs/catalog", v2Logging(handleEggAdminCatalogUpsert(deps)))
	mux.HandleFunc("/api/admin/familiar-eggs/catalog/", v2Logging(handleEggAdminCatalogSubpath(deps)))
	mux.HandleFunc("/webhooks/stripe", v2Logging(handleEggStripeWebhook(deps)))
	return mux
}

// ---------------------------------------------------------------------------
// POST /api/familiar-eggs/checkout
// ---------------------------------------------------------------------------

type checkoutReq struct {
	EggSKU    string `json:"egg_sku"`
	ReturnURL string `json:"return_url"`
}

type checkoutResp struct {
	PurchaseID        string `json:"purchase_id"`
	StripeCheckoutURL string `json:"stripe_checkout_url"`
}

func handleEggCheckout(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if tenantID == "" {
			v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
			return
		}
		gcid := strings.TrimSpace(firstNonEmpty(r.Header.Get("X-GCID"), r.Header.Get("gcid")))
		if gcid == "" {
			v2WriteError(w, http.StatusBadRequest, "x_gcid_required", "X-GCID header required")
			return
		}
		var req checkoutReq
		if err := v2Decode(r, &req); err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		req.EggSKU = strings.TrimSpace(req.EggSKU)
		if req.EggSKU == "" {
			v2WriteError(w, http.StatusUnprocessableEntity, "validation_failed", "egg_sku required")
			return
		}

		// Resolve SKU.
		ctx := r.Context()
		entry, ok, err := d.Catalog.GetBySKU(ctx, tenantID, req.EggSKU)
		if err != nil {
			v2WriteError(w, http.StatusInternalServerError, "catalog_lookup_failed", err.Error())
			return
		}
		if !ok || entry == nil {
			v2WriteError(w, http.StatusNotFound, "sku_not_found", "egg SKU not found")
			return
		}
		if !entry.Purchasable {
			v2WriteError(w, http.StatusForbidden, "sku_not_purchasable", "egg SKU not available for direct purchase")
			return
		}
		if !entry.AvailableAt(d.Now()) {
			v2WriteError(w, http.StatusForbidden, "sku_unavailable", "egg SKU not available at this time")
			return
		}
		// Cross-tenant guard: a tenant-scoped SKU is only purchasable by
		// the owning tenant (platform SKUs entry.TenantID == "" are global).
		if entry.TenantID != "" && entry.TenantID != tenantID {
			v2WriteError(w, http.StatusForbidden, "sku_cross_tenant", "egg SKU not available to this tenant")
			return
		}

		// CHO-2035: validate the breed distribution can actually hatch BEFORE any
		// money moves. Hatch is too late to discover a malformed SKU — an egg
		// whose distribution is empty/unknown 500s at hatch (species roll fails,
		// gateway masks it as a 502) and permanently occupies a roster slot. The
		// admin upsert path enforces this via CatalogEntry.Validate, but legacy /
		// DB-direct rows bypass it, so re-check at purchase time and fail loud
		// with an explicit 4xx rather than a masked hatch-time 5xx.
		if err := entry.BreedDistribution.Validate(); err != nil {
			v2WriteError(w, http.StatusUnprocessableEntity, "sku_invalid_distribution", "egg SKU cannot hatch: "+err.Error())
			return
		}

		// Delegate to chora-payments (ADR-164): chora-payments owns the Stripe
		// SDK + the familiar_egg_purchases persistence. chora-tenancy stays the
		// FamiliarEgg originating service and provisions the egg on the
		// chora.payments.familiar_egg_purchase.payment_captured.v1 event
		// (payments_subscriber_wiring.go). The tenancy table was dropped by
		// migration 0017 — NO local Stripe call + NO local persistence here.
		if d.Payments == nil {
			v2WriteError(w, http.StatusInternalServerError, "payments_unwired", "familiar-egg checkout client not configured")
			return
		}
		successURL := strings.TrimSpace(req.ReturnURL)
		if successURL == "" {
			successURL = d.SuccessURL
		}
		now := d.Now()
		in := EggCheckoutInput{
			IdempotencyKey:       "chora.tenancy.familiar_egg." + d.NewPurchaseID(),
			TenantID:             tenantID,
			LearnerGCID:          gcid,
			EggSKU:               entry.SKU,
			SuggestedFocalAtomID: entry.SuggestedFocalAtomID,
			AmountCents:          entry.PriceCents,
			Currency:             entry.Currency,
			SuccessURL:           successURL,
			CancelURL:            d.CancelURL,
		}
		if entry.SoftExpiryDays > 0 {
			in.SoftExpiryAt = now.AddDate(0, 0, entry.SoftExpiryDays)
		}
		if entry.HardExpiryDays > 0 {
			in.HardExpiryAt = now.AddDate(0, 0, entry.HardExpiryDays)
		}
		out, err := d.Payments.CreateFamiliarEggCheckoutSession(ctx, in)
		if err != nil {
			v2WriteError(w, http.StatusBadGateway, "payments_checkout_failed", err.Error())
			return
		}
		v2WriteJSON(w, http.StatusCreated, checkoutResp{
			PurchaseID:        out.PurchaseID,
			StripeCheckoutURL: out.StripeCheckoutURL,
		})
	}
}

// ---------------------------------------------------------------------------
// GET /api/familiar-eggs/catalog
// GET /api/familiar-eggs/{sku}/odds
// ---------------------------------------------------------------------------

func handleEggCatalogList(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if tenantID == "" {
			v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
			return
		}
		entries, err := d.Catalog.ListForTenant(r.Context(), tenantID, d.Now())
		if err != nil {
			v2WriteError(w, http.StatusInternalServerError, "catalog_list_failed", err.Error())
			return
		}
		out := make([]map[string]any, 0, len(entries))
		for _, e := range entries {
			out = append(out, catalogDTO(e))
		}
		v2WriteJSON(w, http.StatusOK, map[string]any{"items": out, "total": len(out)})
	}
}

func handleEggBySKU(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/familiar-eggs/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) < 2 {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		sku := parts[0]
		switch parts[1] {
		case "odds":
			handleEggOdds(d, sku, w, r)
		default:
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
		}
	}
}

func handleEggOdds(d FamiliarEggDeps, sku string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if strings.TrimSpace(sku) == "" {
		v2WriteError(w, http.StatusBadRequest, "sku_required", "sku required")
		return
	}
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
		return
	}
	e, ok, err := d.Catalog.GetBySKU(r.Context(), tenantID, sku)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "catalog_lookup_failed", err.Error())
		return
	}
	if !ok {
		v2WriteError(w, http.StatusNotFound, "sku_not_found", "egg SKU not found")
		return
	}
	if e.TenantID != "" && e.TenantID != tenantID {
		v2WriteError(w, http.StatusForbidden, "sku_cross_tenant", "egg SKU not visible to this tenant")
		return
	}
	v2WriteJSON(w, http.StatusOK, map[string]any{
		"sku":                     e.SKU,
		"odds":                    e.BreedDistribution.Odds(),
		"total_weight":            e.BreedDistribution.Total(),
		"distribution_updated_at": e.BreedDistributionUpdated.UTC().Format(time.RFC3339Nano),
		// IMDA D2 transparency tag.
		"chora_imda_dimension": "transparency",
	})
}

// ---------------------------------------------------------------------------
// POST /api/admin/familiar-eggs/catalog
// DELETE /api/admin/familiar-eggs/catalog/{sku}
// ---------------------------------------------------------------------------

type adminCatalogReq struct {
	SKU                  string             `json:"sku"`
	DisplayName          string             `json:"display_name"`
	Description          string             `json:"description"`
	PriceCents           int64              `json:"price_cents"`
	Currency             string             `json:"currency"`
	SuggestedFocalAtomID string             `json:"suggested_focal_atom_id"`
	BreedDistribution    map[string]float64 `json:"breed_distribution"`
	Purchasable          *bool              `json:"purchasable,omitempty"`
	IsTrial              bool               `json:"is_trial"`
	SoftExpiryDays       int                `json:"soft_expiry_days"`
	HardExpiryDays       int                `json:"hard_expiry_days"`
	AvailableFrom        *time.Time         `json:"available_from,omitempty"`
	AvailableUntil       *time.Time         `json:"available_until,omitempty"`
}

func handleEggAdminCatalogUpsert(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		// CHO-2075: authorize on the gateway-stamped, non-spoofable
		// x-mesh-user-roles mesh header (the CHO-2072 fail-closed primitive every
		// other H+ admin write gate uses), NOT the legacy X-Chora-Role/X-Role
		// header the gateway only stamps on the tx-history routes.
		if !callerHoldsHPlusAdminRole(r.Header) {
			v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if tenantID == "" {
			v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
			return
		}
		var req adminCatalogReq
		if err := v2Decode(r, &req); err != nil {
			v2WriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		purchasable := true
		if req.Purchasable != nil {
			purchasable = *req.Purchasable
		}
		entry := &familiareag.CatalogEntry{
			SKU:                  strings.TrimSpace(req.SKU),
			TenantID:             tenantID,
			DisplayName:          req.DisplayName,
			Description:          req.Description,
			PriceCents:           req.PriceCents,
			Currency:             strings.ToUpper(strings.TrimSpace(req.Currency)),
			SuggestedFocalAtomID: req.SuggestedFocalAtomID,
			BreedDistribution:    familiareag.BreedDistribution(req.BreedDistribution),
			Purchasable:          purchasable,
			IsTrial:              req.IsTrial,
			SoftExpiryDays:       req.SoftExpiryDays,
			HardExpiryDays:       req.HardExpiryDays,
			AvailableFrom:        req.AvailableFrom,
			AvailableUntil:       req.AvailableUntil,
		}
		if err := entry.Validate(); err != nil {
			v2WriteError(w, http.StatusBadRequest, "validation_failed", err.Error())
			return
		}
		if err := d.Catalog.Upsert(r.Context(), entry); err != nil {
			v2WriteError(w, http.StatusInternalServerError, "catalog_upsert_failed", err.Error())
			return
		}
		// Re-fetch (BreedDistributionUpdated trigger stamp + other defaults).
		saved, _, err := d.Catalog.GetBySKU(r.Context(), tenantID, entry.SKU)
		if err != nil || saved == nil {
			// Return the input shape as a fallback; the trigger-applied
			// fields may be stale.
			v2WriteJSON(w, http.StatusOK, catalogDTO(entry))
			return
		}
		v2WriteJSON(w, http.StatusOK, catalogDTO(saved))
	}
}

func handleEggAdminCatalogSubpath(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sku := strings.TrimPrefix(r.URL.Path, "/api/admin/familiar-eggs/catalog/")
		sku = strings.TrimSpace(sku)
		if sku == "" {
			v2WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		// GET reads one entry, DELETE soft-deletes it. The read exists because the
		// H+ editor saves through the FULL-entry upsert and must therefore read the
		// full entry first: the catalog LIST filters on the availability window, so
		// a SKU outside its dates is invisible to the very admin who needs to edit
		// it, and /api/familiar-eggs/{sku} only serves /odds. Reconstructing an
		// entry the editor never read would wipe every field it did not know about.
		if r.Method != http.MethodDelete && r.Method != http.MethodGet {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		// CHO-2075: authorize on the gateway-stamped, non-spoofable
		// x-mesh-user-roles mesh header (the CHO-2072 fail-closed primitive every
		// other H+ admin write gate uses), NOT the legacy X-Chora-Role/X-Role
		// header the gateway only stamps on the tx-history routes.
		if !callerHoldsHPlusAdminRole(r.Header) {
			v2WriteError(w, http.StatusForbidden, "forbidden", "role not permitted")
			return
		}
		// The delete is scoped to the caller's tenant: the mutation policy on
		// familiar_egg_catalog is tenant_id = chora.tenant_id, so without a
		// tenant the statement matches nothing and reports success anyway.
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if tenantID == "" {
			v2WriteError(w, http.StatusBadRequest, "x_tenant_required", "X-Tenant-Id header required")
			return
		}
		if r.Method == http.MethodGet {
			entry, ok, err := d.Catalog.GetBySKU(r.Context(), tenantID, sku)
			if err != nil {
				v2WriteError(w, http.StatusInternalServerError, "catalog_lookup_failed", err.Error())
				return
			}
			// Fail loud on a miss. A fabricated empty entry here would be saved
			// straight back over a real row by the editor's next POST.
			if !ok || entry == nil {
				v2WriteError(w, http.StatusNotFound, "sku_not_found", "egg SKU not found")
				return
			}
			v2WriteJSON(w, http.StatusOK, catalogDTO(entry))
			return
		}
		if err := d.Catalog.SoftDelete(r.Context(), tenantID, sku); err != nil {
			v2WriteError(w, http.StatusInternalServerError, "catalog_delete_failed", err.Error())
			return
		}
		v2WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "sku": sku})
	}
}

// ---------------------------------------------------------------------------
// POST /webhooks/stripe
// ---------------------------------------------------------------------------

func handleEggStripeWebhook(d FamiliarEggDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			v2WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = r.Body.Close()
		if err != nil {
			v2WriteError(w, http.StatusBadRequest, "body_read_failed", err.Error())
			return
		}
		// Signature verification BEFORE parsing (security-critical).
		if err := webhook.VerifySignature(
			d.WebhookSecret,
			r.Header.Get("Stripe-Signature"),
			body,
			d.Now(),
			d.SignatureSkew,
		); err != nil {
			v2WriteError(w, http.StatusUnauthorized, "stripe_signature_invalid", err.Error())
			return
		}
		pe, err := familiareggstripe.ParseEvent(body)
		if err != nil {
			v2WriteError(w, http.StatusBadRequest, "stripe_event_invalid", err.Error())
			return
		}

		// Idempotency on Stripe event.ID — duplicate retries skip work.
		seen, err := d.WebhookIdem.MarkSeen(r.Context(), pe.ID, d.Now())
		if err != nil {
			v2WriteError(w, http.StatusInternalServerError, "idem_store_failed", err.Error())
			return
		}
		if seen {
			v2WriteJSON(w, http.StatusOK, map[string]any{
				"status":   "duplicate",
				"event_id": pe.ID,
			})
			return
		}

		// Route by event type.
		ctx := r.Context()
		switch pe.Type {
		case familiareggstripe.EventCheckoutSessionCompleted:
			handleCheckoutCompleted(ctx, d, pe, w, r)
		case familiareggstripe.EventCheckoutSessionAsyncPaymentFail, familiareggstripe.EventChargeFailed:
			handleStripeFailure(ctx, d, pe, w, r)
		case familiareggstripe.EventChargeRefunded:
			handleChargeRefunded(ctx, d, pe, w, r)
		default:
			// Ignored event type: 202 so Stripe stops retrying. NOT a 4xx
			// (Stripe would otherwise retry indefinitely).
			v2WriteJSON(w, http.StatusAccepted, map[string]any{
				"status":   "ignored",
				"event_id": pe.ID,
				"type":     pe.Type,
			})
		}
	}
}

func handleCheckoutCompleted(ctx context.Context, d FamiliarEggDeps, pe *familiareggstripe.ParsedEvent, w http.ResponseWriter, r *http.Request) {
	if pe.SessionID == "" {
		v2WriteError(w, http.StatusBadRequest, "session_id_missing", "missing checkout session id")
		return
	}
	p, ok, err := d.Purchases.GetByStripeSessionID(ctx, pe.SessionID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "purchase_lookup_failed", err.Error())
		return
	}
	if !ok || p == nil {
		// Unknown session — log + 202 (don't surface a 4xx; Stripe would
		// keep retrying forever).
		v2WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":   "orphan_session",
			"event_id": pe.ID,
		})
		return
	}
	if err := p.MarkPaid(d.Now(), pe.PaymentIntentID, "", pe.AmountCents); err != nil {
		v2WriteError(w, http.StatusConflict, "purchase_state_invalid", err.Error())
		return
	}
	if err := d.Purchases.UpdateState(ctx, p); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "purchase_persist_failed", err.Error())
		return
	}
	hdr := events.Header{TenantID: p.TenantID, GCID: p.PurchaserGCID, Traceparent: r.Header.Get("traceparent")}
	if _, err := familiareggpub.EmitPaymentSucceeded(d.Publisher, hdr, p); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "event_publish_failed", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusOK, map[string]any{
		"status":      "paid",
		"event_id":    pe.ID,
		"purchase_id": p.PurchaseID,
	})
}

func handleStripeFailure(ctx context.Context, d FamiliarEggDeps, pe *familiareggstripe.ParsedEvent, w http.ResponseWriter, r *http.Request) {
	// For charge.failed the session_id isn't on the event; correlate via
	// payment_intent_id by looking up purchase rows. For Iter G.3 the
	// happy path is the checkout.session.async_payment_failed which DOES
	// carry session_id. Charge.failed without session-correlation lands
	// as orphan.
	if pe.SessionID == "" {
		v2WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":   "uncorrelated_failure",
			"event_id": pe.ID,
			"type":     pe.Type,
		})
		return
	}
	p, ok, err := d.Purchases.GetByStripeSessionID(ctx, pe.SessionID)
	if err != nil {
		v2WriteError(w, http.StatusInternalServerError, "purchase_lookup_failed", err.Error())
		return
	}
	if !ok || p == nil {
		v2WriteJSON(w, http.StatusAccepted, map[string]any{"status": "orphan_session", "event_id": pe.ID})
		return
	}
	if err := p.MarkPaymentFailed(d.Now(), pe.FailureCode, pe.FailureMessage); err != nil {
		// Already paid / refunded — surface 409 conflict.
		v2WriteError(w, http.StatusConflict, "purchase_state_invalid", err.Error())
		return
	}
	if err := d.Purchases.UpdateState(ctx, p); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "purchase_persist_failed", err.Error())
		return
	}
	hdr := events.Header{TenantID: p.TenantID, GCID: p.PurchaserGCID, Traceparent: r.Header.Get("traceparent")}
	if _, err := familiareggpub.EmitPaymentFailed(d.Publisher, hdr, p); err != nil {
		v2WriteError(w, http.StatusInternalServerError, "event_publish_failed", err.Error())
		return
	}
	v2WriteJSON(w, http.StatusOK, map[string]any{
		"status":      "payment_failed",
		"event_id":    pe.ID,
		"purchase_id": p.PurchaseID,
	})
}

func handleChargeRefunded(ctx context.Context, d FamiliarEggDeps, pe *familiareggstripe.ParsedEvent, w http.ResponseWriter, r *http.Request) {
	// charge.refunded carries data.object.id = charge_id; sessions are
	// looked up by charge_id (each purchase row has stripe_charge_id set
	// post-paid). For Iter G.3 we look up via metadata if Stripe injected
	// it; otherwise the orphan path runs.
	purchaseID := pe.Metadata["chora_purchase_id"]
	if purchaseID == "" {
		// Surface the orphan path so M14 can wire a chora_purchase_id
		// metadata pass-through on the Charge.
		v2WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":   "orphan_refund",
			"event_id": pe.ID,
		})
		return
	}
	// Iter G.3 path: GetByStripeSessionID is the only port we have. We
	// surface "needs_followup" as 202 so we don't pretend to have full
	// refund correlation yet. M14 wires GetByPurchaseID.
	v2WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":       "refund_ack",
		"event_id":     pe.ID,
		"purchase_id":  purchaseID,
		"refund_id":    pe.RefundID,
		"amount_cents": pe.AmountCents,
		"todo":         "wire GetByPurchaseID + MarkRefunded in Iter G.5",
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func catalogDTO(e *familiareag.CatalogEntry) map[string]any {
	out := map[string]any{
		"sku":                       e.SKU,
		"tenant_id":                 e.TenantID,
		"display_name":              e.DisplayName,
		"description":               e.Description,
		"price_cents":               e.PriceCents,
		"currency":                  e.Currency,
		"suggested_focal_atom_id":   e.SuggestedFocalAtomID,
		"breed_distribution":        e.BreedDistribution,
		"odds":                      e.BreedDistribution.Odds(),
		"distribution_total_weight": e.BreedDistribution.Total(),
		"distribution_updated_at":   e.BreedDistributionUpdated.UTC().Format(time.RFC3339Nano),
		"purchasable":               e.Purchasable,
		"is_trial":                  e.IsTrial,
		"soft_expiry_days":          e.SoftExpiryDays,
		"hard_expiry_days":          e.HardExpiryDays,
	}
	if e.AvailableFrom != nil {
		out["available_from"] = e.AvailableFrom.UTC().Format(time.RFC3339Nano)
	}
	if e.AvailableUntil != nil {
		out["available_until"] = e.AvailableUntil.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// newUUIDv7 mints a UUIDv7 (kept local to avoid pulling the uuid package).
func newUUIDv7() string {
	var b [16]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < 16; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// InMemoryWebhookIdemStore is a small in-memory implementation of
// WebhookIdemStore. Used in tests; the production wire-up can either use
// the chora_tenancy.idempotency_keys table or a new stripe_webhook_events
// table.
type InMemoryWebhookIdemStore struct {
	seen map[string]time.Time
}

// NewInMemoryWebhookIdemStore constructs an in-memory webhook idem store.
func NewInMemoryWebhookIdemStore() *InMemoryWebhookIdemStore {
	return &InMemoryWebhookIdemStore{seen: make(map[string]time.Time)}
}

// MarkSeen satisfies WebhookIdemStore.
func (s *InMemoryWebhookIdemStore) MarkSeen(_ context.Context, eventID string, at time.Time) (bool, error) {
	if _, dup := s.seen[eventID]; dup {
		return true, nil
	}
	s.seen[eventID] = at
	return false, nil
}

// InMemoryPurchaseStore is the dev-mode PurchaseStore fallback used when
// CHORA_DB_DSN is unset. NOT safe for production; rows lost on restart.
type InMemoryPurchaseStore struct {
	rows      map[string]*familiareag.Purchase // purchase_id -> row
	bySession map[string]string                // session_id -> purchase_id
}

// NewInMemoryPurchaseStore constructs the in-mem PurchaseStore.
func NewInMemoryPurchaseStore() *InMemoryPurchaseStore {
	return &InMemoryPurchaseStore{
		rows:      make(map[string]*familiareag.Purchase),
		bySession: make(map[string]string),
	}
}

// Insert satisfies PurchaseStore.
func (s *InMemoryPurchaseStore) Insert(_ context.Context, p *familiareag.Purchase) error {
	if p == nil {
		return errors.New("inmem: nil purchase")
	}
	if _, dup := s.bySession[p.StripeSessionID]; dup {
		return fmt.Errorf("inmem: duplicate stripe_session_id %s", p.StripeSessionID)
	}
	cp := *p
	s.rows[p.PurchaseID] = &cp
	s.bySession[p.StripeSessionID] = p.PurchaseID
	return nil
}

// UpdateState satisfies PurchaseStore.
func (s *InMemoryPurchaseStore) UpdateState(_ context.Context, p *familiareag.Purchase) error {
	if p == nil {
		return errors.New("inmem: nil purchase")
	}
	if _, ok := s.rows[p.PurchaseID]; !ok {
		return fmt.Errorf("inmem: purchase not found: %s", p.PurchaseID)
	}
	cp := *p
	s.rows[p.PurchaseID] = &cp
	return nil
}

// GetByStripeSessionID satisfies PurchaseStore.
func (s *InMemoryPurchaseStore) GetByStripeSessionID(_ context.Context, sessionID string) (*familiareag.Purchase, bool, error) {
	id, ok := s.bySession[sessionID]
	if !ok {
		return nil, false, nil
	}
	cp := *s.rows[id]
	return &cp, true, nil
}

// AllRows returns a slice of all stored rows. Test helper.
func (s *InMemoryPurchaseStore) AllRows() []*familiareag.Purchase {
	out := make([]*familiareag.Purchase, 0, len(s.rows))
	for _, p := range s.rows {
		cp := *p
		out = append(out, &cp)
	}
	return out
}

// InMemoryCatalogStore is the dev-mode CatalogStore fallback.
type InMemoryCatalogStore struct {
	rows map[string]*familiareag.CatalogEntry
}

// NewInMemoryCatalogStore constructs the in-mem CatalogStore with the
// seed platform-default SKUs from migration 0007 for parity.
func NewInMemoryCatalogStore() *InMemoryCatalogStore {
	now := time.Now().UTC()
	rows := map[string]*familiareag.CatalogEntry{
		"egg.trial.v1": {
			SKU:                      "egg.trial.v1",
			DisplayName:              "Trial Egg",
			Description:              "Your first Familiar — free to try. Uniform odds across all breeds. One per learner, ever.",
			PriceCents:               0,
			Currency:                 "SGD",
			BreedDistribution:        familiareag.BreedDistribution{"owl": 20.0, "fox": 20.0, "penguin": 20.0, "dragon": 20.0, "phoenix": 20.0},
			BreedDistributionUpdated: now,
			Purchasable:              false,
			IsTrial:                  true,
			SoftExpiryDays:           30,
			HardExpiryDays:           60,
			CreatedAt:                now,
			UpdatedAt:                now,
		},
		"egg.standard.v1": {
			SKU:                      "egg.standard.v1",
			DisplayName:              "Standard Egg",
			Description:              "A versatile Familiar egg. Pick any focal atom from your Knowledge Graph at hatching. Common breeds favored.",
			PriceCents:               999,
			Currency:                 "SGD",
			BreedDistribution:        familiareag.BreedDistribution{"owl": 28.0, "fox": 27.0, "penguin": 25.0, "dragon": 12.0, "phoenix": 8.0},
			BreedDistributionUpdated: now,
			Purchasable:              true,
			IsTrial:                  false,
			SoftExpiryDays:           30,
			HardExpiryDays:           60,
			CreatedAt:                now,
			UpdatedAt:                now,
		},
		"egg.premium.v1": {
			SKU:                      "egg.premium.v1",
			DisplayName:              "Premium Egg",
			Description:              "A premium Familiar egg with rarer breeds weighted. Includes Source Revelation preview boost at Stage 3.",
			PriceCents:               2999,
			Currency:                 "SGD",
			BreedDistribution:        familiareag.BreedDistribution{"owl": 15.0, "fox": 13.0, "penguin": 12.0, "dragon": 30.0, "phoenix": 30.0},
			BreedDistributionUpdated: now,
			Purchasable:              true,
			IsTrial:                  false,
			SoftExpiryDays:           45,
			HardExpiryDays:           90,
			CreatedAt:                now,
			UpdatedAt:                now,
		},
	}
	return &InMemoryCatalogStore{rows: rows}
}

// GetBySKU satisfies CatalogStore. Mirrors the familiar_egg_catalog RLS
// visibility rule (platform-global OR own tenant) so the dev fallback does
// not behave more permissively than production.
func (s *InMemoryCatalogStore) GetBySKU(_ context.Context, tenantID, sku string) (*familiareag.CatalogEntry, bool, error) {
	e, ok := s.rows[sku]
	if !ok || (e.DeletedAt != nil) {
		return nil, false, nil
	}
	if e.TenantID != "" && e.TenantID != tenantID {
		return nil, false, nil
	}
	cp := *e
	return &cp, true, nil
}

// ListForTenant satisfies CatalogStore.
func (s *InMemoryCatalogStore) ListForTenant(_ context.Context, tenantID string, at time.Time) ([]*familiareag.CatalogEntry, error) {
	out := make([]*familiareag.CatalogEntry, 0, len(s.rows))
	for _, e := range s.rows {
		if e.DeletedAt != nil {
			continue
		}
		if e.TenantID != "" && e.TenantID != tenantID {
			continue
		}
		if !e.AvailableAt(at) {
			continue
		}
		cp := *e
		out = append(out, &cp)
	}
	return out, nil
}

// Upsert satisfies CatalogStore.
func (s *InMemoryCatalogStore) Upsert(_ context.Context, e *familiareag.CatalogEntry) error {
	if e == nil {
		return errors.New("inmem: nil entry")
	}
	cp := *e
	now := time.Now().UTC()
	cp.BreedDistributionUpdated = now
	cp.UpdatedAt = now
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = now
	}
	s.rows[cp.SKU] = &cp
	return nil
}

// SoftDelete satisfies CatalogStore. Only the owning tenant may delete;
// mirrors the tenant_admin_mutation policy (platform-global rows are not
// app-writable).
func (s *InMemoryCatalogStore) SoftDelete(_ context.Context, tenantID, sku string) error {
	e, ok := s.rows[sku]
	if !ok || e.TenantID != tenantID {
		return nil
	}
	now := time.Now().UTC()
	e.DeletedAt = &now
	return nil
}
