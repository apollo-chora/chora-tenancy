// ownership_handler.go: the HTTP surface for the two-party tenant ownership
// handover (UX Track U, E3 slice 5, first-launch spec 13.3, 13.4 and 13.8).
//
// # WHAT THE CALLER MAY SAY, AND WHAT THE SERVER DECIDES
//
// The body carries the nominee, an optional note, and on the override a
// mandatory reason. Everything else is resolved here:
//
//   - from_gcid is NEVER taken from the body. A GCID alone never proves
//     ownership (spec 13.6), so the outgoing owner is read from the members
//     table. A body field of that name is ignored rather than rejected, for the
//     same reason the closure routes ignore a smuggled gcid: silently dropping
//     it keeps the identity-from-server rule absolute.
//   - initiated_by is the caller, from the mesh-stamped X-GCID header.
//   - the tenant is the X-Tenant-Id header on the me-routes, and the PATH on
//     the operator override, because PLATFORM_OPERATOR is tenant-less by design
//     (ADR-165) and its header tenant is either absent or somewhere else.
//
// # THE TWO GATES
//
// me-routes: the caller must hold the tenant's LIVE owner row to create an
// offer. Accept, decline and revoke are gated by the repository against the
// offer row itself, because the roster can change between nomination and
// answer, so the check that matters is the one at the answer.
//
// operator override: platform_operator on the mesh-stamped roles header, the
// same gate bootstrap_handler.go uses. Acceptance is STILL required from the
// nominee: nothing about the owner being gone makes silent assignment
// acceptable.
//
// # ERROR MAPPING
//
// Every repository refusal maps to its own code. There is no default 4xx arm:
// an unrecognised error is a 500, because dressing an unknown failure as the
// caller's fault is how a real outage reads as user error.
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/ownership"
)

// OwnershipStore is the narrow port the handler needs. Production wiring is
// pg.OwnershipRepository; tests inject a fake.
type OwnershipStore interface {
	OutgoingOwnerGCID(ctx context.Context, tenantID string) (string, bool, error)
	CreateOffer(ctx context.Context, o *ownership.Offer) error
	PendingOffer(ctx context.Context, tenantID string) (*ownership.Offer, error)
	AcceptOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error
	DeclineOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error
	RevokeOffer(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error
}

// OwnershipHandler serves the handover routes.
type OwnershipHandler struct {
	store OwnershipStore
}

// NewOwnershipHandler fails loud on a nil store, so a wiring bug is a boot
// failure rather than a route that 500s in production.
func NewOwnershipHandler(store OwnershipStore) (*OwnershipHandler, error) {
	if store == nil {
		return nil, errors.New("httpapi.NewOwnershipHandler: store required")
	}
	return &OwnershipHandler{store: store}, nil
}

// Route paths. Exact and parametric mounts throughout, never prefixes: the
// service root mux already routes /api/v1/admin/ wholesale to the v2 handler,
// and a /api/v1/admin/tenants/ prefix here would swallow the live me/addons and
// mana-pool routes. A parametric pattern matches a strict subset, so Go's mux
// gives it precedence without capturing anything else.
//
// The override sits under /api/v1/admin/tenants/{tenantId}/... rather than a
// new top-level /api/v1/admin/ownership/ subtree because the Istio
// AuthorizationPolicy for chora-tenancy enumerates paths (no wildcard host
// rule): /api/v1/admin/tenants/* is already allowlisted, and a new top level
// would pass every local test and 403 at the sidecar in production.
const (
	PathMeOwnershipOffer  = "/api/v1/tenants/me/ownership/offer"
	PathMeOwnershipOffers = "/api/v1/tenants/me/ownership/offers"

	PatternMeOwnershipAccept  = "/api/v1/tenants/me/ownership/offers/{offerId}/accept"
	PatternMeOwnershipDecline = "/api/v1/tenants/me/ownership/offers/{offerId}/decline"
	PatternMeOwnershipRevoke  = "/api/v1/tenants/me/ownership/offers/{offerId}/revoke"

	PatternAdminOwnershipOffers = "/api/v1/admin/tenants/{tenantId}/ownership/offers"
)

// RegisterOwnershipRoutes is the ONE registration site: the production router
// calls it and the tests build their mux by calling it too, so a route added
// here is covered and a route added elsewhere is visibly not part of this
// surface.
func RegisterOwnershipRoutes(mux *http.ServeMux, h *OwnershipHandler) {
	mux.HandleFunc(PathMeOwnershipOffer, h.MeGetOffer)
	mux.HandleFunc(PathMeOwnershipOffers, h.MeCreateOffer)
	mux.HandleFunc(PatternMeOwnershipAccept, h.MeAccept)
	mux.HandleFunc(PatternMeOwnershipDecline, h.MeDecline)
	mux.HandleFunc(PatternMeOwnershipRevoke, h.MeRevoke)
	mux.HandleFunc(PatternAdminOwnershipOffers, h.OperatorCreateOffer)
}

// --- wire shapes ------------------------------------------------------------

// createOfferRequest is both create bodies. from_gcid is deliberately NOT
// modelled: the outgoing owner comes from the members table.
type createOfferRequest struct {
	ToGCID string `json:"to_gcid"`
	Note   string `json:"note"`
	Reason string `json:"reason"`
}

// offerDTO is what S7a and S7b render. is_live is computed rather than stored:
// no reaper writes the expired status, so a pending row past its TTL is still
// pending in the table and not live in fact.
type offerDTO struct {
	OfferID     string `json:"offer_id"`
	TenantID    string `json:"tenant_id"`
	FromGCID    string `json:"from_gcid"`
	ToGCID      string `json:"to_gcid"`
	InitiatedBy string `json:"initiated_by"`
	Initiator   string `json:"initiator"`
	Reason      string `json:"reason,omitempty"`
	Note        string `json:"note,omitempty"`
	Status      string `json:"status"`
	IsLive      bool   `json:"is_live"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at"`
}

func toOfferDTO(o *ownership.Offer, now time.Time) offerDTO {
	return offerDTO{
		OfferID:     o.OfferID,
		TenantID:    o.TenantID,
		FromGCID:    o.FromGCID,
		ToGCID:      o.ToGCID,
		InitiatedBy: o.InitiatedBy,
		Initiator:   string(o.Initiator),
		Reason:      o.Reason,
		Note:        o.Note,
		Status:      string(o.Status),
		IsLive:      o.IsLive(now),
		CreatedAt:   o.CreatedAt.UTC().Format(time.RFC3339),
		ExpiresAt:   o.ExpiresAt.UTC().Format(time.RFC3339),
	}
}

// --- me-routes --------------------------------------------------------------

// MeCreateOffer serves POST /api/v1/tenants/me/ownership/offers.
func (h *OwnershipHandler) MeCreateOffer(w http.ResponseWriter, r *http.Request) {
	if !ownRequireMethod(w, r, http.MethodPost) {
		return
	}
	tenantID, gcid, ok := ownRequireTenantAndGCID(w, r)
	if !ok {
		return
	}
	var body createOfferRequest
	if !ownDecode(w, r, &body) {
		return
	}

	owner, live, err := h.store.OutgoingOwnerGCID(r.Context(), tenantID)
	if err != nil {
		ownWriteStoreErr(w, err)
		return
	}
	// A stripped owner row is the operator override's case, and saying so is
	// more useful than a bare 403 the caller cannot act on.
	if !live {
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_NO_LIVE_OWNER",
			"this organisation has no live owner, so a platform operator has to assign one")
		return
	}
	if !strings.EqualFold(strings.TrimSpace(gcid), owner) {
		ownWriteError(w, http.StatusForbidden, "OWNERSHIP_OWNER_REQUIRED",
			"only the current owner can hand over ownership of this organisation")
		return
	}

	h.createOffer(w, r, ownership.OfferInput{
		TenantID:    tenantID,
		FromGCID:    owner,
		ToGCID:      body.ToGCID,
		InitiatedBy: gcid,
		Initiator:   ownership.InitiatorOwner,
		Note:        body.Note,
		Now:         time.Now().UTC(),
	})
}

// MeGetOffer serves GET /api/v1/tenants/me/ownership/offer. Both sides of the
// handover read this one row: the caller is the initiator or the nominee, and
// the screen decides which side to render.
func (h *OwnershipHandler) MeGetOffer(w http.ResponseWriter, r *http.Request) {
	if !ownRequireMethod(w, r, http.MethodGet) {
		return
	}
	tenantID, _, ok := ownRequireTenantAndGCID(w, r)
	if !ok {
		return
	}
	offer, err := h.store.PendingOffer(r.Context(), tenantID)
	if err != nil {
		ownWriteStoreErr(w, err)
		return
	}
	if offer == nil {
		// A nil offer with no error is a broken store, not an absent offer.
		// 404 here would claim there is nothing pending, which is a different
		// and unearned statement.
		ownWriteError(w, http.StatusInternalServerError, "internal_error",
			"ownership offer lookup returned nothing and no error")
		return
	}
	v2WriteJSON(w, http.StatusOK, toOfferDTO(offer, time.Now().UTC()))
}

// MeAccept serves POST .../offers/{offerId}/accept.
func (h *OwnershipHandler) MeAccept(w http.ResponseWriter, r *http.Request) {
	h.settle(w, r, h.store.AcceptOffer)
}

// MeDecline serves POST .../offers/{offerId}/decline.
func (h *OwnershipHandler) MeDecline(w http.ResponseWriter, r *http.Request) {
	h.settle(w, r, h.store.DeclineOffer)
}

// MeRevoke serves POST .../offers/{offerId}/revoke.
func (h *OwnershipHandler) MeRevoke(w http.ResponseWriter, r *http.Request) {
	h.settle(w, r, h.store.RevokeOffer)
}

// settle is the shared shape of the three answer verbs. Each carries the OFFER
// ID rather than acting on "whatever is pending", so a screen cannot answer an
// offer that was withdrawn and replaced while it was open.
func (h *OwnershipHandler) settle(
	w http.ResponseWriter, r *http.Request,
	fn func(ctx context.Context, tenantID, offerID, byGCID string, at time.Time) error,
) {
	if !ownRequireMethod(w, r, http.MethodPost) {
		return
	}
	tenantID, gcid, ok := ownRequireTenantAndGCID(w, r)
	if !ok {
		return
	}
	offerID := strings.TrimSpace(r.PathValue("offerId"))
	if offerID == "" {
		ownWriteError(w, http.StatusBadRequest, "invalid_argument", "offer id required")
		return
	}
	if err := fn(r.Context(), tenantID, offerID, gcid, time.Now().UTC()); err != nil {
		ownWriteStoreErr(w, err)
		return
	}
	v2WriteJSON(w, http.StatusOK, map[string]string{"offer_id": offerID, "status": "settled"})
}

// --- the operator override --------------------------------------------------

// OperatorCreateOffer serves POST /api/v1/admin/tenants/{tenantId}/ownership/offers.
//
// The tenant is the PATH, never the header: PLATFORM_OPERATOR holds no
// tenant_memberships row, so it has no header tenant to speak of. This adds no
// RLS-bypass surface: the write still runs inside RunInTenantTx(path tenant)
// with RLS enforcing, which is the ADR-194 D1 pattern.
func (h *OwnershipHandler) OperatorCreateOffer(w http.ResponseWriter, r *http.Request) {
	if !ownRequireMethod(w, r, http.MethodPost) {
		return
	}
	if !callerIsPlatformOperator(r.Header) {
		ownWriteError(w, http.StatusForbidden, errCodePlatformOperatorRequired,
			"platform_operator role required to assign an owner")
		return
	}
	tenantID := strings.TrimSpace(r.PathValue("tenantId"))
	if tenantID == "" {
		ownWriteError(w, http.StatusBadRequest, "invalid_argument", "tenant id required")
		return
	}
	gcid := ownCallerGCID(r)
	if gcid == "" {
		ownWriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller gcid required (X-GCID header)")
		return
	}
	var body createOfferRequest
	if !ownDecode(w, r, &body) {
		return
	}

	// The outgoing owner is whoever is on record, live or already stripped.
	// The override exists for both: an unreachable owner who still holds the
	// row, and one whose row is already gone.
	owner, _, err := h.store.OutgoingOwnerGCID(r.Context(), tenantID)
	if err != nil {
		ownWriteStoreErr(w, err)
		return
	}

	h.createOffer(w, r, ownership.OfferInput{
		TenantID:    tenantID,
		FromGCID:    owner,
		ToGCID:      body.ToGCID,
		InitiatedBy: gcid,
		Initiator:   ownership.InitiatorOperator,
		Reason:      body.Reason,
		Note:        body.Note,
		Now:         time.Now().UTC(),
	})
}

// createOffer is the shared tail of both create paths: build the aggregate,
// surface its validation as 400, persist, and echo the offer.
func (h *OwnershipHandler) createOffer(w http.ResponseWriter, r *http.Request, in ownership.OfferInput) {
	offer, err := ownership.NewOffer(in)
	if err != nil {
		// Aggregate validation is the caller's input, so it is a 400 and it
		// carries the domain's own sentence rather than a generic one.
		ownWriteError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := h.store.CreateOffer(r.Context(), offer); err != nil {
		ownWriteStoreErr(w, err)
		return
	}
	v2WriteJSON(w, http.StatusCreated, toOfferDTO(offer, time.Now().UTC()))
}

// --- helpers ----------------------------------------------------------------

func ownRequireMethod(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Method != want {
		ownWriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return false
	}
	return true
}

// ownCallerGCID reads the mesh-stamped caller identity. Both header spellings
// are accepted, matching me_addons_handler.go.
func ownCallerGCID(r *http.Request) string {
	return strings.TrimSpace(firstNonEmpty(r.Header.Get("X-GCID"), r.Header.Get("gcid")))
}

func ownRequireTenantAndGCID(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID == "" {
		ownWriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller tenant required (X-Tenant-Id header, chora-gateway stamps it after JWT verify)")
		return "", "", false
	}
	gcid := ownCallerGCID(r)
	if gcid == "" {
		ownWriteError(w, http.StatusUnauthorized, "gateway_unauthenticated",
			"caller gcid required (X-GCID header)")
		return "", "", false
	}
	return tenantID, gcid, true
}

// ownDecode reads an OPTIONAL body. The three answer verbs have none, and a
// create with an empty body fails on its missing nominee rather than on a parse
// error, which is the more useful sentence.
func ownDecode(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil || r.ContentLength == 0 {
		return true
	}
	if err := v2Decode(r, v); err != nil {
		ownWriteError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return false
	}
	return true
}

func ownWriteError(w http.ResponseWriter, status int, code, msg string) {
	v2WriteError(w, status, code, msg)
}

// ownWriteStoreErr maps every repository refusal to its own status and code.
//
// There is NO default 4xx arm. An unrecognised error is a 500: a default arm
// that folds an unknown failure into a named 4xx is exactly the defect that
// hid a refusal behind "duplicate" in E1, and it makes an outage read as the
// caller's mistake.
func ownWriteStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pg.ErrOfferNotFound):
		ownWriteError(w, http.StatusNotFound, "OWNERSHIP_OFFER_NOT_FOUND",
			"there is no ownership offer open for this organisation")
	case errors.Is(err, pg.ErrOfferAlreadyPending):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_OFFER_ALREADY_PENDING",
			"an ownership offer is already open, withdraw it before sending another")
	case errors.Is(err, pg.ErrOfferNotPending):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_OFFER_NOT_PENDING",
			"this ownership offer has already been answered")
	case errors.Is(err, pg.ErrOfferExpired):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_OFFER_EXPIRED",
			"this ownership offer has expired")
	case errors.Is(err, pg.ErrNotTheNominee):
		ownWriteError(w, http.StatusForbidden, "OWNERSHIP_NOT_THE_NOMINEE",
			"only the nominated member can answer this offer")
	case errors.Is(err, pg.ErrNotTheInitiator):
		ownWriteError(w, http.StatusForbidden, "OWNERSHIP_NOT_THE_INITIATOR",
			"only the person who sent this offer can withdraw it")
	case errors.Is(err, pg.ErrNomineeNotAMember):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_NOMINEE_NOT_A_MEMBER",
			"that person is not a member of this organisation")
	case errors.Is(err, pg.ErrNomineeSuspended):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_NOMINEE_SUSPENDED",
			"that member is suspended, so they cannot take ownership yet")
	case errors.Is(err, pg.ErrOutgoingOwnerGone):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_OUTGOING_OWNER_GONE",
			"the current owner no longer holds this organisation, so a platform operator has to assign one")
	case errors.Is(err, pg.ErrNoOwnerEver):
		ownWriteError(w, http.StatusConflict, "OWNERSHIP_NO_OWNER_EVER",
			"this organisation has never had an owner, which is a provisioning fault rather than a handover")
	default:
		ownWriteError(w, http.StatusInternalServerError, "internal_error", "ownership operation failed")
	}
}
