// ownership_handler_test.go: the HTTP surface for the two-party ownership
// handover (UX Track U, E3 slice 5, S7-B4 and S7-B5).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/pg"
	"github.com/apollo-chora/chora-tenancy/internal/domain/ownership"
)

const (
	ownTenantID = "11111111-1111-7111-8111-111111111111"
	ownOwner    = "22222222-2222-7222-8222-222222222222"
	ownNominee  = "33333333-3333-7333-8333-333333333333"
	ownOperator = "44444444-4444-7444-8444-444444444444"
	ownOfferID  = "55555555-5555-7555-8555-555555555555"
)

// fakeOwnershipStore records what the handler asked for and plays back canned
// answers. Every refusal it can return is one the repository actually returns.
type fakeOwnershipStore struct {
	outgoingGCID string
	outgoingLive bool
	outgoingErr  error

	created    *ownership.Offer
	createErr  error
	pending    *ownership.Offer
	pendingErr error

	acceptedID, declinedID, revokedID string
	acceptedBy, declinedBy, revokedBy string
	settleTenant                      string
	acceptErr, declineErr, revokeErr  error
}

func (f *fakeOwnershipStore) OutgoingOwnerGCID(_ context.Context, _ string) (string, bool, error) {
	return f.outgoingGCID, f.outgoingLive, f.outgoingErr
}

func (f *fakeOwnershipStore) CreateOffer(_ context.Context, o *ownership.Offer) error {
	f.created = o
	return f.createErr
}

func (f *fakeOwnershipStore) PendingOffer(_ context.Context, _ string) (*ownership.Offer, error) {
	return f.pending, f.pendingErr
}

func (f *fakeOwnershipStore) AcceptOffer(_ context.Context, tenantID, offerID, byGCID string, _ time.Time) error {
	f.settleTenant, f.acceptedID, f.acceptedBy = tenantID, offerID, byGCID
	return f.acceptErr
}

func (f *fakeOwnershipStore) DeclineOffer(_ context.Context, tenantID, offerID, byGCID string, _ time.Time) error {
	f.settleTenant, f.declinedID, f.declinedBy = tenantID, offerID, byGCID
	return f.declineErr
}

func (f *fakeOwnershipStore) RevokeOffer(_ context.Context, tenantID, offerID, byGCID string, _ time.Time) error {
	f.settleTenant, f.revokedID, f.revokedBy = tenantID, offerID, byGCID
	return f.revokeErr
}

func ownMux(t *testing.T, store OwnershipStore) http.Handler {
	t.Helper()
	h, err := NewOwnershipHandler(store)
	if err != nil {
		t.Fatalf("NewOwnershipHandler: %v", err)
	}
	mux := http.NewServeMux()
	RegisterOwnershipRoutes(mux, h)
	return mux
}

func ownReq(method, path, body string, hdr map[string]string) *http.Request {
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, path, rd)
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	return r
}

func ownerHeaders() map[string]string {
	return map[string]string{"X-Tenant-Id": ownTenantID, "X-GCID": ownOwner}
}

func nomineeHeaders() map[string]string {
	return map[string]string{"X-Tenant-Id": ownTenantID, "X-GCID": ownNominee}
}

func operatorHeaders() map[string]string {
	return map[string]string{
		"X-GCID":            ownOperator,
		"x-mesh-user-roles": "learner,Platform_Operator",
	}
}

func ownCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope %q: %v", rec.Body.String(), err)
	}
	return env.Error.Code
}

func livePendingOffer() *ownership.Offer {
	o, _ := ownership.NewOffer(ownership.OfferInput{
		TenantID: ownTenantID, FromGCID: ownOwner, ToGCID: ownNominee,
		InitiatedBy: ownOwner, Initiator: ownership.InitiatorOwner,
		Note: "handing over", Now: time.Now().UTC(),
	})
	o.OfferID = ownOfferID
	return o
}

// --- owner-initiated create -------------------------------------------------

func TestOwnership_MeCreate_201AndResolvesTheOutgoingOwnerServerSide(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","note":"you have earned it"}`, ownerHeaders()))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created == nil {
		t.Fatal("no offer created")
	}
	if store.created.FromGCID != ownOwner {
		t.Errorf("from_gcid = %q, want the server-resolved owner %q", store.created.FromGCID, ownOwner)
	}
	if store.created.ToGCID != ownNominee {
		t.Errorf("to_gcid = %q, want %q", store.created.ToGCID, ownNominee)
	}
	if store.created.Initiator != ownership.InitiatorOwner {
		t.Errorf("initiator = %q, want owner", store.created.Initiator)
	}
	if store.created.InitiatedBy != ownOwner {
		t.Errorf("initiated_by = %q, want the caller", store.created.InitiatedBy)
	}
}

// A from_gcid in the body is IGNORED. A GCID alone never proves ownership, so
// honouring it would let any member name themselves as the outgoing owner.
func TestOwnership_MeCreate_IgnoresAFromGCIDInTheBody(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"from_gcid":"`+ownNominee+`","to_gcid":"`+ownNominee+`"}`, ownerHeaders()))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created.FromGCID != ownOwner {
		t.Errorf("from_gcid = %q, want the server-resolved owner", store.created.FromGCID)
	}
}

// Only the current owner may hand over. An admin is refused by name, and no
// offer is written.
func TestOwnership_MeCreate_403WhenTheCallerIsNotTheOwner(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, nomineeHeaders()))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_OWNER_REQUIRED" {
		t.Errorf("code = %q, want OWNERSHIP_OWNER_REQUIRED", got)
	}
	if store.created != nil {
		t.Error("an offer was written by a caller who is not the owner")
	}
}

// The owner-initiated path needs a LIVE owner row. If ownership has already
// been stripped, the remedy is the operator override, and saying so is more
// useful than a generic 403.
func TestOwnership_MeCreate_409WhenTheOwnerRowIsAlreadyStripped(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, ownerHeaders()))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_NO_LIVE_OWNER" {
		t.Errorf("code = %q, want OWNERSHIP_NO_LIVE_OWNER", got)
	}
	if store.created != nil {
		t.Error("an offer was written with no live owner row")
	}
}

func TestOwnership_MeCreate_400OnAMissingNominee(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"note":"who?"}`, ownerHeaders()))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
}

// Nominating yourself is refused by the aggregate; the handler must surface it
// as a 400, not a 500.
func TestOwnership_MeCreate_400OnSelfNomination(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownOwner+`"}`, ownerHeaders()))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestOwnership_MeCreate_409WhenAnOfferIsAlreadyPending(t *testing.T) {
	store := &fakeOwnershipStore{
		outgoingGCID: ownOwner, outgoingLive: true,
		createErr: pg.ErrOfferAlreadyPending,
	}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, ownerHeaders()))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_OFFER_ALREADY_PENDING" {
		t.Errorf("code = %q, want OWNERSHIP_OFFER_ALREADY_PENDING", got)
	}
}

func TestOwnership_MeCreate_401WithoutATenantHeader(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, map[string]string{"X-GCID": ownOwner}))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %s)", rec.Code, rec.Body.String())
	}
}

// --- the read S7a and S7b share ---------------------------------------------

func TestOwnership_MeGet_200CarriesBothSidesAndLiveness(t *testing.T) {
	store := &fakeOwnershipStore{pending: livePendingOffer()}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/tenants/me/ownership/offer", "", ownerHeaders()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var dto struct {
		OfferID   string `json:"offer_id"`
		FromGCID  string `json:"from_gcid"`
		ToGCID    string `json:"to_gcid"`
		Initiator string `json:"initiator"`
		Status    string `json:"status"`
		IsLive    bool   `json:"is_live"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dto.OfferID != ownOfferID || dto.FromGCID != ownOwner || dto.ToGCID != ownNominee {
		t.Fatalf("dto = %+v", dto)
	}
	if !dto.IsLive {
		t.Error("is_live = false for an offer inside its TTL")
	}
	if dto.ExpiresAt == "" {
		t.Error("expires_at empty, so the screen cannot say when the offer lapses")
	}
}

// A lapsed row is served, flagged not live. Hiding it would tell the screen
// there is nothing here about a row that exists and still blocks a new offer.
func TestOwnership_MeGet_200ReportsALapsedOfferAsNotLive(t *testing.T) {
	o := livePendingOffer()
	o.ExpiresAt = time.Now().UTC().Add(-time.Hour)
	store := &fakeOwnershipStore{pending: o}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/tenants/me/ownership/offer", "", ownerHeaders()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var dto struct {
		IsLive bool `json:"is_live"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dto.IsLive {
		t.Error("is_live = true for an offer past its TTL")
	}
}

func TestOwnership_MeGet_404WhenNoOfferIsPending(t *testing.T) {
	store := &fakeOwnershipStore{pendingErr: pg.ErrOfferNotFound}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/tenants/me/ownership/offer", "", ownerHeaders()))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_OFFER_NOT_FOUND" {
		t.Errorf("code = %q, want OWNERSHIP_OFFER_NOT_FOUND", got)
	}
}

// --- accept, decline, revoke ------------------------------------------------

func TestOwnership_MeAccept_200AndPassesTheCallerAsTheActor(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/accept", "", nomineeHeaders()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if store.acceptedID != ownOfferID {
		t.Errorf("offer id = %q, want %q", store.acceptedID, ownOfferID)
	}
	if store.acceptedBy != ownNominee {
		t.Errorf("actor = %q, want the caller %q", store.acceptedBy, ownNominee)
	}
	if store.settleTenant != ownTenantID {
		t.Errorf("tenant = %q, want the header tenant %q", store.settleTenant, ownTenantID)
	}
}

func TestOwnership_MeAccept_403WhenTheCallerIsNotTheNominee(t *testing.T) {
	store := &fakeOwnershipStore{acceptErr: pg.ErrNotTheNominee}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/accept", "", ownerHeaders()))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_NOT_THE_NOMINEE" {
		t.Errorf("code = %q, want OWNERSHIP_NOT_THE_NOMINEE", got)
	}
}

// Every repository refusal has to reach the wire as its own 4xx code. A default
// arm folding them together is the defect that hid a refusal in E1 item 4b.
func TestOwnership_MeAccept_MapsEveryRepositoryRefusal(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{pg.ErrOfferNotFound, http.StatusNotFound, "OWNERSHIP_OFFER_NOT_FOUND"},
		{pg.ErrOfferNotPending, http.StatusConflict, "OWNERSHIP_OFFER_NOT_PENDING"},
		{pg.ErrOfferExpired, http.StatusConflict, "OWNERSHIP_OFFER_EXPIRED"},
		{pg.ErrNotTheNominee, http.StatusForbidden, "OWNERSHIP_NOT_THE_NOMINEE"},
		{pg.ErrNomineeNotAMember, http.StatusConflict, "OWNERSHIP_NOMINEE_NOT_A_MEMBER"},
		{pg.ErrNomineeSuspended, http.StatusConflict, "OWNERSHIP_NOMINEE_SUSPENDED"},
		{pg.ErrOutgoingOwnerGone, http.StatusConflict, "OWNERSHIP_OUTGOING_OWNER_GONE"},
	} {
		store := &fakeOwnershipStore{acceptErr: tc.err}
		rec := httptest.NewRecorder()
		ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
			"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/accept", "", nomineeHeaders()))

		if rec.Code != tc.status {
			t.Errorf("%v: status = %d, want %d (body %s)", tc.err, rec.Code, tc.status, rec.Body.String())
			continue
		}
		if got := ownCode(t, rec); got != tc.code {
			t.Errorf("%v: code = %q, want %q", tc.err, got, tc.code)
		}
	}
}

// An unrecognised repository error is a 500, never a 4xx. Dressing an unknown
// failure as the caller's fault is how a real outage reads as user error.
func TestOwnership_MeAccept_500OnAnUnknownError(t *testing.T) {
	store := &fakeOwnershipStore{acceptErr: errors.New("connection reset")}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/accept", "", nomineeHeaders()))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestOwnership_MeDecline_200(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/decline", "", nomineeHeaders()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if store.declinedID != ownOfferID || store.declinedBy != ownNominee {
		t.Errorf("declined %q by %q", store.declinedID, store.declinedBy)
	}
}

func TestOwnership_MeRevoke_200(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/revoke", "", ownerHeaders()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if store.revokedID != ownOfferID || store.revokedBy != ownOwner {
		t.Errorf("revoked %q by %q", store.revokedID, store.revokedBy)
	}
}

func TestOwnership_MeRevoke_403WhenTheCallerIsNotTheInitiator(t *testing.T) {
	store := &fakeOwnershipStore{revokeErr: pg.ErrNotTheInitiator}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/revoke", "", nomineeHeaders()))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_NOT_THE_INITIATOR" {
		t.Errorf("code = %q, want OWNERSHIP_NOT_THE_INITIATOR", got)
	}
}

func TestOwnership_MeVerbs_405OnGet(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/accept", "", nomineeHeaders()))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 (body %s)", rec.Code, rec.Body.String())
	}
}

// --- the operator override (spec 13.4) --------------------------------------

func TestOwnership_OperatorCreate_201WithAReason(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"the owner left the company"}`, operatorHeaders()))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created == nil {
		t.Fatal("no offer created")
	}
	if store.created.Initiator != ownership.InitiatorOperator {
		t.Errorf("initiator = %q, want operator", store.created.Initiator)
	}
	if store.created.InitiatedBy != ownOperator {
		t.Errorf("initiated_by = %q, want the operator %q", store.created.InitiatedBy, ownOperator)
	}
	if store.created.TenantID != ownTenantID {
		t.Errorf("tenant = %q, want the PATH tenant %q", store.created.TenantID, ownTenantID)
	}
	if store.created.Reason != "the owner left the company" {
		t.Errorf("reason = %q", store.created.Reason)
	}
	// The operator is tenant-less by design, so the outgoing owner is the one
	// on record, not the caller.
	if store.created.FromGCID != ownOwner {
		t.Errorf("from_gcid = %q, want the recorded owner %q", store.created.FromGCID, ownOwner)
	}
}

// The override works where the owner-initiated path cannot: a stripped owner
// row is the case it exists for, so it must NOT be refused here.
func TestOwnership_OperatorCreate_201EvenWithALiveOwner(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"the owner is unreachable"}`, operatorHeaders()))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestOwnership_OperatorCreate_403WithoutThePlatformOperatorRole(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"because"}`,
		map[string]string{"X-GCID": ownOperator, "x-mesh-user-roles": "tenant_admin"}))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "AUTH_PLATFORM_OPERATOR_REQUIRED" {
		t.Errorf("code = %q, want AUTH_PLATFORM_OPERATOR_REQUIRED", got)
	}
	if store.created != nil {
		t.Error("an offer was written by a caller who is not a platform operator")
	}
}

// A reason is mandatory: a handover nobody can explain later is not auditable.
func TestOwnership_OperatorCreate_400WithoutAReason(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"   "}`, operatorHeaders()))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created != nil {
		t.Error("an offer was written with no reason")
	}
}

// A tenant with no owner row at all is a bootstrap defect, not a handover case.
func TestOwnership_OperatorCreate_409WhenTheTenantNeverHadAnOwner(t *testing.T) {
	store := &fakeOwnershipStore{outgoingErr: pg.ErrNoOwnerEver}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"nobody owns this"}`, operatorHeaders()))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "OWNERSHIP_NO_OWNER_EVER" {
		t.Errorf("code = %q, want OWNERSHIP_NO_OWNER_EVER", got)
	}
}

// The override does NOT read X-Tenant-Id: the operator is tenant-less, so a
// header tenant would either be absent or belong to somewhere else entirely.
func TestOwnership_OperatorCreate_UsesThePathTenantNotTheHeader(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	hdr := operatorHeaders()
	hdr["X-Tenant-Id"] = "99999999-9999-7999-8999-999999999999"
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"the owner left"}`, hdr))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created.TenantID != ownTenantID {
		t.Errorf("tenant = %q, want the path tenant %q", store.created.TenantID, ownTenantID)
	}
}

// --- routing precedence against the real root mux shape ---------------------

// The service root mux routes /api/v1/admin/ wholesale to the v2 handler and /
// to the legacy handler. The override mounts a PARAMETRIC pattern inside that
// subtree, which Go's mux resolves by "most specific wins" only because the
// pattern matches a strict subset. This fence reproduces the real shape and
// pins both directions: my routes reach me, and nothing else moved.
//
// It also proves registration does not PANIC. Go's ServeMux panics on
// conflicting patterns at registration time, which would be a boot crash rather
// than a test failure anywhere else.
func TestOwnership_RoutingDoesNotDisturbTheSiblingSubtrees(t *testing.T) {
	store := &fakeOwnershipStore{
		outgoingGCID: ownOwner, outgoingLive: true, pending: livePendingOffer(),
	}
	h, err := NewOwnershipHandler(store)
	if err != nil {
		t.Fatalf("NewOwnershipHandler: %v", err)
	}

	mux := http.NewServeMux()
	sentinel := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Reached", name)
			w.WriteHeader(http.StatusOK)
		}
	}
	// The root mux shape from cmd/server/main.go, abbreviated to the parts
	// that could collide.
	mux.Handle("/v2/", sentinel("v2"))
	mux.Handle("/api/v1/admin/", sentinel("v2-admin"))
	mux.Handle("/api/v1/tenants/me", sentinel("me-tenant"))
	mux.Handle("/api/v1/tenants/me/addons", sentinel("me-addons"))
	mux.Handle("/api/v1/tenants/me/branding", sentinel("me-branding"))
	mux.Handle("/api/v1/tenants/bootstrap", sentinel("bootstrap"))
	mux.Handle("/", sentinel("legacy"))
	RegisterOwnershipRoutes(mux, h)

	for _, tc := range []struct{ method, path, want string }{
		// Mine.
		{http.MethodGet, "/api/v1/tenants/me/ownership/offer", "ownership"},
		{http.MethodPost, "/api/v1/tenants/me/ownership/offers", "ownership"},
		{http.MethodPost, "/api/v1/tenants/me/ownership/offers/" + ownOfferID + "/accept", "ownership"},
		{http.MethodPost, "/api/v1/tenants/me/ownership/offers/" + ownOfferID + "/decline", "ownership"},
		{http.MethodPost, "/api/v1/tenants/me/ownership/offers/" + ownOfferID + "/revoke", "ownership"},
		{http.MethodPost, "/api/v1/admin/tenants/" + ownTenantID + "/ownership/offers", "ownership"},
		// Not mine, and they must not have moved.
		{http.MethodGet, "/api/v1/admin/tenants/me/addons", "v2-admin"},
		{http.MethodGet, "/api/v1/admin/tenants/me/mana-pool", "v2-admin"},
		{http.MethodGet, "/api/v1/admin/marketplace/addons", "v2-admin"},
		{http.MethodGet, "/api/v1/admin/tenants/" + ownTenantID + "/ownership", "v2-admin"},
		{http.MethodGet, "/api/v1/tenants/me", "me-tenant"},
		{http.MethodGet, "/api/v1/tenants/me/addons", "me-addons"},
		{http.MethodPatch, "/api/v1/tenants/me/branding", "me-branding"},
		{http.MethodPost, "/api/v1/tenants/bootstrap", "bootstrap"},
		{http.MethodGet, "/v2/tenants", "v2"},
		{http.MethodGet, "/api/tenants/me", "legacy"},
	} {
		rec := httptest.NewRecorder()
		hdr := ownerHeaders()
		hdr["x-mesh-user-roles"] = "Platform_Operator"
		mux.ServeHTTP(rec, ownReq(tc.method, tc.path, `{"to_gcid":"`+ownNominee+`","reason":"r"}`, hdr))

		got := rec.Header().Get("X-Reached")
		if tc.want == "ownership" {
			if got != "" {
				t.Errorf("%s %s reached the %q sentinel, want the ownership handler", tc.method, tc.path, got)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s %s reached %q, want %q (body %s)", tc.method, tc.path, got, tc.want, rec.Body.String())
		}
	}
}

// --- the remaining refusals -------------------------------------------------

func TestOwnership_MeCreate_401WithoutAGCIDHeader(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, map[string]string{"X-Tenant-Id": ownTenantID}))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created != nil {
		t.Error("an offer was written for an unidentified caller")
	}
}

func TestOwnership_OperatorCreate_401WithoutAGCIDHeader(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: false}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers",
		`{"to_gcid":"`+ownNominee+`","reason":"r"}`,
		map[string]string{"x-mesh-user-roles": "platform_operator"}))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created != nil {
		t.Error("an offer was written for an unidentified operator")
	}
}

func TestOwnership_MeCreate_400OnAMalformedBody(t *testing.T) {
	store := &fakeOwnershipStore{outgoingGCID: ownOwner, outgoingLive: true}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":`, ownerHeaders()))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	if got := ownCode(t, rec); got != "invalid_body" {
		t.Errorf("code = %q, want invalid_body", got)
	}
}

// A store that answers with neither an offer nor an error is broken, and the
// route says so. A 404 there would claim there is nothing pending, which is a
// different statement and one this handler has not earned.
func TestOwnership_MeGet_500WhenTheStoreReturnsNothingAndNoError(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/tenants/me/ownership/offer", "", ownerHeaders()))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestOwnership_MeCreate_500WhenTheOwnerLookupFails(t *testing.T) {
	store := &fakeOwnershipStore{outgoingErr: errors.New("connection reset")}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost, "/api/v1/tenants/me/ownership/offers",
		`{"to_gcid":"`+ownNominee+`"}`, ownerHeaders()))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %s)", rec.Code, rec.Body.String())
	}
	if store.created != nil {
		t.Error("an offer was written although the owner lookup failed")
	}
}

func TestOwnership_MeSettle_401WithoutHeaders(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodPost,
		"/api/v1/tenants/me/ownership/offers/"+ownOfferID+"/decline", "", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %s)", rec.Code, rec.Body.String())
	}
	if store.declinedID != "" {
		t.Error("the store was called for an unidentified caller")
	}
}

func TestOwnership_OperatorCreate_405OnGet(t *testing.T) {
	store := &fakeOwnershipStore{}
	rec := httptest.NewRecorder()
	ownMux(t, store).ServeHTTP(rec, ownReq(http.MethodGet,
		"/api/v1/admin/tenants/"+ownTenantID+"/ownership/offers", "", operatorHeaders()))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestOwnership_NewOwnershipHandler_RefusesANilStore(t *testing.T) {
	if _, err := NewOwnershipHandler(nil); err == nil {
		t.Fatal("want NewOwnershipHandler to refuse a nil store")
	}
}
