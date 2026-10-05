// ClosureSubscriber for the federated account-closure saga (Tier 3 D11).
//
// chora-tenancy receives chora.tenancy.pii.pseudonymise.requested.v1 from
// chora-closure-orchestrator. The subscriber:
//
//  1. Validates the payload (saga_id, gcid, tenant_id required).
//  2. Skips with "skipped_agid_closure" if subject is AGID and the domain
//     does not apply (per ddd-enforcement #10 — agents have no lifecycle).
//  3. Applies the per-domain PII_Closure_Map.yaml fields by calling the
//     ClosureRepository (cross-DB-forbidden — only this service's DB).
//  4. Emits chora.tenancy.account.pseudonymised.v1 with IMDA evidence
//     (ack topic reconciled to the taxonomy-correct name, CHO-1719 gap 4).
//  5. On failure, emits chora.tenancy.pii.pseudonymise.failed.v1 (saga
//     compensation path).
//  6. Idempotent on (saga_id, gcid) — repeated invocation is a no-op.
//
// PII fields per chora-tenancy/config/PII_Closure_Map.yaml:
//   - tenant_memberships: invited_by_display_name + invitation_email_snapshot
//   - stripe_customer_mappings: billing_email
//   - tenant_invitations: invitee_email + invitee_display_name
//   - addon_usage_events: actor_display_name
//
// Tenant_id reference is preserved for orphan rows so the tenant continues
// for OTHER members.
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-tenancy/internal/config"
)

// Topic constants for the federated closure saga, chora-tenancy domain.
const (
	TopicPseudonymiseRequested = "chora.tenancy.pii.pseudonymise.requested.v1"
	TopicPseudonymiseCompleted = "chora.tenancy.account.pseudonymised.v1"
	TopicPseudonymiseFailed    = "chora.tenancy.pii.pseudonymise.failed.v1"
)

// InboxTTL is the dedupe-key retention window for closure_subscriber's
// inbox. 24h covers the broker max redelivery window (7d default) reduced
// for the closure saga's typical end-to-end latency.
const InboxTTL = 24 * time.Hour

// PseudonymiseRequestedPayload mirrors the per-domain fan-out payload from
// the closure orchestrator's PseudonymiseRequested event.
type PseudonymiseRequestedPayload struct {
	SagaID      string `json:"saga_id"`
	Gcid        string `json:"gcid"`
	TenantID    string `json:"tenant_id"`
	SubjectKind string `json:"subject_kind,omitempty"` // "gcid" (default) | "agid"
}

// ClosureRepository is the local-domain port the subscriber uses to apply
// pseudonymisation. Implementations operate ONLY on the service's own DB
// (cross-DB queries forbidden per ddd-enforcement.md HARD RULE).
type ClosureRepository interface {
	// Pseudonymise applies the provided fields to all rows owned by the
	// given GCID inside the tenant. Returns the number of rows touched.
	// Implementation MUST be idempotent.
	Pseudonymise(ctx context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error)

	// IsPseudonymised reports whether the (tenantID, gcid) pair has already
	// been processed (used to support saga replay safety).
	//
	// Widened (W0-F5 error-honesty, CHO-2198) from the original bare
	// `IsPseudonymised(gcid string) bool`: that signature carried no
	// context.Context and had no way to report a backing-store failure, so
	// a durable (pg) implementation would have been forced to swallow a
	// real DB error into a false "not yet pseudonymised" result — the
	// exact swallowed-error trap this saga cannot afford (a guard read
	// that fails OPEN reads as "safe to reprocess" to any future caller).
	// tenantID was also added: pseudonymisation state is tenant-scoped
	// (RLS, like every other table in this domain's DB), matching
	// Pseudonymise's existing (tenantID, gcid) shape. Every caller MUST
	// treat a non-nil error as "unknown" — never coerce it to false.
	IsPseudonymised(ctx context.Context, tenantID, gcid string) (bool, error)
}

// ClosurePublisher is the port the subscriber uses to emit completion /
// failure acks. *Recorder satisfies it (dev/tests); production wires
// CloudClosurePublisher over the shared eventbus.ClosureAckPublisher.
type ClosurePublisher interface {
	PublishWithError(topic string, h Header, payload map[string]interface{}) (PublishedEvent, error)
}

// ClosureSubscriber processes chora.tenancy.pii.pseudonymise.requested.v1
// messages from the federated closure saga.
type ClosureSubscriber struct {
	repo ClosureRepository
	pub  ClosurePublisher
	pii  *config.PIIClosureMap

	inbox idempotent.Store
	ttl   time.Duration

	mu sync.Mutex
}

// NewClosureSubscriber wires the local repo + publisher + per-domain PII map.
func NewClosureSubscriber(repo ClosureRepository, pub ClosurePublisher, pii *config.PIIClosureMap, inbox idempotent.Store) *ClosureSubscriber {
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &ClosureSubscriber{
		repo:  repo,
		pub:   pub,
		pii:   pii,
		inbox: inbox,
		ttl:   InboxTTL,
	}
}

// SubscribedTopic returns the inbound topic this subscriber binds to.
func (s *ClosureSubscriber) SubscribedTopic() string { return TopicPseudonymiseRequested }

// Handle processes one pseudonymise.requested.v1 message.
func (s *ClosureSubscriber) Handle(ctx context.Context, env Header, payload PseudonymiseRequestedPayload) error {
	if s == nil || s.repo == nil || s.pub == nil || s.pii == nil {
		return errors.New("events: ClosureSubscriber not initialised")
	}
	if err := validatePayload(payload); err != nil {
		return err
	}

	// Idempotency guard on (saga_id, gcid).
	key := payload.SagaID + ":" + payload.Gcid
	return s.inbox.Process(ctx, key, s.ttl, func() error {

		// AGID handling: if subject is AGID and domain does not apply, ack a
		// "skipped" status and return — saga proceeds without us.
		if strings.EqualFold(payload.SubjectKind, "agid") && !s.pii.AGIDApplicable {
			return s.publishCompleted(env, payload, 0, "skipped_agid_closure")
		}

		// Apply the per-table tokenisation declared in PII_Closure_Map.yaml.
		count, err := s.repo.Pseudonymise(ctx, payload.TenantID, payload.Gcid, s.pii.FieldsToTokenize)
		if err != nil {
			_ = s.publishFailed(env, payload, err.Error())
			return fmt.Errorf("events: closure pseudonymise failed: %w", err)
		}
		return s.publishCompleted(env, payload, count, "ok")
	})
}

func (s *ClosureSubscriber) publishCompleted(in Header, payload PseudonymiseRequestedPayload, rowsTouched int, status string) error {
	header := Header{
		TenantID:    payload.TenantID,
		GCID:        payload.Gcid,
		Traceparent: in.Traceparent,
	}
	rec, err := s.pub.PublishWithError(TopicPseudonymiseCompleted, header, map[string]interface{}{
		"saga_id":              payload.SagaID,
		"gcid":                 payload.Gcid,
		"tenant_id":            payload.TenantID,
		"domain":               "tenancy",
		"rows_touched":         rowsTouched,
		"status":               status,
		"completed_at":         time.Now().UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	})
	_ = rec
	return err
}

func (s *ClosureSubscriber) publishFailed(in Header, payload PseudonymiseRequestedPayload, errMsg string) error {
	header := Header{
		TenantID:    payload.TenantID,
		GCID:        payload.Gcid,
		Traceparent: in.Traceparent,
	}
	_, err := s.pub.PublishWithError(TopicPseudonymiseFailed, header, map[string]interface{}{
		"saga_id":              payload.SagaID,
		"gcid":                 payload.Gcid,
		"tenant_id":            payload.TenantID,
		"domain":               "tenancy",
		"error":                errMsg,
		"failed_at":            time.Now().UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension": "safety_and_robustness",
		"imda_lifecycle_stage": "runtime",
	})
	return err
}

func validatePayload(p PseudonymiseRequestedPayload) error {
	if strings.TrimSpace(p.SagaID) == "" {
		return errors.New("events: closure payload saga_id required")
	}
	if strings.TrimSpace(p.Gcid) == "" {
		return errors.New("events: closure payload gcid required")
	}
	if strings.TrimSpace(p.TenantID) == "" {
		return errors.New("events: closure payload tenant_id required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// InMemoryClosureRepo — test double for ClosureRepository.
// -----------------------------------------------------------------------------

// InMemoryClosureRepo is an in-memory ClosureRepository for tests + MVP.
//
// Real production wires a chora_tenancy pgx repo that runs the SQL UPDATEs
// per the PII_Closure_Map.yaml strategy.
type InMemoryClosureRepo struct {
	mu          sync.Mutex
	subjects    map[string]bool // gcid → seeded
	pseudonymed map[string]bool // gcid → done
	failNext    bool
}

// NewInMemoryClosureRepo returns a fresh in-memory repo.
func NewInMemoryClosureRepo() *InMemoryClosureRepo {
	return &InMemoryClosureRepo{
		subjects:    make(map[string]bool),
		pseudonymed: make(map[string]bool),
	}
}

// SeedSubject registers a (tenant, gcid) pair that exists locally.
func (r *InMemoryClosureRepo) SeedSubject(tenantID, gcid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects[gcid] = true
}

// closureKey composes the InMemoryClosureRepo dedup-map key. Tenant-scoped
// (fixed alongside the W0-F5 port widening, CHO-2198): the prior version
// keyed `pseudonymed` by gcid ALONE, so a GCID that is a member of two
// tenants would have its second tenant's Pseudonymise call incorrectly
// short-circuited as "already done" the moment the first tenant's call
// completed — a latent cross-tenant dedup collision. The durable pg
// adapter (internal/adapter/pg.ClosureRepository) was written correctly
// scoped from the start; this brings the in-memory fallback in line so the
// two implementations agree on what "already pseudonymised" means.
func closureKey(tenantID, gcid string) string { return tenantID + "|" + gcid }

// Pseudonymise marks the (tenantID, gcid) pair as pseudonymised. Idempotent.
func (r *InMemoryClosureRepo) Pseudonymise(_ context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false
		return 0, errors.New("inmem: forced failure")
	}
	key := closureKey(tenantID, gcid)
	if r.pseudonymed[key] {
		// Idempotent — already done.
		return 0, nil
	}
	r.pseudonymed[key] = true
	// Count = sum of per-table column counts touched.
	rows := 0
	for _, t := range spec {
		rows += len(t.Columns)
	}
	return rows, nil
}

// IsPseudonymised reports whether the (tenantID, gcid) pair has been
// pseudonymised. Signature widened per W0-F5 (CHO-2198) — see the
// ClosureRepository doc comment. The in-memory store cannot itself fail,
// so it always returns a nil error; the pg adapter is where the widened
// error return earns its keep.
func (r *InMemoryClosureRepo) IsPseudonymised(_ context.Context, tenantID, gcid string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pseudonymed[closureKey(tenantID, gcid)], nil
}

// SetFailNext makes the next Pseudonymise call return an error.
func (r *InMemoryClosureRepo) SetFailNext(b bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext = b
}

// -----------------------------------------------------------------------------
// Bootstrap helper for cmd/server/main.go
// -----------------------------------------------------------------------------

// BootstrapClosureSubscriber loads the per-domain PII_Closure_Map.yaml and
// builds a fully-wired ClosureSubscriber against the supplied repo +
// publisher (Recorder). Returns an error if the map fails to load.
func BootstrapClosureSubscriber(piiMapPath string, repo ClosureRepository, pub *Recorder, inbox idempotent.Store) (*ClosureSubscriber, error) {
	pii, err := loadPIIMap(piiMapPath)
	if err != nil {
		return nil, err
	}
	return NewClosureSubscriber(repo, pub, pii, inbox), nil
}

func loadPIIMap(path string) (*config.PIIClosureMap, error) {
	return config.LoadFromFile(path)
}
