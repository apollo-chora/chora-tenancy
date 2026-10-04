// Package main wires the chora-tenancy Go service for Cloud Run.
//
// Per CLAUDE.md §6 the production stack is OTLP-everywhere direct to Cloud
// Trace via the Telemetry API + Cloud Logging.
//
// Mounts both the legacy v1 server (paths /api/* + /healthz + /readyz) and
// the new v2 + BE-T-ADD-1 admin server (paths /v2/* + /api/v1/admin/*) on
// the same root mux.
//
// Production wiring (Phyllis Wave-B + M12.3 W2b):
//
//   - When CHORA_DB_DSN is set, a pgxpool.Pool
//     is bootstrapped against chora_tenancy. The pgx-backed
//     tenant.Repository is wired in alongside the legacy in-memory repos
//     (the legacy ones still drive the v1 server and parts of v2 that
//     have not been ported). When unset, falls back to in-memory.
//   - When PUBSUB_PROJECT_ID is set, the Pub/Sub client is
//     bootstrapped + wired as the Bus for the D6.2 outbox dispatcher.
//     When unset, an in-memory bus is used so the service stays runnable
//     in dev.
//   - D6.2 producer-side outbox (M12.3 W2b): the OutboxPublisher writes
//     to outbox_events; a background Dispatcher goroutine drains pending
//     rows to the Pub/Sub bus with retry + DLQ. When CHORA_OUTBOX_DSN is
//     unset, falls back to the in-memory store (NOT durable across
//     restart; dev-only).
//   - Consumer-side inbox: every Pub/Sub subscriber wraps its handler in
//     idempotent.Store.Process(...). When CHORA_OUTBOX_DSN is set, the
//     PostgresStore-backed inbox is wired against the chora_tenancy
//     idempotency_keys table; otherwise an in-memory store is used.
//   - OTLP remains optional and is configured through standard OTLP environment variables.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/tenancy/v1"

	"github.com/apollo-chora/chora-common/durabilityguard"
	grpcconn "github.com/apollo-chora/chora-common/grpcconn"
	"github.com/apollo-chora/chora-common/idempotent"
	cgcobservability "github.com/apollo-chora/chora-common/observability"
	// pgx stdlib driver — registered for sql.Open("pgx", dsn) used by
	// the per-domain outbox PostgresStore in bootstrap.go.
	_ "github.com/jackc/pgx/v5/stdlib"

	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"

	tnevents "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/events"
	familiareggsweeper "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiar_egg_sweeper"
	familiareggstripe "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/familiareggstripe"
	tenancygrpc "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/grpc"
	httpapi "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/manapool"
	tenancyoutbox "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/outbox"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/payments"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
	stripestub "github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/stripe"
	tnconfig "github.com/5007-Capstone/chora/services/chora-tenancy/internal/config"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/bootstrap"
	domain "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenancy"
	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant"
	// Aliased: the local *pgxpool.Pool variable in main() is named `pool`,
	// which would shadow the package name.
	manapooldomain "github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/tenant_mana_pool"
)

const (
	serviceName    = "chora-tenancy"
	serviceVersion = "0.3.0"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ----------------------------------------------------------------------
	// Optional OTLP wiring.
	//
	// Per C(a).S1 path (b) — tracker #151 — OTLP init runs in its own
	// goroutine with its own (env-tunable, default 15s) deadline + fail-
	// soft semantics. Timeout / init-error degrade to a no-op shutdown,
	// so the rest of bootstrap (pgx pool, Pub/Sub clients) gets the FULL
	// CHORA_BOOTSTRAP_TIMEOUT_SECONDS budget. Previously a slow Cloud
	// Trace TLS handshake could swallow the shared budget and crash-
	// loop the pod under PgBouncer 4-container cold-start.
	otlpHandle := cgcobservability.InitOTLPAsync(ctx, serviceName, serviceVersion)
	defer func() {
		// handle.Wait(0) blocks until init settles — usually a no-op by
		// shutdown time because pgx-pool init below already gave OTLP
		// best-effort wall-clock to land.
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		res := otlpHandle.WaitContext(shutdownCtx)
		if err := res.Shutdown(shutdownCtx); err != nil {
			log.Printf("tenancy: trace shutdown error: %v", err)
		}
	}()

	// ----------------------------------------------------------------------
	// Repository wiring — pgx-backed when env is configured; in-mem fallback.
	// pgx pool init now races OTLP for nothing — they have independent
	// deadlines per C(a).S1 path (b).
	// ----------------------------------------------------------------------
	pool, poolShutdown := bootstrapDBPool(ctx)
	if poolShutdown != nil {
		defer poolShutdown()
	}
	var pgTenants tenant.Repository
	if pool != nil {
		pgTenants = pg.NewTenantRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("tenancy: pgx TenantRepository wired (pool=chora_tenancy)")
	}
	// pgTenants is consumed by the Setup Wizard /me/branding handler
	// below (CHO-1655). The v2 admin handlers will be ported to pgTenants
	// in M14+.

	// H+ Setup-Tenant bootstrap repo (CHO-1628 Phase 1). The handler is
	// constructed later (after v2Deps is wired) so it can take the
	// SubscriptionRegistry as its AddOnSubscriber dep for CHO-1751
	// Phase 1 (auto-Subscribe `base` after the 3-row persist).
	//
	// Wired only when the pgx pool is available — the bootstrap saga
	// writes three rows atomically and there is no meaningful in-memory
	// fallback for a self-onboard flow.
	var bootstrapRepo bootstrap.Repository
	if pool != nil {
		bootstrapRepo = pg.NewBootstrapRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("tenancy: H+ Setup-Tenant bootstrap repo wired (CHO-1628)")
	} else {
		log.Printf("tenancy: H+ Setup-Tenant bootstrap repo DISABLED (no pgx pool)")
	}

	// Setup Wizard Phase A — PATCH /api/v1/tenants/me/branding (CHO-1655).
	// Wired against the persistent pg.TenantRepository so wizard updates
	// survive pod restarts. The v2 admin PATCH on /v2/tenants/{id} still
	// uses the in-memory Registry pending M14+ migration.
	var meBrandingHandler *httpapi.MeBrandingHandler
	if pgTenants != nil {
		meBrandingHandler = httpapi.NewMeBrandingHandler(pgTenants)
		log.Printf("tenancy: Setup Wizard /me/branding handler wired (CHO-1655)")
	} else {
		log.Printf("tenancy: Setup Wizard /me/branding handler DISABLED (no pgx pool)")
	}

	// Setup Wizard step 4 — PATCH /api/v1/tenants/me/finish-setup
	// (CHO-1682). Marks `wizard_completed_at` set-once on the caller's
	// tenant. The BFF aggregator (`POST /api/v1/tenants/setup`) calls
	// this immediately after identity's idp-providers Upsert succeeds.
	var meFinishSetupHandler *httpapi.MeFinishSetupHandler
	if pgTenants != nil {
		meFinishSetupHandler = httpapi.NewMeFinishSetupHandler(pgTenants)
		log.Printf("tenancy: Setup Wizard /me/finish-setup handler wired (CHO-1682)")
	} else {
		log.Printf("tenancy: Setup Wizard /me/finish-setup handler DISABLED (no pgx pool)")
	}

	// Setup Wizard re-entry hydration — GET /api/v1/tenants/me (CHO-1692).
	// Returns the pg-backed tenant doc (branding + wizard_completed_at).
	// Distinct from the legacy /api/tenants/{id} which reads from an
	// in-memory registry that doesn't reflect post-PATCH writes.
	var meTenantHandler *httpapi.MeTenantHandler
	if pgTenants != nil {
		meTenantHandler = httpapi.NewMeTenantHandler(pgTenants)
		log.Printf("tenancy: Setup Wizard /api/v1/tenants/me handler wired (CHO-1692)")
	} else {
		log.Printf("tenancy: Setup Wizard /api/v1/tenants/me handler DISABLED (no pgx pool)")
	}

	// Setup Wizard Phase B — POST /api/v1/tenants/me/addons (CHO-1664).
	// Backed by the same in-memory SubscriptionRegistry the v2 admin
	// endpoints use, so wizard-created pending rows are visible to
	// downstream lists. Persistent pg-backed SubscriptionRepository is
	// pending M14+ migration alongside the rest of the v2 surface.
	var meAddOnsHandler *httpapi.MeAddOnsHandler
	// Handler is declared here but wired AFTER v2Deps is constructed
	// further down (v2Deps owns the canonical Subscriptions registry).

	// A7 (docs/m13/handoff-fe-to-be-service-2026-05-14.md §A7) — legacy
	// /api/* persistence ports. The A6-wired gateway routes
	// `GET /api/tenants/{id}` + `GET /api/feature-flags`
	// (→ `GET /api/tenants/{id}/entitlements`) reached this service but
	// returned 404 / empty because the legacy handlers were wired to
	// in-memory stores that start EMPTY on every pod — while the Phyllis
	// demo tenant rows + entitlements live in the chora_tenancy Postgres
	// tables. When the pgx pool is available the legacy handlers now read
	// the real tables (mirrors the D0.4 chora-delivery rewire); the
	// in-memory adapters stay as the local-dev / unit-test fallback.
	var (
		legacyTenants      httpapi.TenantStore
		legacyAddOnCatalog httpapi.AddOnStore
		legacyEntitlements httpapi.EntitlementStore
	)
	if pool != nil {
		legacyQuerier := pg.NewPgxPoolQuerier(pool)
		legacyTenants = pg.NewLegacyTenantStore(legacyQuerier)
		legacyAddOnCatalog = pg.NewLegacyAddOnStore(legacyQuerier)
		// add_on_subscriptions is RLS-protected — the entitlement store
		// takes the TxQuerier so every read/write runs inside a
		// SET LOCAL chora.tenant_id transaction (chora_tenancy_app_rw is
		// NOBYPASSRLS, migration 0013).
		legacyEntitlements = pg.NewLegacyEntitlementStore(legacyQuerier)
		log.Printf("tenancy: pgx legacy /api/* stores wired (Tenants + AddOnCatalog + Entitlements → chora_tenancy)")
	} else {
		legacyTenants = inmem.NewTenantRepo()
		legacyAddOnCatalog = domain.NewAddOnCatalog()
		legacyEntitlements = domain.NewEntitlementRegistry()
		log.Printf("tenancy: in-memory legacy /api/* stores wired (CHORA_DB_DSN unset; NOT durable)")
	}

	// ----------------------------------------------------------------------
	// Pub/Sub bus — Cloud client when env is configured; in-memory bus
	// otherwise. The bus is wired to the D6.2 outbox dispatcher below.
	// ----------------------------------------------------------------------
	pubsubClient, pubsubShutdown := bootstrapPubSubClient(ctx)
	if pubsubShutdown != nil {
		defer pubsubShutdown()
	}
	var bus tenancyoutbox.Bus = cgcpubsub.NewInMemoryBus()
	if pubsubClient != nil {
		bus = cgcpubsub.NewCloudPublisher(pubsubClient)
		log.Printf("tenancy: Pub/Sub client wired (project=%s)", os.Getenv("PUBSUB_PROJECT_ID"))
	}

	// ----------------------------------------------------------------------
	// Federated closure-saga subscriber (CHO-1719 / Tier 3 D11): consumes
	// chora.tenancy.pii.pseudonymise.requested.v1, applies the per-domain
	// PII_Closure_Map.yaml duty, and acks on
	// chora.tenancy.account.pseudonymised.v1.
	//
	// Repo seam (CHO-2198, W0-F1 durability + W0-F5 error-honesty): pg on a
	// healthy pool (durable ack/dedup — migration 0032,
	// closure_pseudonymisation_state), in-memory ONLY when the pool is
	// absent, mirroring the chora-payments else-branch shape. Before this
	// fix the repo was UNGATED — nested only under `pubsubClient != nil`,
	// never gated on pool health — so the ack/dedup state was lost on
	// every pod restart even with a healthy chora_tenancy pool (see
	// docs/references/w0-f1-inmemory-inventory.md §6 item 4). Real
	// per-table pg tokenisation (actually redacting tenant_memberships /
	// stripe_customer_mappings / ... columns) remains separate, deeper
	// M12+ debt — this fix is durability of the ack/dedup SIGNAL only, not
	// the redaction itself. Pull subscription is provisioned by infra
	// (closure deploy runbook); override the name via env.
	//
	// closureRepo is hoisted to function scope so the ADR-236 D5 durability
	// guard (below, before the HTTP mux is built) can classify it alongside the
	// other repos. It stays nil when pubsubClient is absent (or the PII map
	// fails to load) — the guard reports nil as UNKNOWN, never a violation.
	// Mirrors the chora-delivery D5 closureRepo hoist.
	// ----------------------------------------------------------------------
	var closureRepo tnevents.ClosureRepository
	if pubsubClient != nil {
		piiPath := os.Getenv("CHORA_PII_CLOSURE_MAP_PATH")
		if piiPath == "" {
			piiPath = "config/PII_Closure_Map.yaml"
		}
		closureAckPub := tnevents.NewCloudClosurePublisher(
			cgcpubsub.NewClosureAckPublisher(
				cgcpubsub.NewCloudPublisher(pubsubClient),
				os.Getenv("PUBSUB_PROJECT_ID"),
				"chora-tenancy",
			),
		)
		if closurePII, err := tnconfig.LoadFromFile(piiPath); err != nil {
			log.Printf("tenancy: closure subscriber DISABLED (PII map load: %v)", err)
		} else {
			if pool != nil {
				closureRepo = pg.NewClosureRepository(pg.NewPgxPoolQuerier(pool))
				log.Printf("tenancy: pg ClosureRepository wired (table=closure_pseudonymisation_state)")
			} else {
				closureRepo = tnevents.NewInMemoryClosureRepo()
				log.Printf("tenancy: CHORA_DB_DSN unset — closure repo uses in-memory store (NOT durable across restart)")
			}
			closureSub := tnevents.NewClosureSubscriber(closureRepo, closureAckPub, closurePII, nil)
			closureSubName := os.Getenv("CHORA_CLOSURE_SUBSCRIPTION")
			if closureSubName == "" {
				closureSubName = "chora-tenancy.closure-pseudonymise"
			}
			go func() {
				log.Printf("tenancy: closure subscriber binding %s -> %s", closureSubName, tnevents.TopicPseudonymiseRequested)
				if err := cgcpubsub.NewCloudSubscriber(pubsubClient).Subscribe(ctx, closureSubName, tnevents.ClosurePullHandler(closureSub)); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("tenancy: closure subscriber exited: %v", err)
				}
			}()
		}
	}

	// ----------------------------------------------------------------------
	// D6.2 producer-side outbox wiring (M12.3 W2b).
	//
	// Per `feedback_d6_resilience_first_class` + `agentic-resilience-d6`
	// skill Pillar 2. The OutboxPublisher writes to outbox_events; the
	// Dispatcher drains to the Pub/Sub bus on a background goroutine.
	//
	// When CHORA_OUTBOX_DSN is unset we fall back to InMemoryStore so the
	// service stays runnable in dev — production wiring ALWAYS sets the
	// DSN against chora_tenancy.
	// ----------------------------------------------------------------------
	var outboxStore tenancyoutbox.Store
	outboxDB, outboxClose := bootstrapOutboxDB(ctx)
	if outboxClose != nil {
		defer outboxClose()
	}
	if outboxDB != nil {
		outboxStore = tenancyoutbox.NewPostgresStore(
			sqlDBAdapter{db: outboxDB},
			tenancyoutbox.PostgresStoreOptions{WorkerID: workerID()},
		)
		log.Printf("tenancy: outbox PostgresStore wired (worker_id=%s)", workerID())
	} else {
		outboxStore = tenancyoutbox.NewInMemoryStore()
		log.Printf("tenancy: outbox InMemoryStore wired (CHORA_OUTBOX_DSN unset; NOT durable across restart)")
	}

	outboxPublisher := tenancyoutbox.NewPublisher(tenancyoutbox.PublisherConfig{
		Store:         outboxStore,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: serviceName,
	})
	// outboxPublisher satisfies the same Publish / PublishWithError port as
	// events.Recorder / events.CloudPublisher. Call-sites that previously
	// took events.Recorder accept *tenancyoutbox.Publisher transparently.
	// Wired into v2Deps.Events below via wireV2DepsEvents so every
	// /v1/* + /v2/* + /api/v1/admin/* handler emission flows through the
	// outbox → Pub/Sub dispatcher.
	//
	// Debt #50 fix (2026-05-16): prior to this revision the v2 handlers'
	// Events port was the in-memory events.Recorder default returned by
	// NewDefaultV2Deps, so tenant.created / addon.activated /
	// addon.deactivated / addon.upgraded / addon.downgraded /
	// addon.usage_recorded events were LOST in transit (never reached
	// Pub/Sub). The familiar_egg HTTP flow already wired the outbox
	// publisher via eggDeps.Publisher — this fix brings the rest of the
	// HTTP surface to parity.

	// Dispatcher runs in the background, draining the outbox to Pub/Sub.
	dispatcher := tenancyoutbox.NewDispatcher(tenancyoutbox.DispatcherConfig{
		Store:        outboxStore,
		Bus:          bus,
		WorkerID:     workerID(),
		MaxAttempts:  5,
		PollInterval: 250 * time.Millisecond,
	})
	go func() {
		if err := dispatcher.Run(ctx, 100); err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			log.Printf("tenancy: outbox dispatcher exited: %v", err)
		}
	}()
	log.Printf("tenancy: outbox dispatcher goroutine started")

	// ----------------------------------------------------------------------
	// Consumer-side inbox — chora-go-common/idempotent.Store factory.
	//
	// Production wires PostgresStore against the chora_tenancy
	// idempotency_keys table (migration 0006_idempotency_keys.up.sql).
	// Dev / tests use MemoryStore. The factory closure below is the
	// canonical wiring point — subscribers that need an inbox call
	// inboxFactory() at construction time.
	//
	// Per `agentic-resilience-d6` skill Pillar 2 (consumer-side dual of
	// the outbox), every Pub/Sub subscriber MUST wrap its handler in
	// idempotent.Store.Process(...). In-process maps are insufficient
	// under chaos (pod-death loses state; multi-replica → independent
	// dedupe sets).
	// ----------------------------------------------------------------------
	inboxFactory := newInboxFactory(outboxDB)

	// ----------------------------------------------------------------------
	// ADR-164 Wave 1 Stage D — chora-payments Pub/Sub subscriber bootstrap.
	//
	// chora-tenancy is the originating service for the two Purchase
	// aggregates extracted to chora-payments (FamiliarEgg + TenantManaTopUp).
	// chora-payments emits 5 canonical events on those aggregates; this
	// wiring starts CloudSubscriber goroutines per topic.
	//
	// The subscriber wraps the in-memory PoolApplier when the pgx pool is
	// nil (dev) + the pgx-backed pool repo when the pool is wired.
	// Idempotency: inboxFactory() returns a Postgres-backed inbox in prod
	// (chora_tenancy.idempotency_keys) and an in-memory store in dev.
	// ----------------------------------------------------------------------
	// CHO-2414: the pgx port the Stage D comment deferred. tenant_mana_pools
	// already exists (migration 0003), so this is a wiring change, not DDL.
	// With no pool (local dev) the in-memory repo remains, and the durability
	// guard reports it honestly rather than being told to look away.
	//
	// This ONE instance is shared by every consumer of the pool: the v2
	// mana-pool HTTP handlers (via v2Deps.PoolRepo below) and the ADR-164
	// payments PoolApplier. Splitting them would mean a checkout-paid top-up
	// never showing in the H+ read.
	var paymentsPoolRepo manapooldomain.PoolStore
	if pool != nil {
		paymentsPoolRepo = pg.NewManaPoolRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("tenancy: ADR-164 PoolApplier wired (pgx tenant_mana_pools; durable across pod restart, CHO-2414)")
	} else {
		paymentsPoolRepo = manapool.NewInmemPoolRepo()
		log.Printf("tenancy: ADR-164 PoolApplier wired (in-memory tenant_mana_pools; no pgx pool, NOT durable)")
	}
	paymentsPoolApplier, perr := manapool.NewPaymentsPoolApplier(paymentsPoolRepo)
	if perr != nil {
		log.Fatalf("tenancy: ADR-164 PoolApplier wiring failed: %v", perr)
	}

	// ----------------------------------------------------------------------
	// Existing handler wiring (legacy + v2)
	// ----------------------------------------------------------------------
	// Invoices + PaymentMethods stay in-memory for now — the gateway A6
	// wiring touches only Tenants (GET /api/tenants/{id}) + Entitlements
	// (GET /api/feature-flags). Their pgx adapters are a follow-on; the
	// in-memory fallback keeps the POST /api/tenants/{id}/invoices path
	// runnable in the meantime.
	v2Deps := httpapi.NewDefaultV2Deps()

	// CHO-2148 — the external web-egress entitlement (Far Sight tenant opt-in).
	// Wired ONLY with a real pgx pool: a write must land the policy row and the
	// chora.tenancy.external_egress_policy.updated.v1 event in ONE transaction,
	// and there is no honest in-memory stand-in for that (an in-memory fallback
	// would accept an opt-in and never publish, leaving the chora-observability
	// projection — and so the model-gateway's fail-closed gate — permanently
	// stale). With no pool the handler stays unwired and returns 503, which is
	// the loud failure we want rather than a silent OFF.
	if pool != nil {
		v2Deps.EgressPolicies = pg.NewExternalEgressRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("tenancy: external-egress policy repo wired (CHO-2148)")
	} else {
		log.Printf("tenancy: external-egress policy repo DISABLED (no pgx pool) — endpoint will 503")
	}

	// CHO-1811 — the entitlement endpoint (GET /api/feature-flags) read
	// directly from PG add_on_subscriptions while the runtime v2 deactivate
	// handler mutated only the in-memory SubscriptionRegistry. ht.htmm's
	// session JWT kept the cplus_social entitlement even after admin
	// deactivated, because the PG row never updated. Wrap legacyEntitlements
	// with a registry-backed adapter so the entitlement read tracks the
	// registry (the actual runtime source of truth). Subscribe / Cancel on
	// the legacy v1 path still delegate to PG.
	legacyEntitlements = newRegistryEntitlementStore(
		v2Deps.Subscriptions.SubscriptionRegistry,
		legacyEntitlements,
	)

	legacyDeps := httpapi.Deps{
		Tenants:        legacyTenants,
		AddOnCatalog:   legacyAddOnCatalog,
		Entitlements:   legacyEntitlements,
		Invoices:       inmem.NewInvoiceRepo(),
		PaymentMethods: inmem.NewPaymentMethodRepo(),
	}
	legacyHandler := httpapi.NewServer(legacyDeps)
	if u := strings.TrimSpace(os.Getenv("STRIPE_API_URL")); u != "" {
		v2Deps.Stripe = stripestub.New(stripestub.Config{APIURL: u})
	}
	// Debt #50 fix — bind v2 handler events to the canonical OutboxPublisher
	// so HTTP-handler emissions flow through outbox_events → Dispatcher →
	// Cloud Pub/Sub. Pre-fix the default in-memory recorder swallowed every
	// emission.
	wireV2DepsEvents(&v2Deps, outboxPublisher)
	log.Printf("tenancy: v2Deps.Events wired to outbox publisher (debt #50 closed)")
	// CHO-1767 — wire the chora-payments HTTP client so handleAdminChangeAddonTier
	// + handleAdminPreviewTierChange dispatch through Stripe Subscription.update
	// + Invoice.upcoming instead of the in-memory registry / stripestub.
	if addr := strings.TrimSpace(os.Getenv("CHORA_PAYMENTS_HTTP_ADDR")); addr != "" {
		paymentsClient, err := payments.NewHTTPClient(payments.HTTPClientConfig{Addr: addr})
		if err != nil {
			log.Fatalf("tenancy: payments HTTP client init failed (env set, fail-loud): %v", err)
		}
		v2Deps.PaymentsHTTP = newPaymentsHTTPAdapter(paymentsClient)
		httpapi.SetPaymentsErrSentinels(
			payments.ErrNoStripeSubscription,
			payments.ErrStripePriceMissing,
			payments.ErrTierUnchanged,
			payments.ErrAddonNotFound,
		)
		log.Printf("tenancy: chora-payments HTTP client wired (addr=%s) — change-tier + preview-tier dispatch through Stripe", addr)
	} else {
		log.Printf("tenancy: WARNING: CHORA_PAYMENTS_HTTP_ADDR unset — change-tier + preview-tier use legacy direct-registry path (CHO-1767 fallback)")
	}
	// L1 WP-4 (CHO-1705) — the H+ mana-pool read/write surface and the
	// ADR-164 payments subscriber MUST share one PoolStore instance, or
	// checkout-paid top-ups would never show in the H+ dashboard read.
	v2Deps.PoolRepo = paymentsPoolRepo
	log.Printf("tenancy: v2 mana-pool handlers unified on the payments PoolStore (L1 WP-4; %T)", paymentsPoolRepo)

	// CHO-2414 follow-up. The grant path writes the debit, the allocation row
	// and the outbox event in ONE transaction. With no pgx pool there is no
	// durable record possible, so GrantStore stays nil and the endpoint
	// refuses with 503 rather than debiting into a process map, which is how
	// 400 units left a pool and reached nobody.
	var grantStore *pg.ManaGrantRepository
	if pool != nil {
		grantStore = pg.NewManaGrantRepository(pg.NewPgxPoolQuerier(pool))
		v2Deps.GrantStore = grantStore
		log.Printf("tenancy: mana grant store wired (pgx, atomic debit + allocation + outbox; CHO-2414)")
	} else {
		log.Printf("tenancy: mana grant store DISABLED (no pgx pool): the grant endpoint will 503")
	}
	v2Handler := httpapi.NewV2Server(v2Deps)

	// H+ Setup-Tenant bootstrap handler (CHO-1628) — final wiring. Takes
	// the SubscriptionRegistry as its AddOnSubscriber so a successful
	// bootstrap auto-Subscribes the always-on `base` plan into the v2
	// registry (CHO-1751 Phase 1). Without this the H+ /h/addons surface
	// would render empty until the tenant ran Setup Wizard step 2.
	var bootstrapHandler *httpapi.BootstrapHandler
	// subTenantCreator (ADR-217 Phase 2.1) reuses the SAME bootstrap Service to
	// back the Tenancy/CreateSubTenant gRPC RPC — hoisted to function scope so
	// the gRPC server (below) can wire it via WithSubTenantCreator.
	var subTenantCreator tenancygrpc.SubTenantCreator
	if bootstrapRepo != nil {
		bootstrapSubscriber := newRegistryBootstrapSubscriber(v2Deps.Subscriptions.SubscriptionRegistry)
		bootstrapSvc := bootstrap.NewService(bootstrapRepo, bootstrapSubscriber)
		bootstrapHandler = httpapi.NewBootstrapHandler(bootstrapSvc)
		subTenantCreator = bootstrapSvc
		log.Printf("tenancy: H+ Setup-Tenant bootstrap handler wired (CHO-1628 + CHO-1751 base auto-Subscribe)")
	} else {
		log.Printf("tenancy: H+ Setup-Tenant bootstrap handler DISABLED (no pgx pool)")
	}

	// Ownership handover (UX Track U, E3; first-launch spec 13.3, 13.4, 13.8).
	// Pool-only: every route reads or writes chora_tenancy.members and
	// ownership_offers inside RunInTenantTx, so there is no in-memory lane to
	// degrade to. With no pool the routes are simply not mounted, and the
	// boot log says so rather than serving a surface that cannot transfer
	// anything.
	var ownershipHandler *httpapi.OwnershipHandler
	if pool != nil {
		oh, err := httpapi.NewOwnershipHandler(pg.NewOwnershipRepository(pg.NewPgxPoolQuerier(pool)))
		if err != nil {
			log.Fatalf("tenancy: ownership handler init failed: %v", err)
		}
		ownershipHandler = oh
		log.Printf("tenancy: ownership handover routes wired (me offers + operator override)")
	} else {
		log.Printf("tenancy: ownership handover routes DISABLED (no pgx pool)")
	}

	// B2 day zero: reconcile the `add_ons` table against the catalogue this
	// binary is running BEFORE the hydrator replays anything. Order matters:
	// a drifted catalogue makes the hydrator skip rows, so the skip lines
	// below are the symptom and this is the cause. Speaking first means the
	// boot log names the disease rather than the rash.
	//
	// FAIL-CLOSED. Drift stops the boot unless CHORA_ADDON_CATALOGUE_RECONCILE
	// is exactly `warn`. That is deliberately stricter than the neighbouring
	// hydrator, which logs and continues: a hydration miss degrades one
	// tenant's H+ view until the next restart, whereas catalogue drift means
	// the marketplace is quietly selling plans the database cannot represent
	// and the estate stays wrong until somebody happens to look.
	//
	// With no pool (local dev without Postgres) there is nothing to compare,
	// so the guard reports that it did not run rather than passing silently.
	if pool != nil {
		reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 15*time.Second)
		guardErr := reconcileAddOnCatalogue(reconcileCtx,
			pg.NewCatalogueReader(pg.NewPgxPoolQuerier(pool)),
			v2Deps.Catalogue)
		reconcileCancel()
		switch {
		case guardErr == nil:
			log.Printf("tenancy: add-on catalogue guard OK (add_ons agrees with the running catalogue)")
		case addOnCatalogueGuardEnforces(os.Getenv("CHORA_ADDON_CATALOGUE_RECONCILE")):
			log.Fatalf("tenancy: add-on catalogue guard REFUSED to serve: %v "+
				"(apply migration 0035, or set CHORA_ADDON_CATALOGUE_RECONCILE=warn to boot anyway and repair)", guardErr)
		default:
			log.Printf("tenancy: add-on catalogue guard DRIFT (downgraded to a warning by CHORA_ADDON_CATALOGUE_RECONCILE=warn): %v", guardErr)
		}
	} else {
		log.Printf("tenancy: add-on catalogue guard SKIPPED (no pgx pool; nothing to compare against)")
	}

	// CHO-1752 — hydrate the in-memory v2 SubscriptionRegistry from the
	// pg `add_on_subscriptions` table on boot. Without this, every
	// tenancy restart wipes every tenant's H+ management view until they
	// re-Subscribe by hand (Cloud Run revision rollovers in prod; go run
	// restarts in local dev). On error we LOG + CONTINUE: a hydration
	// failure must not block tenancy from serving, because the runtime
	// path (POST /api/v1/admin/tenants/{id}/addons etc.) still works
	// against the registry — the boot pre-population is a UX nicety
	// gating /h/addons after restart, not a correctness requirement.
	if pool != nil {
		hydrator := pg.NewSubscriptionHydrator(pg.NewPgxPoolQuerier(pool))
		hydrateCtx, hydrateCancel := context.WithTimeout(ctx, 30*time.Second)
		hydrated, err := hydrator.LoadActive(hydrateCtx)
		hydrateCancel()
		if err != nil {
			log.Printf("tenancy: subscription hydrator FAILED (continuing without): %v", err)
		}
		replayed := 0
		pastDueReplayed := 0
		var skipped []string
		for _, row := range hydrated {
			// CHO-2414: an unbackfilled add_ons.code is a distinct failure
			// from a code the catalogue does not know, and the two need
			// different fixes. Name both, and always name the product: a
			// skip line carrying only a code cannot tell an operator which
			// add-on stopped working.
			if strings.TrimSpace(row.AddOnCode) == "" {
				log.Printf("tenancy: subscription hydrator: SKIP tenant=%s addon=%q reason=add_ons.code is NULL (row predates the migration-0021 backfill)",
					row.TenantID, row.AddOnName)
				skipped = append(skipped, fmt.Sprintf("%s(no code)", row.AddOnName))
				continue
			}
			if _, subErr := v2Deps.Subscriptions.Subscribe(row.TenantID, row.AddOnCode, row.OwnerGCID); subErr != nil {
				log.Printf("tenancy: subscription hydrator: SKIP tenant=%s addon=%q code=%s reason=%v",
					row.TenantID, row.AddOnName, row.AddOnCode, subErr)
				skipped = append(skipped, fmt.Sprintf("%s(%s)", row.AddOnName, row.AddOnCode))
				continue
			}
			replayed++
			// CHO-1776 — re-apply persisted dunning flag so the FE banner
			// survives pod restarts. Subscribe always creates with
			// PastDue=false; MarkPastDue lifts it back if the row was
			// dunning at shutdown.
			if row.PastDue {
				if _, mpErr := v2Deps.Subscriptions.MarkPastDue(row.TenantID, row.AddOnCode); mpErr != nil {
					log.Printf("tenancy: subscription hydrator: SKIP past_due flag tenant=%s addon=%q code=%s reason=%v",
						row.TenantID, row.AddOnName, row.AddOnCode, mpErr)
					continue
				}
				pastDueReplayed++
			}
		}
		// The count alone hides which rows were lost. Name every residual
		// skip on the same line so a partial replay is diagnosable from the
		// boot log without a second query (CHO-2414).
		if len(skipped) == 0 {
			log.Printf("tenancy: subscription hydrator replayed %d/%d active subscriptions on boot (%d past_due), 0 skipped: CHO-1752/CHO-1776/CHO-2414",
				replayed, len(hydrated), pastDueReplayed)
		} else {
			log.Printf("tenancy: subscription hydrator replayed %d/%d active subscriptions on boot (%d past_due), %d SKIPPED: %s: CHO-1752/CHO-1776/CHO-2414",
				replayed, len(hydrated), pastDueReplayed, len(skipped), strings.Join(skipped, ", "))
		}
	} else {
		log.Printf("tenancy: subscription hydrator DISABLED (no pgx pool) — registry starts empty")
	}

	// CHO-1740 — H+ Marketplace Subscribe saga. The payments subscriber
	// activates the AddOn Subscription on tenant_addon_purchase.payment_
	// captured by calling SubscriptionRegistry.Subscribe + ChangeTier via
	// a thin adapter; deactivates on .refunded via Unsubscribe.
	addOnActivator := newRegistryAddOnActivator(v2Deps.Subscriptions.SubscriptionRegistry)
	paymentsSubscriber := tnevents.NewPaymentsSubscriber(tnevents.PaymentsSubscriberDeps{
		Publisher:      outboxPublisher,
		Pool:           paymentsPoolApplier,
		Inbox:          inboxFactory(),
		AddOnActivator: addOnActivator,
		// CHO-1776 — wire the SubscriptionRegistry's MarkPastDue /
		// MarkRecovered into the dunning handlers so the FE banner
		// driven off /h/addons reflects Stripe's smart-retry state.
		// The activator adapter satisfies both AddOnActivator + the
		// new RegistryPastDueUpdater port to keep wiring tight.
		PastDueRegistry: addOnActivator,
		// CHO-1779 — wire PromoteScheduledTier so the
		// subscription_schedule.released subscriber auto-promotes the
		// in-memory CurrentTier when Stripe fires the release at the
		// cycle anchor. Same adapter satisfies the new port.
		ScheduledTierPromoter: addOnActivator,
		// CHO-1783 — wire AnchorDeactivation so the
		// customer.subscription.deleted subscriber flips the registry
		// directly to DEACTIVATED at the Stripe anchor, replacing the
		// legacy Unsubscribe → 30-day-grace path. Same adapter.
		DeactivationAnchorer: addOnActivator,
	})
	paymentsSubWG, psErr := startPaymentsSubscribers(ctx, pubsubClient, paymentsSubscriber)
	if psErr != nil {
		log.Fatalf("tenancy: ADR-164 payments subscriber bootstrap failed: %v", psErr)
	}
	if paymentsSubWG != nil {
		log.Printf("tenancy: ADR-164 payments subscribers running (10 topics — 5 base + 2 H+ marketplace CHO-1740 + 3 H+ lifecycle CHO-1775)")
	}

	// ── ADR-205 (CHO-1938) transaction-ledger projection subscriber ──────
	// Projects payments + identity + observability events into the
	// transaction_ledger read store (chora_tenancy). Needs the pgx pool for
	// the RLS-scoped upserts; skipped in dev-without-pgx. Its own inbox +
	// chora-tenancy-txledger-* subscriptions are distinct from the payments
	// notification subscriber's, so both process independently.
	var txLedgerSubWG *sync.WaitGroup
	var atomCountSubWG *sync.WaitGroup
	if pool != nil {
		txLedgerRepo := pg.NewTransactionLedgerWriteRepo(pg.NewPgxPoolQuerier(pool))
		txLedgerSub := tnevents.NewTransactionLedgerSubscriber(txLedgerRepo, inboxFactory())
		var tlErr error
		txLedgerSubWG, tlErr = startTransactionLedgerSubscribers(ctx, pubsubClient, txLedgerSub)
		if tlErr != nil {
			log.Fatalf("tenancy: ADR-205 transaction-ledger subscriber bootstrap failed: %v", tlErr)
		}
		if txLedgerSubWG != nil {
			log.Printf("tenancy: ADR-205 transaction-ledger projection subscribers running (18 topics: 16 payments + 1 identity + 1 observability)")
		}

		// ── ADR-217 Debt 3 (CHO-2011) atom-count projection subscriber ───
		// Folds chora.creation.atom.created.v1 (+1) / archived.v1 (-1) into
		// tenant_atom_counts; the tenant-hierarchy read serves each child's
		// atom_count from it. Own inbox + chora-tenancy-atomcount-* subs.
		atomCountRepo := pg.NewAtomCountWriteRepo(pg.NewPgxPoolQuerier(pool))
		atomCountSub := tnevents.NewAtomCountSubscriber(atomCountRepo, inboxFactory())
		var acErr error
		atomCountSubWG, acErr = startAtomCountSubscribers(ctx, pubsubClient, atomCountSub)
		if acErr != nil {
			log.Fatalf("tenancy: ADR-217 atom-count subscriber bootstrap failed: %v", acErr)
		}
		if atomCountSubWG != nil {
			log.Printf("tenancy: ADR-217 atom-count projection subscribers running (2 topics: atom.created + atom.archived)")
		}
	} else {
		log.Printf("tenancy: ADR-205 transaction-ledger subscriber SKIPPED (no pgx pool)")
	}

	// Phase B finishes wiring now that v2Deps owns the canonical
	// SubscriptionRegistry — the wizard handler shares the same
	// registry instance so its pending rows show up in admin lists.
	// CHO-1692 — pg-backed SetupWizardAddonRepository persists wizard
	// selections so the re-entry hydration GET survives pod restart.
	// nil pgRepo falls back to in-memory-only reads (dev-without-pgx).
	var pgSetupWizardAddons *pg.SetupWizardAddonRepository
	if pool != nil {
		pgSetupWizardAddons = pg.NewSetupWizardAddonRepository(pg.NewPgxPoolQuerier(pool))
	}
	meAddOnsHandler = httpapi.NewMeAddOnsHandler(v2Deps.Subscriptions.SubscriptionRegistry, pgSetupWizardAddons)
	if pgSetupWizardAddons != nil {
		log.Printf("tenancy: Setup Wizard /me/addons handler wired (CHO-1664 + CHO-1692 pg-backed)")
	} else {
		log.Printf("tenancy: Setup Wizard /me/addons handler wired (CHO-1664; in-memory only — re-entry hydration NOT durable)")
	}

	// ----------------------------------------------------------------------
	// Iter G.3/G.4 Familiar Egg wiring (ADR-149).
	//
	// PROD-C (Iter G.4) — production stores wired:
	//
	//   - Purchases  → pg.PurchaseRepository  (pgx + chora_tenancy DB)
	//   - Catalog    → pg.CatalogRepository   (pgx + chora_tenancy DB)
	//   - WebhookIdem → pg.StripeWebhookEventRepository (durable de-dup)
	//   - Stripe client → RESTClient when STRIPE_API_KEY is set; StubClient
	//     fallback when unset (dev / pre-bring-up).
	//
	// Production-mode env gate below (validateFamiliarEggProdEnv) FAILS LOUD
	// at startup if CHORA_ENV=prod AND any of STRIPE_API_KEY /
	// STRIPE_WEBHOOK_SECRET / STRIPE_SUCCESS_URL / STRIPE_CANCEL_URL is
	// unset. In dev (CHORA_ENV != "prod") the gate logs a warning and we
	// fall back to the stub client.
	// ----------------------------------------------------------------------
	// Resolve Stripe credentials directly from environment variables loaded by Docker Compose.
	stripeSecretClient, stripeSecretShutdown, err := bootstrapStripeSecretClient(ctx)
	if err != nil {
		log.Fatalf("tenancy: stripe secret client bootstrap failed: %v", err)
	}
	if stripeSecretShutdown != nil {
		defer stripeSecretShutdown()
	}
	stripeCreds, err := resolveStripeCredentials(ctx, stripeSecretClient)
	if err != nil {
		log.Fatalf("tenancy: stripe credential resolution failed: %v", err)
	}
	if stripeCreds.APIKeySource != "unset" {
		log.Printf("tenancy: stripe api key resolved from %s", stripeCreds.APIKeySource)
	}
	if stripeCreds.WebhookSource != "unset" {
		log.Printf("tenancy: stripe webhook secret resolved from %s", stripeCreds.WebhookSource)
	}

	if err := validateFamiliarEggProdEnv(stripeCreds); err != nil {
		log.Fatalf("tenancy: familiar-egg production env validation failed: %v", err)
	}
	var eggStripeClient familiareggstripe.Client
	stripeAPIKey := stripeCreds.APIKey
	stripeAPIBase := envOrDefault("STRIPE_API_BASE", "https://api.stripe.com")
	if stripeAPIKey != "" {
		eggStripeClient = familiareggstripe.NewRESTClient(familiareggstripe.Config{
			APIBase:   stripeAPIBase,
			SecretKey: stripeAPIKey,
		})
		log.Printf("tenancy: familiar-egg Stripe RESTClient wired (base=%s)", stripeAPIBase)
	} else {
		eggStripeClient = familiareggstripe.NewStubClient()
		log.Printf("tenancy: STRIPE_API_KEY unset — familiar-egg using stub Stripe client (dev only)")
	}

	// Wire pg-backed stores when CHORA_DB_DSN is set (pool != nil); otherwise use the in-memory fallback.
	var eggPurchases httpapi.PurchaseStore
	var eggCatalog httpapi.CatalogStore
	var eggWebhookIdem httpapi.WebhookIdemStore
	var sweeperStore familiareggsweeper.PurchaseStore
	if pool != nil {
		querier := pg.NewPgxPoolQuerier(pool)
		purchaseRepo := pg.NewPurchaseRepository(querier)
		eggPurchases = purchaseRepo
		eggCatalog = pg.NewCatalogRepository(querier)
		eggWebhookIdem = pg.NewStripeWebhookEventRepository(querier)
		sweeperStore = purchaseRepo
		log.Printf("tenancy: familiar-egg pg stores wired (Purchases + Catalog + WebhookIdem)")
	} else {
		eggPurchases = httpapi.NewInMemoryPurchaseStore()
		eggCatalog = httpapi.NewInMemoryCatalogStore()
		eggWebhookIdem = httpapi.NewInMemoryWebhookIdemStore()
		sweeperStore = nil // sweeper is disabled in dev by default
		log.Printf("tenancy: familiar-egg in-memory stores wired (CHORA_DB_DSN unset; NOT durable)")
	}
	// ADR-164: the familiar-egg checkout handler delegates the Stripe Checkout
	// session + familiar_egg_purchases persistence to chora-payments via gRPC
	// (chora-tenancy stays the FamiliarEgg originating service + provisions the
	// egg on payment_captured). CHORA_PAYMENTS_GRPC_ADDR is REQUIRED — fail
	// loud, no local-Stripe fallback (the tenancy table was dropped by mig 0017).
	eggPaymentsClient, err := payments.NewGRPCClientFromEnv()
	if err != nil {
		log.Fatalf("tenancy: familiar-egg payments gRPC client init failed (CHORA_PAYMENTS_GRPC_ADDR required, fail-loud): %v", err)
	}
	log.Printf("tenancy: familiar-egg checkout delegates to chora-payments gRPC (ADR-164)")

	eggDeps := httpapi.FamiliarEggDeps{
		Purchases:     eggPurchases,
		Catalog:       eggCatalog,
		Stripe:        eggStripeClient,
		Publisher:     outboxPublisher,
		WebhookIdem:   eggWebhookIdem,
		Payments:      newFamiliarEggCheckoutAdapter(eggPaymentsClient),
		WebhookSecret: stripeCreds.WebhookSecret,
		SuccessURL:    envOrDefault("STRIPE_SUCCESS_URL", "https://chora.site/a/familiar/marketplace/checkout/success"),
		CancelURL:     envOrDefault("STRIPE_CANCEL_URL", "https://chora.site/a/familiar/marketplace/checkout/cancel"),
	}
	eggHandler := httpapi.NewFamiliarEggMux(eggDeps)

	// Hard-expiry sweeper — feature-flagged behind ENABLE_EGG_EXPIRY_SWEEPER
	// (default OFF; Iter E activation gate per Stripe finalization session).
	//
	// PROD-C (Iter G.4) wired sweeperStore from the pg PurchaseRepository
	// when the pgx pool is available; nil otherwise (no candidates to sweep).
	//
	// Iter G.5 (Stripe finalization, 2026-05-13): real CreditIssuer wired
	// with the canonical outbox publisher so refund events get the same
	// DLQ + retry guarantees as every other tenancy event. Idempotency via
	// the IdempotencyKey field on TenantManaAllocation (per ADR-149
	// expiry path).
	//
	// Iter G.7 (2026-05-13, this session): the allocation repository is
	// promoted from manapool.InmemAllocationRepo → pg.AllocationRepository
	// when the pgx pool is available. The pg repo enforces the partial
	// UNIQUE INDEX on idempotency_key (migration 0009) so concurrent
	// sweepers / DLQ replays cannot double-issue. In-memory repo retains
	// for dev (pgx pool nil) — refunds in dev mode are still per-pod
	// state but the boundary is the same.
	var allocRepo interface {
		manapool.AllocationGetter
		manapool.AllocationSaver
	}
	if pool != nil {
		allocRepo = pg.NewAllocationRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("tenancy: familiar-egg CreditIssuer using pgx AllocationRepository (durable across pod restart)")
	} else {
		allocRepo = manapool.NewInmemAllocationRepo()
		log.Printf("tenancy: familiar-egg CreditIssuer using in-memory AllocationRepository (dev mode; NOT durable)")
	}
	manaCreditIssuer, err := newFamiliarEggCreditIssuer(allocRepo, outboxPublisher)
	if err != nil {
		log.Printf("tenancy: familiar-egg CreditIssuer wiring failed (%v) — falling back to NoopManaCreditIssuer", err)
		manaCreditIssuer = familiareggsweeper.NoopManaCreditIssuer{}
	} else {
		log.Printf("tenancy: familiar-egg CreditIssuer wired")
	}
	sweeperEnabled := strings.EqualFold(os.Getenv("ENABLE_EGG_EXPIRY_SWEEPER"), "true")
	sweeper := familiareggsweeper.New(familiareggsweeper.Config{
		Enabled:     sweeperEnabled,
		Store:       sweeperStore,
		Publisher:   outboxPublisher,
		ManaCredits: manaCreditIssuer,
	})
	go func() {
		if err := sweeper.Run(ctx); err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			log.Printf("tenancy: familiar_egg sweeper exited: %v", err)
		}
	}()
	if sweeperEnabled {
		log.Printf("tenancy: familiar_egg hard-expiry sweeper ENABLED")
	} else {
		log.Printf("tenancy: familiar_egg hard-expiry sweeper DISABLED (set ENABLE_EGG_EXPIRY_SWEEPER=true)")
	}

	// ADR-236 D5 — report-only runtime durability guard over the composition
	// root (W0-F1 gate, CHO-2198). Classifies each wired repository by SHAPE
	// (holds a live *pgxpool.Pool ⇒ DURABLE; a data map ⇒ IN_MEMORY) and logs a
	// structured, greppable report at boot. Report-only unless
	// CHORA_DURABILITY_GUARD=enforce AND the binding is allow-listed — nil
	// allow-list matches the chora-payments / chora-identity wirings. Per
	// docs/references/w0-f1-inmemory-inventory.md the always-in-memory sites
	// (invoices, payment_methods, mana_pools) report IN_MEMORY honestly (no pg
	// impl yet); pg-or-nil durable repos (tenant_branding, tenant_bootstrap,
	// egress_policies) and closure report UNKNOWN when a pool is absent. The
	// gRPC-scoped membership / projection repos (constructed inside the later
	// `if pool != nil` gRPC block) are always-durable-or-absent and left in
	// place rather than restructure that block.
	durabilityguard.Guard("chora-tenancy", []durabilityguard.Binding{
		{Port: "tenants", Adapter: legacyTenants},
		{Port: "tenant_branding", Adapter: pgTenants},
		{Port: "tenant_bootstrap", Adapter: bootstrapRepo},
		{Port: "addon_catalog", Adapter: legacyAddOnCatalog},
		{Port: "entitlements", Adapter: legacyEntitlements},
		{Port: "invoices", Adapter: legacyDeps.Invoices},
		{Port: "payment_methods", Adapter: legacyDeps.PaymentMethods},
		{Port: "mana_pools", Adapter: paymentsPoolRepo},
		// ⚠ Two ports, deliberately. `mana_allocations` is the egg-refund
		// CreditIssuer's repo. `mana_grants` is the store the H+ grant path
		// actually writes to, and it was bound to NOTHING until CHO-2414's
		// follow-up: the guard reported mana_allocations DURABLE while the
		// live grant path wrote an in-process map, so it certified a port it
		// had never inspected.
		//
		// ⚠⚠ Binding is not working. The guard classifies by SHAPE (does this
		// object hold a live *pgxpool.Pool), so DURABLE here means "can reach
		// Postgres", NOT "its writes succeed". pg.AllocationRepository below
		// holds a pool and still fails every insert, because it takes the
		// plain Querier with no RunInTenantTx against an RLS table under a
		// NOBYPASSRLS role. Proven 2026-08-24 as app_rw in a rolled-back tx.
		// The RLS-scoped write is proven separately, not by this line.
		{Port: "mana_allocations", Adapter: allocRepo},
		{Port: "mana_grants", Adapter: grantStore},
		{Port: "egg_purchases", Adapter: eggPurchases},
		{Port: "egg_catalog", Adapter: eggCatalog},
		{Port: "egg_webhook_idem", Adapter: eggWebhookIdem},
		{Port: "egress_policies", Adapter: v2Deps.EgressPolicies},
		{Port: "closure", Adapter: closureRepo},
	}, nil)

	root := http.NewServeMux()
	root.Handle("/v2/", v2Handler)
	root.Handle("/api/v1/admin/", v2Handler)
	// H+ Setup-Tenant Phase 1 (CHO-1628) — self-onboard endpoint mounted
	// only when the pgx pool is wired. Matched before the legacy "/"
	// catch-all because Go's mux uses longest-prefix wins on exact paths.
	if bootstrapHandler != nil {
		root.Handle("/api/v1/tenants/bootstrap", bootstrapHandler)
	}
	// Setup Wizard Phase A — CHO-1655. /me/branding is an exact path
	// (no wildcard) so it dispatches before the legacy "/" catch-all
	// per Go mux's longest-prefix wins.
	if meFinishSetupHandler != nil {
		root.Handle("/api/v1/tenants/me/finish-setup", meFinishSetupHandler)
	}
	if meTenantHandler != nil {
		root.Handle("/api/v1/tenants/me", meTenantHandler)
	}
	if meBrandingHandler != nil {
		root.Handle("/api/v1/tenants/me/branding", meBrandingHandler)
	}
	// Setup Wizard Phase B — CHO-1664. Same exact-path discipline as
	// /me/branding so it dispatches before the legacy "/" catch-all.
	if meAddOnsHandler != nil {
		root.Handle("/api/v1/tenants/me/addons", meAddOnsHandler)
	}
	// Ownership handover (E3). Registered through the handler's own registrar
	// so the production router and the routing fence mount the same set. The
	// operator override is a PARAMETRIC path inside the /api/v1/admin/ subtree
	// rather than a prefix, so it wins by specificity without swallowing the
	// live me/addons, mana-pool and marketplace routes the v2 handler serves.
	if ownershipHandler != nil {
		httpapi.RegisterOwnershipRoutes(root, ownershipHandler)
	}
	// Iter G.3 routes — mounted at the root prefix paths defined in
	// internal/adapter/http/familiar_egg_handlers.go.
	root.Handle("/api/familiar-eggs/", eggHandler)
	root.Handle("/api/familiar-eggs/checkout", eggHandler)
	root.Handle("/api/familiar-eggs/catalog", eggHandler)
	root.Handle("/api/admin/familiar-eggs/", eggHandler)
	root.Handle("/api/admin/familiar-eggs/catalog", eggHandler)
	root.Handle("/webhooks/stripe", eggHandler)
	root.Handle("/", legacyHandler)

	// Wrap with the OTLP HTTP middleware so every request emits a span.
	tracedHandler := cgcobservability.HTTPMiddleware()(root)

	addr := ":" + port
	srv := &http.Server{
		Addr:              addr,
		Handler:           tracedHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("service=%s version=%s listening on %s", serviceName, serviceVersion, addr)

	// ----------------------------------------------------------------------
	// gRPC server (Iter G.4 PROD-C #12) — FamiliarEgg PreviewEggOdds.
	//
	// chora-consumption swaps its REST BreedDistributionClient to call this
	// typed RPC via mesh DNS (chora-tenancy:9090). mTLS is provided by
	// Cloud Service Mesh PeerAuth; no special config in this server.
	//
	// Health check + reflection enabled per the existing chora-agent-executor
	// pattern. The server is wired ONLY when the pgx pool is available —
	// in dev (in-memory catalog) the gRPC port stays unbound to avoid
	// surprising callers with empty results.
	// ----------------------------------------------------------------------
	grpcPort := envOrDefault("GRPC_PORT", "9090")
	var grpcSrv *grpc.Server
	grpcErrCh := make(chan error, 1)
	if pool != nil {
		// Keepalive-tolerant server (chora-go-common/grpcconn): the enforcement
		// policy (MinTime 10s <= the identity client's 30s ping, PermitWithoutStream)
		// lets chora-identity hold a warm subchannel without earning a
		// "too_many_pings" GOAWAY — the server side of the cold-start 504 root-fix.
		// Creds unchanged (default insecure / plaintext); mTLS via Cloud Service
		// Mesh PeerAuth.
		grpcSrv = grpc.NewServer(grpcconn.ServerOptions()...)
		catalogRepo := pg.NewCatalogRepository(pg.NewPgxPoolQuerier(pool))
		familiarEggServer := tenancygrpc.NewFamiliarEggServer(catalogRepo)
		tenancyv1.RegisterCompanionEggServer(grpcSrv, familiarEggServer)

		// Bucket 1 (2026-05-14 multi-tenant identity): Tenancy gRPC server
		// exposes ListMembershipsByGCID over members + tenants tables.
		// chora-identity is the sole sanctioned caller — mTLS via Cloud
		// Service Mesh, no JWT validation here.
		membershipRepo := pg.NewMembershipRepository(pg.NewPgxPoolQuerier(pool))
		// ADR-182: UpsertMembership write path — chora-master auto-enrol
		// (chora-identity resolve) + admin add-member authoritative write.
		membershipQuerier := pg.NewPgxPoolQuerier(pool)
		membershipWriteRepo := pg.NewMembershipWriteRepository(membershipQuerier, membershipQuerier)
		// ADR-217 Phase 2.1 (CHO-2006): the same Tenancy server also serves
		// CreateSubTenant, backed by the hoisted bootstrap Service.
		// ADR-217 Phase 2.3b (CHO-2010): + GetTenantHierarchy — direct children
		// of the caller's current tenant with per-child live-member counts
		// (children from the RLS-free tenants registry; counts under each
		// child's own RLS scope).
		tenantHierarchyRepo := pg.NewTenantHierarchyRepo(pg.NewPgxPoolQuerier(pool))
		membershipServer := tenancygrpc.NewMembershipServerWithWriter(membershipRepo, membershipWriteRepo).
			WithSubTenantCreator(subTenantCreator).
			WithHierarchyReader(tenantHierarchyRepo)
		tenancyv1.RegisterTenancyServer(grpcSrv, membershipServer)

		// ── ADR-205 (CHO-1939) Transaction History read service ─────────
		// Serves the contextual transaction_ledger CQRS projection (B1/B2)
		// scope-aware: learner / tenant / master span-all. The MASTER
		// (PLATFORM_OPERATOR) span-all reuses the ADR-165 cross-tenant audit
		// — emitted BEFORE the read via the SAME outbox store the dispatcher
		// drains, on chora.governance.audit.cross_tenant_payments_viewed.v1
		// (consumed by chora-governance, B6/CHO-1942). The BFF (B4/CHO-1940)
		// fronts this with the REST surface + stamps the mesh-trust claims.
		txReadRepo := pg.NewTransactionLedgerReadRepo(pg.NewPgxPoolQuerier(pool))
		txAuditEmitter := tenancyoutbox.NewTransactionAuditEmitter(outboxStore)
		// The read repo also backs the master franchisee directory (S3 / CHO-1930):
		// it reads the RLS-free tenants registry (children of chora-master).
		txHistoryServer := tenancygrpc.NewTransactionHistoryServer(txReadRepo, txAuditEmitter).
			WithFranchisees(txReadRepo)

		// Transaction exports previously depended directly on GCS. They remain
		// disabled until a local/portable object-storage adapter is configured.
		log.Printf("tenancy: transaction export DISABLED (no local object-storage adapter configured)")

		tenancyv1.RegisterTransactionHistoryServiceServer(grpcSrv, txHistoryServer)

		// Health + reflection (per chora-agent-executor pattern).
		healthSrv := health.NewServer()
		healthSrv.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
		healthSrv.SetServingStatus("chora.services.tenancy.v1.FamiliarEgg", healthgrpc.HealthCheckResponse_SERVING)
		healthSrv.SetServingStatus("chora.services.tenancy.v1.Tenancy", healthgrpc.HealthCheckResponse_SERVING)
		healthSrv.SetServingStatus("chora.services.tenancy.v1.TransactionHistoryService", healthgrpc.HealthCheckResponse_SERVING)
		healthgrpc.RegisterHealthServer(grpcSrv, healthSrv)
		reflection.Register(grpcSrv)

		go func() {
			lis, lerr := net.Listen("tcp", ":"+grpcPort)
			if lerr != nil {
				grpcErrCh <- fmt.Errorf("grpc listen :%s: %w", grpcPort, lerr)
				return
			}
			log.Printf("tenancy: gRPC FamiliarEgg server listening on :%s", grpcPort)
			if err := grpcSrv.Serve(lis); err != nil {
				grpcErrCh <- err
			}
		}()
	} else {
		log.Printf("tenancy: gRPC server SKIPPED (no pgx pool; in-memory catalog has no breed_distribution to expose)")
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case err := <-grpcErrCh:
		log.Fatalf("grpc server error: %v", err)
	case <-ctx.Done():
		log.Printf("shutting down...")
		drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(drainCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
		// gRPC graceful drain — bounded by drainCtx.
		if grpcSrv != nil {
			stopped := make(chan struct{})
			go func() {
				grpcSrv.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
				log.Printf("tenancy: gRPC graceful stop complete")
			case <-drainCtx.Done():
				log.Printf("tenancy: gRPC graceful drain deadline exceeded — forcing stop")
				grpcSrv.Stop()
			}
		}
		// Final outbox drain — dispatcher.Run has already exited on ctx
		// cancellation, but we synchronously drain one last batch to push
		// in-flight pending rows out before the binary exits.
		finalDrain, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer fcancel()
		if n, derr := dispatcher.DrainOnce(finalDrain, 200); derr != nil {
			log.Printf("tenancy: final outbox drain error: %v (drained %d)", derr, n)
		} else {
			log.Printf("tenancy: final outbox drain published %d rows", n)
		}

		// ADR-164 payments subscriber drain — bounded so a stuck Cloud
		// Pub/Sub receive loop doesn't hold up pod shutdown.
		drainPaymentsSubscribers(paymentsSubWG, 5*time.Second)
		// ADR-205 transaction-ledger projection subscriber drain (same
		// bounded shutdown; helper is nil-safe).
		drainPaymentsSubscribers(txLedgerSubWG, 5*time.Second)
		// ADR-217 atom-count projection subscriber drain (nil-safe).
		drainPaymentsSubscribers(atomCountSubWG, 5*time.Second)
	}
}

// newInboxFactory returns the canonical idempotent.Store factory for
// consumer-side inbox wiring. When outboxDB is non-nil (production), the
// PostgresStore-backed inbox is returned. Otherwise a MemoryStore — dev
// only; NOT chaos-safe.
//
// Per `agentic-resilience-d6` skill Pillar 2 step 3b: every subscriber
// MUST wrap its handler in idempotent.Store.Process(...). The factory
// pattern keeps the choice (Postgres vs in-memory) in a single seam.
func newInboxFactory(outboxDB *sql.DB) func() idempotent.Store {
	if outboxDB == nil {
		return func() idempotent.Store {
			return idempotent.NewMemoryStore()
		}
	}
	// One PostgresStore instance per process is fine — the underlying
	// *sql.DB pool is shared. Each subscriber gets the same store.
	store := idempotent.NewPostgresStore(idempotentSQLDBAdapter{db: outboxDB})
	return func() idempotent.Store { return store }
}

// idempotentSQLDBAdapter bridges *sql.DB to the idempotent.SQLDB interface
// (which uses idempotent.SQLRows + idempotent.SQLRow so tests can stub).
type idempotentSQLDBAdapter struct {
	db *sql.DB
}

func (a idempotentSQLDBAdapter) ExecContext(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	return a.db.ExecContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryContext(ctx context.Context, q string, args ...interface{}) (idempotent.SQLRows, error) {
	return a.db.QueryContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryRowContext(ctx context.Context, q string, args ...interface{}) idempotent.SQLRow {
	return a.db.QueryRowContext(ctx, q, args...)
}

// envOrDefault returns the env var value or def when unset / empty.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// requiredFamiliarEggProdURLEnv enumerates the URL env vars that MUST be
// set when CHORA_ENV=prod. The Stripe credential pair (API key + webhook
// secret) is validated separately against the resolved values returned by
// resolveStripeCredentials reads the raw environment values.
var requiredFamiliarEggProdURLEnv = []string{
	"STRIPE_SUCCESS_URL",
	"STRIPE_CANCEL_URL",
}

// validateFamiliarEggProdEnv enforces the Iter G.4 PROD-C production
// env contract.
//
// When CHORA_ENV == "prod":
//   - stripeCreds.APIKey + stripeCreds.WebhookSecret MUST be non-empty
//     (read directly from STRIPE_API_KEY / STRIPE_WEBHOOK_SECRET)
//   - STRIPE_SUCCESS_URL + STRIPE_CANCEL_URL MUST be non-empty
//
// In any other CHORA_ENV (dev, staging, empty): a warning is logged for
// each missing value, but startup is permitted (the Stripe stub client +
// in-memory stores keep the binary runnable for local development).
//
// Credentials are supplied through the process environment (normally .env via Compose).
func validateFamiliarEggProdEnv(stripeCreds stripeCredentials) error {
	choraEnv := strings.ToLower(strings.TrimSpace(os.Getenv("CHORA_ENV")))
	isProd := choraEnv == "prod" || choraEnv == "production"

	var missing []string
	if strings.TrimSpace(stripeCreds.APIKey) == "" {
		missing = append(missing, "STRIPE_API_KEY")
	}
	if strings.TrimSpace(stripeCreds.WebhookSecret) == "" {
		missing = append(missing, "STRIPE_WEBHOOK_SECRET")
	}
	for _, key := range requiredFamiliarEggProdURLEnv {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if isProd {
		return fmt.Errorf(
			"CHORA_ENV=%s requires familiar-egg values: %s",
			choraEnv, strings.Join(missing, ", "),
		)
	}
	log.Printf(
		"tenancy: familiar-egg env vars missing (CHORA_ENV=%q permissive): %s",
		choraEnv, strings.Join(missing, ", "),
	)
	return nil
}
