# chora-tenancy

Tenancy, membership, entitlement, billing, and related platform services for Chora.

The service is designed to run locally with Docker Compose and uses environment variables for configuration. No GCP account, Workload Identity, Secret Manager, Cloud SQL, Cloud Build, or Cloud Deploy setup is required.

## Local stack

The default Compose stack contains:

- **chora-tenancy** — HTTP and gRPC service
- **PostgreSQL 18** — tenancy data, durable outbox, and subscriber idempotency
- **Google Pub/Sub emulator** — local event publishing and subscriptions

Stripe is optional. When no Stripe API key is configured, the service uses its development stub.

## Requirements

- Docker with Docker Compose

## Configuration

Create the local environment file:

```sh
cp .env.example .env
```

The checked-in `.env.example` contains the complete local defaults. The actual `.env` file is ignored by Git.

Important variables:

| Variable | Purpose | Local default |
| --- | --- | --- |
| `PORT` | HTTP port | `8080` |
| `CHORA_GRPC_PORT` | gRPC port | `9090` |
| `CHORA_DB_DSN` | PostgreSQL connection string | Compose PostgreSQL |
| `CHORA_OUTBOX_DSN` | Durable outbox/inbox database | Same PostgreSQL instance |
| `PUBSUB_PROJECT_ID` | Logical Pub/Sub project | `chora-local` |
| `PUBSUB_EMULATOR_HOST` | Pub/Sub emulator endpoint | `pubsub:8085` |
| `STRIPE_API_KEY` | Optional Stripe API key | empty |
| `STRIPE_WEBHOOK_SECRET` | Optional Stripe webhook secret | empty |

## Run locally

From the repository root:

```sh
docker compose up --build
```

Run in the background:

```sh
docker compose up --build -d
```

HTTP is exposed on:

```text
http://localhost:8080
```

gRPC is exposed on port `9090` by default.

View service logs:

```sh
docker compose logs -f tenancy
```

Stop the stack:

```sh
docker compose down
```

Remove the local PostgreSQL volume as well:

```sh
docker compose down -v
```

## Database

PostgreSQL is the durable backing store for the service. The same database is also used for the transactional outbox and subscriber idempotency store.

The service reads its connection details from `CHORA_DB_DSN`. `CHORA_OUTBOX_DSN` may be set separately, but defaults operationally to the primary database DSN when omitted.

Database schema changes live in `migrations/`.

## Pub/Sub

Local messaging uses the Google Pub/Sub emulator. The existing Pub/Sub application interfaces are retained so event publishing and subscriber behavior remain compatible with the service's current contracts.

The Google Pub/Sub client recognizes `PUBSUB_EMULATOR_HOST` and connects to the emulator instead of the hosted Google service. Emulator traffic does not require Google credentials.

If `PUBSUB_PROJECT_ID` is unset, the application falls back to its in-memory event bus where supported.

## Stripe

Stripe credentials are read directly from environment variables:

```env
STRIPE_API_KEY=
STRIPE_WEBHOOK_SECRET=
STRIPE_SUCCESS_URL=http://localhost:3000/billing/success
STRIPE_CANCEL_URL=http://localhost:3000/billing/cancel
```

Leaving the API key empty keeps the development Stripe stub enabled.

There is no Secret Manager integration in the local runtime.

## Transaction exports

The previous transaction-export implementation depended directly on Google Cloud Storage. That integration has been disabled while the service is moved away from GCP.

A portable object-storage adapter can be added later if transaction exports are required.

## Development

Run Go tests:

```sh
go test ./...
```

The service depends on the shared `chora-common` module and generated Chora contracts, resolved through Go modules (pinned pseudo-versions in `go.mod`), so no sibling checkout is required for local Go builds or Docker builds.

## Deployment philosophy

Runtime configuration belongs in environment variables, normally supplied through `.env` and Docker Compose.

Infrastructure-specific authentication is intentionally kept out of the service. In particular, the local deployment does not require:

- Google Cloud credentials
- Workload Identity
- Secret Manager
- Cloud SQL
- Cloud Storage
- Cloud Build
- Cloud Deploy
- GKE
- build-evidence buckets or Binary Authorization

This keeps the service deployable on a normal Docker host while preserving its PostgreSQL, outbox, event, HTTP, and gRPC behavior.
