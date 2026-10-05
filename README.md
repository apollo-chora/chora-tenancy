# chora-tenancy

Tenancy, membership, entitlement, billing, and related platform services for Chora.

The service is designed to run locally with Docker Compose and uses environment variables for configuration. No cloud account or managed services (managed SQL, message broker, object storage, secret manager, or CI/CD) are required.

## Local stack

The default Compose stack contains:

- **chora-tenancy** — HTTP and gRPC service
- **PostgreSQL 18** — tenancy data, durable outbox, and subscriber idempotency
- **NATS JetStream** — local event publishing and subscriptions (streams provisioned by `nats-init`)

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
| `NATS_URL` | NATS JetStream event bus | `nats://nats:4222` |
| `S3_ENDPOINT` | S3-compatible object storage | `http://minio:9000` |
| `CHORA_TENANCY_EXPORT_BUCKET` | Bucket for ADR-205 exports (empty = disabled) | empty |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | `http://otel-collector:4317` |
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

## Event bus

Local messaging uses NATS JetStream. The event taxonomy (`chora.{domain}.{aggregate}.{event_type}.v{N}`) is unchanged, and the transport is brokered by `github.com/apollo-chora/chora-common/eventbus`.

`nats-init` provisions two streams: `CHORA_EVENTS` (subjects `chora.>`) and `CHORA_DLQ` (subjects `_dlq.>`, the dead-letter convention). Consumers are durable, created on demand by the service.

If `NATS_URL` is unset, the application falls back to its in-memory event bus where supported.

## Stripe

Stripe credentials are read directly from environment variables:

```env
STRIPE_API_KEY=
STRIPE_WEBHOOK_SECRET=
STRIPE_SUCCESS_URL=http://localhost:3000/billing/success
STRIPE_CANCEL_URL=http://localhost:3000/billing/cancel
```

Leaving the API key empty keeps the development Stripe stub enabled.

## Transaction exports

ADR-205 asynchronous transaction exports write a CSV/JSON object to S3-compatible
object storage (MinIO locally) and mint a presigned GET URL. Set
`CHORA_TENANCY_EXPORT_BUCKET` (with `S3_ENDPOINT` / credentials) to enable the
export worker; leaving the bucket empty keeps it disabled and the export RPCs
return `Unavailable`.

## Development

Run Go tests:

```sh
go test ./...
```

The service depends on the shared `chora-common` module and generated Chora contracts, resolved through Go modules (pinned pseudo-versions in `go.mod`), so no sibling checkout is required for local Go builds or Docker builds.

## Deployment philosophy

Runtime configuration belongs in environment variables, normally supplied through `.env` and Docker Compose.

Infrastructure-specific authentication is intentionally kept out of the service. In particular, the local deployment does not require:

- cloud credentials or federated identity
- a managed secret store
- managed SQL
- managed object storage
- managed message broker
- managed build/deploy pipelines
- Kubernetes

This keeps the service deployable on a normal Docker host while preserving its PostgreSQL, outbox, event, HTTP, and gRPC behavior.
