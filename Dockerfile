# syntax=docker/dockerfile:1.6
#
# chora-tenancy Dockerfile — Go service.
# Generated from chora-infra/templates/Dockerfile.go-service.
# DO NOT edit ad-hoc; sync changes back to the template.
#
# Build context = repo root (monorepo). The build uses a minimal go.work
# synthesised inside the image listing only this service + libs/chora-go-common.
# Standard invocation (chora-infra/scripts/build-publish-local.sh):
#   docker buildx build --platform=linux/amd64 \
#     -f services/chora-tenancy/Dockerfile \
#     --build-arg SERVICE_NAME=chora-tenancy \
#     --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
#     --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     -t asia-southeast1-docker.pkg.dev/chora-489812/chora-services/chora-tenancy:${TAG} \
#     --push \
#     .

ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.23
ARG SERVICE_NAME=chora-tenancy
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown

############################
# Stage 1 — build
############################
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

WORKDIR /src

RUN apk add --no-cache ca-certificates git

# Shared libs first (cache-friendly across rebuilds).
COPY libs/chora-go-common/ ./libs/chora-go-common/

# Generated Protobuf Go code (this service imports chora-contracts).
COPY chora-contracts/gen/go/ ./chora-contracts/gen/go/

# This service.
COPY services/${SERVICE_NAME}/ ./services/${SERVICE_NAME}/

# Synthesise a minimal go.work — service + shared lib only. This avoids the
# full repo's go.work (which lists ~30 modules that aren't all in this image).
RUN cat > /src/go.work <<EOWORK
go 1.26.1

use (
	./libs/chora-go-common
	./chora-contracts/gen/go
	./services/${SERVICE_NAME}
)
EOWORK

# Note: workspace mode is ON (default); the synthesised go.work resolves
# the libs/chora-go-common replace + dep graph. We do NOT run 
# because the dep set is already correct in the service's go.mod (locally
# verified via the full workspace).

WORKDIR /src/services/${SERVICE_NAME}

# Pre-fetch direct deps (workspace mode reads go.work + per-module go.mod).
#  is idempotent + uses the Go module proxy + cache.
RUN go mod download

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64
RUN go build -trimpath \
      -ldflags "-s -w \
        -X main.serviceName=${SERVICE_NAME} \
        -X main.gitSHA=${GIT_SHA} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/service \
      ./cmd/server

############################
# Stage 2 — runtime
############################
FROM gcr.io/distroless/static-debian12:nonroot

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.source="https://github.com/5007-Capstone/chora" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.vendor="Chora Platform" \
      org.opencontainers.image.licenses="UNLICENSED" \
      io.chora.service="${SERVICE_NAME}" \
      io.chora.git-sha="${GIT_SHA}" \
      io.chora.build-time="${BUILD_TIME}"

WORKDIR /

COPY --from=builder /out/service /service

# Per-domain PII_Closure_Map.yaml (CHO-1719) — the federated closure-saga
# subscriber loads it at the default relative path
# config/PII_Closure_Map.yaml (runtime WORKDIR is /). Without this COPY
# the subscriber boots DISABLED (PII map load error).
COPY --from=builder /src/services/${SERVICE_NAME}/config/PII_Closure_Map.yaml /config/PII_Closure_Map.yaml

USER nonroot:nonroot
ENTRYPOINT ["/service"]
