# syntax=docker/dockerfile:1.6

ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.23
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

ARG GIT_SHA
ARG BUILD_TIME

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY . .

RUN go mod tidy

ENV CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH}

RUN go build -trimpath \
    -ldflags "-s -w -X main.gitSHA=${GIT_SHA} -X main.buildTime=${BUILD_TIME}" \
    -o /out/service \
    ./cmd/server

############################
# Stage 2 - runtime
############################

FROM alpine:${ALPINE_VERSION}

ARG GIT_SHA
ARG BUILD_TIME

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

WORKDIR /

COPY --from=builder /out/service /service
COPY --from=builder /src/config/PII_Closure_Map.yaml /config/PII_Closure_Map.yaml

LABEL org.opencontainers.image.title="chora-tenancy" \
      org.opencontainers.image.source="https://github.com/apollo-chora/chora-tenancy" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      io.chora.service="chora-tenancy"

USER app:app
ENTRYPOINT ["/service"]
