// OTLP tracing bootstrap for chora-tenancy.
//
// This is the local replacement for chora-common/observability (which binds a
// cloud-specific exporter + metadata service). It uses the standard OTLP/gRPC
// exporter pointed at OTEL_EXPORTER_OTLP_ENDPOINT — the local OTel Collector
// by default, any OTLP endpoint in production. When the endpoint is unset,
// spans go to stdout so the service stays runnable in dev.
package observability

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/bootstrap"
	cgctracing "github.com/apollo-chora/chora-common/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	defaultInitTimeout     = 15 * time.Second
	defaultShutdownTimeout = 5 * time.Second
)

// InitOTLP wires the trace exporter + a global TracerProvider and returns a
// shutdown func the caller MUST defer in main(). When
// OTEL_EXPORTER_OTLP_ENDPOINT is unset it falls back to a stdout exporter
// (dev). serviceName MUST be non-empty.
func InitOTLP(serviceName, version string) (func() error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultInitTimeout)
	defer cancel()
	shutdown, err := initTracerProvider(ctx, serviceName, version)
	if err != nil {
		return nil, err
	}
	return func() error {
		sctx, c := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer c()
		return shutdown(sctx)
	}, nil
}

// InitOTLPAsync is the fail-soft non-blocking variant. OTLP init runs in its
// own goroutine under its own deadline (CHORA_OTLP_INIT_TIMEOUT_SECONDS,
// default 15s); a timeout or init error degrades to a no-op shutdown so the
// rest of bootstrap is never held hostage by a slow telemetry endpoint.
func InitOTLPAsync(ctx context.Context, serviceName, version string) *bootstrap.OTLPHandle {
	return bootstrap.StartOTLPAsync(ctx, bootstrap.OTLPOptions{
		InitFunc: func(initCtx context.Context) (func(context.Context) error, error) {
			return initTracerProvider(initCtx, serviceName, version)
		},
	})
}

// initTracerProvider builds the exporter + TracerProvider and installs them
// globally. The returned shutdown drains the provider.
func initTracerProvider(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	if strings.TrimSpace(serviceName) == "" {
		return nil, errors.New("observability: service name required")
	}

	exporter, err := newExporter(ctx)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
	))
	if err != nil {
		// A resource-build failure must not block startup; fall back to the
		// default resource so spans still flow.
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

// newExporter returns the OTLP/gRPC exporter when an endpoint is configured,
// and a stdout exporter otherwise (dev). A schemeless / http:// endpoint is
// treated as plaintext; https:// enables TLS.
func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	raw := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	}
	if raw == "" {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}

	opts := []otlptracegrpc.Option{}
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(u.Host))
		if u.Scheme != "https" {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(raw), otlptracegrpc.WithInsecure())
	}
	return otlptracegrpc.New(ctx, opts...)
}

// HTTPMiddleware re-exports the broker-neutral traceparent middleware so
// services importing this package get the same request-span behaviour.
func HTTPMiddleware() func(next http.Handler) http.Handler {
	return cgctracing.Middleware()
}
