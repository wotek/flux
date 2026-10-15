package platform

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/interceptor/tracing"
)

// TracingEnabled reports whether OpenTelemetry tracing is enabled by environment.
func TracingEnabled() bool {
	return os.Getenv("OTEL_TRACES_EXPORTER") == "stdout" || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
}

// InitTracing configures the global TracerProvider when tracing env is set.
// Tracing is enabled when:
//   - OTEL_TRACES_EXPORTER=stdout (console exporter for local dev/demo)
//   - OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g. http://localhost:4318 for Jaeger OTLP HTTP)
//
// When neither is set, tracing is disabled and a no-op shutdown function is returned.
// The returned shutdown function is always non-nil.
func InitTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	noopShutdown := func(context.Context) error { return nil }

	var (
		exp sdktrace.SpanExporter
		err error
	)

	// If both are set, stdout exporter takes precedence over OTLP.
	switch {
	case os.Getenv("OTEL_TRACES_EXPORTER") == "stdout":
		exp, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return noopShutdown, fmt.Errorf("create stdout trace exporter: %w", err)
		}
	case os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "":
		// otlptracehttp.New naturally respects OTEL_EXPORTER_OTLP_ENDPOINT and
		// appends the standard /v1/traces signal path per the OTel specification.
		exp, err = otlptracehttp.New(ctx)
		if err != nil {
			return noopShutdown, fmt.Errorf("create OTLP trace exporter: %w", err)
		}
	default:
		return noopShutdown, nil
	}

	if serviceName == "" {
		serviceName = "flux-temporal-payment"
	}

	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
		),
	)
	if err != nil {
		_ = exp.Shutdown(ctx)
		return noopShutdown, fmt.Errorf("build otel resource: %w", err)
	}

	// Temporal's opentelemetry-v2 plugin requires a ReplaySafeTracerProvider
	// to ensure workflow replay determinism.
	tp := temporalotel.NewReplaySafeTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown := func(shutdownCtx context.Context) error {
		return tp.Shutdown(shutdownCtx)
	}

	return shutdown, nil
}

// TemporalOTelPlugin returns Temporal's opentelemetry-v2 client plugin with Temporal spans enabled.
// When tracing is disabled, it returns nil to skip registration, as NewPlugin requires
// the global provider to be a live ReplaySafeTracerProvider.
func TemporalOTelPlugin() (client.Plugin, error) {
	if !TracingEnabled() {
		// Tracing is disabled; skip plugin registration to avoid requiring a live ReplaySafeTracerProvider.
		return nil, nil
	}
	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		return nil, fmt.Errorf("tracing is enabled but global TracerProvider is not a *ReplaySafeTracerProvider (call InitTracing first)")
	}
	return temporalotel.NewPlugin(temporalotel.PluginOptions{
		TracerOptions: tracing.TracerOptions{
			AddTemporalSpans: true,
		},
	})
}
