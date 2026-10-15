package platform

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"

	"github.com/wotek/flux"
)

func cleanupGlobalOTel(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if otel.GetTextMapPropagator() != prevProp {
			otel.SetTextMapPropagator(prevProp)
		}
	})
}

func TestInitTracing_Disabled(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := InitTracing(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}

	plugin, err := TemporalOTelPlugin()
	if err != nil {
		t.Fatalf("unexpected plugin error: %v", err)
	}
	if plugin != nil {
		t.Fatalf("expected nil plugin when tracing is disabled, got %v", plugin)
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestInitTracing_Stdout(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "stdout")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := InitTracing(context.Background(), "test-stdout")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}

	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		t.Fatalf("expected *temporalotel.ReplaySafeTracerProvider, got %T", otel.GetTracerProvider())
	}

	plugin, err := TemporalOTelPlugin()
	if err != nil {
		t.Fatalf("unexpected plugin error: %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin when tracing is enabled")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestInitTracing_OTLP(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

	shutdown, err := InitTracing(context.Background(), "test-otlp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}

	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		t.Fatalf("expected *temporalotel.ReplaySafeTracerProvider, got %T", otel.GetTracerProvider())
	}

	plugin, err := TemporalOTelPlugin()
	if err != nil {
		t.Fatalf("unexpected plugin error: %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin when tracing is enabled")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestInitTracing_StdoutPrecedence(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "stdout")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

	shutdown, err := InitTracing(context.Background(), "test-precedence")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}

	plugin, err := TemporalOTelPlugin()
	if err != nil {
		t.Fatalf("unexpected plugin error: %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin when tracing is enabled")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestTemporalOTelPlugin_MissingInitTracing(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "stdout")

	_, err := TemporalOTelPlugin()
	if err == nil {
		t.Fatal("expected error when tracing is enabled but InitTracing was not called")
	}
}

func TestInstrumentationFromContext_Invalid(t *testing.T) {
	inst := InstrumentationFromContext(context.Background())
	if inst.IsValid() {
		t.Fatalf("expected invalid instrumentation from empty context, got %v", inst)
	}
}

func TestInstrumentationFromContext_Valid(t *testing.T) {
	cleanupGlobalOTel(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "stdout")
	shutdown, err := InitTracing(context.Background(), "test-inst")
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	ctx, span := otel.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()

	inst := InstrumentationFromContext(ctx)
	if !inst.IsValid() {
		t.Fatalf("expected valid instrumentation, got %v", inst)
	}
	sc := span.SpanContext()
	if inst.TraceID != sc.TraceID().String() {
		t.Errorf("TraceID = %q, want %q", inst.TraceID, sc.TraceID().String())
	}
	if inst.SpanID != sc.SpanID().String() {
		t.Errorf("SpanID = %q, want %q", inst.SpanID, sc.SpanID().String())
	}
	if inst.TraceFlags != sc.TraceFlags().String() {
		t.Errorf("TraceFlags = %q, want %q", inst.TraceFlags, sc.TraceFlags().String())
	}
}

func TestInstrumentationFromEnvelope(t *testing.T) {
	// Empty metadata
	env := flux.Envelope{}
	if inst := InstrumentationFromEnvelope(env); inst.IsValid() {
		t.Fatalf("expected invalid instrumentation for empty envelope, got %v", inst)
	}

	// Partial metadata (missing span_id)
	env.Metadata = map[string]string{
		flux.MetadataTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
	}
	if inst := InstrumentationFromEnvelope(env); inst.IsValid() {
		t.Fatalf("expected invalid instrumentation for partial metadata, got %v", inst)
	}

	// Valid metadata
	env.Metadata = map[string]string{
		flux.MetadataTraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		flux.MetadataSpanID:     "00f067aa0ba902b7",
		flux.MetadataTraceFlags: "01",
	}
	inst := InstrumentationFromEnvelope(env)
	if !inst.IsValid() {
		t.Fatalf("expected valid instrumentation, got %v", inst)
	}
	if inst.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID = %q, want %q", inst.TraceID, "4bf92f3577b34da6a3ce929d0e0e4736")
	}
	if inst.SpanID != "00f067aa0ba902b7" {
		t.Errorf("SpanID = %q, want %q", inst.SpanID, "00f067aa0ba902b7")
	}
	if inst.TraceFlags != "01" {
		t.Errorf("TraceFlags = %q, want %q", inst.TraceFlags, "01")
	}
}
