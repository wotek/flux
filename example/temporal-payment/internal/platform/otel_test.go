package platform

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
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
