package flux_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
)

type contextKey string

func TestInstrumentation_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		inst flux.Instrumentation
		want bool
	}{
		{
			name: "valid trace and span",
			inst: flux.Instrumentation{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7", TraceFlags: "01"},
			want: true,
		},
		{
			name: "missing trace id",
			inst: flux.Instrumentation{TraceID: "", SpanID: "00f067aa0ba902b7", TraceFlags: "01"},
			want: false,
		},
		{
			name: "missing span id",
			inst: flux.Instrumentation{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "", TraceFlags: "01"},
			want: false,
		},
		{
			name: "empty",
			inst: flux.Instrumentation{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.inst.IsValid(); got != tt.want {
				t.Errorf("IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContext_WithInstrumentationAndWithParent(t *testing.T) {
	t.Parallel()

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::iam:1:usr:1")}
	corrID := flux.MustParseIdentifier("urn:corr::wf:1:corr:1")
	causID := flux.MustParseIdentifier("urn:caus::cmd:1:caus:1")
	inst := flux.Instrumentation{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: "01",
	}

	key := contextKey("request_id")
	parent1 := context.WithValue(context.Background(), key, "req-123")

	ctx := flux.NewContext(parent1, actor, corrID, causID, flux.WithInstrumentation(inst))

	if ctx.Actor().Identifier != actor.Identifier {
		t.Errorf("Actor = %v, want %v", ctx.Actor(), actor)
	}
	if ctx.CorrelationIdentifier() != corrID {
		t.Errorf("CorrelationIdentifier = %v, want %v", ctx.CorrelationIdentifier(), corrID)
	}
	if ctx.CausationIdentifier() != causID {
		t.Errorf("CausationIdentifier = %v, want %v", ctx.CausationIdentifier(), causID)
	}
	if ctx.Instrumentation() != inst {
		t.Errorf("Instrumentation = %+v, want %+v", ctx.Instrumentation(), inst)
	}
	if ctx.Value(key) != "req-123" {
		t.Errorf("Value(key) = %v, want 'req-123'", ctx.Value(key))
	}
	if ctx.Logger() == nil {
		t.Fatal("expected Logger to be non-nil")
	}

	// Reparent context
	parent2 := context.WithValue(context.Background(), key, "req-456")
	reparented := flux.WithParent(ctx, parent2)

	if reparented.Actor().Identifier != actor.Identifier {
		t.Errorf("reparented Actor = %v, want %v", reparented.Actor(), actor)
	}
	if reparented.CorrelationIdentifier() != corrID {
		t.Errorf("reparented CorrelationIdentifier = %v, want %v", reparented.CorrelationIdentifier(), corrID)
	}
	if reparented.CausationIdentifier() != causID {
		t.Errorf("reparented CausationIdentifier = %v, want %v", reparented.CausationIdentifier(), causID)
	}
	if reparented.Instrumentation() != inst {
		t.Errorf("reparented Instrumentation = %+v, want %+v", reparented.Instrumentation(), inst)
	}
	if reparented.Value(key) != "req-456" {
		t.Errorf("reparented Value(key) = %v, want 'req-456'", reparented.Value(key))
	}
}
