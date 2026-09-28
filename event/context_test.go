package event_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

func TestEventContext_PreservesCausationAndClonesMetadata(t *testing.T) {
	t.Parallel()

	var _ event.EventMetadata = (event.Context)(nil)

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::iam:1:usr:1")}
	corrID := flux.MustParseIdentifier("urn:corr::wf:1:corr:100")
	causID := flux.MustParseIdentifier("urn:caus::cmd:1:caus:99")
	evtID := flux.MustParseIdentifier("urn:evt::stream:1:evt:42")
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:stream::orders:1:ord:1")}

	initialMetadata := map[string]string{
		"ip":     "127.0.0.1",
		"region": "us-east-1",
	}

	env := flux.Envelope{
		Identifier:            evtID,
		Stream:                stream,
		Revision:              5,
		Position:              105,
		Actor:                 actor,
		CorrelationIdentifier: corrID,
		CausationIdentifier:   causID,
		Event:                 PingEvent{},
		Metadata:              initialMetadata,
	}

	ctx := event.NewContext(context.Background(), env)

	if ctx.EventIdentifier() != evtID {
		t.Errorf("EventIdentifier = %v, want %v", ctx.EventIdentifier(), evtID)
	}
	if ctx.CausationIdentifier() != causID {
		t.Errorf("CausationIdentifier = %v, want %v", ctx.CausationIdentifier(), causID)
	}
	if ctx.CorrelationIdentifier() != corrID {
		t.Errorf("CorrelationIdentifier = %v, want %v", ctx.CorrelationIdentifier(), corrID)
	}
	if ctx.Actor().Identifier != actor.Identifier {
		t.Errorf("Actor = %v, want %v", ctx.Actor().Identifier, actor.Identifier)
	}
	if ctx.Revision() != 5 {
		t.Errorf("Revision = %d, want 5", ctx.Revision())
	}
	if ctx.Position() != 105 {
		t.Errorf("Position = %d, want 105", ctx.Position())
	}

	// Verify Metadata returns a clone
	meta := ctx.Metadata()
	meta["ip"] = "10.0.0.1"
	meta["new_key"] = "leak"

	metaAfter := ctx.Metadata()
	if metaAfter["ip"] != "127.0.0.1" {
		t.Errorf("expected internal metadata 'ip' to be 127.0.0.1, got %v", metaAfter["ip"])
	}
	if _, exists := metaAfter["new_key"]; exists {
		t.Errorf("expected internal metadata not to include mutated key")
	}
}

func TestEventContext_TraceHydrationAndWithParent(t *testing.T) {
	t.Parallel()

	type contextKey string
	key := contextKey("trace_baggage")

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::iam:1:usr:1")}
	corrID := flux.MustParseIdentifier("urn:corr::wf:1:corr:100")
	causID := flux.MustParseIdentifier("urn:caus::cmd:1:caus:99")
	evtID := flux.MustParseIdentifier("urn:evt::stream:1:evt:42")
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:stream::orders:1:ord:1")}

	env := flux.Envelope{
		Identifier:            evtID,
		Stream:                stream,
		Revision:              12,
		Position:              300,
		Actor:                 actor,
		CorrelationIdentifier: corrID,
		CausationIdentifier:   causID,
		Event:                 PingEvent{},
		Metadata: map[string]string{
			flux.MetadataTraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
			flux.MetadataSpanID:     "00f067aa0ba902b7",
			flux.MetadataTraceFlags: "01",
		},
	}

	parent1 := context.WithValue(context.Background(), key, "span-parent-1")
	ctx := event.NewContext(parent1, env)

	// Verify trace metadata was hydrated into Instrumentation
	inst := ctx.Instrumentation()
	if !inst.IsValid() {
		t.Fatal("expected Instrumentation to be valid")
	}
	if inst.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID = %s, want 4bf92f3577b34da6a3ce929d0e0e4736", inst.TraceID)
	}
	if inst.SpanID != "00f067aa0ba902b7" {
		t.Errorf("SpanID = %s, want 00f067aa0ba902b7", inst.SpanID)
	}
	if inst.TraceFlags != "01" {
		t.Errorf("TraceFlags = %s, want 01", inst.TraceFlags)
	}
	if ctx.Value(key) != "span-parent-1" {
		t.Errorf("Value = %v, want span-parent-1", ctx.Value(key))
	}

	// Reparent context
	parent2 := context.WithValue(context.Background(), key, "span-parent-2")
	reparented := ctx.WithParent(parent2)

	var _ flux.Context = reparented

	if reparented.EventIdentifier() != evtID {
		t.Errorf("reparented EventIdentifier = %v, want %v", reparented.EventIdentifier(), evtID)
	}
	if reparented.Stream() != stream {
		t.Errorf("reparented Stream = %v, want %v", reparented.Stream(), stream)
	}
	if reparented.Revision() != 12 {
		t.Errorf("reparented Revision = %d, want 12", reparented.Revision())
	}
	if reparented.Position() != 300 {
		t.Errorf("reparented Position = %d, want 300", reparented.Position())
	}
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
	if reparented.Value(key) != "span-parent-2" {
		t.Errorf("reparented Value = %v, want span-parent-2", reparented.Value(key))
	}
}

func TestEventContext_PartialMetadataNotHydrated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name: "flags only",
			metadata: map[string]string{
				flux.MetadataTraceFlags: "01",
			},
		},
		{
			name: "trace id only",
			metadata: map[string]string{
				flux.MetadataTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			},
		},
		{
			name: "span id only",
			metadata: map[string]string{
				flux.MetadataSpanID: "00f067aa0ba902b7",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := flux.Envelope{
				Identifier: flux.MustParseIdentifier("urn:evt::stream:1:evt:1"),
				Event:      PingEvent{},
				Metadata:   tt.metadata,
			}
			ctx := event.NewContext(context.Background(), env)
			if ctx.Instrumentation().IsValid() {
				t.Errorf("expected invalid instrumentation for partial metadata, got %+v", ctx.Instrumentation())
			}
			if ctx.Instrumentation() != (flux.Instrumentation{}) {
				t.Errorf("expected zero-value instrumentation, got %+v", ctx.Instrumentation())
			}
		})
	}
}


