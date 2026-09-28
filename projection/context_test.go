package projection_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/projection"
)

type contextKey string

type dummyEvent struct{}

func (d dummyEvent) Name() string { return "DummyEvent" }

func TestProjectionContext_PreservesFieldsAndReparents(t *testing.T) {
	t.Parallel()

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::iam:1:usr:1")}
	corrID := flux.MustParseIdentifier("urn:corr::wf:1:corr:100")
	causID := flux.MustParseIdentifier("urn:caus::cmd:1:caus:99")
	evtID := flux.MustParseIdentifier("urn:evt::stream:1:evt:42")
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:stream::orders:1:ord:1")}
	inst := flux.Instrumentation{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: "01",
	}

	env := flux.Envelope{
		Identifier:            evtID,
		Stream:                stream,
		Revision:              7,
		Position:              140,
		Actor:                 actor,
		CorrelationIdentifier: corrID,
		CausationIdentifier:   causID,
		Event:                 dummyEvent{},
		Metadata: map[string]string{
			flux.MetadataTraceID:    inst.TraceID,
			flux.MetadataSpanID:     inst.SpanID,
			flux.MetadataTraceFlags: inst.TraceFlags,
			"custom":                "data",
		},
	}

	key := contextKey("tx_id")
	parent1 := context.WithValue(context.Background(), key, "tx-1")
	evtCtx := event.NewContext(parent1, env)
	projCtx := projection.NewContext(evtCtx)

	var _ event.EventMetadata = (projection.Context)(nil)
	var _ event.EventMetadata = projCtx
	var _ flux.Context = projCtx

	if projCtx.EventIdentifier() != evtID {
		t.Errorf("EventIdentifier = %v, want %v", projCtx.EventIdentifier(), evtID)
	}
	if projCtx.Stream() != stream {
		t.Errorf("Stream = %v, want %v", projCtx.Stream(), stream)
	}
	if projCtx.Revision() != 7 {
		t.Errorf("Revision = %d, want 7", projCtx.Revision())
	}
	if projCtx.Position() != 140 {
		t.Errorf("Position = %d, want 140", projCtx.Position())
	}
	if projCtx.Actor().Identifier != actor.Identifier {
		t.Errorf("Actor = %v, want %v", projCtx.Actor(), actor)
	}
	if projCtx.CorrelationIdentifier() != corrID {
		t.Errorf("CorrelationIdentifier = %v, want %v", projCtx.CorrelationIdentifier(), corrID)
	}
	if projCtx.CausationIdentifier() != causID {
		t.Errorf("CausationIdentifier = %v, want %v", projCtx.CausationIdentifier(), causID)
	}
	if projCtx.Instrumentation() != inst {
		t.Errorf("Instrumentation = %+v, want %+v", projCtx.Instrumentation(), inst)
	}
	if projCtx.Metadata()["custom"] != "data" {
		t.Errorf("Metadata['custom'] = %v, want 'data'", projCtx.Metadata()["custom"])
	}
	if projCtx.Value(key) != "tx-1" {
		t.Errorf("Value = %v, want 'tx-1'", projCtx.Value(key))
	}

	// Reparent context
	parent2 := context.WithValue(context.Background(), key, "tx-2")
	reparented := projCtx.WithParent(parent2)

	if reparented.EventIdentifier() != evtID {
		t.Errorf("reparented EventIdentifier = %v, want %v", reparented.EventIdentifier(), evtID)
	}
	if reparented.Stream() != stream {
		t.Errorf("reparented Stream = %v, want %v", reparented.Stream(), stream)
	}
	if reparented.Revision() != 7 {
		t.Errorf("reparented Revision = %d, want 7", reparented.Revision())
	}
	if reparented.Position() != 140 {
		t.Errorf("reparented Position = %d, want 140", reparented.Position())
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
	if reparented.Metadata()["custom"] != "data" {
		t.Errorf("reparented Metadata['custom'] = %v, want 'data'", reparented.Metadata()["custom"])
	}
	if reparented.Value(key) != "tx-2" {
		t.Errorf("reparented Value = %v, want 'tx-2'", reparented.Value(key))
	}
}
