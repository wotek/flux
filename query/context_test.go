package query_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/query"
)

type contextKey string

func TestQueryContext_PreservesFieldsAndReparents(t *testing.T) {
	t.Parallel()

	queryID := flux.MustParseIdentifier("urn:qry::order:1:get:1")
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::iam:1:usr:42")}
	corrID := flux.MustParseIdentifier("urn:corr::wf:1:corr:10")
	causID := flux.MustParseIdentifier("urn:caus::cmd:1:caus:9")
	inst := flux.Instrumentation{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: "01",
	}

	key := contextKey("request_id")
	parent1 := context.WithValue(context.Background(), key, "req-1")

	ctx := query.NewContext(parent1, queryID, actor, corrID, causID, flux.WithInstrumentation(inst))

	// Verify query.Context implements flux.Context
	var _ flux.Context = ctx

	if ctx.QueryIdentifier() != queryID {
		t.Errorf("QueryIdentifier = %v, want %v", ctx.QueryIdentifier(), queryID)
	}
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
	if ctx.Value(key) != "req-1" {
		t.Errorf("Value(key) = %v, want 'req-1'", ctx.Value(key))
	}

	// Reparent context
	parent2 := context.WithValue(context.Background(), key, "req-2")
	reparented := ctx.WithParent(parent2)

	if reparented.QueryIdentifier() != queryID {
		t.Errorf("reparented QueryIdentifier = %v, want %v", reparented.QueryIdentifier(), queryID)
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
	if reparented.Value(key) != "req-2" {
		t.Errorf("reparented Value(key) = %v, want 'req-2'", reparented.Value(key))
	}
}
