package event_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

func TestEventContext_PreservesCausationAndClonesMetadata(t *testing.T) {
	t.Parallel()

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
