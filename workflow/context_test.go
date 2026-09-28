package workflow_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/workflow"
)

func TestWorkflowContext_QueuedCommandsDefensiveCopy(t *testing.T) {
	t.Parallel()

	env := flux.Envelope{
		Identifier:            flux.MustParseIdentifier("urn:evt::stream:1:evt:1"),
		CorrelationIdentifier: flux.MustParseIdentifier("urn:corr::wf:1:corr:1"),
		Event:                 UserRegistered{Email: "test@example.com"},
	}
	evtCtx := event.NewContext(context.Background(), env)
	wfCtx := workflow.NewContext(evtCtx)

	workflow.EnqueueCommand(wfCtx, SendWelcomeEmail{Email: "test@example.com"})

	cmds := wfCtx.QueuedCommands()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 queued command, got %d", len(cmds))
	}

	// Mutate returned slice
	cmds[0] = "mutated"
	_ = append(cmds, "appended")

	// Ensure internal queue is not affected
	cmdsAfter := wfCtx.QueuedCommands()
	if len(cmdsAfter) != 1 {
		t.Fatalf("expected internal queue to still have 1 command, got %d", len(cmdsAfter))
	}
	if _, ok := cmdsAfter[0].(SendWelcomeEmail); !ok {
		t.Errorf("expected command to remain SendWelcomeEmail, got %T", cmdsAfter[0])
	}
}

func TestWorkflowContext_WithParent_PreservesFieldsAndSharesQueue(t *testing.T) {
	t.Parallel()

	type contextKey string
	key := contextKey("span_token")

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
		Revision:              3,
		Position:              80,
		Actor:                 actor,
		CorrelationIdentifier: corrID,
		CausationIdentifier:   causID,
		Event:                 UserRegistered{Email: "test@example.com"},
		Metadata: map[string]string{
			flux.MetadataTraceID:    inst.TraceID,
			flux.MetadataSpanID:     inst.SpanID,
			flux.MetadataTraceFlags: inst.TraceFlags,
		},
	}

	parent1 := context.WithValue(context.Background(), key, "span-1")
	evtCtx := event.NewContext(parent1, env)
	wfCtx := workflow.NewContext(evtCtx)

	var _ event.EventMetadata = (workflow.Context)(nil)
	var _ event.EventMetadata = wfCtx
	var _ flux.Context = wfCtx

	// Enqueue first command on parent wfCtx
	workflow.EnqueueCommand(wfCtx, SendWelcomeEmail{Email: "first@example.com"})

	// Reparent context (e.g. within an OpenTelemetry span wrapper)
	parent2 := context.WithValue(context.Background(), key, "span-2")
	childWfCtx := wfCtx.WithParent(parent2)

	// Verify all metadata is preserved on child
	if childWfCtx.EventIdentifier() != evtID {
		t.Errorf("child EventIdentifier = %v, want %v", childWfCtx.EventIdentifier(), evtID)
	}
	if childWfCtx.Stream() != stream {
		t.Errorf("child Stream = %v, want %v", childWfCtx.Stream(), stream)
	}
	if childWfCtx.Revision() != 3 {
		t.Errorf("child Revision = %d, want 3", childWfCtx.Revision())
	}
	if childWfCtx.Position() != 80 {
		t.Errorf("child Position = %d, want 80", childWfCtx.Position())
	}
	if childWfCtx.Actor().Identifier != actor.Identifier {
		t.Errorf("child Actor = %v, want %v", childWfCtx.Actor(), actor)
	}
	if childWfCtx.CorrelationIdentifier() != corrID {
		t.Errorf("child CorrelationIdentifier = %v, want %v", childWfCtx.CorrelationIdentifier(), corrID)
	}
	if childWfCtx.CausationIdentifier() != causID {
		t.Errorf("child CausationIdentifier = %v, want %v", childWfCtx.CausationIdentifier(), causID)
	}
	if childWfCtx.Instrumentation() != inst {
		t.Errorf("child Instrumentation = %+v, want %+v", childWfCtx.Instrumentation(), inst)
	}
	if childWfCtx.Value(key) != "span-2" {
		t.Errorf("child Value = %v, want 'span-2'", childWfCtx.Value(key))
	}

	// Enqueue second command on reparented child context
	workflow.EnqueueCommand(childWfCtx, SendWelcomeEmail{Email: "second@example.com"})

	// Verify that parent wfCtx sees BOTH commands
	parentCmds := wfCtx.QueuedCommands()
	if len(parentCmds) != 2 {
		t.Fatalf("expected parent wfCtx to have 2 commands, got %d", len(parentCmds))
	}
	if parentCmds[0].(SendWelcomeEmail).Email != "first@example.com" {
		t.Errorf("cmd[0] = %v, want first@example.com", parentCmds[0])
	}
	if parentCmds[1].(SendWelcomeEmail).Email != "second@example.com" {
		t.Errorf("cmd[1] = %v, want second@example.com", parentCmds[1])
	}

	// Also verify child sees both
	childCmds := childWfCtx.QueuedCommands()
	if len(childCmds) != 2 {
		t.Fatalf("expected childWfCtx to have 2 commands, got %d", len(childCmds))
	}
}

