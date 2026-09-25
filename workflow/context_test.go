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
