package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/workflow"
	workflowstore "github.com/wotek/flux/workflow/store"
)

type UserRegistered struct {
	Email string
}

func (e UserRegistered) Name() string { return "UserRegistered" }

type SendWelcomeEmail struct {
	Email string
}

type OnboardingWorkflow struct {
	ID     flux.Identifier
	Status string
}

func (w *OnboardingWorkflow) Identifier() flux.Identifier { return w.ID }
func (w *OnboardingWorkflow) New() *OnboardingWorkflow {
	return &OnboardingWorkflow{}
}

type TestWelcomeHandler struct {
	received chan string
}

func (h TestWelcomeHandler) Handle(ctx command.Context, cmd SendWelcomeEmail) error {
	h.received <- cmd.Email
	return nil
}

func TestWorkflowOrchestrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmdBus := command.New()
	eventStore := eventstore.New()
	workflowStore := workflowstore.New[*OnboardingWorkflow](cmdBus)

	// Start the background relay
	workflowStore.StartRelay(ctx)

	orchestrator := workflow.NewOrchestrator(eventStore)

	// Register Workflow
	workflow.RegisterHandler(orchestrator, workflowStore, func(ctx workflow.Context, w *OnboardingWorkflow, e UserRegistered) error {
		w.ID = ctx.CorrelationIdentifier()
		w.Status = "AWAITING_WELCOME_EMAIL"

		// Safely queue the command
		workflow.EnqueueCommand(ctx, SendWelcomeEmail(e))
		return nil
	})

	// Register Command Handler to verify outbox delivery
	cmdReceived := make(chan string, 1)
	command.RegisterHandler(cmdBus, TestWelcomeHandler{received: cmdReceived})

	// Start orchestrator
	go func() { _ = orchestrator.Start(ctx) }()

	// Append an event that will trigger the workflow
	correlationID := flux.MustParseIdentifier("urn:user::auth:1:user:abc")
	logID := flux.MustParseIdentifier("urn:users:::::log")
	stream := flux.Stream{Identifier: logID}

	err := eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{
			Event:                 UserRegistered{Email: "test@example.com"},
			CorrelationIdentifier: correlationID,
		},
	})

	if err != nil {
		t.Fatalf("failed to append event: %v", err)
	}

	// Wait for the orchestrator -> workflow -> outbox -> relay -> command_bus pipeline
	select {
	case email := <-cmdReceived:
		if email != "test@example.com" {
			t.Errorf("expected test@example.com, got %s", email)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for command execution from workflow outbox")
	}

	// Verify state was saved
	savedWorkflow, _ := workflowStore.Load(ctx, correlationID)
	if savedWorkflow.Status != "AWAITING_WELCOME_EMAIL" {
		t.Errorf("expected workflow status to be AWAITING_WELCOME_EMAIL, got %s", savedWorkflow.Status)
	}
}
