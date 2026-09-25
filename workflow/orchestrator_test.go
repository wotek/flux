package workflow_test

import (
	"context"
	"fmt"
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
func (w *OnboardingWorkflow) Clone() *OnboardingWorkflow {
	if w == nil {
		return nil
	}
	c := *w
	return &c
}

type TestWelcomeHandler struct {
	received chan string
	lastCtx  chan command.Context
}

func (h TestWelcomeHandler) Handle(ctx command.Context, cmd SendWelcomeEmail) error {
	if h.lastCtx != nil {
		h.lastCtx <- ctx
	}
	h.received <- cmd.Email
	return nil
}

func TestWorkflowOrchestrator(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmdBus := command.New()
	eventStore := eventstore.New()
	workflowStore := workflowstore.New[*OnboardingWorkflow](cmdBus)

	// Start the background relay
	workflowStore.StartRelay(ctx)

	orchID := flux.MustParseIdentifier("urn:flux::workflow:1:orchestrator:onboarding")
	orchestrator := workflow.NewOrchestrator(orchID, eventStore, workflowStore)

	// Register Workflow
	workflow.RegisterHandler(orchestrator, workflowStore, func(ctx workflow.Context, w *OnboardingWorkflow, e UserRegistered) error {
		w.ID = ctx.CorrelationIdentifier()
		w.Status = "AWAITING_WELCOME_EMAIL"

		// Safely queue the command
		workflow.EnqueueCommand(ctx, SendWelcomeEmail(e))
		return nil
	})

	// Register Command Handler to verify outbox delivery and metadata propagation
	cmdReceived := make(chan string, 1)
	ctxReceived := make(chan command.Context, 1)
	command.RegisterHandler(cmdBus, TestWelcomeHandler{received: cmdReceived, lastCtx: ctxReceived})

	// Start orchestrator
	go func() { _ = orchestrator.Start(ctx) }()

	// Append an event that will trigger the workflow
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::auth:1:user:caller")}
	eventID := flux.MustParseIdentifier("urn:event::auth:1:evt:e1")
	correlationID := flux.MustParseIdentifier("urn:user::auth:1:user:abc")
	logID := flux.MustParseIdentifier("urn:users:::::log")
	stream := flux.Stream{Identifier: logID}

	err := eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{
			Identifier:            eventID,
			Event:                 UserRegistered{Email: "test@example.com"},
			Actor:                 actor,
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
		t.Fatalf("timed out waiting for command execution from workflow outbox")
	}

	select {
	case cmdCtx := <-ctxReceived:
		if cmdCtx.Actor().Identifier != actor.Identifier {
			t.Errorf("expected actor %v, got %v", actor.Identifier, cmdCtx.Actor().Identifier)
		}
		if cmdCtx.CorrelationIdentifier() != correlationID {
			t.Errorf("expected correlation %v, got %v", correlationID, cmdCtx.CorrelationIdentifier())
		}
		if cmdCtx.CausationIdentifier() != eventID {
			t.Errorf("expected causation %v (event ID), got %v", eventID, cmdCtx.CausationIdentifier())
		}
	default:
		t.Errorf("failed to receive command context")
	}

	// Verify state was saved
	savedWorkflow, _ := workflowStore.Load(ctx, correlationID)
	if savedWorkflow.Status != "AWAITING_WELCOME_EMAIL" {
		t.Errorf("expected workflow status to be AWAITING_WELCOME_EMAIL, got %s", savedWorkflow.Status)
	}
}

func TestWorkflowOrchestrator_DurableCheckpoint(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventStore := eventstore.New()
	cmdBus := command.New()
	wfStore := workflowstore.New[*OnboardingWorkflow](cmdBus)
	checkpointStore := workflowstore.NewCheckpointStore()

	orchID := flux.MustParseIdentifier("urn:flux::workflow:1:orchestrator:checkpoint_test")
	orchestrator := workflow.NewOrchestrator(orchID, eventStore, checkpointStore)

	processedEvents := make(chan string, 10)
	workflow.RegisterHandler(orchestrator, wfStore, func(ctx workflow.Context, w *OnboardingWorkflow, e UserRegistered) error {
		processedEvents <- e.Email
		return nil
	})

	// Start orchestrator in background
	orchCtx, stopOrch := context.WithCancel(ctx)
	orchDone := make(chan struct{})
	go func() {
		_ = orchestrator.Start(orchCtx)
		close(orchDone)
	}()

	// Append two events
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:users:::::log")}
	corrID1 := flux.MustParseIdentifier("urn:user::auth:1:user:1")
	corrID2 := flux.MustParseIdentifier("urn:user::auth:1:user:2")
	_ = eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:evt:::::1"), Event: UserRegistered{Email: "user1@example.com"}, CorrelationIdentifier: corrID1},
		{Identifier: flux.MustParseIdentifier("urn:evt:::::2"), Event: UserRegistered{Email: "user2@example.com"}, CorrelationIdentifier: corrID2},
	})

	// Wait for both events
	for i := 0; i < 2; i++ {
		select {
		case <-processedEvents:
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for event %d", i+1)
		}
	}

	// Stop orchestrator
	stopOrch()
	<-orchDone

	// Verify checkpoint reached position 2
	pos, err := checkpointStore.GetPosition(ctx, orchID)
	if err != nil {
		t.Fatalf("failed to get checkpoint position: %v", err)
	}
	if pos != 2 {
		t.Fatalf("expected checkpoint position 2, got %d", pos)
	}

	// Start a new orchestrator instance with the SAME checkpoint store
	orchCtx2, stopOrch2 := context.WithCancel(ctx)
	defer stopOrch2()

	orch2 := workflow.NewOrchestrator(orchID, eventStore, checkpointStore)
	processedEvents2 := make(chan string, 10)
	workflow.RegisterHandler(orch2, wfStore, func(ctx workflow.Context, w *OnboardingWorkflow, e UserRegistered) error {
		processedEvents2 <- e.Email
		return nil
	})

	go func() { _ = orch2.Start(orchCtx2) }()

	// Ensure no events are replayed
	select {
	case email := <-processedEvents2:
		t.Fatalf("unexpected replayed event after restart: %s", email)
	case <-time.After(150 * time.Millisecond):
		// Expected: nothing replayed
	}

	// Append a 3rd event
	corrID3 := flux.MustParseIdentifier("urn:user::auth:1:user:3")
	_ = eventStore.Append(ctx, stream, 2, []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:evt:::::3"), Event: UserRegistered{Email: "user3@example.com"}, CorrelationIdentifier: corrID3},
	})

	select {
	case email := <-processedEvents2:
		if email != "user3@example.com" {
			t.Errorf("expected user3@example.com, got %s", email)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for event 3")
	}
}

func TestWorkflow_CloneIsolationOnHandlerFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventStore := eventstore.New()
	cmdBus := command.New()
	wfStore := workflowstore.New[*OnboardingWorkflow](cmdBus)

	orchID := flux.MustParseIdentifier("urn:flux::workflow:1:orchestrator:failure_test")
	orchestrator := workflow.NewOrchestrator(orchID, eventStore, wfStore)

	corrID := flux.MustParseIdentifier("urn:user::auth:1:user:isolated")

	// Pre-seed workflow in store with initial committed state
	initialWf := &OnboardingWorkflow{ID: corrID, Status: "COMMITTED"}
	if err := wfStore.Save(ctx, initialWf, nil); err != nil {
		t.Fatalf("failed to save initial workflow: %v", err)
	}

	// Register handler that mutates and then fails
	workflow.RegisterHandler(orchestrator, wfStore, func(ctx workflow.Context, w *OnboardingWorkflow, e UserRegistered) error {
		w.Status = "DIRTY_MUTATION"
		return fmt.Errorf("handler failed intentionally")
	})

	// Append event
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:users:::::isolated_log")}
	_ = eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:evt:::::fail1"), Event: UserRegistered{Email: "fail@example.com"}, CorrelationIdentifier: corrID},
	})

	// Run orchestrator - expect it to return the handler error
	err := orchestrator.Start(ctx)
	if err == nil {
		t.Fatalf("expected orchestrator to return handler error")
	}

	// Verify that workflow state in store was NOT corrupted
	loaded, err := wfStore.Load(ctx, corrID)
	if err != nil {
		t.Fatalf("failed to load workflow: %v", err)
	}
	if loaded.Status != "COMMITTED" {
		t.Errorf("expected workflow status to remain COMMITTED, got %s", loaded.Status)
	}

	// Also verify that mutating loaded reference doesn't affect subsequent Load
	loaded.Status = "EXTERNAL_MUTATION"
	loadedAgain, _ := wfStore.Load(ctx, corrID)
	if loadedAgain.Status != "COMMITTED" {
		t.Errorf("expected workflow status to remain COMMITTED after external mutation, got %s", loadedAgain.Status)
	}
}

type OrchestratorEvtA struct{}
func (e OrchestratorEvtA) Name() string { return "OrchestratorEvtA" }

type OrchestratorEvtB struct{}
func (e OrchestratorEvtB) Name() string { return "OrchestratorEvtB" }

func TestOrchestrator_ConcurrentRegistrationAndStart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventStore := eventstore.New()
	cmdBus := command.New()
	wfStore := workflowstore.New[*OnboardingWorkflow](cmdBus)

	orchID := flux.MustParseIdentifier("urn:flux::workflow:1:orchestrator:concurrent")
	orchestrator := workflow.NewOrchestrator(orchID, eventStore, wfStore)

	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:stream:::::orch_concurrent:1")}
	_ = eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:evt:::::oa1"), Event: OrchestratorEvtA{}, CorrelationIdentifier: flux.MustParseIdentifier("urn:corr:::::oa1")},
		{Identifier: flux.MustParseIdentifier("urn:evt:::::ob1"), Event: OrchestratorEvtB{}, CorrelationIdentifier: flux.MustParseIdentifier("urn:corr:::::ob1")},
	})

	go func() { _ = orchestrator.Start(ctx) }()

	go func() {
		workflow.RegisterHandler(orchestrator, wfStore, func(ctx workflow.Context, w *OnboardingWorkflow, e OrchestratorEvtA) error {
			return nil
		})
	}()

	go func() {
		workflow.RegisterHandler(orchestrator, wfStore, func(ctx workflow.Context, w *OnboardingWorkflow, e OrchestratorEvtB) error {
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
}
