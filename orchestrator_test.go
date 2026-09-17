package flux_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/store/inmemory"
)

type UserRegistered struct {
	Email string
}

func (e UserRegistered) Name() string { return "UserRegistered" }

type SendWelcomeEmail struct {
	Email string
}

type OnboardingSaga struct {
	ID     flux.Identifier
	Status string
}

func (s *OnboardingSaga) Identifier() flux.Identifier { return s.ID }
func (s *OnboardingSaga) New() *OnboardingSaga {
	return &OnboardingSaga{}
}

type TestWelcomeHandler struct {
	received chan string
}

func (h TestWelcomeHandler) Handle(ctx flux.CommandContext, cmd SendWelcomeEmail) error {
	h.received <- cmd.Email
	return nil
}

func TestSagaOrchestrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmdBus := flux.NewCommandBus()
	eventStore := inmemory.NewEventStore()
	sagaStore := inmemory.NewSagaStore[*OnboardingSaga](cmdBus)

	// Start the background relay
	sagaStore.StartRelay(ctx)

	orchestrator := flux.NewOrchestrator(eventStore)

	// Register Saga
	flux.RegisterSagaHandler(orchestrator, sagaStore, func(ctx flux.SagaContext, saga *OnboardingSaga, e UserRegistered) error {
		saga.ID = ctx.CorrelationIdentifier()
		saga.Status = "AWAITING_WELCOME_EMAIL"

		// Safely queue the command
		flux.EnqueueCommand(ctx, SendWelcomeEmail{Email: e.Email})
		return nil
	})

	// Register Command Handler to verify outbox delivery
	cmdReceived := make(chan string, 1)
	flux.RegisterCommandHandler(cmdBus, TestWelcomeHandler{received: cmdReceived})

	// Start orchestrator
	go orchestrator.Start(ctx)

	// Append an event that will trigger the saga
	correlationID := flux.NewIdentifierFromString("urn:user::auth:1:user:abc")
	logID := flux.NewIdentifierFromString("urn:users:::::log")
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

	// Wait for the orchestrator -> saga -> outbox -> relay -> command_bus pipeline
	select {
	case email := <-cmdReceived:
		if email != "test@example.com" {
			t.Errorf("expected test@example.com, got %s", email)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for command execution from saga outbox")
	}

	// Verify state was saved
	savedSaga, _ := sagaStore.Load(ctx, correlationID)
	if savedSaga.Status != "AWAITING_WELCOME_EMAIL" {
		t.Errorf("expected saga status to be AWAITING_WELCOME_EMAIL, got %s", savedSaga.Status)
	}
}
