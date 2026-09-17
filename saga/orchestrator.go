package saga

import (
	"context"
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

// Orchestrator is the background worker that listens to the global event stream
// and routes events to the appropriate saga instances.
type Orchestrator struct {
	eventStore flux.EventStore
	handlers   map[string]orchestratorHandler
}

// orchestratorHandler wraps the typed logic for a specific event
type orchestratorHandler struct {
	invoke func(ctx context.Context, env flux.Envelope) error
}

// NewOrchestrator creates a new orchestrator engine.
func NewOrchestrator(eventStore flux.EventStore) *Orchestrator {
	return &Orchestrator{
		eventStore: eventStore,
		handlers:   make(map[string]orchestratorHandler),
	}
}

// RegisterHandler wires a specific event type to a saga's state transition.
func RegisterHandler[S Saga[S], E flux.Event](o *Orchestrator, store Store[S], handler func(ctx Context, saga S, event E) error) {
	var evt E
	name := evt.Name()

	if _, exists := o.handlers[name]; exists {
		panic(fmt.Sprintf("handler already registered for saga event %s", name))
	}

	o.handlers[name] = orchestratorHandler{
		invoke: func(ctx context.Context, env flux.Envelope) error {
			id := env.CorrelationIdentifier
			if id.IsEmpty() {
				// Sagas require correlation IDs to know which instance to load
				return nil
			}

			// 1. Load Saga
			sagaInstance, err := store.Load(ctx, id)
			if err != nil {
				return fmt.Errorf("failed to load saga: %w", err)
			}

			// 2. Create Context
			sagaCtx := NewContext(event.NewContext(ctx, env))

			// 3. Execute Handler
			domainEvent, ok := env.Event.(E)
			if !ok {
				return fmt.Errorf("invalid event type for saga handler")
			}

			if err := handler(sagaCtx, sagaInstance, domainEvent); err != nil {
				return err
			}

			// 4. Extract queued commands
			cmds := sagaCtx.QueuedCommands()

			// 5. Transactionally save saga state and outbox commands
			if err := store.Save(ctx, sagaInstance, cmds); err != nil {
				return fmt.Errorf("failed to save saga state and outbox: %w", err)
			}

			return nil
		},
	}
}

// RegisterSagaHandler is an alias for RegisterHandler.
func RegisterSagaHandler[S Saga[S], E flux.Event](o *Orchestrator, store Store[S], handler func(ctx Context, saga S, event E) error) {
	RegisterHandler(o, store, handler)
}

// Start begins tailing the EventStore in the background.
func (o *Orchestrator) Start(ctx context.Context) error {
	var position uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			iterator, err := o.eventStore.Stream(ctx, position)
			if err != nil {
				return fmt.Errorf("failed to stream events: %w", err)
			}

			processedAny := false
			for env, err := range iterator {
				if err != nil {
					return fmt.Errorf("stream iteration error: %w", err)
				}

				if handler, ok := o.handlers[env.Event.Name()]; ok {
					if err := handler.invoke(ctx, env); err != nil {
						return err
					}
				}

				position = env.Position
				processedAny = true
			}

			if !processedAny {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}
