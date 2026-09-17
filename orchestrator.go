package flux

import (
	"context"
	"fmt"
	"time"
)

// Orchestrator is the background worker that listens to the global event stream
// and routes events to the appropriate saga instances.
type Orchestrator struct {
	eventStore EventStore
	handlers   map[string]orchestratorHandler
}

// orchestratorHandler wraps the typed logic for a specific event
type orchestratorHandler struct {
	invoke func(ctx context.Context, env Envelope) error
}

// NewOrchestrator creates a new orchestrator engine.
func NewOrchestrator(eventStore EventStore) *Orchestrator {
	return &Orchestrator{
		eventStore: eventStore,
		handlers:   make(map[string]orchestratorHandler),
	}
}

// RegisterSagaHandler wires a specific event type to a saga's state transition.
// The store is provided here so the orchestrator knows how to load/save this specific saga type.
func RegisterSagaHandler[S Saga[S], E Event](o *Orchestrator, store SagaStore[S], handler func(ctx SagaContext, saga S, event E) error) {
	var event E
	name := event.Name()

	if _, exists := o.handlers[name]; exists {
		// In a real system, multiple sagas could listen to the same event.
		// For this spec, we just append or panic. We'll simplify and overwrite or warn.
		// We'll panic for simplicity to match projector behavior, though sagas could be many-to-one.
		panic(fmt.Sprintf("handler already registered for saga event %s", name))
	}

	o.handlers[name] = orchestratorHandler{
		invoke: func(ctx context.Context, env Envelope) error {
			id := env.CorrelationIdentifier
			if id.IsEmpty() {
				// Sagas require correlation IDs to know which instance to load
				return nil
			}

			// 1. Load Saga
			saga, err := store.Load(ctx, id)
			if err != nil {
				return fmt.Errorf("failed to load saga: %w", err)
			}

			// 2. Create Context
			sagaCtx := NewSagaContext(NewEventContext(ctx, env))

			// 3. Execute Handler
			domainEvent, ok := env.Event.(E)
			if !ok {
				return fmt.Errorf("invalid event type for saga handler")
			}

			if err := handler(sagaCtx, saga, domainEvent); err != nil {
				// If handler returns error, we abort the transaction (don't save)
				return err
			}

			// 4. Extract queued commands
			// sagaCtx is an interface, but we know it's *sagaContext internally
			var cmds []any
			if sctx, ok := sagaCtx.(*sagaContext); ok {
				cmds = sctx.QueuedCommands()
			}

			// 5. Transactionally save saga state and outbox commands
			if err := store.Save(ctx, saga, cmds); err != nil {
				return fmt.Errorf("failed to save saga state and outbox: %w", err)
			}

			return nil
		},
	}
}

// Start begins tailing the EventStore in the background.
// Note: As per API.md, Orchestrator relies on an external cursor or starts from a given position.
// We will start from position 0 for this minimal implementation.
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
						// Log and continue, or abort? For process managers, we typically log and continue,
						// or retry via DLQ. We will abort for strictness here.
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
