package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

// CheckpointStore persists and retrieves the stream position reached by an orchestrator.
type CheckpointStore interface {
	// GetPosition returns the last successfully processed event stream position.
	GetPosition(ctx context.Context, id flux.Identifier) (uint64, error)

	// SetPosition updates the checkpoint position for the orchestrator.
	SetPosition(ctx context.Context, id flux.Identifier, position uint64) error
}

type inMemoryCheckpoint struct {
	mu        sync.RWMutex
	positions map[string]uint64
}

func (c *inMemoryCheckpoint) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.positions[id.String()], nil
}

func (c *inMemoryCheckpoint) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.positions == nil {
		c.positions = make(map[string]uint64)
	}
	c.positions[id.String()] = position
	return nil
}

// Orchestrator is the background worker that listens to the global event stream
// and routes events to the appropriate workflow instances.
type Orchestrator struct {
	mu         sync.RWMutex
	id         flux.Identifier
	eventStore flux.EventStore
	checkpoint CheckpointStore
	handlers   map[string]orchestratorHandler
}

// orchestratorHandler wraps the typed logic for a specific event.
type orchestratorHandler struct {
	invoke func(ctx context.Context, env flux.Envelope) error
}

// NewOrchestrator creates a new orchestrator engine with position checkpointing.
func NewOrchestrator(id flux.Identifier, eventStore flux.EventStore, checkpoint CheckpointStore) *Orchestrator {
	if checkpoint == nil {
		checkpoint = &inMemoryCheckpoint{positions: make(map[string]uint64)}
	}
	return &Orchestrator{
		id:         id,
		eventStore: eventStore,
		checkpoint: checkpoint,
		handlers:   make(map[string]orchestratorHandler),
	}
}

// New is an alias for NewOrchestrator to maintain explicit naming parity with projection.New.
func New(id flux.Identifier, eventStore flux.EventStore, checkpoint CheckpointStore) *Orchestrator {
	return NewOrchestrator(id, eventStore, checkpoint)
}

// RegisterHandler wires a specific event type to a workflow's state transition.
func RegisterHandler[W Workflow[W], E flux.Event](o *Orchestrator, store Store[W], handler func(ctx Context, workflow W, event E) error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var evt E
	name := evt.Name()

	if _, exists := o.handlers[name]; exists {
		panic(fmt.Sprintf("handler already registered for workflow event %s", name))
	}

	o.handlers[name] = orchestratorHandler{
		invoke: func(ctx context.Context, env flux.Envelope) error {
			id := env.CorrelationIdentifier
			if id.IsEmpty() {
				// Workflows require correlation IDs to know which instance to load
				return nil
			}

			// 1. Load Workflow
			workflowInstance, err := store.Load(ctx, id)
			if err != nil {
				return fmt.Errorf("failed to load workflow: %w", err)
			}

			// 2. Create Context
			workflowCtx := NewContext(event.NewContext(ctx, env))

			// 3. Execute Handler
			domainEvent, ok := env.Event.(E)
			if !ok {
				return fmt.Errorf("%w: for workflow handler", flux.ErrInvalidEvent)
			}

			if err := handler(workflowCtx, workflowInstance, domainEvent); err != nil {
				return err
			}

			// 4. Extract queued commands
			cmds := workflowCtx.QueuedCommands()

			// 5. Transactionally save workflow state and outbox commands
			if err := store.Save(workflowCtx, workflowInstance, cmds); err != nil {
				return fmt.Errorf("failed to save workflow state and outbox: %w", err)
			}

			return nil
		},
	}
}

// Start begins tailing the EventStore in the background.
func (o *Orchestrator) Start(ctx context.Context) error {
	position, err := o.checkpoint.GetPosition(ctx, o.id)
	if err != nil {
		return fmt.Errorf("failed to get initial orchestrator position: %w", err)
	}

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

				o.mu.RLock()
				handler, ok := o.handlers[env.Event.Name()]
				o.mu.RUnlock()

				if ok {
					if err := handler.invoke(ctx, env); err != nil {
						return err
					}
				}

				position = env.Position
				if err := o.checkpoint.SetPosition(ctx, o.id, position); err != nil {
					return fmt.Errorf("failed to persist orchestrator position: %w", err)
				}
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
