package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
	"github.com/wotek/flux/event"
)

// CheckpointStore is a deprecated alias for [checkpoint.Store].
// Prefer importing and using [checkpoint.Store] directly.
type CheckpointStore = checkpoint.Store

// Orchestrator is the background worker that listens to the global event stream
// and routes events to the appropriate workflow instances.
type Orchestrator struct {
	mu         sync.RWMutex
	id         flux.Identifier
	eventStore flux.EventStore
	checkpoint checkpoint.Store
	handlers   map[string][]orchestratorHandler
}

// orchestratorHandler wraps the typed logic for a specific event.
type orchestratorHandler struct {
	invoke func(ctx context.Context, env flux.Envelope) error
}

// NewOrchestrator creates a new orchestrator engine with position checkpointing.
//
// Checkpoint Persistence:
// checkpoint must be non-nil. Pass checkpoint/store.New() for in-memory testing
// and local prototypes. Production environments must pass a durable [checkpoint.Store]
// implementation (e.g. MySQL or Redis) to guarantee progress persistence across restarts.
// A nil checkpoint is a programmer error and panics.
func NewOrchestrator(id flux.Identifier, eventStore flux.EventStore, checkpoint checkpoint.Store) *Orchestrator {
	if checkpoint == nil {
		panic("workflow: checkpoint store is required; pass checkpoint/store.New() for in-memory or a durable store for production")
	}
	return &Orchestrator{
		id:         id,
		eventStore: eventStore,
		checkpoint: checkpoint,
		handlers:   make(map[string][]orchestratorHandler),
	}
}

// New is an alias for NewOrchestrator to maintain explicit naming parity with projection.New.
func New(id flux.Identifier, eventStore flux.EventStore, checkpoint checkpoint.Store) *Orchestrator {
	return NewOrchestrator(id, eventStore, checkpoint)
}

// RegisterHandler wires a specific event type to a workflow's state transition.
// Multiple handlers can be registered for the same event name (e.g., routing one
// domain event to multiple distinct workflow types). Handlers are invoked in order
// of registration, failing fast if any handler returns an error.
//
// At-Least-Once Delivery & Idempotency:
// If an envelope matches multiple handlers and handler N fails, earlier handlers
// (1 to N-1) will have already committed their state and outbox commands. Because the
// orchestrator fails fast without advancing the checkpoint position, restarting the
// orchestrator will re-deliver the envelope to all registered handlers. All handlers
// must therefore be idempotent.
func RegisterHandler[W Workflow[W], E flux.Event](o *Orchestrator, store Store[W], handler func(ctx Context, workflow W, event E) error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var evt E
	name := evt.Name()

	h := orchestratorHandler{
		invoke: func(ctx context.Context, env flux.Envelope) error {
			id := env.CorrelationIdentifier
			if id.IsEmpty() {
				slog.WarnContext(ctx, "workflow skipped event missing correlation identifier",
					slog.String("event_name", env.Event.Name()),
					slog.String("event_id", env.Identifier.String()),
					slog.String("orchestrator_id", o.id.String()),
				)
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

	o.handlers[name] = append(o.handlers[name], h)
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
				handlers := slices.Clone(o.handlers[env.Event.Name()])
				o.mu.RUnlock()

				for _, h := range handlers {
					if err := h.invoke(ctx, env); err != nil {
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
