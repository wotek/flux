package flux

import (
	"context"
	"fmt"
	"time"
)

// Projector is the background worker that powers a Projection.
// It tails the EventStore starting from the ProjectionStore.GetPosition().
type Projector struct {
	id         Identifier
	eventStore EventStore
	projStore  ProjectionStore
	handlers   map[string]any // maps Event.Name() to func(ProjectionContext, E) error
}

// NewProjector creates a new Projector instance.
func NewProjector(id Identifier, eventStore EventStore, projStore ProjectionStore) *Projector {
	return &Projector{
		id:         id,
		eventStore: eventStore,
		projStore:  projStore,
		handlers:   make(map[string]any),
	}
}

// RegisterProjectionHandler wires a specific event type to the projection's logic.
func RegisterProjectionHandler[E Event](p *Projector, handler func(ctx ProjectionContext, event E) error) {
	var event E
	name := event.Name()
	if _, exists := p.handlers[name]; exists {
		panic(fmt.Sprintf("handler already registered for projection event %s", name))
	}

	wrapper := func(ctx ProjectionContext, rawEvent any) error {
		return handler(ctx, rawEvent.(E))
	}

	p.handlers[name] = wrapper
}

// Start begins tailing the EventStore in the background until the context is canceled.
func (p *Projector) Start(ctx context.Context) error {
	position, err := p.projStore.GetPosition(ctx, p.id)
	if err != nil {
		return fmt.Errorf("failed to get initial projection position: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continuous tailing would ideally use a long-polling or subscription mechanism.
			// For this spec, we poll the stream.
			iterator, err := p.eventStore.Stream(ctx, position)
			if err != nil {
				return fmt.Errorf("failed to stream events: %w", err)
			}

			processedAny := false
			for env, err := range iterator {
				if err != nil {
					return fmt.Errorf("stream iteration error: %w", err)
				}

				if err := p.processEnvelope(ctx, env); err != nil {
					return err // Stop projection on error
				}

				position = env.Position
				processedAny = true
			}

			// If no events were processed, wait a bit before polling again to prevent a tight loop
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

func (p *Projector) processEnvelope(ctx context.Context, env Envelope) error {
	h, exists := p.handlers[env.Event.Name()]
	if !exists {
		// Ignore events we don't care about
		return p.projStore.Update(ctx, p.id, env, func(txCtx context.Context) error {
			return nil
		})
	}

	return p.projStore.Update(ctx, p.id, env, func(txCtx context.Context) error {
		projCtx := NewProjectionContext(NewEventContext(txCtx, env))

		// Because we store closures of type untypedProjectionHandler, we can safely
		// assert and execute directly without reflection.
		wrapper := h.(func(ProjectionContext, any) error)
		return wrapper(projCtx, env.Event)
	})
}
