package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wotek/flux"
)

// OutboxMessage simulates a durable outbox table record.
type OutboxMessage struct {
	Command any
}

// SagaStore simulates a database table for sagas and an outbox table for commands.
type SagaStore[S flux.Saga[S]] struct {
	mu      sync.RWMutex
	state   map[string]S
	outbox  []OutboxMessage
	cmdBus  *flux.CommandBus // The background relay pushes here
	running bool
}

// New creates a memory-backed Saga store.
// It accepts a CommandBus to simulate the background Outbox Relay.
func New[S flux.Saga[S]](cmdBus *flux.CommandBus) *SagaStore[S] {
	return &SagaStore[S]{
		state:  make(map[string]S),
		outbox: make([]OutboxMessage, 0),
		cmdBus: cmdBus,
	}
}

// NewSagaStore is an alias for New to maintain backwards compatibility.
func NewSagaStore[S flux.Saga[S]](cmdBus *flux.CommandBus) *SagaStore[S] {
	return New[S](cmdBus)
}

func (s *SagaStore[S]) Load(ctx context.Context, id flux.Identifier) (S, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var zero S
	if state, exists := s.state[id.String()]; exists {
		return state, nil
	}

	// Instantiate new empty saga
	return zero.New(), nil
}

func (s *SagaStore[S]) Save(ctx context.Context, saga S, commands []any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Atomically save state and outbox commands
	s.state[saga.Identifier().String()] = saga

	for _, cmd := range commands {
		s.outbox = append(s.outbox, OutboxMessage{Command: cmd})
	}

	return nil
}

// StartRelay starts a background goroutine simulating the Outbox Poller.
func (s *SagaStore[S]) StartRelay(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				s.mu.Lock()
				if len(s.outbox) > 0 {
					// Pop the first message
					msg := s.outbox[0]
					s.outbox = s.outbox[1:]
					s.mu.Unlock()

					// Execute command using a dummy context
					cmdCtx := flux.NewCommandContext(ctx, flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
					err := flux.ExecuteCommand(cmdCtx, s.cmdBus, msg.Command)
					if err != nil {
						fmt.Printf("[Outbox] failed to execute command %T: %v\n", msg.Command, err)
					}
				} else {
					s.mu.Unlock()
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
	}()
}
