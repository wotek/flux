package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/workflow"
)

// OutboxMessage simulates a durable outbox table record.
type OutboxMessage struct {
	Command any
}

// WorkflowStore simulates a database table for workflows and an outbox table for commands.
type WorkflowStore[W workflow.Workflow[W]] struct {
	mu      sync.RWMutex
	state   map[string]W
	outbox  []OutboxMessage
	cmdBus  *command.Bus // The background relay pushes here
	running bool
}

// New creates a memory-backed Workflow store.
// It accepts a CommandBus to simulate the background Outbox Relay.
func New[W workflow.Workflow[W]](cmdBus *command.Bus) *WorkflowStore[W] {
	return &WorkflowStore[W]{
		state:  make(map[string]W),
		outbox: make([]OutboxMessage, 0),
		cmdBus: cmdBus,
	}
}

func (s *WorkflowStore[W]) Load(ctx context.Context, id flux.Identifier) (W, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var zero W
	if state, exists := s.state[id.String()]; exists {
		if c, ok := any(state).(interface{ Clone() W }); ok {
			return c.Clone(), nil
		}
		return state, nil
	}

	// Instantiate new empty workflow
	return zero.New(), nil
}

func (s *WorkflowStore[W]) Save(ctx context.Context, workflowInstance W, commands []any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Atomically save state and outbox commands
	if c, ok := any(workflowInstance).(interface{ Clone() W }); ok {
		s.state[workflowInstance.Identifier().String()] = c.Clone()
	} else {
		s.state[workflowInstance.Identifier().String()] = workflowInstance
	}

	for _, cmd := range commands {
		s.outbox = append(s.outbox, OutboxMessage{Command: cmd})
	}

	return nil
}

// StartRelay starts a background goroutine simulating the Outbox Poller.
func (s *WorkflowStore[W]) StartRelay(ctx context.Context) {
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
					cmdCtx := command.NewContext(ctx, flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
					err := command.Execute(cmdCtx, s.cmdBus, msg.Command)
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
