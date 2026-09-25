package store

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/workflow"
)

// OutboxMessage represents a durable outbox record containing an enqueued command
// along with its causal, correlation, and actor tracing metadata.
type OutboxMessage struct {
	ID                    flux.Identifier
	Command               any
	Actor                 flux.Actor
	CorrelationIdentifier flux.Identifier
	CausationIdentifier   flux.Identifier
}

// WorkflowStore is an in-memory test double that simulates a database table for workflows
// and an outbox table for commands with at-least-once relay delivery semantics.
type WorkflowStore[W workflow.Workflow[W]] struct {
	mu        sync.RWMutex
	state     map[string]W
	outbox    []OutboxMessage
	cmdBus    *command.Bus
	positions map[string]uint64
	cmdSeq    uint64
	running   bool
}

// CheckpointStore provides an in-memory implementation of workflow.CheckpointStore.
type CheckpointStore struct {
	mu        sync.RWMutex
	positions map[string]uint64
}

// NewCheckpointStore creates a new in-memory checkpoint store for orchestrator positions.
func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{
		positions: make(map[string]uint64),
	}
}

// GetPosition returns the stored stream position for the orchestrator ID.
func (s *CheckpointStore) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[id.String()], nil
}

// SetPosition stores the stream position for the orchestrator ID.
func (s *CheckpointStore) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positions[id.String()] = position
	return nil
}

// New creates a memory-backed Workflow store.
// It accepts a CommandBus to simulate the background Outbox Relay.
func New[W workflow.Workflow[W]](cmdBus *command.Bus) *WorkflowStore[W] {
	return &WorkflowStore[W]{
		state:     make(map[string]W),
		outbox:    make([]OutboxMessage, 0),
		cmdBus:    cmdBus,
		positions: make(map[string]uint64),
	}
}

func (s *WorkflowStore[W]) Load(ctx context.Context, id flux.Identifier) (W, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var zero W
	if state, exists := s.state[id.String()]; exists {
		return state.Clone(), nil
	}

	// Instantiate new empty workflow
	return zero.New(), nil
}

func (s *WorkflowStore[W]) Save(ctx context.Context, workflowInstance W, commands []any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var actor flux.Actor
	var correlationID flux.Identifier
	var causationID flux.Identifier
	if fCtx, ok := ctx.(flux.Context); ok {
		actor = fCtx.Actor()
		correlationID = fCtx.CorrelationIdentifier()
		causationID = fCtx.CausationIdentifier()
	}
	if evtCtx, ok := ctx.(event.Context); ok {
		causationID = evtCtx.EventIdentifier()
	}

	// Atomically save state clone and outbox commands
	s.state[workflowInstance.Identifier().String()] = workflowInstance.Clone()

	for _, cmd := range commands {
		s.cmdSeq++
		cmdID := flux.NewIdentifier("flux", "", "workflow", "", "command", fmt.Sprintf("%d", s.cmdSeq), "")
		s.outbox = append(s.outbox, OutboxMessage{
			ID:                    cmdID,
			Command:               cmd,
			Actor:                 actor,
			CorrelationIdentifier: correlationID,
			CausationIdentifier:   causationID,
		})
	}

	return nil
}

// GetPosition returns the stored stream position for the orchestrator ID.
func (s *WorkflowStore[W]) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[id.String()], nil
}

// SetPosition stores the stream position for the orchestrator ID.
func (s *WorkflowStore[W]) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positions[id.String()] = position
	return nil
}

// OutboxMessages returns a snapshot copy of currently pending outbox messages.
func (s *WorkflowStore[W]) OutboxMessages() []OutboxMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.outbox)
}

// StartRelay starts a background goroutine simulating the Outbox Poller.
// It uses ack-after-success semantics: messages are removed from the outbox
// only after command.Execute succeeds. If execution fails, the message remains
// in the outbox to be retried on subsequent ticks.
func (s *WorkflowStore[W]) StartRelay(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}()

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					if ctx.Err() != nil {
						return
					}

					s.mu.Lock()
					if len(s.outbox) == 0 {
						s.mu.Unlock()
						break
					}
					msg := s.outbox[0]
					s.mu.Unlock()

					// Execute command with rebuilt context
					cmdCtx := command.NewContext(ctx, msg.ID, msg.Actor, msg.CorrelationIdentifier, msg.CausationIdentifier)
					if err := command.Execute(cmdCtx, s.cmdBus, msg.Command); err != nil {
						slog.ErrorContext(ctx, "failed to execute outbox command",
							"command", fmt.Sprintf("%T", msg.Command),
							"command_id", msg.ID.String(),
							"error", err,
						)
						// NACK: leave message in outbox to retry on next tick
						break
					}

					// ACK: remove dispatched message on success
					s.mu.Lock()
					if len(s.outbox) > 0 && s.outbox[0].ID == msg.ID {
						s.outbox = s.outbox[1:]
					}
					s.mu.Unlock()
				}
			}
		}
	}()
}
