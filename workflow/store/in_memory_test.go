package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"

	"github.com/wotek/flux/workflow/store"
)

type dummyWorkflow struct {
	id    flux.Identifier
	Count int
}

func (w *dummyWorkflow) Identifier() flux.Identifier { return w.id }
func (w *dummyWorkflow) New() *dummyWorkflow {
	return &dummyWorkflow{}
}
func (w *dummyWorkflow) Clone() *dummyWorkflow {
	c := *w
	return &c
}

type dummyCmd struct{ Val string }

func TestInMemoryWorkflowStore(t *testing.T) {
	t.Parallel()
	cmdBus := command.New()
	s := store.New[*dummyWorkflow](cmdBus)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:workflow:1:test:test")

	// Load should return new if not found
	wf, err := s.Load(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if wf.Count != 0 {
		t.Fatalf("expected count 0")
	}

	// Make changes
	wf.id = id
	wf.Count = 10

	// Save
	cmds := []any{dummyCmd{Val: "cmd1"}}
	err = s.Save(ctx, wf, cmds)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Load again
	wf2, err := s.Load(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if wf2.Count != 10 {
		t.Fatalf("expected state restored")
	}

	// Start relay
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	s.StartRelay(ctx2)
}

type flappyCmdHandler struct {
	attempts int
	failOnce bool
	executed chan string
}

func (h *flappyCmdHandler) Handle(ctx command.Context, cmd dummyCmd) error {
	h.attempts++
	if h.failOnce && h.attempts == 1 {
		return fmt.Errorf("transient command error")
	}
	h.executed <- cmd.Val
	return nil
}

func TestInMemoryWorkflowStore_OutboxAckAfterSuccess(t *testing.T) {
	t.Parallel()
	cmdBus := command.New()
	s := store.New[*dummyWorkflow](cmdBus)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &flappyCmdHandler{
		failOnce: true,
		executed: make(chan string, 5),
	}
	command.RegisterHandler(cmdBus, handler)

	id := flux.MustParseIdentifier("urn:test:prod:workflow:1:test:outbox")
	wf := &dummyWorkflow{id: id, Count: 1}
	cmds := []any{dummyCmd{Val: "msg-retry"}}

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user::test:1:user:caller")}
	corrID := flux.MustParseIdentifier("urn:corr:::::1")
	causID := flux.MustParseIdentifier("urn:caus:::::1")
	fCtx := flux.NewContext(ctx, actor, corrID, causID)

	if err := s.Save(fCtx, wf, cmds); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	// Verify message in outbox before relay
	msgs := s.OutboxMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 outbox message, got %d", len(msgs))
	}
	if msgs[0].Actor.Identifier != actor.Identifier {
		t.Errorf("expected actor %v, got %v", actor.Identifier, msgs[0].Actor.Identifier)
	}
	if msgs[0].CorrelationIdentifier != corrID {
		t.Errorf("expected correlation %v, got %v", corrID, msgs[0].CorrelationIdentifier)
	}
	if msgs[0].CausationIdentifier != causID {
		t.Errorf("expected causation %v, got %v", causID, msgs[0].CausationIdentifier)
	}

	// Start relay - first attempt should fail, message should remain in outbox, second should succeed
	s.StartRelay(ctx)

	// Wait for successful execution on retry
	select {
	case val := <-handler.executed:
		if val != "msg-retry" {
			t.Errorf("expected msg-retry, got %s", val)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for retried command execution")
	}

	if handler.attempts < 2 {
		t.Errorf("expected at least 2 attempts due to retry, got %d", handler.attempts)
	}

	// Wait briefly for outbox ACK removal
	var remaining int
	for range 20 {
		remaining = len(s.OutboxMessages())
		if remaining == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if remaining != 0 {
		t.Errorf("expected outbox to be drained after success, but %d messages remain", remaining)
	}
}

type recordCmdHandler struct {
	handled chan string
}

func (h *recordCmdHandler) Handle(ctx command.Context, cmd dummyCmd) error {
	h.handled <- cmd.Val
	return nil
}

func TestInMemoryWorkflowStore_RelayRestart(t *testing.T) {
	t.Parallel()
	cmdBus := command.New()
	s := store.New[*dummyWorkflow](cmdBus)

	handler := &recordCmdHandler{handled: make(chan string, 10)}
	command.RegisterHandler(cmdBus, handler)

	ctx1, cancel1 := context.WithCancel(context.Background())
	s.StartRelay(ctx1)

	// Stop the first relay
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Start relay again with new context
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s.StartRelay(ctx2)

	// Enqueue a command
	id := flux.MustParseIdentifier("urn:test:prod:workflow:1:test:restart")
	wf := &dummyWorkflow{id: id, Count: 2}
	_ = s.Save(ctx2, wf, []any{dummyCmd{Val: "after-restart"}})

	select {
	case val := <-handler.handled:
		if val != "after-restart" {
			t.Errorf("expected after-restart, got %s", val)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for command after relay restart")
	}
}

func TestCheckpointStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cpStore := store.NewCheckpointStore()
	id := flux.MustParseIdentifier("urn:flux::workflow:1:orchestrator:cp")

	pos, err := cpStore.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected initial position 0, got %d", pos)
	}

	if err := cpStore.SetPosition(ctx, id, 42); err != nil {
		t.Fatalf("failed to set position: %v", err)
	}

	pos, err = cpStore.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected position 42, got %d", pos)
	}
}
