package store_test

import (
	"context"
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
