package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"

	"github.com/wotek/flux/saga/store"
)

type dummySaga struct {
	id    flux.Identifier
	Count int
}

func (s *dummySaga) Identifier() flux.Identifier { return s.id }
func (s *dummySaga) New() *dummySaga {
	return &dummySaga{}
}
func (s *dummySaga) Clone() *dummySaga {
	c := *s
	return &c
}

type dummyCmd struct{ Val string }

func TestInMemorySagaStore(t *testing.T) {
	cmdBus := command.New()
	s := store.New[*dummySaga](cmdBus)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:saga:1:test:test")

	// Load should return new if not found
	sg, err := s.Load(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sg.Count != 0 {
		t.Fatalf("expected count 0")
	}

	// Make changes
	sg.id = id
	sg.Count = 10

	// Save
	cmds := []any{dummyCmd{Val: "cmd1"}}
	err = s.Save(ctx, sg, cmds)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Load again
	sg2, err := s.Load(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sg2.Count != 10 {
		t.Fatalf("expected state restored")
	}

	// Start relay
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	s.StartRelay(ctx2)
}
