package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
)

type dummyCmd struct{ val string }

func TestCommandBus_ExecuteAsync(t *testing.T) {
	bus := command.New()

	done := make(chan struct{})
	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		defer close(done)
		if cmd.val == "panic" {
			panic("intentional panic")
		}
		return nil
	})

	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	// Test normal async
	err := command.ExecuteAsync(ctx, bus, dummyCmd{val: "ok"})
	if err != nil {
		t.Fatalf("expected no error from ExecuteAsync, got %v", err)
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("async execution timed out")
	}

	// Test panic recovery async (we don't get the error back, but it shouldn't crash the program)
	bus2 := command.New()
	done2 := make(chan struct{})
	command.Register(bus2, func(ctx command.Context, cmd dummyCmd) error {
		defer close(done2)
		panic("intentional panic 2")
	})

	command.ExecuteAsync(ctx, bus2, dummyCmd{val: "panic"})
	select {
	case <-done2:
	case <-time.After(1 * time.Second):
		t.Fatal("async execution timed out")
	}
}

func TestCommandBus_ExecuteAsync_NoHandler(t *testing.T) {
	bus := command.New()
	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	err := command.ExecuteAsync(ctx, bus, dummyCmd{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, flux.ErrNoHandler) {
		t.Fatalf("expected ErrNoHandler, got %v", err)
	}
}
