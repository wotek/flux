package command_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
)

type MyCommand struct {
	Data string
}

type MyCommandHandler struct {
	t       *testing.T
	handled *bool
}

func (h MyCommandHandler) Handle(ctx command.Context, cmd MyCommand) error {
	*h.handled = true
	if cmd.Data != "test" {
		h.t.Errorf("expected 'test', got %s", cmd.Data)
	}
	return nil
}

func TestCommandBus(t *testing.T) {
	bus := command.New()

	handled := false
	command.RegisterHandler(bus, MyCommandHandler{t: t, handled: &handled})

	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	err := command.Execute(ctx, bus, MyCommand{Data: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handled {
		t.Errorf("expected handler to be called")
	}
}

func TestCommandBus_FunctionalHandler(t *testing.T) {
	bus := command.New()

	handled := false
	command.Register(bus, func(ctx command.Context, cmd MyCommand) error {
		handled = true
		return nil
	})

	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	err := command.Execute(ctx, bus, MyCommand{Data: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handled {
		t.Errorf("expected handler to be called")
	}
}
