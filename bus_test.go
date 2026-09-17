package flux_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wotek/flux"
)

type MyCommand struct {
	Data string
}

type MyCommandHandler struct {
	t       *testing.T
	handled *bool
}

func (h MyCommandHandler) Handle(ctx flux.CommandContext, cmd MyCommand) error {
	*h.handled = true
	if cmd.Data != "test" {
		h.t.Errorf("expected 'test', got %s", cmd.Data)
	}
	return nil
}

func TestCommandBus(t *testing.T) {
	bus := flux.NewCommandBus()

	handled := false
	flux.RegisterCommandHandler(bus, MyCommandHandler{t: t, handled: &handled})

	ctx := flux.NewCommandContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	err := flux.ExecuteCommand(ctx, bus, MyCommand{Data: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handled {
		t.Errorf("expected handler to be called")
	}
}

type MyQuery struct {
	ID string
}

type MyResult struct {
	Value int
}

type MyQueryHandler struct{}

func (h MyQueryHandler) Handle(ctx flux.QueryContext, q MyQuery) (MyResult, error) {
	if q.ID != "123" {
		return MyResult{}, errors.New("not found")
	}
	return MyResult{Value: 42}, nil
}

func TestQueryBus(t *testing.T) {
	bus := flux.NewQueryBus()

	flux.RegisterQueryHandler(bus, MyQueryHandler{})

	ctx := flux.NewQueryContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	res, err := flux.ExecuteQuery[MyQuery, MyResult](ctx, bus, MyQuery{ID: "123"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Value != 42 {
		t.Errorf("expected 42, got %d", res.Value)
	}

	_, err = flux.ExecuteQuery[MyQuery, MyResult](ctx, bus, MyQuery{ID: "wrong"})
	if err == nil {
		t.Errorf("expected error for wrong query")
	}
}

type PingEvent struct{}

func (e PingEvent) Name() string { return "PingEvent" }

type MyEventHandler struct {
	count *int
}

func (h MyEventHandler) Handle(ctx flux.EventContext, e PingEvent) error {
	*h.count++
	return nil
}

func TestEventBus(t *testing.T) {
	bus := flux.NewEventBus()

	count := 0
	handler := MyEventHandler{count: &count}
	flux.RegisterEventHandler(bus, handler)
	flux.RegisterEventHandler(bus, handler)

	env := flux.Envelope{Event: PingEvent{}, CreatedAt: time.Now()}
	ctx := flux.NewEventContext(context.Background(), env)

	err := flux.PublishEnvelope(ctx, bus, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 handlers to run, got %d", count)
	}
}
