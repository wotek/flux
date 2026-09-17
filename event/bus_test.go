package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

type PingEvent struct{}

func (e PingEvent) Name() string { return "PingEvent" }

type MyEventHandler struct {
	count *int
}

func (h MyEventHandler) Handle(ctx event.Context, e PingEvent) error {
	*h.count++
	return nil
}

func TestEventBus(t *testing.T) {
	bus := event.New()

	count := 0
	handler := MyEventHandler{count: &count}
	event.RegisterHandler(bus, handler)
	event.RegisterHandler(bus, handler)

	env := flux.Envelope{Event: PingEvent{}, CreatedAt: time.Now()}
	ctx := event.NewContext(context.Background(), env)

	err := event.PublishEnvelope(ctx, bus, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 handlers to run, got %d", count)
	}
}

func TestEventBus_FunctionalHandler(t *testing.T) {
	bus := event.New()

	count := 0
	event.Register(bus, func(ctx event.Context, e PingEvent) error {
		count++
		return nil
	})

	env := flux.Envelope{Event: PingEvent{}, CreatedAt: time.Now()}
	ctx := event.NewContext(context.Background(), env)

	err := event.PublishEnvelope(ctx, bus, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 handler to run, got %d", count)
	}
}
