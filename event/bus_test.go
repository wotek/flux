package event_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"github.com/wotek/flux/command"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

type PingEvent struct{}

type mockEventContext struct {
	event.Context
	l *slog.Logger
}
func (m mockEventContext) Logger() *slog.Logger { return m.l }


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

func TestEventBus_Middleware(t *testing.T) {
	bus := event.New()
	
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(handler)
		
	order := []string{}
	
	mw1 := func(ctx event.Context, env flux.Envelope, next func(event.Context, flux.Envelope) error) error {
		order = append(order, "mw1_before")
		ctx.Logger().Info("mw1 event executing")
		err := next(ctx, env)
		order = append(order, "mw1_after")
		return err
	}
	
	mw2 := func(ctx event.Context, env flux.Envelope, next func(event.Context, flux.Envelope) error) error {
		order = append(order, "mw2_before")
		err := next(ctx, env)
		order = append(order, "mw2_after")
		return err
	}
	
	bus.Use(mw1, mw2)
	
	event.Register(bus, func(ctx event.Context, e PingEvent) error {
		order = append(order, "handler1")
		return nil
	})
	event.Register(bus, func(ctx event.Context, e PingEvent) error {
		order = append(order, "handler2")
		return nil
	})
	
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:actor:1")}
	corrID := flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:correlation:2")
	env := flux.Envelope{
		Event: PingEvent{},
		Actor: actor,
		CorrelationIdentifier: corrID,
	}
	
	// Create context with metadata
	baseCtx := command.NewContext(context.Background(), flux.Identifier{}, actor, corrID, flux.Identifier{})
	evtCtx := event.NewContext(baseCtx, env)
	ctx := mockEventContext{
		Context: evtCtx,
		l: logger.With("actor", actor.Identifier.String(), "correlation_id", corrID.String()),
	}
	
	err := event.PublishEnvelope(ctx, bus, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	expected := []string{"mw1_before", "mw2_before", "handler1", "handler2", "mw2_after", "mw1_after"}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("expected order %v, got %v", expected, order)
		}
	}
	
	logOutput := buf.String()
	if !strings.Contains(logOutput, "mw1 event executing") {
		t.Fatalf("expected log to contain message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "\"actor\":\"urn:acme:prod:payments:tenant-1:actor:1\"") {
		t.Fatalf("expected log to contain auto-injected actor, got: %s", logOutput)
	}
}