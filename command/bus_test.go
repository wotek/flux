package command_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
)

type dummyCmd struct{ val string }

type mockCommandContext struct {
	command.Context
	l *slog.Logger
}
func (m mockCommandContext) Logger() *slog.Logger { return m.l }


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

	_ = command.ExecuteAsync(ctx, bus2, dummyCmd{val: "panic"})
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

func TestCommandBus_Middleware(t *testing.T) {
	bus := command.New()

	// Intercept slog.Default() for testing Context Logger
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(handler)

	order := []string{}

	mw1 := func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
		order = append(order, "mw1_before")
		// Test Context Logger automatic propagation
		ctx.Logger().Info("mw1 executing", "cmd_type", "dummyCmd")
		err := next(ctx, cmd)
		order = append(order, "mw1_after")
		return err
	}

	mw2 := func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
		order = append(order, "mw2_before")
		err := next(ctx, cmd)
		order = append(order, "mw2_after")
		return err
	}

	bus.Use(mw1, mw2)

	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		order = append(order, "handler")
		return nil
	})

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:actor:1")}
	corrID := flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:correlation:2")
	
	baseCtx := command.NewContext(context.Background(), flux.Identifier{}, actor, corrID, flux.Identifier{})
	ctx := mockCommandContext{
		Context: baseCtx,
		l: logger.With("actor", actor.Identifier.String(), "correlation_id", corrID.String()),
	}

	err := command.Execute(ctx, bus, dummyCmd{val: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"mw1_before", "mw2_before", "handler", "mw2_after", "mw1_after"}
	if len(order) != len(expected) {
		t.Fatalf("expected order %v, got %v", expected, order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("expected order %v, got %v", expected, order)
		}
	}

	// Assert the logger correctly captured the contextual tracing fields!
	logOutput := buf.String()
	if !strings.Contains(logOutput, "mw1 executing") {
		t.Fatalf("expected log to contain message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "\"actor\":\"urn:acme:prod:payments:tenant-1:actor:1\"") {
		t.Fatalf("expected log to contain auto-injected actor, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "\"correlation_id\":\"urn:acme:prod:payments:tenant-1:correlation:2\"") {
		t.Fatalf("expected log to contain auto-injected correlation_id, got: %s", logOutput)
	}
}

func TestExecuteAsync_ContextDetached(t *testing.T) {
	t.Parallel()
	bus := command.New()

	executed := make(chan struct{})
	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		// Verify context is NOT canceled even though parent was canceled immediately
		select {
		case <-ctx.Done():
			t.Errorf("expected detached context not to be canceled, got: %v", ctx.Err())
		default:
		}
		close(executed)
		return nil
	})

	parentCtx, cancel := context.WithCancel(context.Background())
	cmdCtx := command.NewContext(parentCtx, flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	if err := command.ExecuteAsync(cmdCtx, bus, dummyCmd{val: "async"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Immediately cancel the caller's context
	cancel()

	select {
	case <-executed:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for async handler execution")
	}
}

func TestExecuteAsync_ErrorHook(t *testing.T) {
	t.Parallel()
	bus := command.New()

	expectedErr := fmt.Errorf("async handler failure")
	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		return expectedErr
	})

	errReported := make(chan error, 1)
	bus.SetAsyncErrorHandler(func(ctx command.Context, cmd any, err error) {
		errReported <- err
	})

	cmdCtx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	if err := command.ExecuteAsync(cmdCtx, bus, dummyCmd{val: "fail"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case err := <-errReported:
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for async error hook")
	}
}

func TestCommandBus_ConcurrentMiddlewareUseAndExecute(t *testing.T) {
	t.Parallel()
	bus := command.New()

	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		return nil
	})

	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Concurrently append middleware
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					bus.Use(func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
						return next(ctx, cmd)
					})
					time.Sleep(10 * time.Microsecond)
				}
			}
		}()
	}

	// Concurrently execute commands
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = command.Execute(ctx, bus, dummyCmd{val: "concurrent"})
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestCommandBus_InvalidHandlerType(t *testing.T) {
	t.Parallel()

	bus := command.New()
	command.Register(bus, func(ctx command.Context, cmd dummyCmd) error {
		return nil
	})

	// Middleware replaces cmd with a different type
	bus.Use(func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
		return next(ctx, 12345) // wrong type
	})

	ctx := command.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	err := command.Execute(ctx, bus, dummyCmd{val: "test"})
	if err == nil {
		t.Fatalf("expected error from mismatched command type, got nil")
	}
	if !errors.Is(err, flux.ErrInvalidHandlerType) {
		t.Fatalf("expected ErrInvalidHandlerType, got %v", err)
	}
}