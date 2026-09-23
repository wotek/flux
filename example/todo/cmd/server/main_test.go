package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wotek/flux/example/todo/server"
)

func TestRunServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, "127.0.0.1:0")
	}()

	// Allow server time to bind and start
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop server
	cancel()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("expected clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server failed to shut down within deadline")
	}
}

func TestRunServer_DebugLogger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, "127.0.0.1:0", server.WithLogger(logger))
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("expected clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server failed to shut down within deadline")
	}

	output := buf.String()
	if !strings.Contains(output, "starting todo cqrs server") {
		t.Errorf("expected startup log, got: %s", output)
	}
}
