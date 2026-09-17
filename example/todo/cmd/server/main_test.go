package main

import (
	"context"
	"net/http"
	"testing"
	"time"
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
