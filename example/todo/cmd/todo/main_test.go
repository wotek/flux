package main

import (
	"context"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := run(ctx); err != nil {
		t.Fatalf("expected run() to succeed, got error: %v", err)
	}
}
