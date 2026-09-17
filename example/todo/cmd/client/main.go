package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/client"
)

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "Server base URL")
	listIDStr := flag.String("list", "urn:todo:prod:lists:1:list:abc-123", "Target todo list URN")
	flag.Parse()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(ctx, *serverURL, *listIDStr); err != nil {
		slog.ErrorContext(ctx, "client workflow failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, serverURL, listIDStr string) error {
	slog.InfoContext(ctx, "connecting to server...", "url", serverURL, "list", listIDStr)

	c := client.NewHTTPClient(serverURL)
	listID := flux.NewIdentifierFromString(listIDStr)

	snapshot, err := client.RunWorkflow(ctx, c, listID)
	if err != nil {
		return fmt.Errorf("running client workflow: %w", err)
	}

	slog.InfoContext(ctx, "workflow executed successfully!",
		"active", snapshot.Active,
		"archived", snapshot.Archived,
		"removed", snapshot.Removed,
	)
	return nil
}
