package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/client"
)

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "Server base URL")
	listIDStr := flag.String("list", "urn:todo:prod:lists:1:list:abc-123", "Target todo list URN")
	interactive := flag.Bool("interactive", true, "Run interactive terminal CLI application")
	demo := flag.Bool("demo", false, "Run automated demo workflow instead of interactive mode")
	flag.Parse()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	isInteractive := *interactive && !*demo

	if err := run(ctx, *serverURL, *listIDStr, isInteractive, os.Stdin, os.Stdout); err != nil {
		slog.ErrorContext(ctx, "client execution failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, serverURL, listIDStr string, interactive bool, in io.Reader, out io.Writer) error {
	c := client.NewHTTPClient(serverURL)
	listID := flux.MustParseIdentifier(listIDStr)

	if interactive {
		return client.RunInteractive(ctx, c, listID, in, out)
	}

	slog.InfoContext(ctx, "connecting to server for automated workflow...", "url", serverURL, "list", listIDStr)
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
