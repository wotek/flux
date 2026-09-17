package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/wotek/flux/example/todo/server"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP server listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(ctx, *addr); err != nil {
		slog.ErrorContext(ctx, "server stopped with error", "error", err)
		os.Exit(1)
	}
	slog.InfoContext(context.Background(), "server shut down gracefully")
}

func run(ctx context.Context, addr string) error {
	slog.InfoContext(ctx, "starting todo cqrs server", "addr", addr)
	srv := server.New(server.WithHTTP(addr))
	return srv.Start(ctx)
}
