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
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if err := run(ctx, *addr, server.WithLogger(logger)); err != nil {
		logger.ErrorContext(ctx, "server stopped with error", "error", err)
		os.Exit(1)
	}
	logger.InfoContext(context.Background(), "server shut down gracefully")
}

func run(ctx context.Context, addr string, opts ...server.Option) error {
	baseOpts := []server.Option{server.WithHTTP(addr)}
	srv := server.New(append(baseOpts, opts...)...)
	srv.Logger().InfoContext(ctx, "starting todo cqrs server", "addr", addr)
	return srv.Start(ctx)
}
