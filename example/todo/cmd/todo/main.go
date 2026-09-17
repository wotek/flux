package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/queries"
	projectionstore "github.com/wotek/flux/projection/store"
	"github.com/wotek/flux/query"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(ctx); err != nil {
		slog.ErrorContext(ctx, "application failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	slog.InfoContext(ctx, "initializing infrastructure...")

	// 1. Initialize in-memory infrastructure
	eventStore := eventstore.New()
	projStore := projectionstore.New()
	cmdBus := command.New()
	queryBus := query.New()

	// 1.5 Demonstrate Contextual Logging via Middleware interceptors
	cmdBus.Use(func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
		ctx.Logger().Info("executing command", "command_type", fmt.Sprintf("%T", cmd))
		err := next(ctx, cmd)
		if err != nil {
			ctx.Logger().Error("command failed", "error", err)
		} else {
			ctx.Logger().Info("command succeeded")
		}
		return err
	})

	queryBus.Use(func(ctx query.Context, q any, next func(query.Context, any) (any, error)) (any, error) {
		ctx.Logger().Info("executing query", "query_type", fmt.Sprintf("%T", q))
		return next(ctx, q)
	})

	// 2. Initialize Repositories and Stores
	repo := flux.NewAggregateRepository[*todo.TodoListAggregate, events.TodoEvent](eventStore)
	statsStore := counter.NewMemoryStore()

	// 3. Register Command and Query Handlers from their respective packages
	commands.RegisterHandlers(cmdBus, repo)
	queries.RegisterHandlers(queryBus, statsStore, nil, repo)

	// 4. Configure Read Model Projector
	projIdentifier := flux.MustParseIdentifier("urn:todo:prod:projections:1:counter:main")
	projector := counter.NewProjector(projIdentifier, eventStore, projStore, statsStore)

	g, groupCtx := errgroup.WithContext(ctx)

	// Start Projector in background worker
	g.Go(func() error {
		slog.InfoContext(groupCtx, "starting background counter projector...")
		if err := projector.Start(groupCtx); err != nil && groupCtx.Err() == nil {
			return fmt.Errorf("projector failed: %w", err)
		}
		return nil
	})

	// Run Client workflow
	if err := runClient(groupCtx, cmdBus, queryBus); err != nil {
		return fmt.Errorf("client workflow failed: %w", err)
	}

	return nil
}

func runClient(ctx context.Context, bus *command.Bus, queryBus *query.Bus) error {
	listIdentifier := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:abc-123")
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:todo:prod:users:1:user:alice")}

	// Helper to generate a contextual command context
	newCmdCtx := func(action string) command.Context {
		cmdID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:commands:1:cmd:%s-%d", action, time.Now().UnixNano()))
		return command.NewContext(ctx, cmdID, actor, flux.Identifier{}, flux.Identifier{})
	}

	slog.InfoContext(ctx, "client: adding 10 tasks...")
	for i := 1; i <= 10; i++ {
		cmdCtx := newCmdCtx("add")
		cmd := commands.AddTask{
			ListIdentifier: listIdentifier,
			Task:           fmt.Sprintf("Task %d", i),
		}
		if err := command.Execute(cmdCtx, bus, cmd); err != nil {
			return fmt.Errorf("adding task %d: %w", i, err)
		}
	}

	slog.InfoContext(ctx, "client: removing odd tasks (1, 3, 5, 7, 9)...")
	for i := 1; i <= 10; i += 2 {
		cmdCtx := newCmdCtx("remove")
		cmd := commands.RemoveTask{
			ListIdentifier: listIdentifier,
			Task:           fmt.Sprintf("Task %d", i),
		}
		if err := command.Execute(cmdCtx, bus, cmd); err != nil {
			return fmt.Errorf("removing task %d: %w", i, err)
		}
	}

	slog.InfoContext(ctx, "client: marking Task 6 and Task 10 as completed...")
	doneCmdCtx := newCmdCtx("done")
	doneCmd := commands.DoneTasks{
		ListIdentifier: listIdentifier,
		Tasks:          []string{"Task 6", "Task 10"},
	}
	if err := command.Execute(doneCmdCtx, bus, doneCmd); err != nil {
		return fmt.Errorf("marking tasks done: %w", err)
	}

	// Poll read model until projector catches up to expected state
	queryCtx := query.NewContext(
		ctx, flux.MustParseIdentifier("urn:todo:prod:queries:1:query:check-counter"),
		actor,
		flux.Identifier{},
		flux.Identifier{},
	)

	var snapshot counter.Counter
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err = query.Execute[queries.GetCounter, counter.Counter](queryCtx, queryBus, queries.GetCounter{})
		if err == nil && snapshot.Active == 3 && snapshot.Archived == 2 && snapshot.Removed == 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err != nil {
		return fmt.Errorf("querying counter read model: %w", err)
	}

	slog.InfoContext(ctx, "client: read model verified successfully",
		"active", snapshot.Active,
		"archived", snapshot.Archived,
		"removed", snapshot.Removed,
	)

	return nil
}
