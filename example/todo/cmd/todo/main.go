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

	// 2. Initialize Repositories and Stores
	repo := flux.NewAggregateRepository[*todo.TodoListAggregate, todo.TodoEvent](eventStore)
	statsStore := todo.NewMemoryCounterStore()

	// 3. Register Command and Query Handlers
	todo.RegisterCommandHandlers(cmdBus, repo)
	todo.RegisterQueryHandlers(queryBus, statsStore)

	// 4. Configure Read Model Projector
	projIdentifier := flux.NewIdentifierFromString("urn:todo:prod:projections:1:counter:main")
	projector := todo.NewCounterProjector(projIdentifier, eventStore, projStore, statsStore)

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
	listIdentifier := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:abc-123")
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:alice")}

	// Helper to generate a contextual command context
	newCmdCtx := func(action string) command.Context {
		cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:%s-%d", action, time.Now().UnixNano()))
		return command.NewContext(ctx, cmdID, actor, flux.Identifier{}, flux.Identifier{})
	}

	slog.InfoContext(ctx, "client: adding 10 tasks...")
	for i := 1; i <= 10; i++ {
		cmdCtx := newCmdCtx("add")
		cmd := todo.AddTask{
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
		cmd := todo.RemoveTask{
			ListIdentifier: listIdentifier,
			Task:           fmt.Sprintf("Task %d", i),
		}
		if err := command.Execute(cmdCtx, bus, cmd); err != nil {
			return fmt.Errorf("removing task %d: %w", i, err)
		}
	}

	slog.InfoContext(ctx, "client: marking Task 6 and Task 10 as completed...")
	doneCmdCtx := newCmdCtx("done")
	doneCmd := todo.DoneTasks{
		ListIdentifier: listIdentifier,
		Tasks:          []string{"Task 6", "Task 10"},
	}
	if err := command.Execute(doneCmdCtx, bus, doneCmd); err != nil {
		return fmt.Errorf("marking tasks done: %w", err)
	}

	// Poll read model until projector catches up to expected state
	queryCtx := query.NewContext(
		ctx,
		flux.NewIdentifierFromString("urn:todo:prod:queries:1:query:check-counter"),
		actor,
		flux.Identifier{},
		flux.Identifier{},
	)

	var counter todo.Counter
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		counter, err = query.Execute[todo.GetCounter, todo.Counter](queryCtx, queryBus, todo.GetCounter{})
		if err == nil && counter.Active == 3 && counter.Archived == 2 && counter.Removed == 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err != nil {
		return fmt.Errorf("querying counter read model: %w", err)
	}

	slog.InfoContext(ctx, "client: read model verified successfully",
		"active", counter.Active,
		"archived", counter.Archived,
		"removed", counter.Removed,
	)

	return nil
}
