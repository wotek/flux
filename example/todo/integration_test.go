package todo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections"
	"github.com/wotek/flux/example/todo/queries"
	projectionstore "github.com/wotek/flux/projection/store"
	"github.com/wotek/flux/query"
)

func TestTodoApplication_EndToEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eventStore := eventstore.New()
	projStore := projectionstore.New()
	cmdBus := command.New()
	queryBus := query.New()

	repo := flux.NewAggregateRepository[*todo.TodoListAggregate, events.TodoEvent](eventStore)
	statsStore := projections.NewMemoryCounterStore()

	commands.RegisterHandlers(cmdBus, repo)
	queries.RegisterHandlers(queryBus, statsStore)

	projID := flux.NewIdentifierFromString("urn:todo:prod:projections:1:counter:integration")
	projector := projections.NewCounterProjector(projID, eventStore, projStore, statsStore)

	go func() {
		_ = projector.Start(ctx)
	}()

	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:integration-1")
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:tester")}

	newCmdCtx := func(action string) command.Context {
		cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:%s-%d", action, time.Now().UnixNano()))
		return command.NewContext(ctx, cmdID, actor, flux.Identifier{}, flux.Identifier{})
	}

	// 1. Add 5 tasks
	for i := 1; i <= 5; i++ {
		cmd := commands.AddTask{
			ListIdentifier: listID,
			Task:           fmt.Sprintf("Task %d", i),
		}
		if err := command.Execute(newCmdCtx("add"), cmdBus, cmd); err != nil {
			t.Fatalf("failed to add task %d: %v", i, err)
		}
	}

	// 2. Remove Task 1
	if err := command.Execute(newCmdCtx("remove"), cmdBus, commands.RemoveTask{
		ListIdentifier: listID,
		Task:           "Task 1",
	}); err != nil {
		t.Fatalf("failed to remove task 1: %v", err)
	}

	// 3. Mark Task 2 and Task 4 done
	if err := command.Execute(newCmdCtx("done"), cmdBus, commands.DoneTasks{
		ListIdentifier: listID,
		Tasks:          []string{"Task 2", "Task 4"},
	}); err != nil {
		t.Fatalf("failed to mark tasks done: %v", err)
	}

	// 4. Verify aggregate state via repository load
	loaded, err := repo.Load(newCmdCtx("check"), flux.Stream{Identifier: listID})
	if err != nil {
		t.Fatalf("failed to reload aggregate: %v", err)
	}

	activeTasks := loaded.ActiveTasks()
	if len(activeTasks) != 2 {
		t.Errorf("expected 2 active tasks (Task 3, Task 5), got %v", activeTasks)
	}
	archivedTasks := loaded.ArchivedTasks()
	if len(archivedTasks) != 2 {
		t.Errorf("expected 2 archived tasks (Task 2, Task 4), got %v", archivedTasks)
	}

	// 5. Verify projection read model via query bus
	queryCtx := query.NewContext(
		ctx,
		flux.NewIdentifierFromString("urn:todo:prod:queries:1:query:test-counter"),
		actor,
		flux.Identifier{},
		flux.Identifier{},
	)

	deadline := time.Now().Add(2 * time.Second)
	var counter projections.Counter
	for time.Now().Before(deadline) {
		counter, err = query.Execute[queries.GetCounter, projections.Counter](queryCtx, queryBus, queries.GetCounter{})
		if err == nil && counter.Active == 2 && counter.Archived == 2 && counter.Removed == 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("failed to query counter: %v", err)
	}

	if counter.Active != 2 || counter.Archived != 2 || counter.Removed != 1 {
		t.Errorf("unexpected counter stats: active=%d (want 2), archived=%d (want 2), removed=%d (want 1)",
			counter.Active, counter.Archived, counter.Removed)
	}
}
