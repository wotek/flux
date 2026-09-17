package client_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/client"
	"github.com/wotek/flux/example/todo/server"
)

func TestRunInteractive(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:interactive-test")

	// Simulated terminal user session:
	// 1. Add Task A
	// 2. Add Task B
	// 3. Add Task C
	// 4. Cycle next (selects Task B or C)
	// 5. Select 2 directly
	// 6. Complete task 2 (Task B)
	// 7. Delete currently selected task
	// 8. Quit
	inputCommands := strings.Join([]string{
		"a Task A",
		"add Task B",
		"a Task C",
		"n",
		"p",
		"2",
		"c 2",
		"1",
		"d",
		"nl Personal Projects",
		"a Buy Groceries",
		"q",
	}, "\n") + "\n"

	in := strings.NewReader(inputCommands)
	out := &bytes.Buffer{}

	err := client.RunInteractive(ctx, c, listID, in, out)
	if err != nil {
		t.Fatalf("RunInteractive failed: %v", err)
	}

	output := out.String()

	// Verify key lifecycle markers were rendered in terminal session
	expectedStrings := []string{
		"FLUX CQRS TODO APP",
		"Added task: \"Task A\"",
		"Added task: \"Task B\"",
		"Added task: \"Task C\"",
		"Completed task: \"Task B\"",
		"Removed task: \"Task A\"",
		"Created and switched to list \"Personal Projects\"",
		"Added task: \"Buy Groceries\"",
		"Goodbye!",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(output, expected) {
			t.Errorf("output missing expected text %q\nFull output:\n%s", expected, output)
		}
	}

	// Verify the final aggregate state
	todoList, err := c.GetTodoList(ctx, listID)
	if err != nil {
		t.Fatalf("GetTodoList failed: %v", err)
	}

	// Task B is completed (archived), Task A was removed (deleted), Task C remains active
	if len(todoList.Active) != 1 || todoList.Active[0] != "Task C" {
		t.Errorf("expected Active=[Task C], got %v", todoList.Active)
	}
	if len(todoList.Archived) != 1 || todoList.Archived[0] != "Task B" {
		t.Errorf("expected Archived=[Task B], got %v", todoList.Archived)
	}
}
