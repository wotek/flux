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

func TestRunInteractive_TwoScreenNavigation(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	initialListID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:initial")

	// Simulated terminal user session:
	// 1. On Screen 1 (Lists Screen): create new list "Work Tasks" (navigates into Tasks screen)
	// 2. On Screen 2 (Tasks Screen): add Task A, Task B, Task C
	// 3. Cycle next, prev, select task 2, mark done
	// 4. Delete task 1
	// 5. Navigate back to Screen 1 with 'b'
	// 6. On Screen 1 (Lists Screen): create second list "Personal Tasks"
	// 7. On Screen 2 (Tasks Screen): add "Buy Groceries"
	// 8. Quit with 'q'
	inputCommands := strings.Join([]string{
		"new Work Tasks",
		"a Task A",
		"a Task B",
		"a Task C",
		"n",
		"p",
		"2",
		"c 2",
		"1",
		"d 1",
		"b",
		"new Personal Tasks",
		"a Buy Groceries",
		"q",
	}, "\n") + "\n"

	in := strings.NewReader(inputCommands)
	out := &bytes.Buffer{}

	err := client.RunInteractive(ctx, c, initialListID, in, out)
	if err != nil {
		t.Fatalf("RunInteractive failed: %v", err)
	}

	output := out.String()

	// Verify key screen headers and actions were rendered
	expectedStrings := []string{
		"FLUX CQRS TODO APP — All Todo Lists",
		"Created and opened list \"Work Tasks\"",
		"Added task: \"Task A\"",
		"Added task: \"Task B\"",
		"Added task: \"Task C\"",
		"Completed task: \"Task B\"",
		"Removed task: \"Task A\"",
		"Returned to lists catalog.",
		"Created and opened list \"Personal Tasks\"",
		"Added task: \"Buy Groceries\"",
		"Goodbye!",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(output, expected) {
			t.Errorf("output missing expected text %q\nFull output:\n%s", expected, output)
		}
	}
}
