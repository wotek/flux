package client_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/client"
	"github.com/wotek/flux/example/todo/server"
)

func TestRunInteractive_Smoke(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:smoke")

	// Sending "q" causes Bubble Tea program to exit cleanly
	in := strings.NewReader("q")
	out := &bytes.Buffer{}

	err := client.RunInteractive(ctx, c, listID, in, out)
	if err != nil {
		t.Fatalf("RunInteractive failed: %v", err)
	}

	if !strings.Contains(out.String(), "FLUX CQRS TODO APP") {
		t.Errorf("expected header in output, got: %s", out.String())
	}
}

func TestTUIModel_StateTransitions(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:tui-test")

	// 1. Create initial list
	if err := c.CreateList(ctx, listID, "Work Tasks"); err != nil {
		t.Fatalf("failed to create list: %v", err)
	}
	if err := c.AddTask(ctx, listID, "Task 1"); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	m := client.NewTestTUIModel(ctx, c, listID)

	// 2. Initialize
	initCmd := m.Init()
	if initCmd != nil {
		msg := initCmd()
		var nextCmd tea.Cmd
		m, nextCmd = m.Update(msg)
		_ = nextCmd
	}

	// 3. View should show Lists screen with "Work Tasks"
	view1 := m.View()
	if !strings.Contains(view1, "Work Tasks") {
		t.Errorf("expected Work Tasks in view, got: %s", view1)
	}

	// 4. Press Enter to open the selected list
	var cmd tea.Cmd
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil {
		msg := cmd()
		m, _ = m.Update(msg)
	}

	// 5. View should now show Tasks screen with "Task 1"
	view2 := m.View()
	if !strings.Contains(view2, "Task 1") {
		t.Errorf("expected Task 1 in tasks view, got: %s", view2)
	}

	// 6. Complete Task 1 with 'c'
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	if cmd != nil {
		msg := cmd()
		m, cmd = m.Update(msg)
		if cmd != nil {
			msg2 := cmd()
			m, _ = m.Update(msg2)
		}
	}

	// 7. Return to Lists screen with 'b'
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	if cmd != nil {
		msg := cmd()
		m, _ = m.Update(msg)
	}

	view3 := m.View()
	if !strings.Contains(view3, "Todo Lists Catalog") {
		t.Errorf("expected lists catalog in view, got: %s", view3)
	}
}
