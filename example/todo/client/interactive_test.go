package client_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

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

func execCmd(m tea.Model, cmd tea.Cmd) tea.Model {
	if cmd == nil {
		return m
	}
	done := make(chan tea.Msg, 1)
	go func() {
		done <- cmd()
	}()

	select {
	case msg := <-done:
		if batch, ok := msg.(tea.BatchMsg); ok {
			for _, c := range batch {
				m = execCmd(m, c)
			}
			return m
		}
		var nextCmd tea.Cmd
		m, nextCmd = m.Update(msg)
		return execCmd(m, nextCmd)
	case <-time.After(30 * time.Millisecond):
		return m
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
	m = execCmd(m, m.Init())

	// 3. View should show Lists screen with "Work Tasks"
	view1 := m.View()
	if !strings.Contains(view1, "Work Tasks") {
		t.Errorf("expected Work Tasks in view, got: %s", view1)
	}

	// 4. Press Enter to open the selected list
	var cmd tea.Cmd
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = execCmd(m, cmd)

	// 5. View should now show Tasks screen with "Task 1"
	view2 := m.View()
	if !strings.Contains(view2, "Task 1") {
		t.Errorf("expected Task 1 in tasks view, got: %s", view2)
	}

	// 6. Complete Task 1 with 'c'
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	m = execCmd(m, cmd)

	// 7. Return to Lists screen with 'b'
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	m = execCmd(m, cmd)

	view3 := m.View()
	if !strings.Contains(view3, "Todo Lists Catalog") {
		t.Errorf("expected lists catalog in view, got: %s", view3)
	}
}

func TestTUIModel_RefreshAndFilter(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:refresh-filter")

	if err := c.CreateList(ctx, listID, "Alpha Project"); err != nil {
		t.Fatalf("failed to create list: %v", err)
	}

	m := client.NewTestTUIModel(ctx, c, listID)

	// Initialize
	m = execCmd(m, m.Init())

	// 1. Test Refresh
	var cmd tea.Cmd
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	m = execCmd(m, cmd)

	view := m.View()
	if !strings.Contains(view, "Lists refreshed.") {
		t.Errorf("expected status 'Lists refreshed.', got view: %s", view)
	}
	if strings.Contains(view, "Refreshing lists...") {
		t.Errorf("expected 'Refreshing lists...' to be cleared, got: %s", view)
	}

	// 2. Test Filter key '/'
	m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = execCmd(m, cmd)

	// Type filter text "Alpha"
	for _, r := range "Alpha" {
		m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = execCmd(m, cmd)
	}

	viewFiltered := m.View()
	if !strings.Contains(viewFiltered, "Alpha Project") {
		t.Errorf("expected 'Alpha Project' in filtered view, got: %s", viewFiltered)
	}
}

func TestTUIModel_LiveSync(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus(), client.WithInMemoryEventStore(srv.EventStore()))
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:sync-test")

	if err := c.CreateList(ctx, listID, "Sync Tasks"); err != nil {
		t.Fatalf("failed to create list: %v", err)
	}

	m := client.NewTestTUIModel(ctx, c, listID)

	// Simulate external client completing a task
	notif := client.EventNotification{
		Type:           "TasksDone",
		ListIdentifier: listID.String(),
		Description:    "Tasks completed: Deploy staging",
		Position:       5,
	}

	// Deliver live notification to Client 2's TUI model
	m, cmd := m.Update(client.NewTestEventNotificationMsg(notif))
	if cmd != nil {
		// Execute cmd batch
		m, _ = m.Update(cmd())
	}

	view := m.View()
	if !strings.Contains(view, "⚡ Live: Tasks completed: Deploy staging") {
		t.Errorf("expected live notification in status, got view: %s", view)
	}
}
