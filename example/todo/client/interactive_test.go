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
	"github.com/wotek/flux/example/todo/projections/lists"
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
	listID := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:smoke")

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

	deadline := time.Now().Add(2 * time.Second)
	for cmd != nil {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return m
		}

		done := make(chan tea.Msg, 1)
		go func(c tea.Cmd) {
			done <- c()
		}(cmd)

		timer := time.NewTimer(timeout)
		select {
		case msg := <-done:
			timer.Stop()
			if msg == nil {
				return m
			}
			if batch, ok := msg.(tea.BatchMsg); ok {
				for _, c := range batch {
					m = execCmd(m, c)
				}
				return m
			}
			var nextCmd tea.Cmd
			m, nextCmd = m.Update(msg)
			cmd = nextCmd
		case <-timer.C:
			return m
		}
	}
	return m
}

func TestTUIModel_StateTransitions(t *testing.T) {
	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	c := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
	listID := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:tui-test")

	// 1. Create initial list
	if err := c.CreateList(ctx, listID, "Work Tasks"); err != nil {
		t.Fatalf("failed to create list: %v", err)
	}
	if err := c.AddTask(ctx, listID, "Task 1"); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	// Wait for read model projection to index initial list
	for range 50 {
		listSummaries, err := c.GetLists(ctx)
		if err == nil && len(listSummaries) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
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
	listID := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:refresh-filter")

	if err := c.CreateList(ctx, listID, "Alpha Project"); err != nil {
		t.Fatalf("failed to create list: %v", err)
	}

	m := client.NewTestTUIModel(ctx, c, listID)

	// Initialize
	m = execCmd(m, m.Init())

	// 1. Test Refresh (Wait for eventual consistency)
	var cmd tea.Cmd
	for i := 0; i < 50; i++ {
		m, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
		m = execCmd(m, cmd)
		if strings.Contains(m.View(), "Alpha Project") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

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
	listID := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:sync-test")

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

func TestTUIModel_LiveSync_ListCountersConvergence(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	clientA := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus(), client.WithInMemoryEventStore(srv.EventStore()))
	clientB := client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus(), client.WithInMemoryEventStore(srv.EventStore()))

	listID1 := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:conv-1")
	listID2 := flux.MustParseIdentifier("urn:todo:prod:lists:1:list:conv-2")

	if err := clientA.CreateList(ctx, listID1, "List One"); err != nil {
		t.Fatalf("failed to create list 1: %v", err)
	}
	if err := clientA.CreateList(ctx, listID2, "List Two"); err != nil {
		t.Fatalf("failed to create list 2: %v", err)
	}

	// Wait for read model projection to register both lists
	var listSummaries []lists.ListSummary
	for range 50 {
		var err error
		listSummaries, err = clientA.GetLists(ctx)
		if err == nil && len(listSummaries) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(listSummaries) < 2 {
		t.Fatalf("read model projection timed out indexing lists: got %d", len(listSummaries))
	}

	// Client A starts on Lists screen
	mA := client.NewTestTUIModel(ctx, clientA, listID1)
	mA = execCmd(mA, mA.Init())

	viewBefore := mA.View()
	if !strings.Contains(viewBefore, "List One") || !strings.Contains(viewBefore, "List Two") {
		t.Fatalf("expected both lists in view, got: %s", viewBefore)
	}
	if !strings.Contains(viewBefore, "Active: 0") {
		t.Fatalf("expected Active: 0 before task addition, got: %s", viewBefore)
	}

	// Client B adds a task to List One
	if err := clientB.AddTask(ctx, listID1, "Task from Client B"); err != nil {
		t.Fatalf("client B failed to add task: %v", err)
	}

	// Wait for projector to process the task addition into read model
	for range 50 {
		updatedLists, err := clientA.GetLists(ctx)
		if err == nil {
			for _, l := range updatedLists {
				if l.Identifier == listID1.String() && l.Active == 1 {
					goto Projected
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for lists projection to reflect active task count")

Projected:
	// Live notification arrives at Client A
	notif := client.EventNotification{
		Type:           "TaskAdded",
		ListIdentifier: listID1.String(),
		Description:    `Task added: "Task from Client B"`,
		Position:       3,
	}

	// Deliver live notification to Client A
	mA, _ = mA.Update(client.NewTestEventNotificationMsg(notif))

	// Delayed refresh converges the read model in Client A's TUI
	var cmd tea.Cmd
	mA, cmd = mA.Update(client.NewTestDelayedRefreshMsg())
	mA = execCmd(mA, cmd)

	viewAfter := mA.View()
	if !strings.Contains(viewAfter, "⚡ Live: Task added: \"Task from Client B\"") {
		t.Errorf("expected live notification status, got: %s", viewAfter)
	}
	if !strings.Contains(viewAfter, "Active: 1") {
		t.Errorf("expected List One to show 'Active: 1' after delayed refresh, got: %s", viewAfter)
	}
}
