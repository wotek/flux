package client_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/client"
	"github.com/wotek/flux/example/todo/server"
)

func TestClientImplementations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		makeClient func(t *testing.T, srv *server.Server) client.Client
	}{
		{
			name: "InMemoryClient",
			makeClient: func(t *testing.T, srv *server.Server) client.Client {
				t.Helper()
				return client.NewInMemoryClient(srv.CommandBus(), srv.QueryBus())
			},
		},
		{
			name: "HTTPClient",
			makeClient: func(t *testing.T, srv *server.Server) client.Client {
				t.Helper()
				ts := httptest.NewServer(srv.HTTPHandler())
				t.Cleanup(ts.Close)
				return client.NewHTTPClient(ts.URL, client.WithHTTPClient(ts.Client()))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := server.New()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			go func() {
				_ = srv.Start(ctx)
			}()

			c := tc.makeClient(t, srv)
			listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:" + tc.name)

			snapshot, err := client.RunWorkflow(ctx, c, listID)
			if err != nil {
				t.Fatalf("workflow failed: %v", err)
			}

			if snapshot.Active != 3 || snapshot.Archived != 2 || snapshot.Removed != 5 {
				t.Fatalf("unexpected snapshot: %+v", snapshot)
			}

			// Verify GetTodoList
			list, err := c.GetTodoList(ctx, listID)
			if err != nil {
				t.Fatalf("GetTodoList failed: %v", err)
			}
			if len(list.Active) != 3 {
				t.Fatalf("expected 3 active tasks, got %v", list.Active)
			}
			if len(list.Archived) != 2 {
				t.Fatalf("expected 2 archived tasks, got %v", list.Archived)
			}

			// Create another list and verify GetLists
			secondListID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:" + tc.name + "-secondary")
			if err := c.CreateList(ctx, secondListID, "Secondary List"); err != nil {
				t.Fatalf("CreateList failed: %v", err)
			}

			allLists, err := c.GetLists(ctx)
			if err != nil {
				t.Fatalf("GetLists failed: %v", err)
			}
			if len(allLists) < 1 {
				t.Fatalf("expected at least 1 list, got %d", len(allLists))
			}
		})
	}
}

func TestHTTPClient_SubscribeEvents(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	client1 := client.NewHTTPClient(ts.URL)
	client2 := client.NewHTTPClient(ts.URL)

	eventsChan, err := client1.SubscribeEvents(ctx)
	if err != nil {
		t.Fatalf("subscribe events failed: %v", err)
	}

	// Client 2 creates a list and adds a task
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:sse-sync")
	if err := client2.CreateList(ctx, listID, "Team Sync"); err != nil {
		t.Fatalf("create list: %v", err)
	}
	if err := client2.AddTask(ctx, listID, "Sync Note"); err != nil {
		t.Fatalf("add task: %v", err)
	}

	// Client 1 should receive at least one notification
	select {
	case notif, ok := <-eventsChan:
		if !ok {
			t.Fatal("events channel closed unexpectedly")
		}
		if notif.Type == "" {
			t.Fatal("expected non-empty notification type")
		}
		if !strings.Contains(notif.Description, "Sync") {
			t.Errorf("expected notification description to contain 'Sync', got: %s", notif.Description)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SSE event notification")
	}
}
