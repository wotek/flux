package client_test

import (
	"context"
	"net/http/httptest"
	"testing"

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
		})
	}
}
