package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/wotek/flux/example/todo/server"
)

func TestRunClient(t *testing.T) {
	srv := server.New()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		_ = srv.Start(ctx)
	}()

	ts := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(ts.Close)

	err := run(ctx, ts.URL, "urn:todo:prod:lists:1:list:cmd-client-test")
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
}
