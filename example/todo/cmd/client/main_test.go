package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
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

	t.Run("automated demo workflow", func(t *testing.T) {
		err := run(ctx, ts.URL, "urn:todo:prod:lists:1:list:cmd-demo-test", false, nil, nil)
		if err != nil {
			t.Fatalf("run demo failed: %v", err)
		}
	})

	t.Run("interactive mode session", func(t *testing.T) {
		in := strings.NewReader("q")
		out := &bytes.Buffer{}

		err := run(ctx, ts.URL, "urn:todo:prod:lists:1:list:cmd-interactive-test", true, in, out)
		if err != nil {
			t.Fatalf("run interactive failed: %v", err)
		}

		if !strings.Contains(out.String(), "FLUX CQRS TODO APP") {
			t.Fatalf("expected header in output, got:\n%s", out.String())
		}
	})
}
