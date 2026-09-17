package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/server"
)

func TestServerHTTPHandler(t *testing.T) {
	t.Parallel()

	srv := server.New()
	handler := srv.HTTPHandler()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server projectors in background
	go func() {
		_ = srv.Start(ctx)
	}()

	listID := "urn:todo:prod:lists:1:list:test-http"

	t.Run("invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader([]byte(`invalid`)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("missing required fields", func(t *testing.T) {
		payload, _ := json.Marshal(map[string]string{"list_identifier": ""})
		req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(payload))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("add, remove, done, and get counter", func(t *testing.T) {
		// 1. Add 3 tasks
		for i := 1; i <= 3; i++ {
			body, _ := json.Marshal(map[string]string{
				"list_identifier": listID,
				"task":           "Task " + string(rune('0'+i)),
			})
			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf("add task %d: expected 201, got %d: %s", i, rec.Code, rec.Body.String())
			}
		}

		// 2. Remove Task 1
		removeBody, _ := json.Marshal(map[string]string{
			"list_identifier": listID,
			"task":           "Task 1",
		})
		req := httptest.NewRequest(http.MethodDelete, "/tasks", bytes.NewReader(removeBody))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("remove task: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		// 3. Mark Task 2 done
		doneBody, _ := json.Marshal(map[string]any{
			"list_identifier": listID,
			"tasks":          []string{"Task 2"},
		})
		req = httptest.NewRequest(http.MethodPost, "/tasks/done", bytes.NewReader(doneBody))
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("done tasks: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		// 4. Poll counter read model
		var snapshot counter.Counter
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			req = httptest.NewRequest(http.MethodGet, "/counter", nil)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				_ = json.NewDecoder(rec.Body).Decode(&snapshot)
				if snapshot.Active == 1 && snapshot.Archived == 1 && snapshot.Removed == 1 {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}

		if snapshot.Active != 1 || snapshot.Archived != 1 || snapshot.Removed != 1 {
			t.Fatalf("unexpected snapshot: %+v", snapshot)
		}

		// 5. Query tasks list via HTTP
		req = httptest.NewRequest(http.MethodGet, "/tasks?list_identifier="+listID, nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("get tasks: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var tasksResp struct {
			Active   []string `json:"active"`
			Archived []string `json:"archived"`
		}
		_ = json.NewDecoder(rec.Body).Decode(&tasksResp)
		if len(tasksResp.Active) != 1 || tasksResp.Active[0] != "Task 3" {
			t.Fatalf("expected Active=[Task 3], got %v", tasksResp.Active)
		}
		if len(tasksResp.Archived) != 1 || tasksResp.Archived[0] != "Task 2" {
			t.Fatalf("expected Archived=[Task 2], got %v", tasksResp.Archived)
		}

		// 6. Create a second list and verify GET /lists returns both
		list2ID := "urn:todo:prod:lists:1:list:work"
		createBody, _ := json.Marshal(map[string]string{
			"list_identifier": list2ID,
			"title":           "Work Tasks",
		})
		req = httptest.NewRequest(http.MethodPost, "/lists", bytes.NewReader(createBody))
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create list: expected 201, got %d: %s", rec.Code, rec.Body.String())
		}

		// Poll GET /lists until both lists appear in projection
		deadline = time.Now().Add(2 * time.Second)
		var listSummaries []map[string]any
		for time.Now().Before(deadline) {
			req = httptest.NewRequest(http.MethodGet, "/lists", nil)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				_ = json.NewDecoder(rec.Body).Decode(&listSummaries)
				if len(listSummaries) >= 2 {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}

		if len(listSummaries) < 2 {
			t.Fatalf("expected at least 2 lists, got %d: %v", len(listSummaries), listSummaries)
		}
	})
}

func TestServer_DebugLogging(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv := server.New(server.WithLogger(logger))
	handler := srv.HTTPHandler()

	req := httptest.NewRequest(http.MethodGet, "/counter", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "server: http request received") {
		t.Errorf("expected request received log, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "server: http request completed") {
		t.Errorf("expected request completed log, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "server: executing GetCounter query") {
		t.Errorf("expected query debug log, got: %s", logOutput)
	}
}

func TestServer_SSEStreaming(t *testing.T) {
	t.Parallel()

	srv := server.New()
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()

	// 1. Connect to SSE stream
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?position=0", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to connect to sse: %v", err)
	}
	defer resp.Body.Close()

	// 2. Add a task to generate an event
	listID := "urn:todo:prod:lists:1:list:sse-test"
	taskBody, _ := json.Marshal(map[string]string{
		"list_identifier": listID,
		"task":           "SSE Broadcast Task",
	})
	addResp, err := http.Post(ts.URL+"/tasks", "application/json", bytes.NewReader(taskBody))
	if err != nil {
		t.Fatalf("failed to add task: %v", err)
	}
	_ = addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from add task, got %d", addResp.StatusCode)
	}

	// 3. Read SSE stream lines
	scanner := bufio.NewScanner(resp.Body)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "TaskAdded") && strings.Contains(line, "SSE Broadcast Task") {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("expected SSE stream to receive TaskAdded event")
	}
}
