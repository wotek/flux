package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	})
}
