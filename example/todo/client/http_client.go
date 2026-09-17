package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
)

var _ Client = (*HTTPClient)(nil)

// HTTPClientOption allows configuring the [HTTPClient].
type HTTPClientOption func(*HTTPClient)

// WithHTTPClient sets a custom underlying [*http.Client].
func WithHTTPClient(httpClient *http.Client) HTTPClientOption {
	return func(c *HTTPClient) {
		if httpClient != nil {
			c.httpClient = httpClient
		}
	}
}

// HTTPClient communicates with the Todo CQRS application over HTTP.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPClient constructs a new [HTTPClient] pointing to the given base URL.
func NewHTTPClient(baseURL string, opts ...HTTPClientOption) *HTTPClient {
	c := &HTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CreateList sends a POST /lists request to initialize a new todo list.
func (c *HTTPClient) CreateList(ctx context.Context, listIdentifier flux.Identifier, title string) error {
	payload := map[string]string{
		"list_identifier": listIdentifier.String(),
		"title":           title,
	}
	return c.sendJSON(ctx, http.MethodPost, "/lists", payload, nil)
}

// AddTask sends a POST /tasks request to the server.
func (c *HTTPClient) AddTask(ctx context.Context, listIdentifier flux.Identifier, task string) error {
	payload := map[string]string{
		"list_identifier": listIdentifier.String(),
		"task":           task,
	}
	return c.sendJSON(ctx, http.MethodPost, "/tasks", payload, nil)
}

// RemoveTask sends a DELETE /tasks request to the server.
func (c *HTTPClient) RemoveTask(ctx context.Context, listIdentifier flux.Identifier, task string) error {
	payload := map[string]string{
		"list_identifier": listIdentifier.String(),
		"task":           task,
	}
	return c.sendJSON(ctx, http.MethodDelete, "/tasks", payload, nil)
}

// DoneTasks sends a POST /tasks/done request to the server.
func (c *HTTPClient) DoneTasks(ctx context.Context, listIdentifier flux.Identifier, tasks ...string) error {
	payload := map[string]any{
		"list_identifier": listIdentifier.String(),
		"tasks":          tasks,
	}
	return c.sendJSON(ctx, http.MethodPost, "/tasks/done", payload, nil)
}

// GetCounter sends a GET /counter request to the server and returns the parsed snapshot.
func (c *HTTPClient) GetCounter(ctx context.Context) (counter.Counter, error) {
	var snapshot counter.Counter
	if err := c.sendJSON(ctx, http.MethodGet, "/counter", nil, &snapshot); err != nil {
		return counter.Counter{}, fmt.Errorf("http get counter: %w", err)
	}
	return snapshot, nil
}

// GetTodoList sends a GET /tasks?list_identifier=... request to the server and returns the task list.
func (c *HTTPClient) GetTodoList(ctx context.Context, listIdentifier flux.Identifier) (queries.TodoList, error) {
	endpoint := fmt.Sprintf("/tasks?list_identifier=%s", url.QueryEscape(listIdentifier.String()))
	var todoList queries.TodoList
	if err := c.sendJSON(ctx, http.MethodGet, endpoint, nil, &todoList); err != nil {
		return queries.TodoList{}, fmt.Errorf("http get todo list: %w", err)
	}
	return todoList, nil
}

// GetLists sends a GET /lists request to the server and returns all known todo lists.
func (c *HTTPClient) GetLists(ctx context.Context) ([]lists.ListSummary, error) {
	var allLists []lists.ListSummary
	if err := c.sendJSON(ctx, http.MethodGet, "/lists", nil, &allLists); err != nil {
		return nil, fmt.Errorf("http get lists: %w", err)
	}
	return allLists, nil
}

func (c *HTTPClient) sendJSON(ctx context.Context, method, path string, requestBody any, responseTarget any) error {
	var bodyReader io.Reader
	if requestBody != nil {
		data, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http execute %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http %s %s returned status %d: %s", method, path, resp.StatusCode, string(bodyBytes))
	}

	if responseTarget != nil {
		if err := json.NewDecoder(resp.Body).Decode(responseTarget); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}
