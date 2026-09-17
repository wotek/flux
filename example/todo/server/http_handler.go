package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
	"github.com/wotek/flux/query"
)

// HTTPHandler routes HTTP gateway endpoints to CQRS command and query buses using Echo.
type HTTPHandler struct {
	cmdBus     *command.Bus
	queryBus   *query.Bus
	eventStore flux.EventStore
	logger     *slog.Logger
	echo       *echo.Echo
}

type createListRequest struct {
	ListIdentifier string `json:"list_identifier"`
	Title          string `json:"title"`
}

type addTaskRequest struct {
	ListIdentifier string `json:"list_identifier"`
	Task           string `json:"task"`
}

type removeTaskRequest struct {
	ListIdentifier string `json:"list_identifier"`
	Task           string `json:"task"`
}

type doneTasksRequest struct {
	ListIdentifier string   `json:"list_identifier"`
	Tasks          []string `json:"tasks"`
}

// NewHTTPHandler creates and configures a new [HTTPHandler] backed by Echo.
func NewHTTPHandler(cmdBus *command.Bus, queryBus *query.Bus, eventStore flux.EventStore, logger *slog.Logger) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	h := &HTTPHandler{
		cmdBus:     cmdBus,
		queryBus:   queryBus,
		eventStore: eventStore,
		logger:     logger,
		echo:       e,
	}

	// Logging middleware capturing request lifecycle and status code
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			req := c.Request()
			h.logger.DebugContext(req.Context(), "server: http request received",
				"method", req.Method,
				"path", req.URL.Path,
				"query", req.URL.RawQuery,
				"remote_addr", req.RemoteAddr,
			)

			err := next(c)

			h.logger.DebugContext(req.Context(), "server: http request completed",
				"method", req.Method,
				"path", req.URL.Path,
				"status", c.Response().Status,
				"duration", time.Since(start),
			)
			return err
		}
	})

	e.POST("/lists", h.handleCreateList)
	e.GET("/lists", h.handleGetLists)
	e.POST("/tasks", h.handleAddTask)
	e.DELETE("/tasks", h.handleRemoveTask)
	e.POST("/tasks/done", h.handleDoneTasks)
	e.GET("/tasks", h.handleGetTasks)
	e.GET("/counter", h.handleGetCounter)
	e.GET("/events", h.handleEvents)

	return h
}

// Echo returns the underlying [*echo.Echo] engine instance.
func (h *HTTPHandler) Echo() *echo.Echo {
	return h.echo
}

// ServeHTTP delegates to the Echo engine, satisfying the standard [http.Handler] interface.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.echo.ServeHTTP(w, r)
}

func decodeJSON(c echo.Context, v any) error {
	return json.NewDecoder(c.Request().Body).Decode(v)
}

func (h *HTTPHandler) handleCreateList(c echo.Context) error {
	var req createListRequest
	if err := decodeJSON(c, &req); err != nil {
		h.logger.DebugContext(c.Request().Context(), "server: invalid json payload for create list", "error", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
	}
	if req.ListIdentifier == "" {
		h.logger.DebugContext(c.Request().Context(), "server: create list missing list_identifier")
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "list_identifier is required"})
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:createlist-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(c.Request().Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(cmdCtx, "server: executing CreateList command",
		"cmd_id", cmdID.String(),
		"list_id", req.ListIdentifier,
		"title", req.Title,
	)

	cmd := commands.CreateList{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Title:          req.Title,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		h.logger.ErrorContext(cmdCtx, "server: CreateList command failed", "cmd_id", cmdID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(cmdCtx, "server: CreateList command succeeded", "cmd_id", cmdID.String(), "list_id", req.ListIdentifier)
	return c.JSON(http.StatusCreated, map[string]string{"status": "list created"})
}

func (h *HTTPHandler) handleGetLists(c echo.Context) error {
	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:lists-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(c.Request().Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(queryCtx, "server: executing GetLists query", "query_id", queryID.String())

	res, err := query.Execute[queries.GetLists, []lists.ListSummary](queryCtx, h.queryBus, queries.GetLists{})
	if err != nil {
		h.logger.ErrorContext(queryCtx, "server: GetLists query failed", "query_id", queryID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(queryCtx, "server: GetLists query succeeded", "query_id", queryID.String(), "count", len(res))
	return c.JSON(http.StatusOK, res)
}

func (h *HTTPHandler) handleAddTask(c echo.Context) error {
	var req addTaskRequest
	if err := decodeJSON(c, &req); err != nil {
		h.logger.DebugContext(c.Request().Context(), "server: invalid json payload for add task", "error", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
	}
	if req.ListIdentifier == "" || req.Task == "" {
		h.logger.DebugContext(c.Request().Context(), "server: add task missing list_identifier or task")
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:add-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(c.Request().Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(cmdCtx, "server: executing AddTask command",
		"cmd_id", cmdID.String(),
		"list_id", req.ListIdentifier,
		"task", req.Task,
	)

	cmd := commands.AddTask{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Task:           req.Task,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		h.logger.ErrorContext(cmdCtx, "server: AddTask command failed", "cmd_id", cmdID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(cmdCtx, "server: AddTask command succeeded", "cmd_id", cmdID.String(), "task", req.Task)
	return c.JSON(http.StatusCreated, map[string]string{"status": "task added"})
}

func (h *HTTPHandler) handleRemoveTask(c echo.Context) error {
	var req removeTaskRequest
	if err := decodeJSON(c, &req); err != nil {
		h.logger.DebugContext(c.Request().Context(), "server: invalid json payload for remove task", "error", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
	}
	if req.ListIdentifier == "" || req.Task == "" {
		h.logger.DebugContext(c.Request().Context(), "server: remove task missing list_identifier or task")
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:remove-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(c.Request().Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(cmdCtx, "server: executing RemoveTask command",
		"cmd_id", cmdID.String(),
		"list_id", req.ListIdentifier,
		"task", req.Task,
	)

	cmd := commands.RemoveTask{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Task:           req.Task,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		h.logger.ErrorContext(cmdCtx, "server: RemoveTask command failed", "cmd_id", cmdID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(cmdCtx, "server: RemoveTask command succeeded", "cmd_id", cmdID.String(), "task", req.Task)
	return c.JSON(http.StatusOK, map[string]string{"status": "task removed"})
}

func (h *HTTPHandler) handleDoneTasks(c echo.Context) error {
	var req doneTasksRequest
	if err := decodeJSON(c, &req); err != nil {
		h.logger.DebugContext(c.Request().Context(), "server: invalid json payload for done tasks", "error", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
	}
	if req.ListIdentifier == "" || len(req.Tasks) == 0 {
		h.logger.DebugContext(c.Request().Context(), "server: done tasks missing list_identifier or tasks")
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "list_identifier and tasks are required"})
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:done-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(c.Request().Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(cmdCtx, "server: executing DoneTasks command",
		"cmd_id", cmdID.String(),
		"list_id", req.ListIdentifier,
		"tasks", req.Tasks,
	)

	cmd := commands.DoneTasks{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Tasks:          req.Tasks,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		h.logger.ErrorContext(cmdCtx, "server: DoneTasks command failed", "cmd_id", cmdID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(cmdCtx, "server: DoneTasks command succeeded", "cmd_id", cmdID.String(), "tasks", req.Tasks)
	return c.JSON(http.StatusOK, map[string]string{"status": "tasks marked done"})
}

func (h *HTTPHandler) handleGetTasks(c echo.Context) error {
	listIDStr := c.QueryParam("list_identifier")
	if listIDStr == "" {
		h.logger.DebugContext(c.Request().Context(), "server: get tasks missing list_identifier")
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "list_identifier query parameter is required"})
	}

	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:tasks-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(c.Request().Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(queryCtx, "server: executing GetTodoList query",
		"query_id", queryID.String(),
		"list_id", listIDStr,
	)

	q := queries.GetTodoList{
		ListIdentifier: flux.NewIdentifierFromString(listIDStr),
	}
	result, err := query.Execute[queries.GetTodoList, queries.TodoList](queryCtx, h.queryBus, q)
	if err != nil {
		h.logger.ErrorContext(queryCtx, "server: GetTodoList query failed", "query_id", queryID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(queryCtx, "server: GetTodoList query succeeded",
		"query_id", queryID.String(),
		"active_count", len(result.Active),
		"archived_count", len(result.Archived),
	)
	return c.JSON(http.StatusOK, result)
}

func (h *HTTPHandler) handleGetCounter(c echo.Context) error {
	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:counter-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(c.Request().Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(queryCtx, "server: executing GetCounter query", "query_id", queryID.String())

	result, err := query.Execute[queries.GetCounter, counter.Counter](queryCtx, h.queryBus, queries.GetCounter{})
	if err != nil {
		h.logger.ErrorContext(queryCtx, "server: GetCounter query failed", "query_id", queryID.String(), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	h.logger.DebugContext(queryCtx, "server: GetCounter query succeeded",
		"query_id", queryID.String(),
		"active", result.Active,
		"archived", result.Archived,
		"removed", result.Removed,
	)
	return c.JSON(http.StatusOK, result)
}

func (h *HTTPHandler) handleEvents(c echo.Context) error {
	res := c.Response()
	req := c.Request()

	res.Header().Set("Content-Type", "text/event-stream")
	res.Header().Set("Cache-Control", "no-cache")
	res.Header().Set("Connection", "keep-alive")
	res.Header().Set("Access-Control-Allow-Origin", "*")
	res.WriteHeader(http.StatusOK)
	res.Flush()

	var currentPos uint64
	if posStr := c.QueryParam("position"); posStr != "" {
		if p, err := strconv.ParseUint(posStr, 10, 64); err == nil {
			currentPos = p
		}
	} else if lastID := req.Header.Get("Last-Event-ID"); lastID != "" {
		if p, err := strconv.ParseUint(lastID, 10, 64); err == nil {
			currentPos = p
		}
	} else if h.eventStore != nil {
		// Default to current latest position so connected client receives new events
		iter, _ := h.eventStore.Stream(req.Context(), 0)
		if iter != nil {
			for env := range iter {
				if env.Position > currentPos {
					currentPos = env.Position
				}
			}
		}
	}

	h.logger.DebugContext(req.Context(), "server: sse client connected", "remote_addr", req.RemoteAddr, "start_position", currentPos)
	defer h.logger.DebugContext(req.Context(), "server: sse client disconnected", "remote_addr", req.RemoteAddr)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-req.Context().Done():
			return nil
		case <-ticker.C:
			if h.eventStore == nil {
				continue
			}
			iter, err := h.eventStore.Stream(req.Context(), currentPos)
			if err != nil {
				return err
			}
			for env, err := range iter {
				if err != nil {
					return err
				}
				currentPos = env.Position

				payload := sseEventPayload{
					Type:           env.Event.Name(),
					ListIdentifier: env.Stream.Identifier.String(),
					Position:       env.Position,
				}

				switch e := env.Event.(type) {
				case events.TaskAdded:
					payload.Description = fmt.Sprintf("Task added: %q", e.Task)
				case events.TaskRemoved:
					payload.Description = fmt.Sprintf("Task removed: %q", e.Task)
				case events.TasksDone:
					payload.Description = fmt.Sprintf("Tasks completed: %s", strings.Join(e.Tasks, ", "))
				case events.ListCreated:
					payload.Description = fmt.Sprintf("List created: %q", e.Title)
				default:
					payload.Description = env.Event.Name()
				}

				data, _ := json.Marshal(payload)
				if _, err := fmt.Fprintf(res, "id: %d\nevent: %s\ndata: %s\n\n", env.Position, env.Event.Name(), string(data)); err != nil {
					return err
				}
				res.Flush()
			}
		}
	}
}
