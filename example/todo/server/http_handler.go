package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
	"github.com/wotek/flux/query"
)

// HTTPHandler routes HTTP gateway endpoints to CQRS command and query buses.
type HTTPHandler struct {
	cmdBus   *command.Bus
	queryBus *query.Bus
	logger   *slog.Logger
	mux      *http.ServeMux
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

// NewHTTPHandler creates and configures a new [HTTPHandler].
func NewHTTPHandler(cmdBus *command.Bus, queryBus *query.Bus, logger *slog.Logger) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}

	h := &HTTPHandler{
		cmdBus:   cmdBus,
		queryBus: queryBus,
		logger:   logger,
		mux:      http.NewServeMux(),
	}

	h.mux.HandleFunc("POST /lists", h.handleCreateList)
	h.mux.HandleFunc("GET /lists", h.handleGetLists)
	h.mux.HandleFunc("POST /tasks", h.handleAddTask)
	h.mux.HandleFunc("DELETE /tasks", h.handleRemoveTask)
	h.mux.HandleFunc("POST /tasks/done", h.handleDoneTasks)
	h.mux.HandleFunc("GET /tasks", h.handleGetTasks)
	h.mux.HandleFunc("GET /counter", h.handleGetCounter)

	return h
}

// ServeHTTP delegates to the internal multiplexer and logs request lifecycle at debug level.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logger.DebugContext(r.Context(), "server: http request received",
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"remote_addr", r.RemoteAddr,
	)

	rec := newResponseRecorder(w)
	h.mux.ServeHTTP(rec, r)

	h.logger.DebugContext(r.Context(), "server: http request completed",
		"method", r.Method,
		"path", r.URL.Path,
		"status", rec.statusCode,
		"duration", time.Since(start),
	)
}

func (h *HTTPHandler) handleCreateList(w http.ResponseWriter, r *http.Request) {
	var req createListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.DebugContext(r.Context(), "server: invalid json payload for create list", "error", err)
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" {
		h.logger.DebugContext(r.Context(), "server: create list missing list_identifier")
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier is required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:createlist-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

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
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(cmdCtx, "server: CreateList command succeeded", "cmd_id", cmdID.String(), "list_id", req.ListIdentifier)
	respondJSON(w, http.StatusCreated, map[string]string{"status": "list created"})
}

func (h *HTTPHandler) handleGetLists(w http.ResponseWriter, r *http.Request) {
	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:lists-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(r.Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(queryCtx, "server: executing GetLists query", "query_id", queryID.String())

	res, err := query.Execute[queries.GetLists, []lists.ListSummary](queryCtx, h.queryBus, queries.GetLists{})
	if err != nil {
		h.logger.ErrorContext(queryCtx, "server: GetLists query failed", "query_id", queryID.String(), "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(queryCtx, "server: GetLists query succeeded", "query_id", queryID.String(), "count", len(res))
	respondJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) handleAddTask(w http.ResponseWriter, r *http.Request) {
	var req addTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.DebugContext(r.Context(), "server: invalid json payload for add task", "error", err)
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || req.Task == "" {
		h.logger.DebugContext(r.Context(), "server: add task missing list_identifier or task")
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:add-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

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
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(cmdCtx, "server: AddTask command succeeded", "cmd_id", cmdID.String(), "task", req.Task)
	respondJSON(w, http.StatusCreated, map[string]string{"status": "task added"})
}

func (h *HTTPHandler) handleRemoveTask(w http.ResponseWriter, r *http.Request) {
	var req removeTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.DebugContext(r.Context(), "server: invalid json payload for remove task", "error", err)
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || req.Task == "" {
		h.logger.DebugContext(r.Context(), "server: remove task missing list_identifier or task")
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:remove-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

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
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(cmdCtx, "server: RemoveTask command succeeded", "cmd_id", cmdID.String(), "task", req.Task)
	respondJSON(w, http.StatusOK, map[string]string{"status": "task removed"})
}

func (h *HTTPHandler) handleDoneTasks(w http.ResponseWriter, r *http.Request) {
	var req doneTasksRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.DebugContext(r.Context(), "server: invalid json payload for done tasks", "error", err)
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || len(req.Tasks) == 0 {
		h.logger.DebugContext(r.Context(), "server: done tasks missing list_identifier or tasks")
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and tasks are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:done-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

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
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(cmdCtx, "server: DoneTasks command succeeded", "cmd_id", cmdID.String(), "tasks", req.Tasks)
	respondJSON(w, http.StatusOK, map[string]string{"status": "tasks marked done"})
}

func (h *HTTPHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	listIDStr := r.URL.Query().Get("list_identifier")
	if listIDStr == "" {
		h.logger.DebugContext(r.Context(), "server: get tasks missing list_identifier")
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier query parameter is required"})
		return
	}

	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:tasks-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(r.Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

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
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(queryCtx, "server: GetTodoList query succeeded",
		"query_id", queryID.String(),
		"active_count", len(result.Active),
		"archived_count", len(result.Archived),
	)
	respondJSON(w, http.StatusOK, result)
}

func (h *HTTPHandler) handleGetCounter(w http.ResponseWriter, r *http.Request) {
	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:counter-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(r.Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	h.logger.DebugContext(queryCtx, "server: executing GetCounter query", "query_id", queryID.String())

	result, err := query.Execute[queries.GetCounter, counter.Counter](queryCtx, h.queryBus, queries.GetCounter{})
	if err != nil {
		h.logger.ErrorContext(queryCtx, "server: GetCounter query failed", "query_id", queryID.String(), "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.DebugContext(queryCtx, "server: GetCounter query succeeded",
		"query_id", queryID.String(),
		"active", result.Active,
		"archived", result.Archived,
		"removed", result.Removed,
	)
	respondJSON(w, http.StatusOK, result)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
