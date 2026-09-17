package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/queries"
	"github.com/wotek/flux/query"
)

// HTTPHandler routes HTTP gateway endpoints to CQRS command and query buses.
type HTTPHandler struct {
	cmdBus   *command.Bus
	queryBus *query.Bus
	mux      *http.ServeMux
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
func NewHTTPHandler(cmdBus *command.Bus, queryBus *query.Bus) *HTTPHandler {
	h := &HTTPHandler{
		cmdBus:   cmdBus,
		queryBus: queryBus,
		mux:      http.NewServeMux(),
	}

	h.mux.HandleFunc("POST /tasks", h.handleAddTask)
	h.mux.HandleFunc("DELETE /tasks", h.handleRemoveTask)
	h.mux.HandleFunc("POST /tasks/done", h.handleDoneTasks)
	h.mux.HandleFunc("GET /tasks", h.handleGetTasks)
	h.mux.HandleFunc("GET /counter", h.handleGetCounter)

	return h
}

// ServeHTTP delegates to the internal multiplexer.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *HTTPHandler) handleAddTask(w http.ResponseWriter, r *http.Request) {
	var req addTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || req.Task == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:add-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	cmd := commands.AddTask{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Task:           req.Task,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	respondJSON(w, http.StatusCreated, map[string]string{"status": "task added"})
}

func (h *HTTPHandler) handleRemoveTask(w http.ResponseWriter, r *http.Request) {
	var req removeTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || req.Task == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and task are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:remove-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	cmd := commands.RemoveTask{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Task:           req.Task,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"status": "task removed"})
}

func (h *HTTPHandler) handleDoneTasks(w http.ResponseWriter, r *http.Request) {
	var req doneTasksRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}
	if req.ListIdentifier == "" || len(req.Tasks) == 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier and tasks are required"})
		return
	}

	cmdID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:commands:1:cmd:done-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{}, flux.Identifier{})

	cmd := commands.DoneTasks{
		ListIdentifier: flux.NewIdentifierFromString(req.ListIdentifier),
		Tasks:          req.Tasks,
	}

	if err := command.Execute(cmdCtx, h.cmdBus, cmd); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"status": "tasks marked done"})
}

func (h *HTTPHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	listIDStr := r.URL.Query().Get("list_identifier")
	if listIDStr == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "list_identifier query parameter is required"})
		return
	}

	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:tasks-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(r.Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	q := queries.GetTodoList{
		ListIdentifier: flux.NewIdentifierFromString(listIDStr),
	}
	result, err := query.Execute[queries.GetTodoList, queries.TodoList](queryCtx, h.queryBus, q)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	respondJSON(w, http.StatusOK, result)
}

func (h *HTTPHandler) handleGetCounter(w http.ResponseWriter, r *http.Request) {
	queryID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:queries:1:query:counter-%d", time.Now().UnixNano()))
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:todo:prod:users:1:user:http-client")}
	queryCtx := query.NewContext(r.Context(), queryID, actor, flux.Identifier{}, flux.Identifier{})

	result, err := query.Execute[queries.GetCounter, counter.Counter](queryCtx, h.queryBus, queries.GetCounter{})
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	respondJSON(w, http.StatusOK, result)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
