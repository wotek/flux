package todo_test

import (
	"slices"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo"
)

func TestTodoListAggregate_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operations    func(list *todo.TodoListAggregate)
		wantActive    []string
		wantArchived  []string
		wantChangeset int
	}{
		{
			name: "create list with title",
			operations: func(list *todo.TodoListAggregate) {
				list.Create("Groceries")
			},
			wantActive:    nil,
			wantArchived:  nil,
			wantChangeset: 1,
		},
		{
			name: "create list is idempotent",
			operations: func(list *todo.TodoListAggregate) {
				list.Create("Groceries")
				list.Create("Different Title")
			},
			wantActive:    nil,
			wantArchived:  nil,
			wantChangeset: 1,
		},
		{
			name: "add single task",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Buy milk")
			},
			wantActive:    []string{"Buy milk"},
			wantArchived:  nil,
			wantChangeset: 1,
		},
		{
			name: "add duplicate task is idempotent",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Buy milk")
				list.Add("Buy milk")
			},
			wantActive:    []string{"Buy milk"},
			wantArchived:  nil,
			wantChangeset: 1,
		},
		{
			name: "add empty task is ignored",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("")
			},
			wantActive:    nil,
			wantArchived:  nil,
			wantChangeset: 0,
		},
		{
			name: "remove active task",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Buy milk")
				list.Add("Read book")
				list.Remove("Buy milk")
			},
			wantActive:    []string{"Read book"},
			wantArchived:  nil,
			wantChangeset: 3,
		},
		{
			name: "remove non-existent task is no-op",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Buy milk")
				list.Remove("Non existent")
			},
			wantActive:    []string{"Buy milk"},
			wantArchived:  nil,
			wantChangeset: 1,
		},
		{
			name: "mark task as done moves to archived",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Task 1")
				list.Add("Task 2")
				list.Done("Task 1")
			},
			wantActive:    []string{"Task 2"},
			wantArchived:  []string{"Task 1"},
			wantChangeset: 3,
		},
		{
			name: "mark multiple tasks done",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Task 1")
				list.Add("Task 2")
				list.Add("Task 3")
				list.Done("Task 1", "Task 3")
			},
			wantActive:    []string{"Task 2"},
			wantArchived:  []string{"Task 1", "Task 3"},
			wantChangeset: 4,
		},
		{
			name: "mark non-existent tasks done is no-op",
			operations: func(list *todo.TodoListAggregate) {
				list.Add("Task 1")
				list.Done("Non existent")
			},
			wantActive:    []string{"Task 1"},
			wantArchived:  nil,
			wantChangeset: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stream := flux.Stream{Identifier: flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:test")}
			list := todo.NewTodoListAggregate(stream)

			tt.operations(list)

			gotActive := list.ActiveTasks()
			if !slices.Equal(gotActive, tt.wantActive) && (len(gotActive) != 0 || len(tt.wantActive) != 0) {
				t.Errorf("active tasks mismatch: got %v, want %v", gotActive, tt.wantActive)
			}

			gotArchived := list.ArchivedTasks()
			if !slices.Equal(gotArchived, tt.wantArchived) && (len(gotArchived) != 0 || len(tt.wantArchived) != 0) {
				t.Errorf("archived tasks mismatch: got %v, want %v", gotArchived, tt.wantArchived)
			}

			if gotLen := len(list.Changeset().Events()); gotLen != tt.wantChangeset {
				t.Errorf("changeset length mismatch: got %d, want %d", gotLen, tt.wantChangeset)
			}
		})
	}
}
