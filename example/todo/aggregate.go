package todo

import (
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/events"
)

// TodoListAggregate is the domain aggregate root maintaining the lifecycle of a todo list.
type TodoListAggregate struct {
	flux.AggregateRoot[events.TodoEvent]

	title    string
	active   []string
	archived []string
}

// NewTodoListAggregate constructs a new [TodoListAggregate] bound to the given stream.
func NewTodoListAggregate(stream flux.Stream) *TodoListAggregate {
	list := &TodoListAggregate{}
	list.AggregateRoot = flux.NewAggregateRoot[events.TodoEvent](stream, flux.NewChangeset[events.TodoEvent](), list.apply)
	return list
}

// New implements the self-referencing generic constraint for [flux.Aggregate] instantiation.
func (l *TodoListAggregate) New(stream flux.Stream) *TodoListAggregate {
	return NewTodoListAggregate(stream)
}

// apply mutates the aggregate's internal state in response to historical or uncommitted events.
func (l *TodoListAggregate) apply(event events.TodoEvent) error {
	switch e := event.(type) {
	case events.ListCreated:
		l.title = e.Title
	case events.TaskAdded:
		if !slices.Contains(l.active, e.Task) {
			l.active = append(l.active, e.Task)
		}
	case events.TaskRemoved:
		l.active = slices.DeleteFunc(l.active, func(t string) bool {
			return t == e.Task
		})
	case events.TasksDone:
		for _, task := range e.Tasks {
			l.active = slices.DeleteFunc(l.active, func(t string) bool {
				return t == task
			})
			if !slices.Contains(l.archived, task) {
				l.archived = append(l.archived, task)
			}
		}
	}
	return nil
}

// Create initializes the todo list with a title (idempotent).
func (l *TodoListAggregate) Create(title string) {
	if l.title != "" {
		return
	}
	if title == "" {
		title = "Untitled List"
	}

	event := events.ListCreated{Title: title}
	l.Changeset().Record(event)
	_ = l.apply(event)
}

// Title returns the human-readable title of this todo list.
func (l *TodoListAggregate) Title() string {
	if l.title == "" {
		return "Default List"
	}
	return l.title
}

// Add appends a new task to the active list if it is not already present (idempotent).
func (l *TodoListAggregate) Add(task string) {
	if task == "" || slices.Contains(l.active, task) {
		return
	}

	event := events.TaskAdded{Task: task}
	l.Changeset().Record(event)
	_ = l.apply(event)
}

// Remove deletes a task from the active list if present.
func (l *TodoListAggregate) Remove(task string) {
	if !slices.Contains(l.active, task) {
		return
	}

	event := events.TaskRemoved{Task: task}
	l.Changeset().Record(event)
	_ = l.apply(event)
}

// Done marks one or more active tasks as completed and archives them.
func (l *TodoListAggregate) Done(tasks ...string) {
	validTasks := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if slices.Contains(l.active, task) && !slices.Contains(validTasks, task) {
			validTasks = append(validTasks, task)
		}
	}

	if len(validTasks) == 0 {
		return
	}

	event := events.TasksDone{Tasks: validTasks}
	l.Changeset().Record(event)
	_ = l.apply(event)
}

// ActiveTasks returns a copy of the currently active tasks in this list.
func (l *TodoListAggregate) ActiveTasks() []string {
	return slices.Clone(l.active)
}

// ArchivedTasks returns a copy of the completed/archived tasks in this list.
func (l *TodoListAggregate) ArchivedTasks() []string {
	return slices.Clone(l.archived)
}
