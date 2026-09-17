package todo

import (
	"slices"

	"github.com/wotek/flux"
)

// TodoListAggregate is the domain aggregate root maintaining the lifecycle of a todo list.
type TodoListAggregate struct {
	flux.AggregateRoot[TodoEvent]

	active   []string
	archived []string
}

// NewTodoListAggregate constructs a new [TodoListAggregate] bound to the given stream.
func NewTodoListAggregate(stream flux.Stream) *TodoListAggregate {
	list := &TodoListAggregate{}
	list.AggregateRoot = flux.NewAggregateRoot[TodoEvent](stream, flux.NewChangeset[TodoEvent](), list.apply)
	return list
}

// New implements the self-referencing generic constraint for [flux.Aggregate] instantiation.
func (l *TodoListAggregate) New(stream flux.Stream) *TodoListAggregate {
	return NewTodoListAggregate(stream)
}

// apply mutates the aggregate's internal state in response to historical or uncommitted events.
func (l *TodoListAggregate) apply(event TodoEvent) error {
	switch e := event.(type) {
	case TaskAdded:
		if !slices.Contains(l.active, e.Task) {
			l.active = append(l.active, e.Task)
		}
	case TaskRemoved:
		l.active = slices.DeleteFunc(l.active, func(t string) bool {
			return t == e.Task
		})
	case TasksDone:
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

// Add appends a new task to the active list if it is not already present (idempotent).
func (l *TodoListAggregate) Add(task string) {
	if task == "" || slices.Contains(l.active, task) {
		return
	}

	event := TaskAdded{Task: task}
	l.Changeset().Record(event)
	_ = l.apply(event)
}

// Remove deletes a task from the active list if present.
func (l *TodoListAggregate) Remove(task string) {
	if !slices.Contains(l.active, task) {
		return
	}

	event := TaskRemoved{Task: task}
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

	event := TasksDone{Tasks: validTasks}
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
