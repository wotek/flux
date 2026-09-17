package events

// TaskAdded indicates that a new task was added to a todo list.
type TaskAdded struct {
	Task string
}

// Name returns the canonical domain event name for [TaskAdded].
func (e TaskAdded) Name() string {
	return "TaskAdded"
}

// isTodoEvent seals [TaskAdded] to the [TodoEvent] interface.
func (e TaskAdded) isTodoEvent() {}
