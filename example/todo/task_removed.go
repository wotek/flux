package todo

// TaskRemoved indicates that an active task was removed from a todo list.
type TaskRemoved struct {
	Task string
}

// Name returns the canonical domain event name for [TaskRemoved].
func (e TaskRemoved) Name() string {
	return "TaskRemoved"
}

// isTodoEvent seals [TaskRemoved] to the [TodoEvent] interface.
func (e TaskRemoved) isTodoEvent() {}
