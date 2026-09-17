package todo

// TasksDone indicates that one or more active tasks were marked as completed.
type TasksDone struct {
	Tasks []string
}

// Name returns the canonical domain event name for [TasksDone].
func (e TasksDone) Name() string {
	return "TasksDone"
}

// isTodoEvent seals [TasksDone] to the [TodoEvent] interface.
func (e TasksDone) isTodoEvent() {}
