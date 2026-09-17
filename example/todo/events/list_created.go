package events

// ListCreated is emitted when a new todo list aggregate is initialized.
type ListCreated struct {
	Title string `json:"title"`
}

// Name returns the unique event name identifier for serialization and routing.
func (ListCreated) Name() string {
	return "todo.list.created.v1"
}

// isTodoEvent seals the event interface to prevent external package leakage.
func (ListCreated) isTodoEvent() {}
