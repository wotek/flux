package flux

// Event represents a strongly-typed domain event payload.
// This is an interface that concrete event types (e.g., UserCreated) implement.
type Event interface {
	// Name returns the string representation of the event type.
	Name() string
}
