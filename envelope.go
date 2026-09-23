package flux

import "time"

// Envelope wraps a domain event with standard framework metadata.
type Envelope struct {
	// Identifier is the globally unique identifier of this specific event occurrence.
	Identifier Identifier

	// Stream is the stream this event belongs to.
	Stream Stream

	// Revision is the sequence number of this event within its specific stream.
	Revision uint64

	// Position is the sequence number of this event in the Global Event Stream.
	Position uint64

	// Event is the actual strongly-typed domain payload.
	Event Event

	// Metadata contains optional, custom headers for the event (e.g., tracing spans, feature flags).
	Metadata map[string]string

	// CreatedAt is the time the event was generated.
	CreatedAt time.Time

	// Actor represents the user, system, or service that caused this event.
	Actor Actor

	// CorrelationIdentifier ties this event to a specific command, request, or transaction.
	CorrelationIdentifier Identifier

	// CausationIdentifier points to the ID of the specific event or command that directly caused this.
	CausationIdentifier Identifier
}
