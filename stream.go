package flux

import "iter"

// Stream represents a sequence of events for a specific aggregate or entity.
type Stream struct {
	// Identifier is the unique identifier of the stream.
	Identifier Identifier
}

// StreamIterator is an idiomatic Go 1.23+ iterator that yields Envelopes and errors.
// It is used for safely streaming large numbers of events from the EventStore.
type StreamIterator iter.Seq2[Envelope, error]
