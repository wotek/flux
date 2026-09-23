package codec

import (
	"errors"

	"github.com/wotek/flux"
)

var (
	// ErrNilEvent is returned when attempting to marshal an envelope with a nil Event.
	ErrNilEvent = errors.New("envelope event cannot be nil")

	// ErrEmptyEventName is returned when unmarshaling an envelope that lacks an event name.
	ErrEmptyEventName = errors.New("event name cannot be empty")
)

// Serializer marshals and unmarshals [flux.Envelope] instances to and from byte slices.
type Serializer interface {
	// Marshal encodes a [flux.Envelope] into its binary representation.
	Marshal(env flux.Envelope) ([]byte, error)

	// Unmarshal decodes a binary representation into a [flux.Envelope].
	Unmarshal(data []byte) (flux.Envelope, error)
}
