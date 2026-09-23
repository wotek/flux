package event

import (
	"context"

	"github.com/wotek/flux"
)

// Context provides strongly-typed access to Envelope metadata
// while keeping the event payload clean.
type Context interface {
	flux.Context
	EventIdentifier() flux.Identifier
	Stream() flux.Stream
	Revision() uint64
	Position() uint64
	Metadata() map[string]string
}

type eventContext struct {
	flux.Context
	eventID  flux.Identifier
	stream   flux.Stream
	revision uint64
	position uint64
	metadata map[string]string
}

func (e *eventContext) EventIdentifier() flux.Identifier { return e.eventID }
func (e *eventContext) Stream() flux.Stream              { return e.stream }
func (e *eventContext) Revision() uint64                 { return e.revision }
func (e *eventContext) Position() uint64                 { return e.position }
func (e *eventContext) Metadata() map[string]string      { return e.metadata }

// NewContext creates a new event Context from a parent context and Envelope.
func NewContext(parent context.Context, env flux.Envelope) Context {
	return &eventContext{
		Context:  flux.NewContext(parent, env.Actor, env.CorrelationIdentifier, env.Identifier),
		eventID:  env.Identifier,
		stream:   env.Stream,
		revision: env.Revision,
		position: env.Position,
		metadata: env.Metadata,
	}
}
