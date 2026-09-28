package event

import (
	"context"
	"maps"

	"github.com/wotek/flux"
)

// EventMetadata is flux.Context plus read-only access to the envelope currently
// being handled (stream coordinates and application metadata).
// It is embedded by [Context], [projection.Context], and [workflow.Context].
// It is not an io.Reader and not a CQRS read-model type.
type EventMetadata interface {
	flux.Context
	EventIdentifier() flux.Identifier
	Stream() flux.Stream
	Revision() uint64
	Position() uint64
	Metadata() map[string]string
}

// Context provides EventMetadata and supports reparenting the underlying Go context.
type Context interface {
	EventMetadata
	WithParent(parent context.Context) Context
}

type eventContext struct {
	flux.Context
	eventID  flux.Identifier
	stream   flux.Stream
	revision uint64
	position uint64
	metadata map[string]string
}

var _ EventMetadata = (*eventContext)(nil)
var _ Context = (*eventContext)(nil)
var _ flux.Context = (*eventContext)(nil)

func (e *eventContext) EventIdentifier() flux.Identifier { return e.eventID }
func (e *eventContext) Stream() flux.Stream              { return e.stream }
func (e *eventContext) Revision() uint64                 { return e.revision }
func (e *eventContext) Position() uint64                 { return e.position }
func (e *eventContext) Metadata() map[string]string      { return maps.Clone(e.metadata) }

// WithParent returns a clone of the event Context with the underlying Go context replaced.
func (e *eventContext) WithParent(parent context.Context) Context {
	return &eventContext{
		Context: flux.NewContext(
			parent,
			e.Actor(),
			e.CorrelationIdentifier(),
			e.CausationIdentifier(),
			flux.WithInstrumentation(e.Instrumentation()),
		),
		eventID:  e.eventID,
		stream:   e.stream,
		revision: e.revision,
		position: e.position,
		metadata: e.metadata,
	}
}

// NewContext creates a new event Context from a parent context and Envelope.
func NewContext(parent context.Context, env flux.Envelope, opts ...flux.ContextOption) Context {
	var allOpts []flux.ContextOption
	if len(env.Metadata) > 0 {
		traceID := env.Metadata[flux.MetadataTraceID]
		spanID := env.Metadata[flux.MetadataSpanID]
		if traceID != "" && spanID != "" {
			allOpts = append(allOpts, flux.WithInstrumentation(flux.Instrumentation{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: env.Metadata[flux.MetadataTraceFlags],
			}))
		}
	}
	allOpts = append(allOpts, opts...)

	return &eventContext{
		Context:  flux.NewContext(parent, env.Actor, env.CorrelationIdentifier, env.CausationIdentifier, allOpts...),
		eventID:  env.Identifier,
		stream:   env.Stream,
		revision: env.Revision,
		position: env.Position,
		metadata: env.Metadata,
	}
}

