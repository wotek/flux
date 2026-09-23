package flux

import (
	"context"
	"log/slog"
)

// Context is the base typed context for the framework, providing guaranteed
// access to audit metadata (Actor, CorrelationIdentifier, CausationIdentifier)
// across all operations.
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
	Logger() *slog.Logger
}

type baseContext struct {
	context.Context
	actor       Actor
	correlation Identifier
	causation   Identifier
	logger      *slog.Logger
}

func (b *baseContext) Actor() Actor                      { return b.actor }
func (b *baseContext) CorrelationIdentifier() Identifier { return b.correlation }
func (b *baseContext) CausationIdentifier() Identifier   { return b.causation }
func (b *baseContext) Logger() *slog.Logger              { return b.logger }

// NewContext creates a new generic base Context.
func NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier) Context {
	logger := slog.Default()
	logger = logger.With(
		slog.String("actor", actor.Identifier.String()),
		slog.String("correlation_id", correlationId.String()),
	)
	if !causationId.IsEmpty() {
		logger = logger.With(slog.String("causation_id", causationId.String()))
	}

	return &baseContext{
		Context:     parent,
		actor:       actor,
		correlation: correlationId,
		causation:   causationId,
		logger:      logger,
	}
}
