package flux

import (
	"context"
	"log/slog"
)

// Instrumentation carries distributed tracing identifiers agnostic of any
// specific telemetry vendor.
type Instrumentation struct {
	TraceID    string
	SpanID     string
	TraceFlags string // Canonical single-byte hex (e.g., "01")
}

// IsValid reports whether the instrumentation contains valid trace and span identifiers.
func (i Instrumentation) IsValid() bool {
	return i.TraceID != "" && i.SpanID != ""
}

// Context is the base typed context for the framework, providing guaranteed
// access to audit metadata (Actor, CorrelationIdentifier, CausationIdentifier)
// and distributed tracing instrumentation across all operations.
//
// Reparenting Design Note:
// In Go, interface embedding forbids overlapping method names with differing signatures
// (duplicate method error). Because derived contexts (command.Context, query.Context, etc.)
// return their own strongly-typed interfaces from WithParent(parent context.Context),
// flux.Context intentionally does not declare WithParent to remain the universal base
// interface that all typed contexts satisfy. For untyped base contexts, use the package-level
// [WithParent] helper or call WithParent directly on concrete context types.
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
	Logger() *slog.Logger
	Instrumentation() Instrumentation
}

type baseContext struct {
	context.Context
	actor           Actor
	correlation     Identifier
	causation       Identifier
	logger          *slog.Logger
	instrumentation Instrumentation
}

var _ Context = (*baseContext)(nil)

func (b *baseContext) Actor() Actor                      { return b.actor }
func (b *baseContext) CorrelationIdentifier() Identifier { return b.correlation }
func (b *baseContext) CausationIdentifier() Identifier   { return b.causation }
func (b *baseContext) Logger() *slog.Logger              { return b.logger }
func (b *baseContext) Instrumentation() Instrumentation  { return b.instrumentation }

// WithParent returns a clone of the context with the underlying Go context.Context updated.
func (b *baseContext) WithParent(parent context.Context) Context {
	clone := *b
	clone.Context = parent
	return &clone
}

// ContextOption configures optional properties of a baseContext.
type ContextOption func(*baseContext)

// WithInstrumentation configures distributed tracing instrumentation for the context.
func WithInstrumentation(inst Instrumentation) ContextOption {
	return func(c *baseContext) {
		c.instrumentation = inst
	}
}

// WithParent returns a clone of the provided Context with the underlying Go context replaced.
func WithParent(ctx Context, parent context.Context) Context {
	if r, ok := ctx.(interface{ WithParent(context.Context) Context }); ok {
		return r.WithParent(parent)
	}
	return NewContext(parent, ctx.Actor(), ctx.CorrelationIdentifier(), ctx.CausationIdentifier(), WithInstrumentation(ctx.Instrumentation()))
}

// NewContext creates a new generic base Context.
func NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier, opts ...ContextOption) Context {
	logger := slog.Default()
	logger = logger.With(
		slog.String("actor", actor.Identifier.String()),
		slog.String("correlation_id", correlationId.String()),
	)
	if !causationId.IsEmpty() {
		logger = logger.With(slog.String("causation_id", causationId.String()))
	}

	b := &baseContext{
		Context:     parent,
		actor:       actor,
		correlation: correlationId,
		causation:   causationId,
		logger:      logger,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
	return b
}

