package query

import (
	"context"

	"github.com/wotek/flux"
)

// Context provides strongly-typed access to query metadata.
type Context interface {
	flux.Context
	QueryIdentifier() flux.Identifier
	WithParent(parent context.Context) Context
}

type queryContext struct {
	flux.Context
	queryID flux.Identifier
}

var _ Context = (*queryContext)(nil)
var _ flux.Context = (*queryContext)(nil)

func (q *queryContext) QueryIdentifier() flux.Identifier {
	return q.queryID
}

// WithParent returns a clone of the query Context with the underlying Go context replaced.
func (q *queryContext) WithParent(parent context.Context) Context {
	return &queryContext{
		Context: flux.NewContext(
			parent,
			q.Actor(),
			q.CorrelationIdentifier(),
			q.CausationIdentifier(),
			flux.WithInstrumentation(q.Instrumentation()),
		),
		queryID: q.queryID,
	}
}

// NewContext creates a new query Context with the given metadata.
func NewContext(parent context.Context, queryID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier, opts ...flux.ContextOption) Context {
	return &queryContext{
		Context: flux.NewContext(parent, actor, correlationID, causationID, opts...),
		queryID: queryID,
	}
}

