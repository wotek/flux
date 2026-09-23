package query

import (
	"context"

	"github.com/wotek/flux"
)

// Context provides strongly-typed access to query metadata.
type Context interface {
	flux.Context
	QueryIdentifier() flux.Identifier
}

type queryContext struct {
	flux.Context
	queryID flux.Identifier
}

func (q *queryContext) QueryIdentifier() flux.Identifier {
	return q.queryID
}

// NewContext creates a new query Context with the given metadata.
func NewContext(parent context.Context, queryID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier) Context {
	return &queryContext{
		Context: flux.NewContext(parent, actor, correlationID, causationID),
		queryID: queryID,
	}
}
