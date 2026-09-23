package queries

import (
	"fmt"

	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/query"
)

// GetLists is a read-only query requesting all known todo lists and their summary statistics.
type GetLists struct{}

// GetListsHandler executes the [GetLists] query against a [lists.Store].
type GetListsHandler struct {
	store lists.Store
}

// NewGetListsHandler constructs a new [GetListsHandler].
func NewGetListsHandler(store lists.Store) *GetListsHandler {
	return &GetListsHandler{store: store}
}

// Handle executes the [GetLists] query.
func (h *GetListsHandler) Handle(ctx query.Context, _ GetLists) ([]lists.ListSummary, error) {
	items, err := h.store.GetLists(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving lists: %w", err)
	}
	return items, nil
}

// RegisterGetListsHandler registers the [GetListsHandler] on the given query bus.
func RegisterGetListsHandler(bus *query.Bus, store lists.Store) {
	query.RegisterHandler(bus, NewGetListsHandler(store))
}
