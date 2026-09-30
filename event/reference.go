package event

import "github.com/wotek/flux"

// EventReference identifies a single event occurrence in the Event Store.
// It is intentionally payload-free so callers (e.g. Temporal workflows) can
// pass a stable pointer and load the envelope via flux.EventStore.Find.
type EventReference struct {
	Stream  flux.Identifier `json:"stream"`
	EventID flux.Identifier `json:"event_id"`
}
