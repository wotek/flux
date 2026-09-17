package saga

import (
	"github.com/wotek/flux"
)

// Saga defines the contract for a process manager.
// It leverages Go 1.26 self-referencing constraints for reflection-free instantiation.
type Saga[S Saga[S]] interface {
	// Identifier returns the globally unique ID of this saga instance.
	// This is typically derived from the CorrelationIdentifier of the triggering event.
	Identifier() flux.Identifier

	// New creates a new, empty instance of the saga.
	// This is called on a nil pointer by the Orchestrator during loading.
	New() S
}
