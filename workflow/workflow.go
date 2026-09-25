package workflow

import (
	"github.com/wotek/flux"
)

// Workflow defines the contract for a process manager.
// It leverages Go 1.26 self-referencing constraints for reflection-free instantiation.
type Workflow[W Workflow[W]] interface {
	// Identifier returns the globally unique ID of this workflow instance.
	// This is typically derived from the CorrelationIdentifier of the triggering event.
	Identifier() flux.Identifier

	// New creates a new, empty instance of the workflow.
	// This is called on a nil pointer by the Orchestrator during loading.
	New() W

	// Clone creates an isolated copy of the workflow instance to guarantee
	// that in-flight mutations in handlers do not leak into the store before Save.
	Clone() W
}
