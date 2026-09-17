package todo

import "github.com/wotek/flux"

// DoneTasks is a command instructing the system to mark one or more tasks as completed.
type DoneTasks struct {
	ListIdentifier flux.Identifier
	Tasks          []string
}
