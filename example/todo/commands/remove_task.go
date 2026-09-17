package commands

import "github.com/wotek/flux"

// RemoveTask is a command instructing the system to remove a task from a specific todo list.
type RemoveTask struct {
	ListIdentifier flux.Identifier
	Task           string
}
