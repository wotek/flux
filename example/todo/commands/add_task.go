package commands

import "github.com/wotek/flux"

// AddTask is a command instructing the system to add a new task to a specific todo list.
type AddTask struct {
	ListIdentifier flux.Identifier
	Task           string
}
