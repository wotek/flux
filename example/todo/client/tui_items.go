package client

import (
	"fmt"

	"github.com/charmbracelet/bubbles/list"
	"github.com/wotek/flux"
)

var _ list.DefaultItem = (*todoListItem)(nil)
var _ list.DefaultItem = (*todoTaskItem)(nil)

// todoListItem represents a todo list aggregate inside a [list.Model].
type todoListItem struct {
	id       flux.Identifier
	title    string
	active   int
	archived int
}

func (i todoListItem) Title() string {
	return i.title
}

func (i todoListItem) Description() string {
	return fmt.Sprintf("Active: %d | Archived: %d • %s", i.active, i.archived, i.id.String())
}

func (i todoListItem) FilterValue() string {
	return i.title + " " + i.id.String()
}

// todoTaskItem represents an active or completed task inside a [list.Model].
type todoTaskItem struct {
	task     string
	archived bool
}

func (i todoTaskItem) Title() string {
	if i.archived {
		return "✓ " + i.task
	}
	return "○ " + i.task
}

func (i todoTaskItem) Description() string {
	if i.archived {
		return "Status: Completed / Archived"
	}
	return "Status: Active"
}

func (i todoTaskItem) FilterValue() string {
	return i.task
}
