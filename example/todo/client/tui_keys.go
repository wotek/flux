package client

import (
	"github.com/charmbracelet/bubbles/key"
)

type listsKeyMap struct {
	open    key.Binding
	create  key.Binding
	refresh key.Binding
}

func newListsKeyMap() listsKeyMap {
	return listsKeyMap{
		open: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "open list"),
		),
		create: key.NewBinding(
			key.WithKeys("a", "n"),
			key.WithHelp("a/n", "new list"),
		),
		refresh: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "refresh"),
		),
	}
}

type tasksKeyMap struct {
	add      key.Binding
	complete key.Binding
	delete   key.Binding
	back     key.Binding
	refresh  key.Binding
}

func newTasksKeyMap() tasksKeyMap {
	return tasksKeyMap{
		add: key.NewBinding(
			key.WithKeys("a"),
			key.WithHelp("a", "add task"),
		),
		complete: key.NewBinding(
			key.WithKeys("c", " "),
			key.WithHelp("c/space", "complete"),
		),
		delete: key.NewBinding(
			key.WithKeys("d", "x"),
			key.WithHelp("d/x", "delete"),
		),
		back: key.NewBinding(
			key.WithKeys("esc", "b"),
			key.WithHelp("esc/b", "back to lists"),
		),
		refresh: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "refresh"),
		),
	}
}
