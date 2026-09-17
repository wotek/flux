package client

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wotek/flux"
)

// NewTestTUIModel exports newTUIModel for testing.
func NewTestTUIModel(ctx context.Context, c Client, initialListID flux.Identifier) tea.Model {
	return newTUIModel(ctx, c, initialListID)
}

// NewTestEventNotificationMsg wraps an EventNotification for testing.
func NewTestEventNotificationMsg(notif EventNotification) tea.Msg {
	return eventNotificationMsg{notification: notif}
}
