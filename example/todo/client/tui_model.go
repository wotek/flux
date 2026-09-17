package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
)

type screenView int

const (
	viewLists screenView = iota
	viewTasks
)

type inputMode int

const (
	inputNone inputMode = iota
	inputNewList
	inputNewTask
)

// Internal messages
type listsLoadedMsg struct {
	lists []lists.ListSummary
	stats counter.Counter
	err   error
}

type tasksLoadedMsg struct {
	tasks queries.TodoList
	stats counter.Counter
	err   error
}

type actionCompletedMsg struct {
	message string
	err     error
}

type eventNotificationMsg struct {
	notification EventNotification
}

type delayedRefreshMsg struct{}

type tuiModel struct {
	client           Client
	ctx              context.Context
	eventsChan       <-chan EventNotification
	view             screenView
	listsModel       list.Model
	tasksModel       list.Model
	textInput        textinput.Model
	inputMode        inputMode
	currentListID    flux.Identifier
	currentListTitle string
	listsKeys        listsKeyMap
	tasksKeys        tasksKeyMap
	statusMessage    string
	isError          bool
	stats            counter.Counter
	width            int
	height           int
}

func newTUIModel(ctx context.Context, c Client, initialListID flux.Identifier) *tuiModel {
	// Initialize text input
	ti := textinput.New()
	ti.Prompt = "❯ "
	ti.CharLimit = 80
	ti.Width = 40

	// Initialize lists model
	listsDelegate := list.NewDefaultDelegate()
	listsList := list.New([]list.Item{}, listsDelegate, 80, 20)
	listsList.Title = "Todo Lists Catalog"
	listsList.SetShowStatusBar(true)
	listsList.SetFilteringEnabled(true)

	listsKeys := newListsKeyMap()
	listsList.AdditionalShortHelpKeys = func() []key.Binding {
		return []key.Binding{listsKeys.open, listsKeys.create, listsKeys.refresh}
	}

	// Initialize tasks model
	tasksDelegate := list.NewDefaultDelegate()
	tasksList := list.New([]list.Item{}, tasksDelegate, 80, 20)
	tasksList.Title = "Tasks"
	tasksList.SetShowStatusBar(true)
	tasksList.SetFilteringEnabled(true)

	tasksKeys := newTasksKeyMap()
	tasksList.AdditionalShortHelpKeys = func() []key.Binding {
		return []key.Binding{tasksKeys.add, tasksKeys.complete, tasksKeys.delete, tasksKeys.back, tasksKeys.refresh}
	}

	var eventsChan <-chan EventNotification
	if c != nil {
		eventsChan, _ = c.SubscribeEvents(ctx)
	}

	return &tuiModel{
		client:           c,
		ctx:              ctx,
		eventsChan:       eventsChan,
		view:             viewLists,
		listsModel:       listsList,
		tasksModel:       tasksList,
		textInput:        ti,
		inputMode:        inputNone,
		currentListID:    initialListID,
		currentListTitle: "Default List",
		listsKeys:        listsKeys,
		tasksKeys:        tasksKeys,
		statusMessage:    "Welcome! Select a list to open, or press 'a' to create one.",
		width:            80,
		height:           24,
	}
}

func (m *tuiModel) waitForEventCmd() tea.Cmd {
	return func() tea.Msg {
		if m.eventsChan == nil {
			return nil
		}
		notif, ok := <-m.eventsChan
		if !ok {
			return nil
		}
		return eventNotificationMsg{notification: notif}
	}
}

func (m *tuiModel) Init() tea.Cmd {
	cmds := []tea.Cmd{m.fetchListsCmd()}
	if m.eventsChan != nil {
		cmds = append(cmds, m.waitForEventCmd())
	}
	return tea.Batch(cmds...)
}

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case eventNotificationMsg:
		if msg.notification.Type == "" {
			return m, nil
		}
		m.statusMessage = fmt.Sprintf("⚡ Live: %s", msg.notification.Description)
		m.isError = false

		cmds := []tea.Cmd{
			m.waitForEventCmd(),
			tea.Tick(200*time.Millisecond, func(_ time.Time) tea.Msg {
				return delayedRefreshMsg{}
			}),
		}
		if m.view == viewTasks {
			cmds = append(cmds, m.fetchTasksCmd())
		} else {
			cmds = append(cmds, m.fetchListsCmd())
		}
		return m, tea.Batch(cmds...)

	case delayedRefreshMsg:
		if m.view == viewTasks {
			return m, m.fetchTasksCmd()
		}
		return m, m.fetchListsCmd()

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		listWidth := msg.Width - 4
		if listWidth < 20 {
			listWidth = 20
		}
		listHeight := msg.Height - 6
		if listHeight < 10 {
			listHeight = 10
		}
		m.listsModel.SetSize(listWidth, listHeight)
		m.tasksModel.SetSize(listWidth, listHeight)
		return m, nil

	case listsLoadedMsg:
		if msg.err != nil {
			m.statusMessage = "Error loading lists: " + msg.err.Error()
			m.isError = true
			return m, nil
		}
		if m.statusMessage == "Refreshing lists..." {
			m.statusMessage = "Lists refreshed."
			m.isError = false
		}
		m.stats = msg.stats
		items := make([]list.Item, 0, len(msg.lists))
		for _, l := range msg.lists {
			items = append(items, todoListItem{
				id:       flux.MustParseIdentifier(l.Identifier),
				title:    l.Title,
				active:   l.Active,
				archived: l.Archived,
			})
		}
		cmd := m.listsModel.SetItems(items)
		return m, cmd

	case tasksLoadedMsg:
		if msg.err != nil {
			m.statusMessage = "Error loading tasks: " + msg.err.Error()
			m.isError = true
			return m, nil
		}
		if m.statusMessage == "Refreshing tasks..." {
			m.statusMessage = "Tasks refreshed."
			m.isError = false
		}
		m.stats = msg.stats
		items := make([]list.Item, 0, len(msg.tasks.Active)+len(msg.tasks.Archived))
		for _, task := range msg.tasks.Active {
			items = append(items, todoTaskItem{task: task, archived: false})
		}
		for _, task := range msg.tasks.Archived {
			items = append(items, todoTaskItem{task: task, archived: true})
		}
		cmd := m.tasksModel.SetItems(items)
		return m, cmd

	case actionCompletedMsg:
		if msg.err != nil {
			m.statusMessage = "Error: " + msg.err.Error()
			m.isError = true
		} else {
			m.statusMessage = msg.message
			m.isError = false
		}
		tickCmd := tea.Tick(200*time.Millisecond, func(_ time.Time) tea.Msg {
			return delayedRefreshMsg{}
		})
		if m.view == viewLists {
			return m, tea.Batch(m.fetchListsCmd(), tickCmd)
		}
		return m, tea.Batch(m.fetchTasksCmd(), tickCmd)

	case tea.KeyMsg:
		// When input modal is active
		if m.inputMode != inputNone {
			switch msg.Type {
			case tea.KeyEnter:
				text := strings.TrimSpace(m.textInput.Value())
				m.textInput.Blur()
				mode := m.inputMode
				m.inputMode = inputNone

				if text == "" {
					m.statusMessage = "Input cannot be empty."
					m.isError = true
					return m, nil
				}

				if mode == inputNewList {
					slug := strings.ToLower(strings.ReplaceAll(text, " ", "-"))
					newListID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:lists:1:list:%s-%d", slug, time.Now().UnixNano()%10000))
					m.currentListID = newListID
					m.currentListTitle = text
					m.tasksModel.Title = fmt.Sprintf("Tasks — %s", text)
					m.view = viewTasks
					return m, m.createListCmd(newListID, text)
				} else if mode == inputNewTask {
					return m, m.addTaskCmd(m.currentListID, text)
				}

			case tea.KeyEsc:
				m.textInput.Blur()
				m.inputMode = inputNone
				m.statusMessage = "Canceled."
				m.isError = false
				return m, nil
			}

			var cmd tea.Cmd
			m.textInput, cmd = m.textInput.Update(msg)
			return m, cmd
		}

		// When filtering in list, forward all keys to list model
		if (m.view == viewLists && m.listsModel.FilterState() == list.Filtering) ||
			(m.view == viewTasks && m.tasksModel.FilterState() == list.Filtering) {
			var cmd tea.Cmd
			if m.view == viewLists {
				m.listsModel, cmd = m.listsModel.Update(msg)
			} else {
				m.tasksModel, cmd = m.tasksModel.Update(msg)
			}
			return m, cmd
		}

		// Global shortcuts
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}

		// View: Lists Screen
		if m.view == viewLists {
			switch {
			case key.Matches(msg, m.listsKeys.open):
				selected := m.listsModel.SelectedItem()
				if selected != nil {
					item := selected.(todoListItem)
					m.currentListID = item.id
					m.currentListTitle = item.title
					m.tasksModel.Title = fmt.Sprintf("Tasks — %s", item.title)
					m.view = viewTasks
					m.statusMessage = fmt.Sprintf("Opened %q", item.title)
					m.isError = false
					return m, m.fetchTasksCmd()
				}

			case key.Matches(msg, m.listsKeys.create):
				m.inputMode = inputNewList
				m.textInput.Reset()
				m.textInput.Placeholder = "e.g. Work Projects"
				m.textInput.Focus()
				return m, textinput.Blink

			case key.Matches(msg, m.listsKeys.refresh):
				m.statusMessage = "Refreshing lists..."
				return m, m.fetchListsCmd()
			}

			var cmd tea.Cmd
			m.listsModel, cmd = m.listsModel.Update(msg)
			return m, cmd
		}

		// View: Tasks Screen
		if m.view == viewTasks {
			switch {
			case key.Matches(msg, m.tasksKeys.back):
				if m.tasksModel.FilterState() != list.Unfiltered {
					var cmd tea.Cmd
					m.tasksModel, cmd = m.tasksModel.Update(msg)
					return m, cmd
				}
				m.view = viewLists
				m.statusMessage = "Returned to lists catalog."
				m.isError = false
				return m, m.fetchListsCmd()

			case key.Matches(msg, m.tasksKeys.add):
				m.inputMode = inputNewTask
				m.textInput.Reset()
				m.textInput.Placeholder = "e.g. Buy groceries"
				m.textInput.Focus()
				return m, textinput.Blink

			case key.Matches(msg, m.tasksKeys.complete):
				selected := m.tasksModel.SelectedItem()
				if selected != nil {
					taskItem := selected.(todoTaskItem)
					if !taskItem.archived {
						return m, m.doneTaskCmd(m.currentListID, taskItem.task)
					}
					m.statusMessage = "Task is already completed."
					return m, nil
				}

			case key.Matches(msg, m.tasksKeys.delete):
				selected := m.tasksModel.SelectedItem()
				if selected != nil {
					taskItem := selected.(todoTaskItem)
					return m, m.removeTaskCmd(m.currentListID, taskItem.task)
				}

			case key.Matches(msg, m.tasksKeys.refresh):
				m.statusMessage = "Refreshing tasks..."
				return m, m.fetchTasksCmd()
			}

			var cmd tea.Cmd
			m.tasksModel, cmd = m.tasksModel.Update(msg)
			return m, cmd
		}
	}

	// Forward any remaining messages (e.g. list.FilterMatchesMsg, paginator, cursor ticks)
	// to the active list model so asynchronous filter matching functions properly.
	var cmd tea.Cmd
	if m.view == viewLists {
		m.listsModel, cmd = m.listsModel.Update(msg)
	} else {
		m.tasksModel, cmd = m.tasksModel.Update(msg)
	}
	return m, cmd
}

func (m *tuiModel) View() string {
	var b strings.Builder

	// Header banner
	header := appTitleStyle.Render("FLUX CQRS TODO APP")
	stats := counterStatsStyle.Render(fmt.Sprintf("Global Stats: %d Active | %d Archived | %d Removed", m.stats.Active, m.stats.Archived, m.stats.Removed))
	b.WriteString(fmt.Sprintf("%s  %s\n\n", header, stats))

	// Active screen content
	if m.view == viewLists {
		b.WriteString(m.listsModel.View())
	} else {
		b.WriteString(m.tasksModel.View())
	}

	// Text input modal
	if m.inputMode != inputNone {
		var prompt string
		if m.inputMode == inputNewList {
			prompt = inputPromptStyle.Render("Create New Todo List:")
		} else {
			prompt = inputPromptStyle.Render("Add New Task:")
		}
		help := counterStatsStyle.Render("(Enter to submit • Esc to cancel)")
		dialog := inputBoxStyle.Render(fmt.Sprintf("%s\n\n%s\n\n%s", prompt, m.textInput.View(), help))
		b.WriteString("\n" + dialog + "\n")
	}

	// Status line
	if m.statusMessage != "" {
		b.WriteString("\n")
		if m.isError {
			b.WriteString(statusErrorStyle.Render("❌ " + m.statusMessage))
		} else {
			b.WriteString(statusSuccessStyle.Render("✓ " + m.statusMessage))
		}
		b.WriteString("\n")
	}

	return b.String()
}

// Commands
func (m *tuiModel) fetchListsCmd() tea.Cmd {
	return func() tea.Msg {
		listsData, err := m.client.GetLists(m.ctx)
		stats, _ := m.client.GetCounter(m.ctx)
		return listsLoadedMsg{lists: listsData, stats: stats, err: err}
	}
}

func (m *tuiModel) fetchTasksCmd() tea.Cmd {
	return func() tea.Msg {
		tasksData, err := m.client.GetTodoList(m.ctx, m.currentListID)
		stats, _ := m.client.GetCounter(m.ctx)
		return tasksLoadedMsg{tasks: tasksData, stats: stats, err: err}
	}
}

func (m *tuiModel) createListCmd(id flux.Identifier, title string) tea.Cmd {
	return func() tea.Msg {
		err := m.client.CreateList(m.ctx, id, title)
		return actionCompletedMsg{message: fmt.Sprintf("Created list %q", title), err: err}
	}
}

func (m *tuiModel) addTaskCmd(id flux.Identifier, task string) tea.Cmd {
	return func() tea.Msg {
		err := m.client.AddTask(m.ctx, id, task)
		return actionCompletedMsg{message: fmt.Sprintf("Added task %q", task), err: err}
	}
}

func (m *tuiModel) removeTaskCmd(id flux.Identifier, task string) tea.Cmd {
	return func() tea.Msg {
		err := m.client.RemoveTask(m.ctx, id, task)
		return actionCompletedMsg{message: fmt.Sprintf("Removed task %q", task), err: err}
	}
}

func (m *tuiModel) doneTaskCmd(id flux.Identifier, task string) tea.Cmd {
	return func() tea.Msg {
		err := m.client.DoneTasks(m.ctx, id, task)
		return actionCompletedMsg{message: fmt.Sprintf("Completed task %q", task), err: err}
	}
}
