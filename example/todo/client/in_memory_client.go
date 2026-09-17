package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
	"github.com/wotek/flux/query"
)

var _ Client = (*InMemoryClient)(nil)

// InMemoryClientOption configures an [InMemoryClient].
type InMemoryClientOption func(*InMemoryClient)

// WithInMemoryEventStore configures the event store to enable live event subscription.
func WithInMemoryEventStore(store flux.EventStore) InMemoryClientOption {
	return func(c *InMemoryClient) {
		c.eventStore = store
	}
}

// InMemoryClient executes Todo commands and queries directly against in-memory buses.
type InMemoryClient struct {
	cmdBus     *command.Bus
	queryBus   *query.Bus
	eventStore flux.EventStore
	actor      flux.Actor
}

// NewInMemoryClient constructs a new [InMemoryClient] using the provided buses.
func NewInMemoryClient(cmdBus *command.Bus, queryBus *query.Bus, opts ...InMemoryClientOption) *InMemoryClient {
	c := &InMemoryClient{
		cmdBus:   cmdBus,
		queryBus: queryBus,
		actor: flux.Actor{
			Identifier: flux.MustParseIdentifier("urn:todo:prod:users:1:user:in-memory-client"),
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CreateList dispatches a [commands.CreateList] command directly to the command bus.
func (c *InMemoryClient) CreateList(ctx context.Context, listIdentifier flux.Identifier, title string) error {
	cmdID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:commands:1:cmd:createlist-%d", time.Now().UnixNano()))
	cmdCtx := command.NewContext(ctx, cmdID, c.actor, flux.Identifier{}, flux.Identifier{})
	cmd := commands.CreateList{
		ListIdentifier: listIdentifier,
		Title:          title,
	}
	if err := command.Execute(cmdCtx, c.cmdBus, cmd); err != nil {
		return fmt.Errorf("in-memory create list: %w", err)
	}
	return nil
}

// AddTask dispatches an [commands.AddTask] command directly to the command bus.
func (c *InMemoryClient) AddTask(ctx context.Context, listIdentifier flux.Identifier, task string) error {
	cmdID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:commands:1:cmd:add-%d", time.Now().UnixNano()))
	cmdCtx := command.NewContext(ctx, cmdID, c.actor, flux.Identifier{}, flux.Identifier{})
	cmd := commands.AddTask{
		ListIdentifier: listIdentifier,
		Task:           task,
	}
	if err := command.Execute(cmdCtx, c.cmdBus, cmd); err != nil {
		return fmt.Errorf("in-memory add task: %w", err)
	}
	return nil
}

// RemoveTask dispatches an [commands.RemoveTask] command directly to the command bus.
func (c *InMemoryClient) RemoveTask(ctx context.Context, listIdentifier flux.Identifier, task string) error {
	cmdID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:commands:1:cmd:remove-%d", time.Now().UnixNano()))
	cmdCtx := command.NewContext(ctx, cmdID, c.actor, flux.Identifier{}, flux.Identifier{})
	cmd := commands.RemoveTask{
		ListIdentifier: listIdentifier,
		Task:           task,
	}
	if err := command.Execute(cmdCtx, c.cmdBus, cmd); err != nil {
		return fmt.Errorf("in-memory remove task: %w", err)
	}
	return nil
}

// DoneTasks dispatches an [commands.DoneTasks] command directly to the command bus.
func (c *InMemoryClient) DoneTasks(ctx context.Context, listIdentifier flux.Identifier, tasks ...string) error {
	cmdID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:commands:1:cmd:done-%d", time.Now().UnixNano()))
	cmdCtx := command.NewContext(ctx, cmdID, c.actor, flux.Identifier{}, flux.Identifier{})
	cmd := commands.DoneTasks{
		ListIdentifier: listIdentifier,
		Tasks:          tasks,
	}
	if err := command.Execute(cmdCtx, c.cmdBus, cmd); err != nil {
		return fmt.Errorf("in-memory done tasks: %w", err)
	}
	return nil
}

// GetCounter dispatches an [queries.GetCounter] query directly to the query bus.
func (c *InMemoryClient) GetCounter(ctx context.Context) (counter.Counter, error) {
	queryID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:queries:1:query:counter-%d", time.Now().UnixNano()))
	queryCtx := query.NewContext(ctx, queryID, c.actor, flux.Identifier{}, flux.Identifier{})
	res, err := query.Execute[queries.GetCounter, counter.Counter](queryCtx, c.queryBus, queries.GetCounter{})
	if err != nil {
		return counter.Counter{}, fmt.Errorf("in-memory get counter: %w", err)
	}
	return res, nil
}

// GetTodoList dispatches an [queries.GetTodoList] query directly to the query bus.
func (c *InMemoryClient) GetTodoList(ctx context.Context, listIdentifier flux.Identifier) (queries.TodoList, error) {
	queryID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:queries:1:query:tasks-%d", time.Now().UnixNano()))
	queryCtx := query.NewContext(ctx, queryID, c.actor, flux.Identifier{}, flux.Identifier{})
	res, err := query.Execute[queries.GetTodoList, queries.TodoList](queryCtx, c.queryBus, queries.GetTodoList{ListIdentifier: listIdentifier})
	if err != nil {
		return queries.TodoList{}, fmt.Errorf("in-memory get todo list: %w", err)
	}
	return res, nil
}

// GetLists dispatches an [queries.GetLists] query directly to the query bus.
func (c *InMemoryClient) GetLists(ctx context.Context) ([]lists.ListSummary, error) {
	queryID := flux.MustParseIdentifier(fmt.Sprintf("urn:todo:prod:queries:1:query:lists-%d", time.Now().UnixNano()))
	queryCtx := query.NewContext(ctx, queryID, c.actor, flux.Identifier{}, flux.Identifier{})
	res, err := query.Execute[queries.GetLists, []lists.ListSummary](queryCtx, c.queryBus, queries.GetLists{})
	if err != nil {
		return nil, fmt.Errorf("in-memory get lists: %w", err)
	}
	return res, nil
}

// SubscribeEvents streams live event notifications using the underlying [flux.EventStore] if available.
func (c *InMemoryClient) SubscribeEvents(ctx context.Context) (<-chan EventNotification, error) {
	ch := make(chan EventNotification, 32)
	if c.eventStore == nil {
		go func() {
			<-ctx.Done()
			close(ch)
		}()
		return ch, nil
	}

	go func() {
		defer close(ch)
		var currentPos uint64
		iter, _ := c.eventStore.Stream(ctx, 0)
		if iter != nil {
			for env, _ := range iter {
				if env.Position > currentPos {
					currentPos = env.Position
				}
			}
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				iter, err := c.eventStore.Stream(ctx, currentPos)
				if err != nil {
					return
				}
				for env, err := range iter {
					if err != nil {
						return
					}
					currentPos = env.Position
					notif := EventNotification{
						Type:           env.Event.Name(),
						ListIdentifier: env.Stream.Identifier.String(),
						Position:       env.Position,
					}
					switch e := env.Event.(type) {
					case events.TaskAdded:
						notif.Description = fmt.Sprintf("Task added: %q", e.Task)
					case events.TaskRemoved:
						notif.Description = fmt.Sprintf("Task removed: %q", e.Task)
					case events.TasksDone:
						notif.Description = fmt.Sprintf("Tasks completed: %s", strings.Join(e.Tasks, ", "))
					case events.ListCreated:
						notif.Description = fmt.Sprintf("List created: %q", e.Title)
					default:
						notif.Description = env.Event.Name()
					}
					select {
					case ch <- notif:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}
