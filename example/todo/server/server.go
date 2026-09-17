package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/example/todo/queries"
	"github.com/wotek/flux/projection"
	projectionstore "github.com/wotek/flux/projection/store"
	"github.com/wotek/flux/query"
)

// Server coordinates the todo domain infrastructure, read-model projectors, and API gateway.
type Server struct {
	eventStore     flux.EventStore
	projStore      projection.Store
	cmdBus         *command.Bus
	queryBus       *query.Bus
	statsStore     counter.Store
	listsStore     lists.Store
	projector      *projection.Projector
	listsProjector *projection.Projector
	httpAddr       string
}

// New creates and configures a new [Server] instance.
func New(opts ...Option) *Server {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.eventStore == nil {
		cfg.eventStore = eventstore.New()
	}
	if cfg.projectionStore == nil {
		cfg.projectionStore = projectionstore.New()
	}

	cmdBus := command.New()
	queryBus := query.New()
	repo := flux.NewAggregateRepository[*todo.TodoListAggregate, events.TodoEvent](cfg.eventStore)
	statsStore := counter.NewMemoryStore()
	listsStore := lists.NewMemoryStore()

	commands.RegisterHandlers(cmdBus, repo)
	queries.RegisterHandlers(queryBus, statsStore, listsStore, repo)

	projIdentifier := flux.NewIdentifierFromString("urn:todo:prod:projections:1:counter:main")
	projector := counter.NewProjector(projIdentifier, cfg.eventStore, cfg.projectionStore, statsStore)

	listsProjID := flux.NewIdentifierFromString("urn:todo:prod:projections:1:lists:main")
	listsProjector := lists.NewProjector(listsProjID, cfg.eventStore, cfg.projectionStore, listsStore)

	return &Server{
		eventStore:     cfg.eventStore,
		projStore:      cfg.projectionStore,
		cmdBus:         cmdBus,
		queryBus:       queryBus,
		statsStore:     statsStore,
		listsStore:     listsStore,
		projector:      projector,
		listsProjector: listsProjector,
		httpAddr:       cfg.httpAddr,
	}
}

// CommandBus returns the underlying [command.Bus].
func (s *Server) CommandBus() *command.Bus {
	return s.cmdBus
}

// QueryBus returns the underlying [query.Bus].
func (s *Server) QueryBus() *query.Bus {
	return s.queryBus
}

// EventStore returns the underlying [flux.EventStore].
func (s *Server) EventStore() flux.EventStore {
	return s.eventStore
}

// StatsStore returns the read-model [counter.Store].
func (s *Server) StatsStore() counter.Store {
	return s.statsStore
}

// ListsStore returns the read-model [lists.Store].
func (s *Server) ListsStore() lists.Store {
	return s.listsStore
}

// HTTPHandler returns an [http.Handler] exposing the HTTP gateway endpoints.
func (s *Server) HTTPHandler() http.Handler {
	return NewHTTPHandler(s.cmdBus, s.queryBus)
}

// Start boots the background projectors and the optional HTTP server until the context is canceled.
func (s *Server) Start(ctx context.Context) error {
	g, groupCtx := errgroup.WithContext(ctx)

	// Start counter projector loop
	g.Go(func() error {
		slog.InfoContext(groupCtx, "server: starting background counter projector...")
		if err := s.projector.Start(groupCtx); err != nil && groupCtx.Err() == nil {
			return fmt.Errorf("counter projector error: %w", err)
		}
		return nil
	})

	// Start lists projector loop
	g.Go(func() error {
		slog.InfoContext(groupCtx, "server: starting background lists projector...")
		if err := s.listsProjector.Start(groupCtx); err != nil && groupCtx.Err() == nil {
			return fmt.Errorf("lists projector error: %w", err)
		}
		return nil
	})

	// Start HTTP server if configured
	if s.httpAddr != "" {
		httpServer := &http.Server{
			Addr:              s.httpAddr,
			Handler:           s.HTTPHandler(),
			ReadHeaderTimeout: 5 * time.Second,
		}

		g.Go(func() error {
			slog.InfoContext(groupCtx, "server: starting HTTP gateway...", "addr", s.httpAddr)
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("http server error: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-groupCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownCtx)
			return nil
		})
	}

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
