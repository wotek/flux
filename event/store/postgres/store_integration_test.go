//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
	postgresstore "github.com/wotek/flux/event/store/postgres"
	"github.com/wotek/flux/internal/sqllive"
)

var (
	livePGDB *sql.DB
	tableSeq uint64
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenPostgres(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start live postgres container: %v\n", err)
		os.Exit(1)
	}

	livePGDB = db
	code := m.Run()

	cleanup()
	os.Exit(code)
}

func uniqueTableName(prefix string) string {
	id := atomic.AddUint64(&tableSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

func setupLiveEventStore(t *testing.T, opts ...postgresstore.Option) (*postgresstore.EventStore, *event.Types, string) {
	t.Helper()

	tableName := uniqueTableName("events")
	tableDDL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    position BIGSERIAL PRIMARY KEY,
    stream_id TEXT NOT NULL,
    stream_type TEXT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    revision BIGINT NOT NULL,
    event_data BYTEA NOT NULL,
    causation_id TEXT NULL,
    correlation_id TEXT NULL,
    timestamp TIMESTAMPTZ(6) DEFAULT now() NOT NULL,
    application_metadata JSONB NULL,
    CONSTRAINT %s_uk_stream_revision UNIQUE (stream_id, revision),
    CONSTRAINT %s_uk_stream_event UNIQUE (stream_id, event_id)
);
CREATE INDEX IF NOT EXISTS %s_idx_event_type ON %s (event_type);
CREATE INDEX IF NOT EXISTS %s_idx_correlation ON %s (correlation_id);
`, tableName, tableName, tableName, tableName, tableName, tableName, tableName)

	sqllive.MustExecSchema(t, livePGDB, tableDDL)

	registry := event.NewTypes()
	event.RegisterType[orderPlaced](registry)
	serializer := jsoncodec.New(registry)

	allOpts := append([]postgresstore.Option{postgresstore.WithTableName(tableName)}, opts...)
	store := postgresstore.New(livePGDB, serializer, allOpts...)

	return store, registry, tableName
}

func TestLiveEventStore_AppendAndRead(t *testing.T) {
	t.Parallel()

	store, _, _ := setupLiveEventStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:order:ord-101"),
	}

	env1 := flux.Envelope{
		Identifier:            flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-101"),
		Event:                 orderPlaced{OrderNumber: "ord-101", Amount: 250},
		Revision:              1,
		CreatedAt:             time.Now().UTC(),
		CorrelationIdentifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:command:cmd-101"),
		CausationIdentifier:   flux.MustParseIdentifier("urn:acme:prod:sales:1:command:cmd-101"),
		Metadata:              map[string]string{"source": "live-test"},
	}

	// 1. Append at expectedRevision 0 on empty stream succeeds
	if err := store.Append(ctx, stream, 0, []flux.Envelope{env1}); err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	// 2. Read back from stream
	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	var readEnvs []flux.Envelope
	for env, err := range iter {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		readEnvs = append(readEnvs, env)
	}

	if len(readEnvs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(readEnvs))
	}
	if readEnvs[0].Identifier != env1.Identifier {
		t.Errorf("expected identifier %s, got %s", env1.Identifier, readEnvs[0].Identifier)
	}
	if readEnvs[0].Revision != 1 {
		t.Errorf("expected revision 1, got %d", readEnvs[0].Revision)
	}
	if readEnvs[0].Position == 0 {
		t.Errorf("expected positive global position, got %d", readEnvs[0].Position)
	}
	if !reflect.DeepEqual(readEnvs[0].Event, env1.Event) {
		t.Errorf("expected payload %+v, got %+v", env1.Event, readEnvs[0].Event)
	}
}

func TestLiveEventStore_OptimisticConcurrency(t *testing.T) {
	t.Parallel()

	store, _, _ := setupLiveEventStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:order:ord-102"),
	}

	env1 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-201"),
		Event:      orderPlaced{OrderNumber: "ord-102", Amount: 100},
		Revision:   1,
		CreatedAt:  time.Now().UTC(),
	}

	// First append succeeds
	if err := store.Append(ctx, stream, 0, []flux.Envelope{env1}); err != nil {
		t.Fatalf("initial append failed: %v", err)
	}

	envConflict := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-202"),
		Event:      orderPlaced{OrderNumber: "ord-102", Amount: 200},
		Revision:   1, // conflicting revision
		CreatedAt:  time.Now().UTC(),
	}

	// Second append at expectedRevision 0 must fail with ErrConcurrency
	err := store.Append(ctx, stream, 0, []flux.Envelope{envConflict})
	if err == nil {
		t.Fatal("expected concurrency error, got nil")
	}
	if !errors.Is(err, flux.ErrConcurrency) {
		t.Fatalf("expected ErrConcurrency, got: %v", err)
	}
}

func TestLiveEventStore_GlobalStreamPositions(t *testing.T) {
	t.Parallel()

	store, _, _ := setupLiveEventStore(t)
	ctx := context.Background()

	streamA := flux.Stream{Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:order:ord-A")}
	streamB := flux.Stream{Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:order:ord-B")}

	envA := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-A"),
		Event:      orderPlaced{OrderNumber: "ord-A", Amount: 50},
		Revision:   1,
		CreatedAt:  time.Now().UTC(),
	}
	envB := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-B"),
		Event:      orderPlaced{OrderNumber: "ord-B", Amount: 75},
		Revision:   1,
		CreatedAt:  time.Now().UTC(),
	}

	if err := store.Append(ctx, streamA, 0, []flux.Envelope{envA}); err != nil {
		t.Fatalf("append streamA failed: %v", err)
	}
	if err := store.Append(ctx, streamB, 0, []flux.Envelope{envB}); err != nil {
		t.Fatalf("append streamB failed: %v", err)
	}

	// Stream global positions
	iter, err := store.Stream(ctx, 0)
	if err != nil {
		t.Fatalf("global stream failed: %v", err)
	}

	var positions []uint64
	for env, err := range iter {
		if err != nil {
			t.Fatalf("stream iteration error: %v", err)
		}
		positions = append(positions, env.Position)
	}

	if len(positions) != 2 {
		t.Fatalf("expected exactly 2 global positions, got %d", len(positions))
	}
	if positions[0] >= positions[1] {
		t.Errorf("expected strictly increasing positions, got %v", positions)
	}
}

func TestLiveEventStore_Find(t *testing.T) {
	t.Parallel()

	store, _, _ := setupLiveEventStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:order:ord-103"),
	}
	eventID := flux.MustParseIdentifier("urn:acme:prod:sales:1:event:evt-301")

	env := flux.Envelope{
		Identifier: eventID,
		Event:      orderPlaced{OrderNumber: "ord-103", Amount: 300},
		Revision:   1,
		CreatedAt:  time.Now().UTC(),
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// 1. Hit
	found, err := store.Find(ctx, stream, eventID)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if found.Identifier != eventID {
		t.Errorf("expected eventID %s, got %s", eventID, found.Identifier)
	}
	if found.Revision != 1 {
		t.Errorf("expected revision 1, got %d", found.Revision)
	}
	if !reflect.DeepEqual(found.Event, env.Event) {
		t.Errorf("expected payload %+v, got %+v", env.Event, found.Event)
	}

	// 2. Miss
	missingID := flux.MustParseIdentifier("urn:acme:prod:sales:1:event:missing")
	_, err = store.Find(ctx, stream, missingID)
	if err == nil {
		t.Fatal("expected error on missing event, got nil")
	}
	if !errors.Is(err, flux.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got: %v", err)
	}
}
