package query_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/query"
)

type MyQuery struct {
	ID string
}

type MyResult struct {
	Value int
}

type MyQueryHandler struct{}

func (h MyQueryHandler) Handle(ctx query.Context, q MyQuery) (MyResult, error) {
	if q.ID != "123" {
		return MyResult{}, errors.New("not found")
	}
	return MyResult{Value: 42}, nil
}

func TestQueryBus(t *testing.T) {
	bus := query.New()

	query.RegisterHandler(bus, MyQueryHandler{})

	ctx := query.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	res, err := query.Execute[MyQuery, MyResult](ctx, bus, MyQuery{ID: "123"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Value != 42 {
		t.Errorf("expected 42, got %d", res.Value)
	}

	_, err = query.Execute[MyQuery, MyResult](ctx, bus, MyQuery{ID: "wrong"})
	if err == nil {
		t.Errorf("expected error for wrong query")
	}
}

func TestQueryBus_FunctionalHandler(t *testing.T) {
	bus := query.New()

	query.Register(bus, func(ctx query.Context, q MyQuery) (MyResult, error) {
		if q.ID != "123" {
			return MyResult{}, errors.New("not found")
		}
		return MyResult{Value: 100}, nil
	})

	ctx := query.NewContext(context.Background(), flux.Identifier{}, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	res, err := query.Execute[MyQuery, MyResult](ctx, bus, MyQuery{ID: "123"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Value != 100 {
		t.Errorf("expected 100, got %d", res.Value)
	}
}

type dummyQuery struct{}

func TestQueryBus_Middleware(t *testing.T) {
	bus := query.New()
	
	// Intercept slog.Default() for testing Context Logger
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(handler)
	oldDefault := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(oldDefault)
	
	order := []string{}
	
	mw1 := func(ctx query.Context, q any, next func(query.Context, any) (any, error)) (any, error) {
		order = append(order, "mw1_before")
		ctx.Logger().Info("mw1 query executing")
		res, err := next(ctx, q)
		order = append(order, "mw1_after")
		return res, err
	}
	
	mw2 := func(ctx query.Context, q any, next func(query.Context, any) (any, error)) (any, error) {
		order = append(order, "mw2_before")
		res, err := next(ctx, q)
		order = append(order, "mw2_after")
		return res, err
	}
	
	bus.Use(mw1, mw2)
	
	query.Register(bus, func(ctx query.Context, q dummyQuery) (string, error) {
		order = append(order, "handler")
		return "result", nil
	})
	
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:actor:1")}
	corrID := flux.MustParseIdentifier("urn:acme:prod:payments:tenant-1:correlation:2")
	ctx := query.NewContext(context.Background(), flux.Identifier{}, actor, corrID, flux.Identifier{})
	res, err := query.Execute[dummyQuery, string](ctx, bus, dummyQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "result" {
		t.Fatalf("expected result, got %v", res)
	}
	
	expected := []string{"mw1_before", "mw2_before", "handler", "mw2_after", "mw1_after"}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("expected order %v, got %v", expected, order)
		}
	}
	
	logOutput := buf.String()
	if !strings.Contains(logOutput, "mw1 query executing") {
		t.Fatalf("expected log to contain message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "\"actor\":\"urn:acme:prod:payments:tenant-1:actor:1\"") {
		t.Fatalf("expected log to contain auto-injected actor, got: %s", logOutput)
	}
}