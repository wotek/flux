package query_test

import (
	"context"
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
