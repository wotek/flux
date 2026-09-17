package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	catalogcommands "github.com/wotek/flux/example/e-commerce/internal/catalog/commands"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	salesevents "github.com/wotek/flux/example/e-commerce/internal/sales/events"
	salestypes "github.com/wotek/flux/example/e-commerce/internal/sales/types"
	"github.com/wotek/flux/example/e-commerce/internal/workflows/payment"
	"github.com/wotek/flux/saga"
	sagastore "github.com/wotek/flux/saga/store"
)

func TestPaymentSaga_SuccessfulPayment(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmdBus := command.New()
	es := eventstore.New()
	store := sagastore.New[*payment.PaymentSaga](cmdBus)
	store.StartRelay(ctx)

	orchestrator := saga.NewOrchestrator(es)
	payment.RegisterPaymentSaga(orchestrator, store)

	go func() {
		_ = orchestrator.Start(ctx)
	}()

	orderID := "ord-success-1"
	orderURN := identity.NewOrderIdentifier(orderID)
	stream := flux.Stream{Identifier: orderURN}
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:user:test")}

	// 1. OrderPlaced
	_ = es.Append(ctx, stream, 0, []flux.Envelope{
		{
			Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:     stream,
			Revision:   1,
			Event: salesevents.OrderPlaced{
				CustomerID: "cust-1",
				Items: []salestypes.LineItem{
					{ProductID: "prod-1", Name: "Shoes", Price: 7900, Quantity: 1},
				},
			},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// 2. OrderPaid
	_ = es.Append(ctx, stream, 1, []flux.Envelope{
		{
			Identifier:            flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:                stream,
			Revision:              2,
			Event:                 salesevents.OrderPaid{},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	deadline := time.Now().Add(2 * time.Second)
	var sagaState *payment.PaymentSaga
	for time.Now().Before(deadline) {
		s, err := store.Load(ctx, orderURN)
		if err == nil && s.IsPaid {
			sagaState = s
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if sagaState == nil {
		t.Fatalf("timed out waiting for saga to reflect paid status")
	}

	if !sagaState.IsPaid {
		t.Errorf("expected saga to be marked as paid")
	}
	if sagaState.IsCancelled {
		t.Errorf("expected saga not to be cancelled")
	}
}

func TestPaymentSaga_CancellationCompensation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmdBus := command.New()
	es := eventstore.New()
	store := sagastore.New[*payment.PaymentSaga](cmdBus)
	store.StartRelay(ctx)

	orchestrator := saga.NewOrchestrator(es)
	payment.RegisterPaymentSaga(orchestrator, store)

	adjustStockReceived := make(chan catalogcommands.AdjustStock, 1)
	command.RegisterHandler(cmdBus, mockStockAdjustHandler{received: adjustStockReceived})

	go func() {
		_ = orchestrator.Start(ctx)
	}()

	orderID := "ord-cancel-1"
	orderURN := identity.NewOrderIdentifier(orderID)
	stream := flux.Stream{Identifier: orderURN}
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:user:test")}

	// 1. OrderPlaced
	_ = es.Append(ctx, stream, 0, []flux.Envelope{
		{
			Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:     stream,
			Revision:   1,
			Event: salesevents.OrderPlaced{
				CustomerID: "cust-1",
				Items: []salestypes.LineItem{
					{ProductID: "prod-42", Name: "Tablet", Price: 49900, Quantity: 2},
				},
			},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// 2. OrderCancelled (Customer action)
	_ = es.Append(ctx, stream, 1, []flux.Envelope{
		{
			Identifier:            flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:                stream,
			Revision:              2,
			Event:                 salesevents.OrderCancelled{Reason: "customer changed mind"},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	select {
	case cmd := <-adjustStockReceived:
		if cmd.ProductID != "prod-42" {
			t.Errorf("expected product 'prod-42', got %q", cmd.ProductID)
		}
		if cmd.Quantity != 2 {
			t.Errorf("expected quantity 2 restored, got %d", cmd.Quantity)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stock compensation command")
	}
}

type mockStockAdjustHandler struct {
	received chan catalogcommands.AdjustStock
}

func (h mockStockAdjustHandler) Handle(_ command.Context, cmd catalogcommands.AdjustStock) error {
	select {
	case h.received <- cmd:
	default:
	}
	return nil
}
