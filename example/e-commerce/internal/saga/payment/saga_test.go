package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	ecommercecmd "github.com/wotek/flux/example/e-commerce/internal/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/saga/payment"
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
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:test")}

	// 1. OrderPlaced event
	_ = es.Append(ctx, stream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:                stream,
			Revision:              1,
			Event: order.OrderPlaced{
				CustomerID: "cust-1",
				Items: []types.LineItem{
					{ProductID: "prod-1", Name: "Laptop", Price: 99900, Quantity: 1},
				},
			},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// 2. OrderPaid event
	_ = es.Append(ctx, stream, 1, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:                stream,
			Revision:              2,
			Event:                 order.OrderPaid{},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// Poll saga state
	deadline := time.Now().Add(2 * time.Second)
	var s *payment.PaymentSaga
	for time.Now().Before(deadline) {
		var err error
		s, err = store.Load(ctx, orderURN)
		if err == nil && s.IsPaid {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if s == nil || !s.IsPaid {
		t.Fatalf("expected saga to be marked IsPaid")
	}
	if s.IsCancelled {
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

	// Channel to intercept compensated stock commands
	adjustStockReceived := make(chan ecommercecmd.AdjustStock, 1)

	type mockStockHandler struct{}
	command.RegisterHandler(cmdBus, mockStockAdjustHandler{received: adjustStockReceived})

	go func() {
		_ = orchestrator.Start(ctx)
	}()

	orderID := "ord-cancel-1"
	orderURN := identity.NewOrderIdentifier(orderID)
	stream := flux.Stream{Identifier: orderURN}
	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:test")}

	// 1. OrderPlaced
	_ = es.Append(ctx, stream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:                stream,
			Revision:              1,
			Event: order.OrderPlaced{
				CustomerID: "cust-1",
				Items: []types.LineItem{
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
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:                stream,
			Revision:              2,
			Event:                 order.OrderCancelled{Reason: "customer changed mind"},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// 3. Verify stock compensation command was enqueued and dispatched
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
	received chan ecommercecmd.AdjustStock
}

func (h mockStockAdjustHandler) Handle(ctx command.Context, cmd ecommercecmd.AdjustStock) error {
	select {
	case h.received <- cmd:
	default:
	}
	return nil
}
