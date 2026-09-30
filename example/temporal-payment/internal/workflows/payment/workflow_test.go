package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/types"
	"github.com/wotek/flux/example/temporal-payment/internal/workflows/payment"
	"go.temporal.io/sdk/testsuite"
)

func findPlacedEventRef(t *testing.T, es flux.EventStore, orderID string) event.EventReference {
	t.Helper()
	iter, err := es.Read(context.Background(), order.StreamFor(orderID), 0)
	if err != nil {
		t.Fatalf("reading order stream: %v", err)
	}
	for env, err := range iter {
		if err != nil {
			t.Fatalf("iterating stream: %v", err)
		}
		if _, ok := env.Event.(events.OrderPlaced); ok {
			return event.EventReference{
				Stream:  env.Stream.Identifier,
				EventID: env.Identifier,
			}
		}
	}
	t.Fatalf("OrderPlaced event not found for order %s", orderID)
	return event.EventReference{}
}

func TestOrderFulfillmentWorkflow_PaysWhenSignaled(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	es := eventstore.New()
	repo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	cmdBus := command.New()
	commands.Register(cmdBus, repo)
	acts := &payment.Activities{EventStore: es, CmdBus: cmdBus}

	orderID := "ord-test-1"
	actor := flux.MustParseIdentifier("urn:shop:demo:iam:1:user:alice")
	corr := flux.MustParseIdentifier("urn:shop:demo:corr:1:correlation:" + orderID)
	placeCtx := command.NewContext(context.Background(), flux.MustParseIdentifier("urn:shop:demo:orders:1:command:place"), flux.Actor{Identifier: actor}, corr, flux.Identifier{})
	if err := command.Execute(placeCtx, cmdBus, commands.PlaceOrder{OrderID: orderID}); err != nil {
		t.Fatalf("place: %v", err)
	}

	env.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	env.RegisterActivity(acts.PayOrderActivity)
	env.RegisterActivity(acts.CancelOrderActivity)

	ref := findPlacedEventRef(t, es, orderID)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(payment.PaymentReceivedSignal, nil)
	}, time.Millisecond)

	env.ExecuteWorkflow(payment.OrderFulfillmentWorkflow, ref)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	final, err := repo.Load(placeCtx, order.StreamFor(orderID))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if final.Status() != types.OrderStatusPaid {
		t.Fatalf("status=%q, want paid", final.Status())
	}
}

func TestOrderFulfillmentWorkflow_CancelsWhenNotPaid(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	es := eventstore.New()
	repo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	cmdBus := command.New()
	commands.Register(cmdBus, repo)
	acts := &payment.Activities{EventStore: es, CmdBus: cmdBus}

	orderID := "ord-test-2"
	actor := flux.MustParseIdentifier("urn:shop:demo:iam:1:user:bob")
	corr := flux.MustParseIdentifier("urn:shop:demo:corr:1:correlation:" + orderID)
	placeCtx := command.NewContext(context.Background(), flux.MustParseIdentifier("urn:shop:demo:orders:1:command:place2"), flux.Actor{Identifier: actor}, corr, flux.Identifier{})
	if err := command.Execute(placeCtx, cmdBus, commands.PlaceOrder{OrderID: orderID}); err != nil {
		t.Fatalf("place: %v", err)
	}

	env.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	env.RegisterActivity(acts.PayOrderActivity)
	env.RegisterActivity(acts.CancelOrderActivity)

	// Speed up the payment timeout for the test environment.
	env.SetTestTimeout(2 * time.Minute)

	ref := findPlacedEventRef(t, es, orderID)
	env.ExecuteWorkflow(payment.OrderFulfillmentWorkflow, ref)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	final, err := repo.Load(placeCtx, order.StreamFor(orderID))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if final.Status() != types.OrderStatusCancelled {
		t.Fatalf("status=%q, want cancelled", final.Status())
	}
}
