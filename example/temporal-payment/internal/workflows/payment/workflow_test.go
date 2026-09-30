package payment_test

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/types"
	"github.com/wotek/flux/example/temporal-payment/internal/workflows/payment"
	"go.temporal.io/sdk/testsuite"
)

func TestOrderFulfillmentWorkflow_PaysWhenReady(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	es := eventstore.New()
	repo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	cmdBus := command.New()
	commands.Register(cmdBus, repo)
	acts := &payment.Activities{CmdBus: cmdBus}

	orderID := "ord-test-1"
	actor := flux.MustParseIdentifier("urn:shop:demo:iam:1:user:alice")
	corr := flux.MustParseIdentifier("urn:shop:demo:corr:1:correlation:" + orderID)
	placeCtx := command.NewContext(context.Background(), flux.MustParseIdentifier("urn:shop:demo:orders:1:command:place"), flux.Actor{Identifier: actor}, corr, flux.Identifier{})
	if err := command.Execute(placeCtx, cmdBus, commands.PlaceOrder{OrderID: orderID}); err != nil {
		t.Fatalf("place: %v", err)
	}

	acts.MarkPaymentReady(orderID)

	env.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	env.RegisterActivity(acts.CheckPaymentReceivedActivity)
	env.RegisterActivity(acts.PayOrderActivity)
	env.RegisterActivity(acts.CancelOrderActivity)

	in := payment.FulfillmentInput{
		OrderID:        orderID,
		ActorURN:       actor.String(),
		CorrelationURN: corr.String(),
		CausationURN:   corr.String(),
	}
	env.ExecuteWorkflow(payment.OrderFulfillmentWorkflow, in)
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
	acts := &payment.Activities{CmdBus: cmdBus}

	orderID := "ord-test-2"
	actor := flux.MustParseIdentifier("urn:shop:demo:iam:1:user:bob")
	corr := flux.MustParseIdentifier("urn:shop:demo:corr:1:correlation:" + orderID)
	placeCtx := command.NewContext(context.Background(), flux.MustParseIdentifier("urn:shop:demo:orders:1:command:place2"), flux.Actor{Identifier: actor}, corr, flux.Identifier{})
	if err := command.Execute(placeCtx, cmdBus, commands.PlaceOrder{OrderID: orderID}); err != nil {
		t.Fatalf("place: %v", err)
	}

	env.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	env.RegisterActivity(acts.CheckPaymentReceivedActivity)
	env.RegisterActivity(acts.PayOrderActivity)
	env.RegisterActivity(acts.CancelOrderActivity)

	in := payment.FulfillmentInput{
		OrderID:        orderID,
		ActorURN:       actor.String(),
		CorrelationURN: corr.String(),
		CausationURN:   corr.String(),
	}
	env.ExecuteWorkflow(payment.OrderFulfillmentWorkflow, in)
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
