package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
	"github.com/wotek/flux/example/temporal-payment/internal/workflows/payment"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx := context.Background()

	host := envOr("TEMPORAL_HOST_PORT", "localhost:7233")
	tc, err := client.Dial(client.Options{HostPort: host})
	if err != nil {
		log.Fatalf("temporal client: %v (start stack with: docker compose up -d)", err)
	}
	defer tc.Close()

	es := eventstore.New()
	repo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	cmdBus := command.New()
	eventBus := event.New()

	commands.Register(cmdBus, repo)
	acts := &payment.Activities{CmdBus: cmdBus}

	// EventBus → Temporal StartWorkflow (stable ID; duplicate OrderPlaced should not fork a second saga).
	event.Register(eventBus, func(ctx event.Context, e events.OrderPlaced) error {
		in := payment.FulfillmentInput{
			OrderID:        e.OrderID,
			ActorURN:       ctx.Actor().Identifier.String(),
			CorrelationURN: ctx.CorrelationIdentifier().String(),
			CausationURN:   ctx.EventIdentifier().String(),
			TraceID:        ctx.Instrumentation().TraceID,
			SpanID:         ctx.Instrumentation().SpanID,
			TraceFlags:     ctx.Instrumentation().TraceFlags,
		}
		wfID := payment.WorkflowID(e.OrderID)
		_, err := tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        wfID,
			TaskQueue: payment.TaskQueue,
		}, payment.OrderFulfillmentWorkflow, in)
		if err != nil {
			log.Printf("start workflow %s: %v", wfID, err)
		}
		return nil
	})

	w := worker.New(tc, payment.TaskQueue, worker.Options{})
	w.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	w.RegisterActivity(acts.CheckPaymentReceivedActivity)
	w.RegisterActivity(acts.PayOrderActivity)
	w.RegisterActivity(acts.CancelOrderActivity)

	go func() {
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Fatalf("worker: %v", err)
		}
	}()

	orderID := fmt.Sprintf("ord-%d", time.Now().UnixNano())
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:shop:demo:iam:1:user:alice")}
	corr := flux.MustParseIdentifier("urn:shop:demo:corr:1:correlation:" + orderID)
	cmdCtx := command.NewContext(ctx, flux.MustParseIdentifier("urn:shop:demo:orders:1:command:place-"+orderID), actor, corr, flux.Identifier{})

	if err := command.Execute(cmdCtx, cmdBus, commands.PlaceOrder{OrderID: orderID}); err != nil {
		log.Fatalf("place order: %v", err)
	}

	env := flux.Envelope{
		Identifier:            flux.MustParseIdentifier("urn:shop:demo:orders:1:event:placed-" + orderID),
		Stream:                order.StreamFor(orderID),
		Event:                 events.OrderPlaced{OrderID: orderID},
		Actor:                 actor,
		CorrelationIdentifier: corr,
		Position:              1,
	}
	if err := event.PublishEnvelope(event.NewContext(ctx, env), eventBus, env); err != nil {
		log.Fatalf("publish: %v", err)
	}

	acts.MarkPaymentReady(orderID)

	run := tc.GetWorkflow(ctx, payment.WorkflowID(orderID), "")
	if err := run.Get(ctx, nil); err != nil {
		log.Fatalf("workflow: %v", err)
	}

	final, err := repo.Load(cmdCtx, order.StreamFor(orderID))
	if err != nil {
		log.Fatalf("reload: %v", err)
	}
	fmt.Printf("order %s status=%s\n", final.OrderID(), final.Status())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
