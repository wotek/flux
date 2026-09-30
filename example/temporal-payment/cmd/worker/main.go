package main

import (
	"context"
	"log"

	"github.com/wotek/flux/example/temporal-payment/internal/platform"
	"github.com/wotek/flux/example/temporal-payment/internal/workflows/payment"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx := context.Background()

	es, cleanup, err := platform.OpenEventStore(ctx)
	if err != nil {
		log.Fatalf("event store: %v", err)
	}
	defer cleanup()

	_, cmdBus := platform.NewSalesStack(es)
	acts := &payment.Activities{EventStore: es, CmdBus: cmdBus}

	host := platform.Env("TEMPORAL_HOST_PORT", "localhost:7233")
	tc, err := client.Dial(client.Options{HostPort: host})
	if err != nil {
		log.Fatalf("temporal client: %v (start stack with: docker compose up -d)", err)
	}
	defer tc.Close()

	w := worker.New(tc, payment.TaskQueue, worker.Options{})
	w.RegisterWorkflow(payment.OrderFulfillmentWorkflow)
	w.RegisterActivity(acts.PayOrderActivity)
	w.RegisterActivity(acts.CancelOrderActivity)

	log.Printf("worker listening on task queue %q (temporal=%s)", payment.TaskQueue, host)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
