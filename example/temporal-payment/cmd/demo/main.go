package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/example/temporal-payment/internal/platform"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
	"github.com/wotek/flux/example/temporal-payment/internal/workflows/payment"
	"go.temporal.io/sdk/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx := context.Background()
	switch os.Args[1] {
	case "place-order":
		if err := placeOrder(ctx, os.Args[2:]); err != nil {
			log.Fatalf("place-order: %v", err)
		}
	case "pay-order":
		if err := payOrder(ctx, os.Args[2:]); err != nil {
			log.Fatalf("pay-order: %v", err)
		}
	case "status":
		if err := status(ctx, os.Args[2:]); err != nil {
			log.Fatalf("status: %v", err)
		}
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  demo place-order [--order-id ID]   Place an order and start the fulfillment workflow
  demo pay-order --order-id ID       Signal payment received (workflow then pays)
  demo status --order-id ID          Print current order status from the Event Store

Environment:
  TEMPORAL_HOST_PORT       default localhost:7233
  EVENTSTORE_REDIS_ADDR    required for multi-process use (e.g. localhost:6379)
`)
}

func placeOrder(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("place-order", flag.ExitOnError)
	orderID := fs.String("order-id", "", "order id (default: generated)")
	_ = fs.Parse(args)

	if *orderID == "" {
		*orderID = order.NewID().String()
	} else if _, err := flux.ParseIdentifier(*orderID); err != nil {
		return fmt.Errorf("invalid --order-id: %w", err)
	}

	es, cleanup, err := platform.OpenEventStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	_, cmdBus := platform.NewSalesStack(es)

	tc, err := dialTemporal()
	if err != nil {
		return err
	}
	defer tc.Close()

	streamID := flux.MustParseIdentifier(*orderID)
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:shop:demo:iam:1:user:alice")}
	corr := flux.NewIdentifier("shop", "demo", "corr", "1", "correlation", streamID.ResourceID(), "")
	cmdID := flux.NewIdentifier("shop", "demo", "orders", "1", "command", "place-"+streamID.ResourceID(), "")
	cmdCtx := command.NewContext(ctx, cmdID, actor, corr, flux.Identifier{})

	if err := command.Execute(cmdCtx, cmdBus, commands.PlaceOrder{OrderID: *orderID}); err != nil {
		return fmt.Errorf("execute PlaceOrder: %w", err)
	}

	ref, err := findPlacedEventRef(ctx, es, *orderID)
	if err != nil {
		return err
	}

	wfID := payment.WorkflowID(*orderID)
	_, err = tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: payment.TaskQueue,
	}, payment.OrderFulfillmentWorkflow, ref)
	if err != nil {
		return fmt.Errorf("start workflow %s: %w", wfID, err)
	}

	fmt.Printf("placed order_id=%s workflow_id=%s event_id=%s\n", *orderID, wfID, ref.EventID)
	return nil
}

func payOrder(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pay-order", flag.ExitOnError)
	orderID := fs.String("order-id", "", "order id (required)")
	_ = fs.Parse(args)
	if *orderID == "" {
		return fmt.Errorf("--order-id is required")
	}
	if _, err := flux.ParseIdentifier(*orderID); err != nil {
		return fmt.Errorf("invalid --order-id: %w", err)
	}

	tc, err := dialTemporal()
	if err != nil {
		return err
	}
	defer tc.Close()

	wfID := payment.WorkflowID(*orderID)
	if err := tc.SignalWorkflow(ctx, wfID, "", payment.PaymentReceivedSignal, nil); err != nil {
		return fmt.Errorf("signal %s: %w", wfID, err)
	}
	fmt.Printf("signaled payment for order_id=%s workflow_id=%s\n", *orderID, wfID)
	return nil
}

func status(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	orderID := fs.String("order-id", "", "order id (required)")
	_ = fs.Parse(args)
	if *orderID == "" {
		return fmt.Errorf("--order-id is required")
	}
	if _, err := flux.ParseIdentifier(*orderID); err != nil {
		return fmt.Errorf("invalid --order-id: %w", err)
	}

	es, cleanup, err := platform.OpenEventStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	repo, _ := platform.NewSalesStack(es)

	streamID := flux.MustParseIdentifier(*orderID)
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:shop:demo:iam:1:user:alice")}
	corr := flux.NewIdentifier("shop", "demo", "corr", "1", "correlation", streamID.ResourceID(), "")
	cmdID := flux.NewIdentifier("shop", "demo", "orders", "1", "command", "status-"+streamID.ResourceID(), "")
	cmdCtx := command.NewContext(ctx, cmdID, actor, corr, flux.Identifier{})

	final, err := repo.Load(cmdCtx, order.StreamFor(*orderID))
	if err != nil {
		return err
	}
	fmt.Printf("order %s status=%s\n", final.OrderID(), final.Status())
	return nil
}

func dialTemporal() (client.Client, error) {
	host := platform.Env("TEMPORAL_HOST_PORT", "localhost:7233")
	tc, err := client.Dial(client.Options{HostPort: host})
	if err != nil {
		return nil, fmt.Errorf("temporal client at %s: %w (start stack with: docker compose up -d)", host, err)
	}
	return tc, nil
}

func findPlacedEventRef(ctx context.Context, es flux.EventStore, orderID string) (event.EventReference, error) {
	iter, err := es.Read(ctx, order.StreamFor(orderID), 0)
	if err != nil {
		return event.EventReference{}, err
	}
	for env, err := range iter {
		if err != nil {
			return event.EventReference{}, err
		}
		if _, ok := env.Event.(events.OrderPlaced); ok {
			return event.EventReference{
				Stream:  env.Stream.Identifier,
				EventID: env.Identifier,
			}, nil
		}
	}
	return event.EventReference{}, fmt.Errorf("OrderPlaced not found for order %s", orderID)
}
