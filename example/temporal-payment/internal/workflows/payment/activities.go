package payment

import (
	"context"
	"fmt"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// Activities holds flux dependencies used from Temporal activities.
// Cross-domain: listens to sales events (via starter) and dispatches sales commands.
type Activities struct {
	EventStore flux.EventStore
	CmdBus     *command.Bus

	mu sync.Mutex
	// paymentsReady simulates external payment gateway confirmation (e.g. webhook).
	// Because external payment arrival is not yet persisted as an event in our store,
	// this demo state is stored separately from EventStore.
	paymentsReady map[string]bool
}

// MarkPaymentReady records that an external payment succeeded (demo hook).
func (a *Activities) MarkPaymentReady(orderID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.paymentsReady == nil {
		a.paymentsReady = make(map[string]bool)
	}
	a.paymentsReady[orderID] = true
}

// CheckPaymentReceivedActivity reports whether payment was marked ready.
// It point-reads the initiating OrderPlaced event from the EventStore via Find.
func (a *Activities) CheckPaymentReceivedActivity(ctx context.Context, ref event.EventReference) (bool, error) {
	env, err := a.EventStore.Find(ctx, flux.Stream{Identifier: ref.Stream}, ref.EventID)
	if err != nil {
		return false, fmt.Errorf("finding order placed event: %w", err)
	}

	orderID, err := extractOrderID(env.Event)
	if err != nil {
		return false, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	return a.paymentsReady[orderID], nil
}

// PayOrderActivity loads the initiating event from the EventStore and dispatches [commands.PayOrder].
func (a *Activities) PayOrderActivity(ctx context.Context, ref event.EventReference) error {
	env, err := a.EventStore.Find(ctx, flux.Stream{Identifier: ref.Stream}, ref.EventID)
	if err != nil {
		return fmt.Errorf("finding order placed event: %w", err)
	}

	orderID, err := extractOrderID(env.Event)
	if err != nil {
		return err
	}

	return a.execute(ctx, env, orderID, commands.PayOrder{OrderID: orderID})
}

// CancelOrderActivity loads the initiating event from the EventStore and dispatches [commands.CancelOrder].
func (a *Activities) CancelOrderActivity(ctx context.Context, in CancelOrderInput) error {
	env, err := a.EventStore.Find(ctx, flux.Stream{Identifier: in.Reference.Stream}, in.Reference.EventID)
	if err != nil {
		return fmt.Errorf("finding order placed event: %w", err)
	}

	orderID, err := extractOrderID(env.Event)
	if err != nil {
		return err
	}

	return a.execute(ctx, env, orderID, commands.CancelOrder{OrderID: orderID, Reason: in.Reason})
}

func extractOrderID(ev flux.Event) (string, error) {
	switch e := ev.(type) {
	case events.OrderPlaced:
		return e.OrderID, nil
	case *events.OrderPlaced:
		return e.OrderID, nil
	default:
		return "", fmt.Errorf("unexpected event payload %T for OrderPlaced", ev)
	}
}

func (a *Activities) execute(ctx context.Context, env flux.Envelope, orderID string, cmd any) error {
	cmdID := flux.MustParseIdentifier("urn:shop:demo:orders:1:command:" + orderID)
	var inst flux.Instrumentation
	if len(env.Metadata) > 0 {
		inst = flux.Instrumentation{
			TraceID:    env.Metadata[flux.MetadataTraceID],
			SpanID:     env.Metadata[flux.MetadataSpanID],
			TraceFlags: env.Metadata[flux.MetadataTraceFlags],
		}
	}
	cmdCtx := command.NewContext(ctx, cmdID, env.Actor, env.CorrelationIdentifier, env.Identifier, flux.WithInstrumentation(inst))
	return command.Execute(cmdCtx, a.CmdBus, cmd)
}
