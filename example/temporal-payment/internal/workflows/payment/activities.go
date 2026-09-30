package payment

import (
	"context"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
)

// Activities holds flux dependencies used from Temporal activities.
type Activities struct {
	EventStore flux.EventStore
	CmdBus     *command.Bus
}

// PayOrderActivity loads the initiating event from the EventStore and dispatches [commands.PayOrder].
func (a *Activities) PayOrderActivity(ctx context.Context, ref event.EventReference) error {
	env, err := a.EventStore.Find(ctx, flux.Stream{Identifier: ref.Stream}, ref.EventID)
	if err != nil {
		return fmt.Errorf("finding order placed event: %w", err)
	}

	orderID := env.Stream.Identifier.String()
	return a.execute(ctx, env, orderID, commands.PayOrder{OrderID: orderID})
}

// CancelOrderActivity loads the initiating event from the EventStore and dispatches [commands.CancelOrder].
func (a *Activities) CancelOrderActivity(ctx context.Context, in CancelOrderInput) error {
	env, err := a.EventStore.Find(ctx, flux.Stream{Identifier: in.Reference.Stream}, in.Reference.EventID)
	if err != nil {
		return fmt.Errorf("finding order placed event: %w", err)
	}

	orderID := env.Stream.Identifier.String()
	return a.execute(ctx, env, orderID, commands.CancelOrder{OrderID: orderID, Reason: in.Reason})
}

func (a *Activities) execute(ctx context.Context, env flux.Envelope, orderID string, cmd any) error {
	cmdID := flux.NewIdentifier("shop", "demo", "orders", "1", "command", env.Stream.Identifier.ResourceID(), "")
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
