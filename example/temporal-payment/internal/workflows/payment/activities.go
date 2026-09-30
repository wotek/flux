package payment

import (
	"context"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
)

// Activities holds flux dependencies used from Temporal activities.
// Cross-domain: listens to sales events (via starter) and dispatches sales commands.
type Activities struct {
	CmdBus *command.Bus

	mu            sync.Mutex
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
func (a *Activities) CheckPaymentReceivedActivity(ctx context.Context, orderID string) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.paymentsReady[orderID], nil
}

// PayOrderActivity dispatches [commands.PayOrder] via the flux command bus.
func (a *Activities) PayOrderActivity(ctx context.Context, in FulfillmentInput) error {
	return a.execute(ctx, in, commands.PayOrder{OrderID: in.OrderID})
}

// CancelOrderActivity dispatches [commands.CancelOrder] via the flux command bus.
func (a *Activities) CancelOrderActivity(ctx context.Context, in CancelOrderInput) error {
	return a.execute(ctx, in.FulfillmentInput, commands.CancelOrder{OrderID: in.OrderID, Reason: in.Reason})
}

func (a *Activities) execute(ctx context.Context, in FulfillmentInput, cmd any) error {
	cmdID := flux.MustParseIdentifier("urn:shop:demo:orders:1:command:" + in.OrderID)
	actor := flux.Actor{Identifier: flux.MustParseIdentifier(in.ActorURN)}
	corr := flux.MustParseIdentifier(in.CorrelationURN)
	caus := flux.MustParseIdentifier(in.CausationURN)
	inst := flux.Instrumentation{
		TraceID:    in.TraceID,
		SpanID:     in.SpanID,
		TraceFlags: in.TraceFlags,
	}
	cmdCtx := command.NewContext(ctx, cmdID, actor, corr, caus, flux.WithInstrumentation(inst))
	return command.Execute(cmdCtx, a.CmdBus, cmd)
}
