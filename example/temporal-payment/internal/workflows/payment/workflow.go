package payment

import (
	"time"

	"github.com/wotek/flux/event"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// PaymentReceivedSignal is sent by the demo `pay-order` command (or a real payment webhook)
// to unblock the fulfillment workflow.
const PaymentReceivedSignal = "PaymentReceived"

// OrderFulfillmentWorkflow waits for a payment signal (or times out), then pays or cancels.
// It receives a lightweight event.EventReference; activities point-read the envelope via Find.
func OrderFulfillmentWorkflow(ctx workflow.Context, ref event.EventReference) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	signalChan := workflow.GetSignalChannel(ctx, PaymentReceivedSignal)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	timer := workflow.NewTimer(timerCtx, 30*time.Second)

	selector := workflow.NewSelector(ctx)
	paid := false
	selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, nil)
		paid = true
		cancelTimer()
	})
	selector.AddFuture(timer, func(f workflow.Future) {
		_ = f.Get(ctx, nil)
	})
	selector.Select(ctx)

	var acts *Activities
	if paid {
		return workflow.ExecuteActivity(ctx, acts.PayOrderActivity, ref).Get(ctx, nil)
	}
	return workflow.ExecuteActivity(ctx, acts.CancelOrderActivity, CancelOrderInput{
		Reference: ref,
		Reason:    "payment_timeout",
	}).Get(ctx, nil)
}
