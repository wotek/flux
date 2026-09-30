package payment

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// OrderFulfillmentWorkflow waits briefly for payment, then pays or cancels.
// Production apps would typically wait on a Signal; this demo uses a short timer.
func OrderFulfillmentWorkflow(ctx workflow.Context, in FulfillmentInput) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	_ = workflow.Sleep(ctx, 500*time.Millisecond)

	var acts *Activities

	var paid bool
	if err := workflow.ExecuteActivity(ctx, acts.CheckPaymentReceivedActivity, in.OrderID).Get(ctx, &paid); err != nil {
		return err
	}

	if paid {
		return workflow.ExecuteActivity(ctx, acts.PayOrderActivity, in).Get(ctx, nil)
	}
	return workflow.ExecuteActivity(ctx, acts.CancelOrderActivity, CancelOrderInput{
		FulfillmentInput: in,
		Reason:           "payment_timeout",
	}).Get(ctx, nil)
}
