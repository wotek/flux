package payment

import (
	"time"

	"github.com/wotek/flux/event"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// OrderFulfillmentWorkflow waits briefly for payment, then pays or cancels.
// It receives a lightweight event.EventReference rather than a duplicated domain payload;
// individual activities point-read the persisted envelope from the EventStore via Find.
func OrderFulfillmentWorkflow(ctx workflow.Context, ref event.EventReference) error {
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
	if err := workflow.ExecuteActivity(ctx, acts.CheckPaymentReceivedActivity, ref).Get(ctx, &paid); err != nil {
		return err
	}

	if paid {
		return workflow.ExecuteActivity(ctx, acts.PayOrderActivity, ref).Get(ctx, nil)
	}
	return workflow.ExecuteActivity(ctx, acts.CancelOrderActivity, CancelOrderInput{
		Reference: ref,
		Reason:    "payment_timeout",
	}).Get(ctx, nil)
}
