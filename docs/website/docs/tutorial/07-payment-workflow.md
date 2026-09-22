# 7. The Payment Workflow

When a user checks out, we need to charge their credit card. This is a long-running process that spans multiple bounded contexts.

Instead of writing complex state machines in Go, we use Temporal!

## The Workflow

```go
// internal/workflows/payment/workflow.go
func PaymentOrchestrator(ctx workflow.Context, orderID string) error {
    // 1. Dispatch a Command to charge the card
    workflow.ExecuteActivity(ctx, ChargeCardActivity, orderID).Get(ctx, nil)
    
    // 2. Wait up to 10 minutes for a PaymentConfirmed event
    // ... selector logic here ...
    
    // 3. Dispatch a Command to the Sales domain to mark Order as Paid
    return workflow.ExecuteActivity(ctx, MarkOrderPaidActivity, orderID).Get(ctx, nil)
}
```

This perfectly isolates failure, retries, and compensation across our distributed system!
