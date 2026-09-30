# Workflows & Temporal

A **Workflow** coordinates long-running business processes that span multiple aggregates (for example: place an order, wait for payment, then ship).

`flux` ships an in-process `workflow.Orchestrator` with a durable **outbox** and `checkpoint.Store` cursor. That path is good enough for prototypes, embedded demos, and quick proof of concepts. Handlers must stay **idempotent** under at-least-once delivery; sophisticated deduplication or delivery guarantees are left to the consumer (see [Beyond the basics](#beyond-the-basics-consumer-responsibility) below).

When you already operate (or want) a dedicated durable workflow engine—timers, retries, visibility UI—**[Temporal](https://temporal.io/) is an optional, production-friendly fit**. `flux` stays the CQRS / event-sourcing core; Temporal owns long-running step durability. There is no `flux/temporal` adapter package: wire Temporal yourself using the conventions below.

Runnable Temporal reference: [Temporal Payment example](/examples/temporal-payment) (`example/temporal-payment`). Native in-process reference: [E-Commerce](/examples/e-commerce).

## Choosing a Workflow Runtime

| Feature | Native `workflow.Orchestrator` | Temporal (optional) |
| --- | --- | --- |
| **Role** | Prototypes, PoCs, embedded demos, tests | Durable timers, distributed retries, shared Temporal ops |
| **Cursor / progress** | `checkpoint.Store` | Workflow history + `ContinueAsNew` |
| **State** | `workflow.Store` + outbox | Temporal history |
| **Timers** | Manual / external | Durable `workflow.Sleep` / `NewTimer` |
| **Command side effects** | Outbox relay → command bus | Activities call `command.Execute` |
| **Infrastructure** | In-process only | Temporal cluster |

Pick **one** cursor owner per logical consumer. Never dual-write flux `checkpoint.Store` from Temporal activities for the same consumer.

## Native runtime: `workflow` package

Use this path when you want flux-only orchestration without a Temporal cluster.

### Workflow contract

```go
type Workflow[W Workflow[W]] interface {
	Identifier() flux.Identifier
	New() W
	Clone() W
}
```

Handlers mutate an isolated clone from `Load`. The store commits only after success.

### Checkpoint + outbox

The `Orchestrator` tails the event store and persists progress with `checkpoint.Store`. Pass a non-nil store (`checkpoint/store.New()` for prototypes). `nil` panics.

```go
orch := workflow.NewOrchestrator(
	flux.MustParseIdentifier("urn:shop:demo:workflows:1:orchestrator:payment"),
	eventStore,
	checkpointstore.New(),
)
workflow.RegisterHandler(orch, paymentStore, func(ctx workflow.Context, wf *PaymentWorkflow, e OrderPlaced) error {
	workflow.EnqueueCommand(ctx, ReserveStock{OrderID: e.OrderID})
	workflow.EnqueueCommand(ctx, ChargePayment{OrderID: e.OrderID})
	return nil
})
go orch.Start(ctx)
```

Commands are enqueued with `workflow.EnqueueCommand` and stored with workflow state (`Store.Save`). A relay dispatches them with ack-after-success and restores instrumentation onto `command.Context`.

Handlers must be **idempotent**: at-least-once redelivery can re-invoke handlers and enqueue new outbox rows. Aggregate invariants (natural no-ops) are usually enough for PoCs.

See the e-commerce example’s in-process payment workflow under `internal/workflows/payment/`.

## Optional: Temporal integration

Temporal is **not required**. Add it when durable timers, activity retries, or Temporal’s operational tooling matter more than staying in-process.

### Conventions

| Convention | Rule |
| --- | --- |
| **Workflow ID** | Derive from a flux URN / correlation ID, e.g. `order-fulfillment:{orderURN}`. Use a reuse policy so a duplicate `OrderPlaced` does not start a second run. |
| **Start vs signal** | `StartWorkflow` on initiating domain events; `SignalWorkflow` (e.g. `WakeUpSignal`) to wake projection tailers. |
| **Activity → command** | Build `command.Context` from envelope metadata (actor, correlation, causation, instrumentation) inside the activity. |
| **Payloads** | Pass serializable DTOs / IDs into Temporal. Never put a `flux.Event` interface value on the Temporal wire; decode via codecs/registries at the edge if needed. |
| **Idempotency** | Temporal activity completion + aggregate no-ops. No second flux command-dedup ledger. |
| **Observability** | Optional: [`fluxotel`](https://github.com/wotek/flux-opentelemetry) on buses so envelope-stamped instrumentation continues into command spans. |

### Trigger a Temporal Workflow from the EventBus

```go
event.Register(eventBus, func(ctx event.Context, e OrderPlaced) error {
	wfID := "order-fulfillment:" + e.OrderID
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: "shop",
		// Prefer reuse/conflict policies that avoid a second run on redelivery
	}, OrderFulfillmentWorkflow, OrderFulfillmentInput{
		OrderID:         e.OrderID,
		Actor:           ctx.Actor(),
		CorrelationID:   ctx.CorrelationIdentifier(),
		CausationID:     ctx.EventIdentifier(),
		Instrumentation: ctx.Instrumentation(),
	})
	return err
})
```

> [!TIP]
> While passing custom DTOs (like `OrderFulfillmentInput` above) works when conveying external parameters, prefer passing an [`event.EventReference`](#optional-eventreference--eventstorefind) whenever the initiating fact is already persisted in the Event Store.

### Activity → command

```go
func PayOrderActivity(ctx context.Context, input PayOrderInput) error {
	cmdID := flux.MustParseIdentifier("urn:shop:prod:pay:1:command:" + input.OrderID)
	cmdCtx := command.NewContext(
		ctx,
		cmdID,
		input.Actor,
		input.CorrelationID,
		input.CausationID,
		flux.WithInstrumentation(input.Instrumentation),
	)
	return command.Execute(cmdCtx, cmdBus, PayOrder{OrderID: input.OrderID})
}
```

Pass `Actor`, correlation/causation IDs, and `Instrumentation` as fields on a DTO built when the workflow started (from the triggering envelope)—not as a live `flux.Event` interface.

Steps:

1. **Trigger:** EventBus handler calls `StartWorkflow` with a stable workflow ID.
2. **Run:** Temporal owns state, timers (e.g. “cancel if unpaid in 10 minutes”), and retries.
3. **Act:** Activities call `command.Execute` against aggregates.

### Optional: EventReference + EventStore.Find

When an initiating domain event has already been appended to the Event Store, copying all of its payload and metadata fields into custom Temporal input DTOs duplicates state across systems and bloats Temporal execution history.

Instead, workflows can pass a lightweight, payload-free reference—`event.EventReference` (`stream` + `event_id`)—and have individual activities load the full envelope on-demand using `EventStore.Find`. The Event Store remains the single source of truth, while Temporal coordinates step execution.

#### Starting a Workflow with EventReference

When triggering a workflow from an EventBus subscriber (or transactional outbox relay), construct an `event.EventReference` from the persisted envelope coordinates:

```go
event.Register(eventBus, func(ctx event.Context, e OrderPlaced) error {
	ref := event.EventReference{
		Stream:  ctx.Stream().Identifier,
		EventID: ctx.EventIdentifier(),
	}

	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "order-fulfillment:" + e.OrderID,
		TaskQueue: "shop",
	}, OrderFulfillmentWorkflow, ref)
	return err
})
```

> [!NOTE]
> `ctx.EventIdentifier()` is only resolvable via `EventStore.Find` if that envelope was already persisted to the store. Always align EventBus dispatch with store commits (such as after `repository.Save` or via an outbox relay).

#### Loading the Envelope Inside Activities

Activities receive the `event.EventReference`, load the envelope directly from the `EventStore`, reconstruct the command context from the stored envelope metadata, and execute the domain command:

```go
func PayOrderActivity(ctx context.Context, ref event.EventReference) error {
	env, err := eventStore.Find(ctx, flux.Stream{Identifier: ref.Stream}, ref.EventID)
	if err != nil {
		// Transient errors or outbox replication lag are retried automatically by Temporal
		return err
	}

	placed, ok := env.Event.(OrderPlaced)
	if !ok {
		return fmt.Errorf("unexpected event %T", env.Event)
	}

	cmdCtx := command.NewContext(
		ctx,
		flux.MustParseIdentifier("urn:shop:prod:pay:1:command:"+placed.OrderID),
		env.Actor,
		env.CorrelationIdentifier,
		env.Identifier, // causation = triggering event ID
		flux.WithInstrumentation(/* restored from env.Metadata if tracing is configured */),
	)
	return command.Execute(cmdCtx, cmdBus, PayOrder{OrderID: placed.OrderID})
}
```

#### Best Practices for EventReference in Temporal

- **Keep Activity Outputs Slim:** Activities should return only `error` or a minimal status struct. Never return a `flux.Envelope` or domain event from an activity back to the workflow, as doing so would serialize full payloads into Temporal history.
- **Custom DTOs When Appropriate:** Use custom activity input structs only when transmitting external data not present in the stored domain event (such as a third-party payment gateway transaction token). When facts already exist in the event log, pass `EventReference` and let the activity retrieve them with `Find`.

### Hybrid tailing projections on Temporal

For long-running read-model rebuilds with durable retries, you can optionally run a Temporal workflow that:

1. Fetches a batch from `EventStore.Stream` / `Read` starting at `lastPosition`.
2. Runs an activity to apply each envelope to the read model.
3. When caught up, waits on `WakeUpSignal` (plus a short fallback timer).
4. Uses `ContinueAsNew` periodically to bound history size.

```go
func DashboardProjectionWorkflow(ctx workflow.Context, lastPosition uint64) error {
	wakeupChan := workflow.GetSignalChannel(ctx, "WakeUpSignal")

	for {
		var batch []EnvelopeDTO // serializable DTO, not flux.Envelope with interface Event
		err := workflow.ExecuteActivity(ctx, FetchEventsActivity, lastPosition).Get(ctx, &batch)
		if err != nil {
			return err
		}

		for _, env := range batch {
			err := workflow.ExecuteActivity(ctx, UpdateDashboardActivity, env).Get(ctx, nil)
			if err != nil {
				return err
			}
			lastPosition = env.Position
		}

		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
			return workflow.NewContinueAsNewError(ctx, DashboardProjectionWorkflow, lastPosition)
		}

		if len(batch) == 0 {
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(wakeupChan, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
			})
			timerCtx, cancelTimer := workflow.WithCancel(ctx)
			selector.AddFuture(workflow.NewTimer(timerCtx, time.Minute), func(f workflow.Future) {})
			selector.Select(ctx)
			cancelTimer()
		}
	}
}
```

Wake the workflow from the EventBus when new events land:

```go
event.RegisterGlobal(eventBus, func(ctx event.Context, e flux.Event) error {
	return temporalClient.SignalWorkflow(
		context.Background(),
		"DashboardProjection",
		"",
		"WakeUpSignal",
		nil,
	)
})
```

Cursor progress lives in Temporal (`lastPosition` / ContinueAsNew args)—**not** in flux `checkpoint.Store` for that same consumer.

For embedded / no-Temporal services, keep using the native [`Projector`](/guide/projections) with `checkpoint.Store`.

## Beyond the basics (consumer responsibility)

`flux` does **not** ship a framework command-dedup store or `IdempotencyPolicy`. Core outbox + checkpoint + aggregate no-ops cover prototyping.

If you need stronger delivery semantics, build them in your application. A basic sketch:

```go
// Pseudo-code: consumer-owned processed-commands ledger
func (h *PayOrderHandler) Handle(ctx command.Context, cmd PayOrder) error {
	if h.ledger.Seen(ctx.CommandIdentifier()) {
		return nil // already applied
	}
	if err := h.apply(ctx, cmd); err != nil {
		return err
	}
	return h.ledger.Mark(ctx.CommandIdentifier())
}
```

Under Temporal, completed activities are not re-executed on replay, so many apps rely on that plus aggregate invariants instead of a ledger. Choose what fits your deployment.

## Placement

Whether you use the native orchestrator or Temporal, put workflow-related code under `internal/workflows/<name>/` (see [Project Layout](/reference/project-layout)).
