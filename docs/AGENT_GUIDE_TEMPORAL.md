# Coding Agent Guide: Flux + Temporal

Optional patterns for integrating [Temporal](https://temporal.io/) with `github.com/wotek/flux`. Aligns with the website [Workflows & Temporal](https://flux.keylight.io/guide/workflows) guide.

## Product stance (do not contradict)

| Concern | Runtime |
| --- | --- |
| Workflows (prototypes / PoCs / embedded) | Native `workflow.Orchestrator` + outbox + `checkpoint.Store` |
| Workflows (optional Temporal) | Temporal activities → `command.Execute` |
| Embedded read models | Native `projection.Projector` + `checkpoint.Store` |
| Heavy / durable projection rebuilds | Temporal hybrid tail (optional) |
| Command dedup store in flux | **Out of scope** — aggregate no-ops; Temporal activity completion when using Temporal; consumer-owned ledgers if needed |

Temporal is **optional**. Prefer native orchestrator for quick proofs of concept. Never dual-write flux `checkpoint.Store` from Temporal for the same logical consumer.

There is **no** `flux/temporal` helper package—use docs, conventions, and examples only.

## Core flux rules (current APIs)

- **Events:** implement `flux.Event` with `Name() string`.
- **Aggregates:** embed `flux.AggregateRoot[E]`; provide `New(stream flux.Stream) *T` and infallible `apply(E)`; domain methods call `apply` then `Changeset().Record`.
- **Repositories:** `flux.NewAggregateRepository[*T, E](eventStore)`.
- **Buses:** `command.New()`, `event.New()`, `query.New()`.
- **Typed contexts:** use `command.NewContext` / `event.NewContext` with actor, correlation, causation, and optional `flux.WithInstrumentation`.
- **Global stream position:** `flux.Envelope.Position` (uint64)—not `GlobalPosition`.
- **Go:** 1.27+.
- **Placement:** workflow code under `internal/workflows/<name>/` (native or Temporal)—see `docs/PROJECT_LAYOUT.md`.

## Conventions (when using Temporal)

1. **Workflow ID:** `order-fulfillment:{orderID}` (or URN). Reject duplicate starts on redelivery.
2. **Start vs signal:** `StartWorkflow` on initiating events; `SignalWorkflow("WakeUpSignal")` for projection wakeups.
3. **Activity → command:** Build `command.Context` from envelope metadata retrieved via `Find` (actor, correlation, causation, instrumentation)—not from a live `flux.Event` interface on the Temporal wire.
4. **Payloads & EventReference:** Prefer passing `event.EventReference` (`stream` + `event_id`) over fat fulfillment DTOs when the initiating fact is a stored event. Activities point-read the envelope via `EventStore.Find`. Custom activity DTOs are reserved for external inputs not present in the stored event.
5. **Idempotency:** Temporal + aggregate invariants; no flux command-dedup middleware required.
6. **Observability:** optional `fluxotel` on the command bus.

## Workflow pattern (Temporal)

```go
event.Register(bus, func(ctx event.Context, e OrderPlaced) error {
	ref := event.EventReference{
		Stream:  ctx.Stream().Identifier,
		EventID: ctx.EventIdentifier(),
	}
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "order-fulfillment:" + e.OrderID,
		TaskQueue: "shop",
		// Prefer reuse/conflict policies that avoid a second run on redelivery
	}, OrderFulfillmentWorkflow, ref)
	return err
})
```

```go
func PayOrderActivity(ctx context.Context, ref event.EventReference) error {
	env, err := eventStore.Find(ctx, flux.Stream{Identifier: ref.Stream}, ref.EventID)
	if err != nil {
		return err // Temporal automatically retries on transient errors / outbox lag
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
		flux.WithInstrumentation(/* restored from env.Metadata if used */),
	)
	return command.Execute(cmdCtx, cmdBus, PayOrder{OrderID: placed.OrderID})
}
```

## Temporal projection tail (optional)

- Activity fetches from `EventStore.Stream(ctx, lastPosition)` (or Read) into **DTOs**.
- Activity applies read-model updates.
- Workflow waits on `WakeUpSignal` + fallback timer; `ContinueAsNew` with `lastPosition`.
- EventBus signals the stable projection workflow ID.
- Cursor = Temporal args/history only.

## Reference implementation

See `example/temporal-payment` (domain-first layout: `internal/sales/…`, Temporal under `internal/workflows/payment/`; uses `event.EventReference` and `EventStore.Find`) and https://flux.keylight.io/examples/temporal-payment.

Native in-process workflows: `example/e-commerce`.

## Task for agents

When asked to build a Temporal + flux demo, follow this guide and the temporal-payment example. Prefer the native orchestrator for simple PoCs unless the human asks for Temporal. Do **not** add a flux command-dedup package unless the human explicitly requests consumer-side dedup design.
