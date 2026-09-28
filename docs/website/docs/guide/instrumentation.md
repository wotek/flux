# Instrumentation & Observability

`flux` stays vendor-agnostic: the core module never imports OpenTelemetry. Instead it carries a small, portable tracing model on typed contexts and event envelopes. The official bridge that turns that model into OpenTelemetry spans and metrics lives in a separate repository: [`flux-opentelemetry`](https://github.com/wotek/flux-opentelemetry) (`package fluxotel`).

## Vendor-Neutral Primitives in Core

Every typed context (`flux.Context`, `command.Context`, `query.Context`, `event.Context`, `projection.Context`, `workflow.Context`) exposes:

```go
Instrumentation() Instrumentation
```

```go
type Instrumentation struct {
	TraceID    string
	SpanID     string
	TraceFlags string // Canonical single-byte hex (e.g. "01")
}
```

`Instrumentation.IsValid()` is true when both `TraceID` and `SpanID` are non-empty.

### Reparenting with `WithParent`

Handlers often need to attach a child span (or a deadline) without losing actor, correlation, causation, or envelope coordinates. Use the typed `WithParent(parent context.Context)` method (or `flux.WithParent` for the base context) so the Go `context.Context` is replaced while all flux metadata is preserved.

::: tip
See [Typed Contexts](/reference/api#typed-contexts) for the full interface surface, including `event.EventMetadata`.
:::

### Envelope Metadata Keys

When an aggregate is saved, `AggregateRepository.Save` copies a valid `Instrumentation` onto each new envelope using OpenTelemetry-compatible keys:

| Constant | Metadata key |
| --- | --- |
| `flux.MetadataTraceID` | `trace_id` |
| `flux.MetadataSpanID` | `span_id` |
| `flux.MetadataTraceFlags` | `trace_flags` |

When events are later handled, `event.NewContext` hydrates `Instrumentation` back from those keys so projectors and workflows continue the same distributed trace.

### Outbox Continuity

Workflow stores persist the triggering event's `Instrumentation` on outbox messages. When the relay dispatches a command, that instrumentation is restored on the `command.Context`, so asynchronous command handling stays on the originating `TraceID`.

## Official OpenTelemetry Integration

[`github.com/wotek/flux-opentelemetry`](https://github.com/wotek/flux-opentelemetry) translates flux instrumentation into OpenTelemetry. It provides:

- **Bus middlewares** — spans and metrics for command and query handlers
- **Store decorators** — client spans for event, snapshot, and projection stores
- **Async wrappers** — projection and workflow handlers that continue remote parents
- **Low-cardinality metrics** — latency, throughput, and error rates with locked `ok` / `error` status attributes

### Installation

```bash
go get github.com/wotek/flux-opentelemetry
```

Requires `github.com/wotek/flux@v1.3.0` or later.

```go
import (
	"go.opentelemetry.io/otel"

	fluxotel "github.com/wotek/flux-opentelemetry"
)

tracer := otel.Tracer("my-service")
meter := otel.Meter("my-service")
```

### Command and Query Buses

```go
cmdBus := command.New()
cmdBus.Use(fluxotel.CommandMiddleware(tracer, meter))

queryBus := query.New()
queryBus.Use(fluxotel.QueryMiddleware(tracer, meter))
```

When a command arrives from an outbox relay with a valid remote `Instrumentation`, `CommandMiddleware` parents the new span via `trace.ContextWithRemoteSpanContext`, preserving end-to-end continuity.

### Storage Decorators

Wrap stores at composition time so domain code stays unaware of telemetry:

```go
eventStore := fluxotel.WrapEventStore(rawEventStore, tracer, meter)
snapshotStore := fluxotel.WrapSnapshotStore(rawSnapshotStore, tracer, meter)
projectionStore := fluxotel.WrapProjectionStore(rawProjStore, tracer, meter)
```

::: info Iterator spans
`Read` and `Stream` use a two-phase span lifecycle: a short setup span for opening the iterator, then an iteration span that ends when ranging finishes, stops early, or fails. Always consume returned iterators so spans close cleanly.
:::

::: tip Snapshot misses
`flux.ErrSnapshotNotFound` is expected cache-miss behavior. Decorators keep span status `ok`, set `flux.snapshot.miss=true`, and increment `flux.snapshot.load.misses`. Real store failures still record errors.
:::

### Projection and Workflow Handlers

Wrap handlers before registration so each invocation is a child span under the event's remote parent:

```go
projection.RegisterHandler(projector, fluxotel.WrapProjectionHandler(tracer, meter,
	func(ctx projection.Context, e OrderPlaced) error {
		return projectionStore.Update(ctx, projectorID, env, func(txCtx context.Context) error {
			// Read-model mutation
			return nil
		})
	},
))

workflow.RegisterHandler(orchestrator, wfStore, fluxotel.WrapWorkflowHandler(tracer, meter,
	func(ctx workflow.Context, wf *OrderFulfillmentWorkflow, e OrderPlaced) error {
		workflow.EnqueueCommand(ctx, SendWelcomeEmailCommand{OrderID: e.OrderID})
		return nil
	},
))
```

## End-to-End Trace Continuity

A typical happy path shares one `TraceID` across synchronous and asynchronous work:

1. **Command bus** — `flux.command` span; middleware stamps active span IDs onto the typed context.
2. **Aggregate save** — repository writes `trace_id` / `span_id` / `trace_flags` into envelope metadata; event store append is a child client span.
3. **Projector** — hydrates instrumentation from the envelope; wrapper starts `flux.projection` as a remote child of the originating command span.
4. **Workflow** — same pattern with `flux.workflow`; enqueued commands carry the event's instrumentation in the outbox.
5. **Outbox relay** — dispatches the command; `CommandMiddleware` remote-parents the follow-up `flux.command` span onto the same trace.

## Metrics Catalog (Summary)

Status attributes are only `"ok"` or `"error"`. High-cardinality values (stream IDs, aggregate IDs, user IDs) are never metric attributes—those belong on spans when useful for correlation.

| Area | Instruments |
| --- | --- |
| Commands / queries | `flux.command.duration`, `flux.command.count`, `flux.query.duration`, `flux.query.count` |
| Event store | `flux.event_store.append.duration`, `.concurrency_conflicts`, `.events`, `flux.event_store.read.duration` |
| Snapshots | `flux.snapshot.load.duration`, `.save.duration`, `.load.misses` |
| Projections | `flux.projection.store.update.duration`, `flux.projection.handler.duration`, `.count` |
| Workflows | `flux.workflow.handler.duration`, `flux.workflow.handler.count` |

Outbox queue depth / dispatch metrics are reserved for a later `fluxotel` release; v1 still traces outbox-driven commands through `CommandMiddleware`.

## Further Reading

- [`flux-opentelemetry` repository](https://github.com/wotek/flux-opentelemetry) — full wiring examples and metric attribute tables
- [Command, Query, and Event Buses](/guide/buses) — middleware chaining
- [Events & Envelopes](/guide/events) — envelope metadata
- [Workflows & Temporal Integration](/guide/workflows) — outbox propagation
- [Core API Reference](/reference/api) — typed contexts and `WithParent`
