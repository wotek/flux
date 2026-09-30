# Temporal Payment Example

Optional **Temporal** + **flux** integration for a durable payment **Workflow**.

The native `workflow.Orchestrator` in `example/e-commerce` remains the in-process path for prototypes and PoCs. This example shows how to wire Temporal when you want Temporal’s timers, retries, and UI.

Layout follows [`docs/PROJECT_LAYOUT.md`](../../docs/PROJECT_LAYOUT.md): domain-first `internal/sales/…`, workflow code under `internal/workflows/payment/`.

## What it demonstrates

1. **Stable Temporal workflow IDs** (`order-fulfillment:{orderID}`) started from an EventBus handler.
2. **Serializable DTOs** across the Temporal boundary (URN strings + trace fields—not `flux.Event` interfaces).
3. **Activities → `command.Execute`** with `command.Context` rebuilt from DTO metadata (actor, correlation, causation, instrumentation).
4. **Aggregate idempotency** (`Pay` / `Cancel` no-ops)—no framework command-dedup store.
5. **Automated tests** via Temporal’s `testsuite` (no Docker required for `go test`).

## Tests (no Temporal server)

```bash
cd example/temporal-payment
go test ./...
```

## Manual run (Temporal stack)

```bash
cd example/temporal-payment
docker compose up -d
go run ./cmd/demo
```

Expected output (payment marked ready before the workflow’s check):

```text
order ord-… status=paid
```

UI: http://localhost:8080 — workflow ID `order-fulfillment:{orderID}`.

Stop:

```bash
docker compose down -v
```

## Layout

```text
cmd/demo/                         # Wiring: stores, buses, Temporal worker, demo driver
internal/
├── sales/                        # Bounded context: Sales
│   ├── aggregates/order/         # Order write model
│   ├── commands/                 # Place / Pay / Cancel handlers
│   ├── events/                   # OrderPlaced, OrderPaid, OrderCancelled
│   └── types/                    # OrderStatus value object
└── workflows/
    └── payment/                  # Temporal DTOs, workflow, activities, tests
```

## Docs

- Website: [Temporal Payment](https://flux.keylight.io/examples/temporal-payment)
- Guide: [Workflows & Temporal](https://flux.keylight.io/guide/workflows)
- Agent guide: [`docs/AGENT_GUIDE_TEMPORAL.md`](../../docs/AGENT_GUIDE_TEMPORAL.md)
- Project layout: [`docs/PROJECT_LAYOUT.md`](../../docs/PROJECT_LAYOUT.md)
