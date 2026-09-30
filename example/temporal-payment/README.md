# Temporal Payment Example

Optional **Temporal** + **flux** integration for a durable payment **Workflow**.

The native `workflow.Orchestrator` in `example/e-commerce` remains the in-process path for prototypes and PoCs. This example shows how to wire Temporal when you want Temporal’s timers, retries, and UI.

Layout follows [`docs/PROJECT_LAYOUT.md`](../../docs/PROJECT_LAYOUT.md): domain-first `internal/sales/…`, workflow code under `internal/workflows/payment/`.

## What it demonstrates

1. **Stable Temporal workflow IDs** (`order-fulfillment:{orderID}`) started from an EventBus handler.
2. **Event Store as source of truth:** Workflows receive `event.EventReference` (`stream` + `event_id`) instead of duplicated domain payloads.
3. **Point-reading via `EventStore.Find`:** Activities load the persisted envelope by reference on-demand, keeping Temporal history lightweight.
4. **Context reconstruction:** Activities rebuild `command.Context` from envelope metadata (actor, correlation, causation, instrumentation) and call `command.Execute`.
5. **Slim activity outputs:** Activities return only `error` or minimal status, never full envelopes.
6. **Aggregate idempotency** (`Pay` / `Cancel` no-ops)—no framework command-dedup store.
7. **Automated tests** via Temporal’s `testsuite` (no Docker required for `go test`).

## Tests (no Temporal server)

```bash
cd example/temporal-payment
go test ./...
```

## Manual run (Temporal stack)

Local compose uses maintained images (`temporalio/server` + `temporalio/admin-tools` + `temporalio/ui:2.54.1`). The older `temporalio/auto-setup` image is deprecated.

```bash
cd example/temporal-payment
docker compose up -d
# wait until temporal + UI are healthy, then:
go run ./cmd/demo
```

Expected output (payment marked ready before the workflow’s check):

```text
order ord-… status=paid
```

> **Note:** Payment readiness (`MarkPaymentReady`) simulates an external payment webhook/gateway confirmation before a domain event is recorded, which is why it lives in demo activity state rather than the Event Store.

UI: http://localhost:8080 — workflow ID `order-fulfillment:{orderID}`. Frontend gRPC: `localhost:7233`.

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
