# Temporal Payment Example

Optional **Temporal** + **flux** integration for a durable payment **Workflow**.

The native `workflow.Orchestrator` in `example/e-commerce` remains the in-process path for prototypes and PoCs. This example shows how to wire Temporal when you want Temporal’s timers, retries, and UI.

Layout follows [`docs/PROJECT_LAYOUT.md`](../../docs/PROJECT_LAYOUT.md): domain-first `internal/sales/…`, workflow code under `internal/workflows/payment/`.

## What it demonstrates

1. **Split processes:** `cmd/worker` (Temporal worker + flux activities) and `cmd/demo` (CLI: place / pay / status).
2. **Stable Temporal workflow IDs** (`order-fulfillment:{orderID}`).
3. **Event Store as source of truth:** Workflows receive `event.EventReference` (`stream` + `event_id`) instead of duplicated domain payloads.
4. **Point-reading via `EventStore.Find`:** Activities load the persisted envelope by reference on-demand.
5. **Payment via Temporal signal:** `pay-order` signals `PaymentReceived` (like a payment webhook); timeout cancels.
6. **Shared Redis Event Store** so worker and demo share persisted events across processes.
7. **Aggregate idempotency** (`Pay` / `Cancel` no-ops)—no framework command-dedup store.

## How the demo works

### Processes

| Process | Role |
| --- | --- |
| `cmd/worker` | Long-running Temporal worker; registers the workflow and Pay/Cancel activities against a shared Event Store + command bus. |
| `cmd/demo place-order` | Executes `PlaceOrder`, reads the persisted `OrderPlaced` reference, starts the fulfillment workflow. |
| `cmd/demo pay-order` | Signals `PaymentReceived` on the workflow (simulates an external payment confirmation). |
| `cmd/demo status` | Loads the order aggregate and prints status. |

Worker and demo must share the same Event Store (`EVENTSTORE_REDIS_ADDR`). In-memory store is only for single-process unit tests.

### Workflow: `OrderFulfillmentWorkflow`

1. Wait on signal `PaymentReceived` **or** a 30s timer.
2. If signaled → `PayOrderActivity`.
3. If timer fires first → `CancelOrderActivity` with reason `payment_timeout`.

### Activities

| Activity | Role |
| --- | --- |
| `PayOrderActivity` | `Find` initiating envelope → rebuild `command.Context` → `command.Execute(PayOrder)`. |
| `CancelOrderActivity` | Same hydrate → `command.Execute(CancelOrder{Reason})`. |

### End state

- `pay-order` before timeout → status **`paid`**.
- No signal before timeout → status **`cancelled`**.

## Tests (no Temporal / Redis server)

```bash
cd example/temporal-payment
go test ./...
```

## Manual run

Local compose: Temporal (`server` + `admin-tools` + `ui:2.54.1`) and **Redis** for the shared Event Store.

```bash
cd example/temporal-payment
docker compose up -d
```

Terminal 1 — worker:

```bash
export EVENTSTORE_REDIS_ADDR=localhost:6379
go run ./cmd/worker
```

Terminal 2 — place then pay:

```bash
export EVENTSTORE_REDIS_ADDR=localhost:6379

go run ./cmd/demo place-order
# note the printed order_id, then:
go run ./cmd/demo pay-order --order-id 'urn:shop:demo:orders:1:order:…'
go run ./cmd/demo status --order-id 'urn:shop:demo:orders:1:order:…'
```

Expected status after pay:

```text
order urn:shop:demo:orders:1:order:… status=paid
```

UI: http://localhost:8080 — workflow ID `order-fulfillment:{orderID}`. Frontend gRPC: `localhost:7233`.

Stop:

```bash
docker compose down -v
```

## Layout

```text
cmd/
├── worker/                       # Temporal worker + activities
└── demo/                         # CLI: place-order | pay-order | status
internal/
├── platform/                     # Shared EventStore / command-bus wiring
├── sales/                        # Bounded context: Sales
│   ├── aggregates/order/
│   ├── commands/
│   ├── events/
│   └── types/
└── workflows/
    └── payment/                  # Workflow, activities, tests
```

## Docs

- Website: [Temporal Payment](https://flux.keylight.io/examples/temporal-payment)
- Guide: [Workflows & Temporal](https://flux.keylight.io/guide/workflows)
- Agent guide: [`docs/AGENT_GUIDE_TEMPORAL.md`](../../docs/AGENT_GUIDE_TEMPORAL.md)
- Project layout: [`docs/PROJECT_LAYOUT.md`](../../docs/PROJECT_LAYOUT.md)
