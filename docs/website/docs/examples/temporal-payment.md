# Temporal Payment Example

Optional [Temporal](https://temporal.io/) integration with `flux` aggregates and the command bus. Demonstrates a durable **Workflow** outside the native `workflow.Orchestrator` (that path remains available for prototypes and PoCs—see [E-Commerce](/examples/e-commerce)).

Source: [`example/temporal-payment`](https://github.com/wotek/flux/tree/main/example/temporal-payment).

## What it shows

- Domain-first layout (`internal/sales/…`, `internal/workflows/payment/`) per [Project Layout](/reference/project-layout)
- EventBus starts a Temporal workflow with a stable ID (`order-fulfillment:{orderID}`)
- Serializable DTOs (URNs / trace fields) across Temporal—not `flux.Event` interfaces
- Activities call `command.Execute` with context rebuilt from the DTO
- Aggregate no-ops for pay/cancel (no framework command-dedup store)
- `go test` via Temporal `testsuite` without Docker

> [!NOTE]
> The example currently passes a fulfillment DTO; migrating to `event.EventReference` + `EventStore.Find` is the next step.

## Run tests

```bash
cd example/temporal-payment
go test ./...
```

## Run against Temporal

```bash
cd example/temporal-payment
docker compose up -d
go run ./cmd/demo
```

See the example README for ports, UI, and teardown.

## Related guides

- [Workflows & Temporal](/guide/workflows) — native + optional Temporal conventions
- [Projections](/guide/projections) — native vs Temporal cursors
