# Temporal Payment Example

Optional [Temporal](https://temporal.io/) integration with `flux` aggregates and the command bus. Demonstrates a durable **Workflow** outside the native `workflow.Orchestrator` (that path remains available for prototypes and PoCs—see [E-Commerce](/examples/e-commerce)).

Source: [`example/temporal-payment`](https://github.com/wotek/flux/tree/main/example/temporal-payment).

## What it shows

- Split processes: `cmd/worker` and `cmd/demo` (`place-order` / `pay-order` / `status`)
- Domain-first layout (`internal/sales/…`, `internal/workflows/payment/`) per [Project Layout](/reference/project-layout)
- Workflows pass `event.EventReference`; activities use `EventStore.Find`
- Payment confirmation via Temporal signal (`pay-order`); timeout cancels
- Shared Redis Event Store across worker and demo
- `go test` via Temporal `testsuite` without Docker

## Run tests

```bash
cd example/temporal-payment
go test ./...
```

## Run against Temporal

```bash
cd example/temporal-payment
docker compose up -d
export EVENTSTORE_REDIS_ADDR=localhost:6379
go run ./cmd/worker   # terminal 1
go run ./cmd/demo place-order
go run ./cmd/demo pay-order --order-id 'urn:shop:demo:orders:1:order:…'
```

See the example README for ports, UI, and teardown.

## Related guides

- [Workflows & Temporal](/guide/workflows) — native + optional Temporal conventions
- [Projections](/guide/projections) — native vs Temporal cursors
