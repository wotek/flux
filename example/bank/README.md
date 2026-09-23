# Bank Account Quick Start Example

A standalone, minimal reference implementation demonstrating the core concepts of the `flux` framework from the README Quick Start guide.

## Features Demonstrated

- **Domain Events**: `AccountCreated`, `MoneyDeposited` implementing `flux.Event`.
- **Aggregate Root**: `BankAccount` embedding `flux.AggregateRoot[flux.Event]` with self-referencing generic factory (`New`) and event applicator (`apply`).
- **Event Store & Repository**: In-memory `eventstore.New()` and `flux.NewAggregateRepository`.
- **Command Bus**: `command.New()`, `command.Register`, and `command.Execute` handling `CreateAccountCommand` and `DepositCommand`.
- **Rehydration**: Loading aggregate state from the event stream after each command.

## Running

From the repository root:

```bash
cd example/bank
go run main.go
```

Expected Output:

```
Loaded Account: Alice, Balance: $0
Loaded Account: Alice, Balance: $250
```
