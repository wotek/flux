# Bank Account Example

The `example/bank` application is the absolute most minimal implementation of the `flux` framework. 

It is designed to show you how the core pieces fit together in a single `main.go` file without any advanced project layout or network routing.

## What it demonstrates

1. **Domain Events:** Defining basic structs like `AccountCreated` and `MoneyDeposited`.
2. **The Aggregate Root:** Embedding `flux.AggregateRoot` into a `BankAccount` struct.
3. **The `apply` mutator:** Changing the aggregate's internal balance exclusively through events.
4. **Command Bus:** Wiring up a basic command dispatcher.
5. **In-Memory Storage:** Using `eventstore.New()` and `flux.NewAggregateRepository` to save and load state locally.

## Running the Example

Navigate to the directory and run it:

```bash
cd example/bank
go run main.go
```

Expected Output:
```text
Loaded Account: Alice, Balance: $0
Loaded Account: Alice, Balance: $250
```

## Source Code Highlight

The most important part of this example is the Aggregate definition. Notice how `Deposit()` enforces business invariants, but state mutation only happens inside `apply()`!

```go
type BankAccount struct {
	flux.AggregateRoot[flux.Event]
	Owner   string
	Balance int
}

func (a *BankAccount) Deposit(amount int) error {
	if amount <= 0 {
		return errors.New("cannot deposit negative amount")
	}
	evt := MoneyDeposited{Amount: amount}
	a.apply(evt)
	a.Changeset().Record(evt)
	return nil
}

func (a *BankAccount) apply(event flux.Event) {
	switch e := event.(type) {
	case AccountCreated:
		a.Owner = e.Owner
	case MoneyDeposited:
		a.Balance += e.Amount
	}
}
```
