# Testing Best Practices

One of the greatest benefits of the CQRS and Event Sourcing pattern is how effortlessly testable it is. Because business logic is entirely decoupled from databases and network I/O, you can unit test your entire domain incredibly fast.

## Testing Aggregates

To test an Aggregate, you don't need a mock database. You simply instantiate the Aggregate, call the public method, and assert against the resulting `Changeset`.

```go
func TestAccount_Deposit(t *testing.T) {
    // 1. Arrange
    account := bank.New(flux.Stream{Identifier: flux.MustParseIdentifier("urn:bank:account:1")})
    
    // 2. Act
    account.Deposit(500)
    
    // 3. Assert
    events := account.Changeset().Events()
    if len(events) != 1 {
        t.Fatalf("expected 1 event, got %d", len(events))
    }
    
    depositedEvent, ok := events[0].(bank.MoneyDeposited)
    if !ok || depositedEvent.Amount != 500 {
        t.Fatal("expected MoneyDeposited with amount 500")
    }
}
```

## Using the In-Memory Store

For integration tests that cover Command Handlers and Buses, use the built-in `eventstore.New()` to spin up a blazing-fast, thread-safe memory backend.
