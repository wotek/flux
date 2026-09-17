# Sentinel Errors Implementation Plan

This document outlines the blueprint for standardizing error handling across the `flux` framework using **Wrapped Sentinel Errors**. This allows consumers to programmatically evaluate failure reasons using Go's standard `errors.Is()`.

## 1. Create Core Errors File

Create a new file `flux/errors.go` to hold the framework's exported sentinel errors:

```go
package flux

import "errors"

var (
	// ErrAggregateNotFound is returned when an aggregate cannot be loaded from the EventStore or SnapshotStore.
	ErrAggregateNotFound = errors.New("aggregate not found")

	// ErrConcurrency is returned by an EventStore when an optimistic concurrency check fails.
	ErrConcurrency = errors.New("optimistic concurrency check failed")

	// ErrInvalidEvent is returned when an aggregate's FromEvents encounters an event type it cannot apply.
	ErrInvalidEvent = errors.New("invalid event type for aggregate")
)
```

## 2. Refactor Core Package

Update the following files in the `flux` root package to wrap the sentinels using the `%w` verb:

### `aggregate.go`
In `FromEvents(events StreamIterator)`:
- **Before:** `return fmt.Errorf("aggregate %s cannot apply event of type %T", a.stream.Identifier.String(), env.Event)`
- **After:** `return fmt.Errorf("%w: aggregate %s cannot apply %T", ErrInvalidEvent, a.stream.Identifier.String(), env.Event)`

### `repository.go`
In `Load(ctx Context, stream Stream)`:
- **Before:** `return zero, fmt.Errorf("aggregate not found: %s", stream.Identifier.String())`
- **After:** `return zero, fmt.Errorf("%w: %s", ErrAggregateNotFound, stream.Identifier.String())`

### `snapshot_repository.go`
In `Load(ctx Context, stream Stream)`:
- **Before:** `return zero, fmt.Errorf("aggregate not found: %s", stream.Identifier.String())`
- **After:** `return zero, fmt.Errorf("%w: %s", ErrAggregateNotFound, stream.Identifier.String())`

## 3. Enforce Concurrency Contract in EventStore

All `flux.EventStore` implementations MUST return `flux.ErrConcurrency` if the `expectedRevision` does not match the current stream state.

### `event/store/in_memory.go`
In `Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope)`:
- **Before:** `return fmt.Errorf("optimistic concurrency failure: expected revision %d, got %d", expectedRevision, currentRev)` *(or similar)*
- **After:** `return fmt.Errorf("%w: expected revision %d, got %d", flux.ErrConcurrency, expectedRevision, currentRev)`

## 4. Update Test Assertions

Update the unit tests across the framework to assert against the new sentinels using `errors.Is(err, ...)` rather than relying on raw string comparisons.

### Files to review for test updates:
- `repository_test.go` (Check the `TestAggregateRepository_ConcurrencyError` case)
- `snapshot_repository_test.go` (Check the `TestSnapshotRepository_NotFound` case)
- `event/store/in_memory_test.go` (Check any concurrency enforcement tests)

**Example Test Fix:**
```go
// Before:
if err == nil {
    t.Fatalf("expected error loading non-existent aggregate, got nil")
}

// After:
if !errors.Is(err, flux.ErrAggregateNotFound) {
    t.Fatalf("expected ErrAggregateNotFound, got %v", err)
}
```

## Agent Instructions
1. Implement `errors.go`.
2. Apply the `%w` wrapping refactors across `aggregate.go`, `repository.go`, and `snapshot_repository.go`.
3. Update `event/store/in_memory.go` to explicitly wrap `flux.ErrConcurrency`.
4. Update the test suites to validate using `errors.Is`.
5. Run `go test -v ./...` to ensure all tests pass.
6. Commit the changes adhering to `.rules`.
