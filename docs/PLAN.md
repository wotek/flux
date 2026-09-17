# Flux E-Commerce Implementation Plan

This document outlines the detailed step-by-step implementation plan for the E-Commerce domain using the `flux` framework.

## 1. Global Identity Management
**Target:** `internal/identity/identity.go`
- In `flux`, Aggregates are identified by full URNs (`flux.Identifier`), not just UUIDs.
- Create factory functions to ensure strict URN formulation (omitting versioning, which does not map to aggregate revision here):
  - `NewProductIdentifier(id string)` $\rightarrow$ `urn:flux:ecommerce:shop:default:product:<id>:`
  - `NewPricingIdentifier(id string)` $\rightarrow$ `urn:flux:ecommerce:shop:default:pricing:<id>:`
  - `NewCustomerIdentifier(id string)` $\rightarrow$ `urn:flux:ecommerce:shop:default:customer:<id>:`
  - `NewOrderIdentifier(id string)` $\rightarrow$ `urn:flux:ecommerce:shop:default:order:<id>:`
- *Key Benefit:* Product and Pricing aggregates will safely share the same `<id>` UUID because their Resource Types (`product` vs `pricing`) make their URN stream IDs unique.

## 2. Event Definitions
**Target:** `internal/domain/*/events.go`
- **Marker Interfaces:** To satisfy Go's generic constraints safely (without unions), every aggregate domain will define a marker interface.
  ```go
  type ProductEvent interface {
      flux.Event
      isProductEvent()
  }
  ```
- **Event Structs:** Define the payload structs.
  ```go
  type ProductCreated struct { Name string; Stock int }
  func (ProductCreated) Name() string { return "shop.product.created" }
  func (ProductCreated) isProductEvent() {}
  ```
- Define corresponding events for `PricingEvent`, `CustomerEvent`, and `OrderEvent`.

## 3. Aggregate Implementations
**Target:** `internal/domain/*/aggregate.go`
For each of the 4 aggregates (Product, Pricing, Customer, Order), implement the following boilerplate:
- **Struct Definition:** Embed `flux.AggregateRoot[E]` and internal state.
  ```go
  type ProductAggregate struct {
      flux.AggregateRoot[ProductEvent]
      name  string
      stock int
  }
  ```
- **Instantiation:** Implement the `New(stream flux.Stream)` interface method.
  ```go
  func (p *ProductAggregate) New(stream flux.Stream) *ProductAggregate {
      newP := &ProductAggregate{}
      newP.AggregateRoot = flux.NewAggregateRoot[ProductEvent](stream, flux.NewChangeset[ProductEvent](), newP.apply)
      return newP
  }
  ```
- **State Mutator:** Implement the private `apply(evt E) error` method with a type switch over the domain events to update internal state.
- **Business Methods (Commands):** Implement public methods (e.g., `Create(name, stock)`, `AdjustStock(qty)`) that validate business rules and then call `p.Changeset().Record(event)`.

## 4. Projections (Read Models)
**Target:** `internal/projection/catalog/projector.go`
- **View Model:** Define `ProductView { ID, Name, Price, Stock }`.
- **Projector Setup:** Instantiate using `projection.NewProjector(projID, eventStore, store)`.
- **Event Wiring:** Use the framework's type-erased closure wrappers to register strongly-typed handlers without reflection:
  - `projection.RegisterHandler[ProductCreated](projector, func(ctx, evt) { ... })`
  - `projection.RegisterHandler[ProductRenamed](projector, ...)`
  - `projection.RegisterHandler[StockAdjusted](projector, ...)`
  - `projection.RegisterHandler[PricingSet](projector, ...)`
- *Note:* The projector naturally listens to both the `product` and `pricing` event streams and merges them into a single `ProductView` based on their shared Resource ID UUID.

## 5. Sagas (Workflows)
**Target:** `internal/saga/payment/saga.go`
- **State Definition:** Define `PaymentSaga` implementing `saga.Saga[PaymentSaga]`.
  - State fields: `Items []LineItem`, `IsPaid bool`, `IsCancelled bool`.
- **Orchestrator Setup:** Instantiate using `saga.NewOrchestrator(eventStore)`.
- **Event Wiring:** Register handlers using `saga.RegisterHandler`.
  - **`OrderPlaced`**: Captures `LineItems` to state. Emits a deferred/timeout command to a scheduler.
  - **`OrderPaid`**: Marks saga as successfully completed.
  - **`OrderCancelled`**: Triggers immediate stock compensation.
  - **`PaymentTimeout`** *(Timeout Event)*: If received and not paid, triggers `CancelOrder` and stock compensation.
- **Compensation Execution:** Use `saga.EnqueueCommand(ctx, cmd)` to dispatch `AdjustStockCmd` (adding quantity back) and `CancelOrderCmd` back into the system.

## 6. Implementation Guidelines & Go 1.27 Best Practices

The implementation must strictly adhere to the guidelines laid out in `.rules`, targeting modern Go 1.27.

### Standard Library & Generics
- Utilize the `slices`, `maps`, and `cmp` packages for all collection manipulations (e.g., `slices.Contains`, `slices.DeleteFunc`). Do not write custom iteration helpers.
- Rely on built-in generics like `clear()`, `min()`, and `max()`.
- Use Go 1.23+ `iter.Seq` and `iter.Seq2` for iteration where applicable (consistent with `StreamIterator`).

### Idiomatic Go API
- **Narrow Interfaces:** Functions and constructors should accept narrow interfaces and return concrete struct types.
- **Context:** Every function that blocks, performs I/O, or handles requests MUST take `ctx context.Context` as its first parameter. Never store `context.Context` in a struct.
- **Preallocation:** Always preallocate slices and maps when capacity is known in advance (`make([]T, 0, cap)`).

### Error Handling
- **Wrapping:** Always wrap errors to preserve the causal chain using `fmt.Errorf("action description: %w", err)`. Keep the message lowercase with no trailing punctuation.
- **Inspection:** Use `errors.Is()` for sentinels and `errors.AsType[T](err)` (or `errors.As()`) for generic error extraction instead of direct type assertions. Use `errors.Join()` for aggregating non-fatal errors.
- **Panics:** Never panic in the implementation. Always return descriptive errors.

### Observability & Concurrency
- **Logging:** Use `log/slog` for structured logging. Always pass context (e.g., `slog.InfoContext(ctx, ...)`). Never use standard `log.Println`.
- **Concurrency:** Use `golang.org/x/sync/errgroup` for coordinating any concurrent background tasks with context propagation.

### Testing
- Rely on standard table-driven test patterns with `t.Parallel()`, `t.Helper()`, and `t.Cleanup()`.
- Use `github.com/google/go-cmp/cmp` for complex structure comparisons.

### Documentation
- Every exported identifier must have a Go doc comment starting with the identifier's name as a complete English sentence. Do not use docblocks (`@param`, `@return`).
