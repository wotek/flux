# E-Commerce Reference Application

A comprehensive, multi-domain reference application demonstrating how to build complex, production-ready distributed systems using the `flux` framework.

Unlike the minimal Bank Account or Todo examples, this application showcases **advanced Domain-Driven Design (DDD)** patterns, including bounded contexts, cross-aggregate coordination, read-model projections, and long-running process managers (Workflows).

## Bounded Contexts

The application is cleanly divided into isolated business domains:

*   **Catalog (`internal/catalog/`):**
    *   **Aggregates:** `Product` (tracks basic product info and stock), `Pricing` (manages product price history and active price).
    *   **Projections:** `ProductView` merges events from the `Product` and `Pricing` aggregates into a unified read model (database table) optimized for frontend display.
*   **Sales (`internal/sales/`):**
    *   **Aggregates:** `Customer` (manages customer profiles and address books), `Order` (manages the lifecycle of a shopping cart, checkout, and fulfillment).
*   **Identity (`internal/identity/`):** 
    *   A simulated IAM domain providing authentication context (Actors).

## Advanced Patterns Demonstrated

### 1. Cross-Domain Projections (CQRS)
The `catalog` domain features a `ProductView` projector that listens to the global event stream. It catches both `catalogevents.ProductCreated` and `catalogevents.PricingSet` events, denormalizing them into a single read-optimized struct (`ProductView`). This demonstrates how to break apart complex Write models while keeping Read models highly cohesive.

### 2. Sagas / Workflows (Process Managers)
The `payment` workflow (`internal/workflows/payment/`) demonstrates a long-running process manager that orchestrates the payment lifecycle of an `Order`. 
*   It listens to `salesevents.OrderPlaced`.
*   It tracks its own state in the `WorkflowStore`.
*   It uses the `command` outbox to dispatch `salescmd.PayOrder` or `salescmd.CancelOrder` back to the Sales domain depending on business rules (e.g., compensating transactions if the payment times out).

### 3. Integration Testing
The root `integration_test.go` wires the entire ecosystem together in memory. It simulates a complete end-to-end user journey:
1. Creating a product and setting its price.
2. Registering a customer and adding a shipping address.
3. Placing an order with line items.
4. Simulating the payment workflow successfully completing the order.
5. Verifying the final denormalized state in the Catalog Projection.

## Running

This reference application is designed as an executable test suite to demonstrate the framework's behavior programmatically without requiring external databases.

```bash
cd example/e-commerce
go test -v -race ./...
```
