# E-Commerce Reference Application

The `example/e-commerce` application is an advanced reference architecture demonstrating how to build complex, multi-domain enterprise systems.

Unlike the minimal Bank Account or Todo examples, this application showcases **advanced Domain-Driven Design (DDD)** patterns. It proves how `flux` enforces strict boundaries while allowing asynchronous cross-domain communication.

---

## The Walkthrough: Project In-and-Outs

The application is cleanly divided into Bounded Contexts (domains) following the `flux` standard project layout. Let's trace how an order flows through this ecosystem.

### 1. The Catalog Domain (`internal/catalog/`)

The `catalog` domain is entirely responsible for products and pricing. It knows absolutely nothing about customers or sales.

*   **Aggregates:** 
    *   The `Product` aggregate handles inventory and naming.
    *   The `Pricing` aggregate handles price history. We separated these because pricing rules often change independently of core product data, minimizing lock contention.
*   **The Projection (Read Model):**
    *   Look at `internal/catalog/projections/productview/projector.go`. 
    *   This is a **Cross-Aggregate Denormalizer**. It listens to *both* `ProductCreated` and `PricingSet` events. When the frontend asks for product information, it queries this unified projection instantly without needing to perform slow SQL joins across different aggregate streams!

### 2. The Identity Domain (`internal/identity/`)

A simulated IAM (Identity and Access Management) domain. It provides the `flux.Actor` context. When a command is dispatched, the Identity domain ensures the traceability of *who* performed the action.

### 3. The Sales Domain (`internal/sales/`)

The `sales` domain handles the shopping cart and checkout process.

*   **Aggregates:**
    *   `Customer`: Manages the address book and profile.
    *   `Order`: Manages the lifecycle of a purchase.
*   **The Flow:**
    *   When a user clicks "Checkout", a `salescmd.PlaceOrder` command is dispatched to the `Order` aggregate.
    *   The `Order` aggregate validates the input and records a `salesevents.OrderPlaced` event.
    *   **Crucial Concept:** The `Order` aggregate *does not* process the credit card! It simply states that the order was placed and waits.

### 4. The Global Workflow (`internal/workflows/payment/`)

How does the order actually get paid? This requires cross-domain coordination, which is the job of a **Saga / Process Manager**.

*   Look at `internal/workflows/payment/workflow.go`.
*   This workflow listens to the global `flux.EventBus` for the `salesevents.OrderPlaced` event.
*   When it intercepts an order, it tracks its own state in a `WorkflowStore`.
*   It attempts to process the payment (simulated).
*   **Dispatching Back:** If the payment succeeds, the workflow uses the `flux.CommandBus` to dispatch a `salescmd.PayOrder` command back to the Sales domain. If it fails or times out, it dispatches `salescmd.CancelOrder`.

This perfectly illustrates **Choreography vs Orchestration**. The Aggregates are purely reactive and ignorant of the outside world, while the Workflow orchestrates the complex multi-step process.

---

## Running the Example

This reference application does not have a frontend UI. It is designed as an executable integration test suite to demonstrate the framework's behavior programmatically.

```bash
cd example/e-commerce
go test -v -race ./...
```

### The Integration Test

Open `integration_test.go` at the root of the e-commerce folder. This test wires the entire distributed system together in-memory.

It simulates a complete end-to-end user journey:
1. Creating a product and setting its price.
2. Registering a customer and adding a shipping address.
3. Placing an order with line items.
4. Simulating the payment workflow successfully completing the order.
5. Verifying the final denormalized state in the Catalog Projection.

It proves that the entire CQRS architecture functions perfectly in a heavily tested, decoupled environment.
