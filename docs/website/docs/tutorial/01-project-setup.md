# 1. Project Setup & Architecture

Welcome to the comprehensive E-Commerce Tutorial! Over the next few chapters, we will build a production-ready, event-sourced E-Commerce backend.

Unlike a simple Todo app, a real-world system requires strict boundaries to prevent it from becoming a "Big Ball of Mud." We will build this application using **Domain-Driven Design (DDD)** principles and strict **CQRS** (Command Query Responsibility Segregation) isolation.

## Initializing the Workspace

Let's create our workspace and initialize the Go module.

```bash
mkdir e-commerce && cd e-commerce
go mod init e-commerce
go get github.com/wotek/flux
```

## The Domain Layout

We will use a highly decoupled directory structure based on the official `flux` Project Layout recommendations. Our application is split into distinct **Bounded Contexts**.

A Bounded Context is a linguistic and architectural boundary. For example, the `Catalog` domain manages what products exist and how much they cost. The `Sales` domain manages customer orders. They communicate strictly through Domain Events, never by reaching into each other's databases.

Create the following skeleton:

```text
e-commerce/
├── cmd/
│   └── server/          # Application entrypoint
├── internal/
│   ├── catalog/         # Catalog bounded context
│   │   ├── aggregates/  # Write models (Product)
│   │   ├── commands/    # Intents & command handlers
│   │   ├── events/      # Domain events (ProductCreated)
│   │   ├── projections/ # Read models (Product View)
│   │   └── queries/     # Read handlers
│   ├── sales/           # Sales bounded context
│   │   ├── aggregates/  # Write models (Order)
│   │   ├── commands/    
│   │   └── events/      
│   └── workflows/       # Cross-domain process managers
```

### CQRS State Isolation

One of the most dangerous anti-patterns in CQRS is data leakage—using the exact same Go struct for both the database write model and the UI read model. 

We will strictly enforce CQRS by ensuring our Write models and Read models use completely separate Go structs, even if they share the same name (like `Product`). 

We achieve this structural rigidity by nesting a `types` package deeply inside the aggregate and the projection:

* `internal/catalog/aggregates/product/types/product.go` (The pure state for business logic)
* `internal/catalog/projections/catalog_list/types/product.go` (The view state for the UI)

::: info Why so strict?
By isolating the Read and Write models into distinct structs in localized subpackages, we guarantee zero data leakage. Your Projection's `types.Product` can safely omit sensitive Write-side fields (like `FraudRiskScore`) and include additional UI-specific joined data without polluting the Write Model.
:::

In the next chapter, we will build the Product aggregate using this strict structural pattern.
