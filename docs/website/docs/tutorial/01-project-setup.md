# 1. Project Setup

Welcome to the comprehensive E-Commerce Tutorial! Over the next few chapters, we will build a production-ready, event-sourced E-Commerce backend.

Unlike a simple Todo app, a real system requires strict boundaries. We will build this application using **Domain-Driven Design (DDD)** principles and strict CQRS data isolation.

## Initializing the Workspace

Let's create our workspace and initialize the Go module.

```bash
mkdir e-commerce && cd e-commerce
go mod init e-commerce
go get github.com/wotek/flux
```

## The Domain Layout

We will use a highly decoupled directory structure based on the official `flux` Project Layout recommendations. Our application is split into Bounded Contexts.

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

To prevent accidental data leakage, we will strictly enforce CQRS by ensuring our Write models and Read models use completely separate Go structs, even if they share the same name (like `Product`). 

We achieve this by nesting a `types` package inside the aggregate and the projection:

* `internal/catalog/aggregates/product/types/product.go` (The pure state for business logic)
* `internal/catalog/projections/catalog_list/types/product.go` (The view state for the UI)

In the next chapter, we will build the Product aggregate using this strict structural pattern.
