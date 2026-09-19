# Project Layout & Architecture Guide

This document outlines the recommended directory structure and architectural guidelines for scaling large, multi-domain applications using Event Sourcing and CQRS with the `flux` framework in Go.

## Architectural Philosophy

This layout follows a **Domain-First (Bounded Context)** approach. Code is primarily grouped by business capability rather than technical concern. This prevents "mega-packages," enforces clear boundaries, and allows domains to evolve independently. Technical layers (`events`, `commands`, `aggregates`, etc.) are nested _inside_ these domains.

Global orchestrations (like workflows) and truly shared concepts are hoisted to the top-level `internal/` directory.

---

## High-Level Directory Structure

```text
internal/
├── catalog/                     # Bounded Context: Catalog
│   ├── aggregates/              # Domain entities ensuring business invariants
│   │   ├── product/
│   │   │   ├── aggregate.go     # Behavior wrapper & command execution
│   │   │   └── types/           # Pure Write-model state (used for Snapshots)
│   │   │       └── product.go   # 'type Product struct'
│   │   └── pricing/
│   ├── commands/                # Write-side intents (e.g., CreateProductCmd)
│   ├── events/                  # Domain events (e.g., ProductCreated)
│   ├── projections/             # Domain-local read models
│   │   └── catalog_list/
│   │       ├── projector.go     # Event listener & DB updater
│   │       └── types/           # Pure Read-model state
│   │           └── product.go   # 'type Product struct' (No private fields)
│   ├── queries/                 # Read-side intents (e.g., GetProductByIDQuery)
│   └── types/                   # Domain-level shared value objects (e.g., SKU, Weight)
│
├── sales/                       # Bounded Context: Sales
│   ├── aggregates/
│   │   ├── order/
│   │   └── customer/
│   ├── commands/
│   ├── events/
│   ├── projections/
│   ├── queries/
│   └── types/
│
├── reporting/                   # Bounded Context: A "Consumer" Domain
│   ├── projections/             # Cross-domain projections serving a specific business need
│   └── queries/
│
├── workflows/                   # Global cross-domain Sagas / Process Managers
│   ├── payment/                 # e.g., Orchestrates Sales (Order) and Catalog (Stock)
│   └── fulfillment/
│
├── projections/                 # Ambiguous/Global cross-domain read models
│   └── company_dashboard/
│
└── types/                       # Globally shared value objects
    ├── money.go
    └── address.go
```

---

## 1. Domain Modules (`internal/<domain>/`)

Each domain represents a cohesive business boundary.

- **`aggregates/`**: Contains the write-model aggregates. These embed `flux.AggregateRoot`. They consume commands, enforce rules, and emit events.
  - **Subpackages & Snapshots:** Each aggregate gets its own folder (e.g., `aggregates/product/`). If the aggregate is complex, its pure state data is decoupled into an internal `types/` subpackage. This pure data struct is returned by the aggregate's `Snapshot()` method and persisted by the `SnapshotRepository`.
- **`commands/`**: Contains the command definitions and their respective handlers. Command handlers load the aggregate from the `AggregateRepository`, invoke business methods, and save the aggregate.
- **`events/`**: Defines the event payloads (structs) and marker interfaces. This package represents the public contract of the domain. Other domains will import this package to listen to what happened.
- **`projections/`**: Contains projectors (using `flux/projection`) that listen to local domain events and build optimized read models.
  - **Subpackages:** Each projection gets its own folder (e.g., `projections/catalog_list/`). The exact shape of the read model is defined in an internal `types/` subpackage.
- **`queries/`**: Defines query definitions and handlers that read directly from the database populated by the projections.
- **`types/`**: Contains **Value Objects** specific to this domain (e.g., `SKU`, `PricingTier`). These are shared across the domain's aggregates and projections, unlike the state structs which are strictly localized.

## 2. Cross-Domain Modules

### Workflows (Sagas) (`internal/workflows/`)

Workflows typically orchestrate complex processes spanning multiple domains (e.g., charging a customer, reserving stock, and updating an order).

- **Why global?** Keeping workflows at the top level prevents circular dependencies. A workflow in the `sales` domain shouldn't tightly couple itself to the internal command structures of the `catalog` domain.
- **Responsibility:** Workflows listen to domain events via `flux/saga`, maintain long-running state, and dispatch commands to various domains using `saga.EnqueueCommand`.

### Cross-Domain Projections (`internal/projections/`)

Read models that aggregate data from multiple domains (e.g., joining `ProductCreated` from `catalog` with `OrderPlaced` from `sales`).

- **Consumer Domains:** If the projection serves a specific business capability (like analytics or reporting), it should live in its own consumer domain (e.g., `internal/reporting/projections/`).
- **Ambiguous Projections:** For read models that don't neatly fit a specific consumer domain but are universally used, place them in the global `internal/projections/` directory.

### Global Types (`internal/types/`)

Value objects that are ubiquitous across the entire organization.

- Examples: `Money`, `Currency`, `Address`, `CountryCode`.
- **Rule of Thumb:** Start types in the domain `types/` folder. Only promote them to the global `types/` folder when a second domain demonstrably requires them.

---

## 3. Dependency Flow Rules

To keep the architecture clean and prevent import cycles in Go:

1.  **Events are the Source of Truth:** The `events/` package of a domain should have zero dependencies on other parts of the application. It is the core contract.
2.  **Commands depend on Aggregates:** `commands/` handlers load `aggregates/`. Aggregates never depend on `commands/`.
3.  **Projections depend on Events:** `projections/` listen to `events/` from their own domain (or imported from other domains) to build read models.
4.  **Workflows depend on Events and Commands:** `workflows/` listen to `events/` across the application and import `commands/` from various domains to orchestrate actions.
5.  **Strict CQRS Separation:** The Write side (`aggregates/`, `commands/`) **must never** depend on the Read side (`projections/`, `queries/`), and vice versa.

## 4. Strict CQRS: State and View Isolation

To prevent accidental data leakage and ensure perfect decoupling between the Write model and the Read model, developers should avoid sharing state structs (e.g., using the exact same struct for both the aggregate's snapshot state and the projection's read model).

You can achieve optimal Domain-Driven Design rigidity by utilizing nested `types/` subpackages localized to the specific Aggregate or Projection. This allows you to keep clean, domain-centric struct names (like `Product`) without collisions.

### Aggregate State (Write Model)

The pure data struct representing the Aggregate's internal state (used for business logic and snapshots) is named `Product` and lives in a `types` subpackage strictly localized to the aggregate.

```text
internal/catalog/aggregates/product/
├── aggregate.go         # Contains ProductAggregate
└── types/
    └── product.go       # Contains 'type Product struct'
```

```go
// Usage inside aggregate.go
import "github.com/myorg/ecommerce/internal/catalog/aggregates/product/types"

func (p *ProductAggregate) Snapshot() types.Product { ... }
```

### Projection View (Read Model)

The Read model gets its own entirely distinct struct, also named `Product`, nested under the specific projection's `types` subpackage.

```text
internal/catalog/projections/catalog_list/
├── projector.go
└── types/
    └── product.go       # Contains 'type Product struct' (The Read Model)
```

**Why this is perfect:**

1. **Zero Data Leakage:** The Projection's `types.Product` can safely omit sensitive Write-side fields (like `FraudRiskScore` or internal flags) and include additional UI-specific joined data.
2. **Clean Naming:** By leveraging Go's package system, you get to name both structs `Product` without any stuttering or suffixing (e.g., avoiding `ProductState` or `ProductView`). Inside the aggregate, it's `types.Product`. Inside the projector, it's `types.Product`.
3. **Strict Isolation:** Because the Read and Write models are completely isolated and define their own types, they never need to import each other's structs, adhering perfectly to CQRS boundaries.
