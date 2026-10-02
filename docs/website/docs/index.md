# Flux

<div class="flux-intro-banner">
  <img src="/logo.png" alt="Flux Logo" class="flux-logo" />
  <p class="flux-tagline">
    Event-Sourcing Framework for Go; Build distributed, event-sourced applications with type-safe aggregates, commands, and projections.
  </p>
</div>

## Introduction

Welcome to the `flux` framework documentation!

`flux` is a lightweight, high-performance, and strictly-typed Event Sourcing & CQRS framework for Go 1.27+. It is designed to provide you with the architectural benefits of distributed event sourcing without the immense boilerplate usually associated with it.

## Why flux?

Most Go event-sourcing libraries rely heavily on `reflect` to decode events, map handlers, and hydrate aggregates. This introduces significant runtime performance penalties and completely bypasses the compiler's type safety.

`flux` was built from the ground up to leverage **Go 1.27+ Generics**. By utilizing strict generic constraints on Aggregates and explicitly typed decoding mechanisms, `flux` ensures that if your event-sourced application compiles, it is fundamentally sound.

## Framework Highlights & Core Principles

- **Event-Sourced Aggregates:** Embed a base aggregate type, register typed event handlers, and let the framework handle versioning, persistence, and replay automatically—keeping business logic decoupled from storage and network boundaries.
- **Reflection-Free Generics:** Statically typed command handlers, queries, and aggregates provide blazing-fast execution without runtime reflection overhead.
- **Type-Safe Commands & Buses:** Dispatch and handle commands and queries with full generic type safety, cleanly separating your read and write models.
- **Domain Purity (Serialization Agnostic):** The core domain is entirely free of infrastructure pollution. You will never write a single `json:` or `xml:` struct tag on your core Domain Events. Serialization is handled at the edges via private DTOs, generic Type Registries, and "Batteries-Included" Codecs.
- **Projection Toolkit:** Build scalable read models with continuous stream tailing. Decouple your write-path from your read-path entirely.
- **Durable Workflows:** Coordinate long-running business processes across services with first-class Temporal integration—featuring durable commands, timeouts, and explicit compensation.
- **Batteries-Included Storage:** Ships with a fast In-Memory engine for unit testing and out-of-the-box drivers for MySQL and Redis via a clean `EventStore` interface.

## Next Steps

Head over to the [Installation](/getting-started/installation) guide to add `flux` to your Go module, and then dive into the [Quick Start](/getting-started/quick-start) to build your first event-sourced application! You can also explore the step-by-step [Tutorial](/tutorial/01-project-setup) or check out the [Temporal Payment](/examples/temporal-payment) example.
